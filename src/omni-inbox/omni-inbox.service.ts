import {
  BadRequestException,
  Injectable,
  Logger,
  NotFoundException,
  OnModuleDestroy,
  OnModuleInit,
} from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";
import { OmniInboxRealtimeService } from "./omni-inbox.realtime";
import { ListConversationsDto } from "./dto/list-conversations.dto";
import { OrderService } from "../order/order.service";
import { Readable } from "stream";
import cloudinary from "../utils/cloudinary";

function safeText(value: any) {
  return String(value || "").trim();
}

function last6(value: string) {
  return safeText(value).slice(-6) || "unknown";
}

function isFallbackCustomerName(value?: string | null) {
  const text = safeText(value);
  return !text || /^Khách\s+\d{4,}$/i.test(text);
}

function isUsableProfileName(value?: string | null) {
  const text = safeText(value);
  return Boolean(text) && !isFallbackCustomerName(text);
}

type MetaProfile = {
  id?: string;
  name?: string;
  first_name?: string;
  last_name?: string;
  profile_pic?: string;
  picture?: {
    data?: {
      url?: string;
      is_silhouette?: boolean;
    };
  };
};

type MetaFeedChange = {
  field?: string;
  value?: any;
};

@Injectable()
export class OmniInboxService implements OnModuleInit, OnModuleDestroy {
  private lastStaleAssignmentSweepAt = 0;
  private lastMorningQueueSweepAt = 0;
  private lastUnassignedSweepAt = 0;
  private unassignedSweepRunning = false;
  private unassignedSweepTimer: ReturnType<typeof setInterval> | null = null;
  private readonly logger = new Logger(OmniInboxService.name);
  private readonly facebookPostSourceCache = new Map<string, { expiresAt: number; data: any }>();

  // Lazy backfill Messenger: chỉ sync lịch sử của khách thực sự quay lại nhắn,
  // không quét toàn bộ Page. Map này chống gọi Meta lặp lại liên tục trong cùng
  // một process; DB vẫn chống duplicate bằng providerMessageId nên restart/deploy
  // có chạy lại cũng an toàn.
  private readonly messengerHistoryBackfillDone = new Set<string>();
  private readonly messengerHistoryBackfillRunning = new Map<string, Promise<any>>();

  constructor(
    private readonly prisma: PrismaService,
    private readonly realtime: OmniInboxRealtimeService,
    private readonly orderService: OrderService,
  ) {}

  onModuleInit() {
    // Chạy một lượt ngay sau khi backend khởi động để xử lý hội thoại tồn đọng.
    const initialTimer = setTimeout(() => {
      void this.sweepUnassignedConversations("SERVER_START").catch((error: any) =>
        this.logger.warn(
          `[OMNI_UNASSIGNED_SWEEP_START_SKIP] ${error?.message || error}`,
        ),
      );
    }, 5_000);
    initialTimer.unref?.();

    // Sau đó tự quét mỗi phút. Không cần admin bấm gán tay và cũng không phụ
    // thuộc việc có webhook/tin nhắn mới đi vào.
    this.unassignedSweepTimer = setInterval(() => {
      void this.sweepUnassignedConversations("SCHEDULED_SWEEP").catch(
        (error: any) =>
          this.logger.warn(
            `[OMNI_UNASSIGNED_SWEEP_SKIP] ${error?.message || error}`,
          ),
      );
    }, 60_000);
    this.unassignedSweepTimer.unref?.();
  }

  onModuleDestroy() {
    if (this.unassignedSweepTimer) {
      clearInterval(this.unassignedSweepTimer);
      this.unassignedSweepTimer = null;
    }
  }

  private get pageAccessToken() {
    return safeText(
      process.env.META_INBOX_PAGE_ACCESS_TOKEN ||
        process.env.META_INBOX ||
        process.env.META_ACCESS_TOKEN,
    );
  }

  private get configuredPageId() {
    return safeText(
      process.env.META_INBOX_PAGE_ID ||
        process.env.META_PAGE_ID ||
        process.env.FACEBOOK_PAGE_ID,
    );
  }

  private get graphVersion() {
    return safeText(process.env.META_GRAPH_VERSION) || "v25.0";
  }

  private get verboseMetaLogs() {
    return process.env.META_INBOX_VERBOSE_LOGS === "true";
  }

  private logMetaDebug(message: string) {
    if (this.verboseMetaLogs) this.logger.debug(message);
  }

  private get webhookPath() {
    return "/webhooks/meta/inbox";
  }

  private get defaultSubscribedFields() {
    return [
      "messages",
      "message_echoes",
      "message_reads",
      "message_deliveries",
      "message_reactions",
      "messaging_postbacks",
      "messaging_referrals",
      "feed",
    ];
  }

  private async metaFetch<T = any>(
    path: string,
    params: Record<string, string> = {},
  ): Promise<T> {
    const token = this.pageAccessToken;
    if (!token)
      throw new BadRequestException("Thiếu META_INBOX_PAGE_ACCESS_TOKEN.");

    const url = new URL(
      `https://graph.facebook.com/${this.graphVersion}/${path.replace(/^\/+/, "")}`,
    );
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined && value !== null)
        url.searchParams.set(key, value);
    }
    url.searchParams.set("access_token", token);

    const res = await fetch(url.toString(), { method: "GET" });
    const json = await res.json().catch(() => ({}));

    if (!res.ok) {
      const message =
        json?.error?.message || `Meta Graph API lỗi ${res.status}`;
      this.logMetaDebug(`[META_GRAPH_GET_FAILED] ${path} | ${message}`);
      throw new BadRequestException(message);
    }

    return json as T;
  }

  private isGenericMetaPageSenderName(value?: string | null) {
    const name = safeText(value).toLocaleLowerCase("vi-VN");
    if (!name) return true;
    const configuredNames = new Set([
      "the 1970",
      "facebook page",
      safeText(this.configuredPageId).toLocaleLowerCase("vi-VN"),
    ]);
    return configuredNames.has(name) || name.startsWith("page ");
  }

  /**
   * Meta Conversations API chỉ trả `from` là Page cho tin OUT nên không có tên
   * nhân viên Business Suite. Nếu tin đó từng được gửi qua Omni Inbox, DB local
   * đã có senderName thật. Ghép theo nội dung + thời điểm để giữ lại đúng nhân viên.
   */
  private async findKnownLocalOutboundSender(params: {
    conversationId: string;
    sentAt: Date;
    text?: string | null;
    type?: "TEXT" | "IMAGE";
    excludeId?: string | null;
  }) {
    const windowMs = params.type === "IMAGE" ? 15_000 : 120_000;
    const from = new Date(params.sentAt.getTime() - windowMs);
    const to = new Date(params.sentAt.getTime() + windowMs);
    const where: any = {
      conversationId: params.conversationId,
      direction: "OUT",
      sentAt: { gte: from, lte: to },
    };
    if (params.excludeId) where.id = { not: params.excludeId };
    if (params.type) where.type = params.type;
    if (safeText(params.text)) where.text = safeText(params.text);

    const candidates = await this.prisma.omniMessage.findMany({
      where,
      orderBy: { sentAt: "desc" },
      take: 10,
      select: {
        id: true,
        senderId: true,
        senderName: true,
        providerMessageId: true,
        sentAt: true,
      },
    });

    return (candidates || []).find(
      (item: any) =>
        safeText(item?.senderName) &&
        !this.isGenericMetaPageSenderName(item?.senderName),
    ) || null;
  }

  /**
   * Lấy lịch sử gần nhất của đúng 1 khách Messenger khi khách đó quay lại nhắn.
   * Không bao giờ quét /{PAGE_ID}/conversations toàn bộ Page.
   *
   * Meta Conversations API hỗ trợ lọc theo user_id (PSID). Sau khi resolve được
   * conversation id thật của Meta, chỉ lấy tối đa 200 message gần nhất của thread.
   * providerMessageId là unique key nên webhook hiện tại + backfill không nhân đôi.
   */
  private async backfillMessengerHistoryForCustomer(params: {
    pageId: string;
    customerPsid: string;
    conversationId: string;
    customerName?: string | null;
    force?: boolean;
  }) {
    const pageId = safeText(params.pageId);
    const customerPsid = safeText(params.customerPsid);
    const conversationId = safeText(params.conversationId);
    if (!pageId || !customerPsid || !conversationId) {
      return { skipped: true, reason: "missing_backfill_identity" };
    }

    const key = `${pageId}:${customerPsid}`;
    if (!params.force && this.messengerHistoryBackfillDone.has(key)) {
      return { skipped: true, reason: "already_backfilled_in_process" };
    }

    const running = this.messengerHistoryBackfillRunning.get(key);
    if (running) return running;

    const job = (async () => {
      try {
        this.logger.log(
          `[META_HISTORY_BACKFILL_START] page=${pageId} psid=${last6(customerPsid)} conversation=${conversationId}`,
        );

        // Chỉ nhận thread khi Meta xác nhận participant đúng PSID khách.
        // Không bao giờ lấy data[0] một cách mù quáng: nếu user_id bị Meta bỏ qua
        // hoặc trả sai, làm vậy sẽ nhét lịch sử của khách A vào conversation khách B.
        const found: any = await this.metaFetch(`${pageId}/conversations`, {
          platform: "messenger",
          user_id: customerPsid,
          fields: "id,updated_time,participants.limit(20){id,name}",
          limit: "10",
        });
        const candidates = Array.isArray(found?.data) ? found.data : [];
        const matchedThread = candidates.find((item: any) => {
          const participants = Array.isArray(item?.participants?.data)
            ? item.participants.data
            : [];
          return participants.some(
            (participant: any) => safeText(participant?.id) === customerPsid,
          );
        });
        const metaConversationId = safeText(matchedThread?.id);
        if (!metaConversationId) {
          this.logger.warn(
            `[META_HISTORY_BACKFILL_THREAD_MISMATCH] page=${pageId} psid=${last6(customerPsid)} candidates=${candidates.length}`,
          );
          // Không đánh dấu done để lần sau có thể thử lại; quan trọng nhất là KHÔNG
          // import nhầm bất kỳ thread nào khi chưa verify participant.
          return { skipped: true, reason: "meta_conversation_not_verified" };
        }

        let detail: any;
        try {
          detail = await this.metaFetch(metaConversationId, {
            fields:
              "messages.limit(200){id,created_time,from,to,message,attachments}",
          });
        } catch (error: any) {
          // Một số Page/API version không expose attachments trong nested expansion.
          // Vẫn backfill text/timestamp thay vì làm hỏng toàn bộ sync.
          this.logger.warn(
            `[META_HISTORY_BACKFILL_ATTACHMENT_FALLBACK] thread=${metaConversationId} | ${error?.message || error}`,
          );
          detail = await this.metaFetch(metaConversationId, {
            fields: "messages.limit(200){id,created_time,from,to,message}",
          });
        }

        const rows = Array.isArray(detail?.messages?.data)
          ? detail.messages.data
          : [];
        if (!rows.length) {
          this.messengerHistoryBackfillDone.add(key);
          return { ok: true, imported: 0, threadId: metaConversationId };
        }

        // Meta thường trả newest -> oldest. Lưu oldest -> newest cho timestamp ổn định.
        const ordered = [...rows].reverse();
        let imported = 0;
        let skipped = 0;

        // Tự dọn lỗi của bản lazy-history cũ: trong Messenger 1:1, mọi tin IN của
        // conversation này phải có senderId đúng bằng PSID khách. Chỉ xóa trường hợp
        // chắc chắn sai; không đụng OUT vì senderId OUT có thể là Page hoặc staff local.
        const wrongInbound = await this.prisma.omniMessage.findMany({
          where: {
            conversationId,
            direction: "IN" as any,
            senderId: { not: customerPsid },
          },
          select: { id: true, providerMessageId: true, senderId: true },
          take: 500,
        });
        if (wrongInbound.length) {
          await this.prisma.omniMessage.deleteMany({
            where: { id: { in: wrongInbound.map((item: any) => item.id) } },
          });
          this.logger.warn(
            `[META_HISTORY_CORRUPT_INBOUND_CLEANED] conversation=${conversationId} psid=${last6(customerPsid)} deleted=${wrongInbound.length}`,
          );
        }

        for (const raw of ordered) {
          const providerMessageId = safeText(raw?.id);
          if (!providerMessageId) {
            skipped += 1;
            continue;
          }

          const fromId = safeText(raw?.from?.id);
          const fromName = safeText(raw?.from?.name);
          const toRows = Array.isArray(raw?.to?.data) ? raw.to.data : [];
          const toIds = toRows.map((item: any) => safeText(item?.id)).filter(Boolean);
          const isPageSender =
            fromId === pageId ||
            (Boolean(this.configuredPageId) && fromId === this.configuredPageId);
          const isTargetCustomerSender = fromId === customerPsid;

          // Messenger 1:1 của khách này chỉ được chứa IN từ đúng PSID khách, hoặc
          // OUT từ Page. Bất kỳ sender nào khác nghĩa là Meta trả nhầm thread/data;
          // bỏ qua tuyệt đối để không làm lẫn hội thoại giữa hai khách.
          if (!isPageSender && !isTargetCustomerSender) {
            this.logger.warn(
              `[META_HISTORY_MESSAGE_OWNER_MISMATCH] conversation=${conversationId} expected=${last6(customerPsid)} from=${last6(fromId)} message=${providerMessageId}`,
            );
            skipped += 1;
            continue;
          }

          // Với tin OUT, nếu Meta có trường to thì phải hướng tới đúng khách.
          if (isPageSender && toIds.length && !toIds.includes(customerPsid)) {
            this.logger.warn(
              `[META_HISTORY_MESSAGE_RECIPIENT_MISMATCH] conversation=${conversationId} expected=${last6(customerPsid)} message=${providerMessageId}`,
            );
            skipped += 1;
            continue;
          }

          const direction = isPageSender ? "OUT" : "IN";
          const sentAtRaw = raw?.created_time ? new Date(raw.created_time) : new Date();
          const sentAt = Number.isNaN(sentAtRaw.getTime()) ? new Date() : sentAtRaw;
          const messageText = safeText(raw?.message);

          const existed = await this.prisma.omniMessage.findUnique({
            where: { providerMessageId },
            select: {
              id: true,
              senderId: true,
              senderName: true,
              direction: true,
              text: true,
              type: true,
              sentAt: true,
            },
          });
          if (existed) {
            // Backfill cũ có thể đã ghi OUT = "The 1970". Nếu local còn một
            // message cùng nội dung/thời điểm có tên nhân viên thật thì phục hồi.
            if (
              direction === "OUT" &&
              this.isGenericMetaPageSenderName((existed as any)?.senderName)
            ) {
              const knownSender = await this.findKnownLocalOutboundSender({
                conversationId,
                sentAt,
                text: messageText || (existed as any)?.text,
                type: (existed as any)?.type === "IMAGE" ? "IMAGE" : "TEXT",
                excludeId: existed.id,
              });
              if (knownSender) {
                await this.prisma.omniMessage.update({
                  where: { id: existed.id },
                  data: {
                    senderId: knownSender.senderId || null,
                    senderName: knownSender.senderName,
                  },
                });
                this.logger.log(
                  `[META_HISTORY_SENDER_REPAIRED] conversation=${conversationId} message=${providerMessageId} sender=${safeText(knownSender.senderName)}`,
                );
              }
            }
            skipped += 1;
            continue;
          }
          const attachmentRows = Array.isArray(raw?.attachments?.data)
            ? raw.attachments.data
            : [];

          const knownSender = direction === "OUT"
            ? await this.findKnownLocalOutboundSender({
                conversationId,
                sentAt,
                text: messageText || null,
                type: messageText || !attachmentRows.length ? "TEXT" : "IMAGE",
              })
            : null;

          if (messageText || !attachmentRows.length) {
            // Nếu đã có message local cũ (thường providerMessageId=null) do Omni
            // gửi, gắn Meta id vào chính row đó thay vì tạo thêm row "The 1970".
            if (knownSender && !safeText(knownSender.providerMessageId)) {
              await this.prisma.omniMessage.update({
                where: { id: knownSender.id },
                data: { providerMessageId, sentAt },
              });
              skipped += 1;
            } else {
            await this.prisma.omniMessage.create({
              data: {
                conversationId,
                providerMessageId,
                direction: direction as any,
                type: "TEXT",
                text: messageText || "[Tin nhắn]",
                attachmentUrl: null,
                senderId: fromId || (direction === "OUT" ? pageId : customerPsid),
                senderName:
                  safeText(knownSender?.senderName) ||
                  fromName ||
                  (direction === "OUT"
                    ? "The 1970"
                    : safeText(params.customerName) || `Khách ${last6(customerPsid)}`),
                sentAt,
              },
            });
            imported += 1;
            }
          }

          // Nếu Meta trả attachment cùng message text, lưu attachment bằng synthetic
          // provider id giống convention webhook hiện tại để không vi phạm unique key.
          for (let index = 0; index < attachmentRows.length; index += 1) {
            const item = attachmentRows[index];
            const attachmentUrl = safeText(
              item?.image_data?.url ||
                item?.video_data?.url ||
                item?.file_url ||
                item?.url ||
                item?.payload?.url,
            );
            if (!attachmentUrl) continue;

            const attachmentProviderId = messageText || index > 0
              ? `${providerMessageId}:attachment:${index}`
              : providerMessageId;
            const attachmentExisted = await this.prisma.omniMessage.findUnique({
              where: { providerMessageId: attachmentProviderId },
              select: { id: true },
            });
            if (attachmentExisted) continue;

            await this.prisma.omniMessage.create({
              data: {
                conversationId,
                providerMessageId: attachmentProviderId,
                direction: direction as any,
                type: "IMAGE",
                text: null,
                attachmentUrl,
                senderId: fromId || (direction === "OUT" ? pageId : customerPsid),
                senderName:
                  safeText(knownSender?.senderName) ||
                  fromName ||
                  (direction === "OUT"
                    ? "The 1970"
                    : safeText(params.customerName) || `Khách ${last6(customerPsid)}`),
                sentAt: new Date(sentAt.getTime() + index + (messageText ? 1 : 0)),
              },
            });
            imported += 1;
          }
        }

        this.messengerHistoryBackfillDone.add(key);
        this.logger.log(
          `[META_HISTORY_BACKFILL_OK] page=${pageId} psid=${last6(customerPsid)} thread=${metaConversationId} fetched=${rows.length} imported=${imported} skipped=${skipped}`,
        );
        return {
          ok: true,
          threadId: metaConversationId,
          fetched: rows.length,
          imported,
          skipped,
        };
      } catch (error: any) {
        // Backfill chỉ là bổ sung lịch sử. Không được làm webhook tin mới fail vì
        // thiếu quyền Conversations API / lỗi Meta tạm thời.
        this.logger.warn(
          `[META_HISTORY_BACKFILL_SKIP] page=${pageId} psid=${last6(customerPsid)} conversation=${conversationId} | ${error?.message || error}`,
        );
        return { skipped: true, reason: safeText(error?.message) || "backfill_failed" };
      } finally {
        this.messengerHistoryBackfillRunning.delete(key);
      }
    })();

    this.messengerHistoryBackfillRunning.set(key, job);
    return job;
  }

  private async metaPost<T = any>(
    path: string,
    body: Record<string, any> = {},
  ): Promise<T> {
    const token = this.pageAccessToken;
    if (!token)
      throw new BadRequestException("Thiếu META_INBOX_PAGE_ACCESS_TOKEN.");

    const url = new URL(
      `https://graph.facebook.com/${this.graphVersion}/${path.replace(/^\/+/, "")}`,
    );
    url.searchParams.set("access_token", token);

    const res = await fetch(url.toString(), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const json = await res.json().catch(() => ({}));

    if (!res.ok) {
      const message =
        json?.error?.message || `Meta Graph API lỗi ${res.status}`;
      this.logger.warn(`[META_GRAPH_POST_FAILED] ${path} | ${message}`);
      throw new BadRequestException(message);
    }

    return json as T;
  }

  private isMetaOutsideStandardMessagingWindow(error: any) {
    const raw = [
      error?.message,
      error?.response?.message,
      error?.response?.data?.error?.message,
      error?.cause?.message,
    ]
      .map((value) => safeText(value).toLowerCase())
      .filter(Boolean)
      .join(" | ");

    return (
      raw.includes("(#10)") &&
      (raw.includes("outside") ||
        raw.includes("allowed window") ||
        raw.includes("messaging window") ||
        raw.includes("24 hour") ||
        raw.includes("24-hour") ||
        raw.includes("khoảng thời gian") ||
        raw.includes("ngoài khoảng"))
    );
  }

  private async sendMetaHumanAgentMessage(
    recipientPsid: string,
    message: Record<string, any>,
  ) {
    return this.metaPost("me/messages", {
      recipient: { id: recipientPsid },
      messaging_type: "MESSAGE_TAG",
      tag: "HUMAN_AGENT",
      message,
    });
  }

  private async metaFormPost<T = any>(
    path: string,
    body: Record<string, any> = {},
  ): Promise<T> {
    const token = this.pageAccessToken;
    if (!token)
      throw new BadRequestException("Thiếu META_INBOX_PAGE_ACCESS_TOKEN.");

    const url = new URL(
      `https://graph.facebook.com/${this.graphVersion}/${path.replace(/^\/+/, "")}`,
    );
    const params = new URLSearchParams();
    params.set("access_token", token);
    for (const [key, value] of Object.entries(body)) {
      if (value !== undefined && value !== null) params.set(key, String(value));
    }

    const res = await fetch(url.toString(), {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: params.toString(),
    });
    const json = await res.json().catch(() => ({}));

    if (!res.ok) {
      const message =
        json?.error?.message || `Meta Graph API lỗi ${res.status}`;
      this.logger.warn(`[META_GRAPH_FORM_POST_FAILED] ${path} | ${message}`);
      throw new BadRequestException(message);
    }

    return json as T;
  }


  private readonly defaultAssignmentPriorities = [
    "ONLINE",
    "BRANCH",
    "LOWEST_LOAD",
    "DRAFT_OWNER",
  ];

  private normalizeSharedText(value: any) {
    return safeText(value).toLocaleLowerCase("vi-VN").replace(/\s+/g, " ");
  }

  private isAdminUser(user?: any) {
    const roles = [
      user?.role,
      user?.roleName,
      user?.activeRole,
      ...(Array.isArray(user?.roles) ? user.roles : []),
    ]
      .map((value) => safeText(value).toUpperCase())
      .filter(Boolean);
    return roles.includes("OWNER") || roles.includes("ADMIN");
  }

  async heartbeat(staff?: any, dto?: { activeBranchId?: string; manualAway?: boolean }) {
    const staffId = safeText(staff?.id || staff?.sub);
    if (!staffId) throw new BadRequestException("Không xác định được nhân viên.");
    const now = new Date();
    const presence = await (this.prisma as any).omniStaffPresence.upsert({
      where: { staffId },
      update: {
        staffName: safeText(staff?.name || staff?.username) || null,
        activeBranchId: safeText(dto?.activeBranchId) || null,
        manualAway: Boolean(dto?.manualAway),
        status: dto?.manualAway ? "AWAY" : "ONLINE",
        lastHeartbeatAt: now,
        lastActiveAt: now,
      },
      create: {
        staffId,
        staffName: safeText(staff?.name || staff?.username) || null,
        activeBranchId: safeText(dto?.activeBranchId) || null,
        manualAway: Boolean(dto?.manualAway),
        status: dto?.manualAway ? "AWAY" : "ONLINE",
        lastHeartbeatAt: now,
        lastActiveAt: now,
      },
    });
    if (Date.now() - this.lastStaleAssignmentSweepAt > 60_000) {
      this.lastStaleAssignmentSweepAt = Date.now();
      void this.reassignStaleUnreadConversations().catch((error: any) =>
        this.logger.warn(`[OMNI_ASSIGNMENT_SWEEP_SKIP] ${error?.message || error}`),
      );
    }

    // Nhân viên vừa online có thể làm các hội thoại từng bị "không có nhân viên
    // phù hợp" trở nên đủ điều kiện. Quét bù ngay, có throttle để nhiều máy
    // heartbeat cùng lúc không tạo tải thừa.
    if (Date.now() - this.lastUnassignedSweepAt > 10_000) {
      this.lastUnassignedSweepAt = Date.now();
      void this.sweepUnassignedConversations("STAFF_ONLINE").catch(
        (error: any) =>
          this.logger.warn(
            `[OMNI_UNASSIGNED_HEARTBEAT_SKIP] ${error?.message || error}`,
          ),
      );
    }

    // Khi có heartbeat, kiểm tra hàng chờ qua đêm. Hàm có throttle riêng nên
    // nhiều máy cùng online không làm chạy lặp liên tục.
    if (Date.now() - this.lastMorningQueueSweepAt > 10_000) {
      this.lastMorningQueueSweepAt = Date.now();
      void this.processMorningQueue().catch((error: any) =>
        this.logger.warn(`[OMNI_MORNING_QUEUE_SKIP] ${error?.message || error}`),
      );
    }
    return presence;
  }

  async getAssignmentSettings() {
    const setting = await (this.prisma as any).omniAssignmentSetting.upsert({
      where: { id: "default" },
      update: {},
      create: {
        id: "default",
        priorityOrder: this.defaultAssignmentPriorities,
        workDays: [1, 2, 3, 4, 5, 6, 0],
      },
      include: { members: { orderBy: [{ sortOrder: "asc" }, { staffName: "asc" }] } },
    });

    const staffIds = (setting.members || []).map((item: any) => item.staffId);
    const presences = staffIds.length
      ? await (this.prisma as any).omniStaffPresence.findMany({ where: { staffId: { in: staffIds } } })
      : [];
    const presenceByStaff = new Map(presences.map((item: any) => [item.staffId, item]));
    const onlineCutoff = Date.now() - Number(setting.onlineWindowSeconds || 90) * 1000;

    return {
      ...setting,
      priorityOrder: Array.isArray(setting.priorityOrder)
        ? setting.priorityOrder
        : this.defaultAssignmentPriorities,
      members: (setting.members || []).map((member: any) => {
        const presence: any = presenceByStaff.get(member.staffId);
        const online = Boolean(
          presence &&
            !presence.manualAway &&
            new Date(presence.lastHeartbeatAt).getTime() >= onlineCutoff,
        );
        return {
          ...member,
          presence: presence || null,
          isOnline: online,
        };
      }),
    };
  }

  async updateAssignmentSettings(dto: any, staff?: any) {
    const priorityOrder = Array.isArray(dto.priorityOrder)
      ? dto.priorityOrder.filter((item: string) => this.defaultAssignmentPriorities.includes(item))
      : undefined;

    const scalarData: any = {};
    const scalarKeys = [
      "isActive", "mode", "requireOnline", "branchPriorityEnabled",
      "lowestLoadEnabled", "draftOwnerPriorityEnabled", "keepPreviousAssignee",
      "keepPreviousDays", "reassignIfAssigneeOffline", "workingHoursOnly",
      "workStartMinute", "workEndMinute", "workDays", "outsideHoursMode", "onlineWindowSeconds", "maxActiveEnabled",
      "maxActiveConversations", "maxUnreadEnabled", "maxUnreadConversations",
      "branchRoutingEnabled", "fallbackBranchId", "noCandidateMode",
      "onlyAssignedCanView", "managerCanViewBranch", "onlyAssignedCanReply",
      "shuffleEachRound", "reassignUnreadEnabled", "reassignAfterMinutes",
      "morningQueueEnabled", "morningQueueInitialBatchSize",
      "morningQueueRepeatIntervalMinutes", "morningQueueRepeatBatchSize",
    ];
    scalarKeys.forEach((key) => {
      if (dto[key] !== undefined) scalarData[key] = dto[key] === "" ? null : dto[key];
    });
    if (priorityOrder?.length) scalarData.priorityOrder = priorityOrder;
    scalarData.updatedById = safeText(staff?.id || staff?.sub) || null;
    scalarData.updatedByName = safeText(staff?.name || staff?.username) || null;

    await (this.prisma as any).$transaction(async (tx: any) => {
      await tx.omniAssignmentSetting.upsert({
        where: { id: "default" },
        update: scalarData,
        create: {
          id: "default",
          priorityOrder: priorityOrder?.length ? priorityOrder : this.defaultAssignmentPriorities,
          workDays: Array.isArray(dto.workDays) ? dto.workDays : [1, 2, 3, 4, 5, 6, 0],
          ...scalarData,
        },
      });

      if (Array.isArray(dto.members)) {
        const ids = dto.members.map((item: any) => safeText(item.staffId)).filter(Boolean);
        await tx.omniAssignmentMember.deleteMany({
          where: { settingId: "default", ...(ids.length ? { staffId: { notIn: ids } } : {}) },
        });
        for (const member of dto.members) {
          const staffId = safeText(member.staffId);
          if (!staffId) continue;
          await tx.omniAssignmentMember.upsert({
            where: { settingId_staffId: { settingId: "default", staffId } },
            update: {
              staffName: safeText(member.staffName) || staffId,
              branchId: safeText(member.branchId) || null,
              branchName: safeText(member.branchName) || null,
              isActive: member.isActive !== false,
              receiveMessages: member.receiveMessages !== false,
              receiveComments: Boolean(member.receiveComments),
              sortOrder: Number(member.sortOrder || 0),
              weight: Math.max(1, Number(member.weight || 1)),
              maxActiveConversations: member.maxActiveConversations || null,
              maxUnreadConversations: member.maxUnreadConversations || null,
            },
            create: {
              settingId: "default",
              staffId,
              staffName: safeText(member.staffName) || staffId,
              branchId: safeText(member.branchId) || null,
              branchName: safeText(member.branchName) || null,
              isActive: member.isActive !== false,
              receiveMessages: member.receiveMessages !== false,
              receiveComments: Boolean(member.receiveComments),
              sortOrder: Number(member.sortOrder || 0),
              weight: Math.max(1, Number(member.weight || 1)),
              maxActiveConversations: member.maxActiveConversations || null,
              maxUnreadConversations: member.maxUnreadConversations || null,
            },
          });
        }
      }
    });

    // Cấu hình hoặc danh sách nhân viên vừa thay đổi có thể mở khóa hàng loạt
    // hội thoại đang chưa gán. Chạy quét bù ngay sau khi transaction hoàn tất.
    void this.sweepUnassignedConversations("SETTINGS_UPDATED").catch(
      (error: any) =>
        this.logger.warn(
          `[OMNI_UNASSIGNED_SETTINGS_SKIP] ${error?.message || error}`,
        ),
    );

    return this.getAssignmentSettings();
  }

  async listAssignmentHistory(limit = 100) {
    return (this.prisma as any).omniAssignmentHistory.findMany({
      orderBy: { createdAt: "desc" },
      take: Math.min(Math.max(Number(limit || 100), 10), 500),
    });
  }

  async getAssignmentReport(query: {
    from?: string;
    to?: string;
    branchId?: string;
    staffId?: string;
    channel?: string;
    assignmentType?: string;
  }) {
    const now = new Date();
    const defaultFrom = new Date(now);
    defaultFrom.setHours(0, 0, 0, 0);

    const from = query.from ? new Date(query.from) : defaultFrom;
    const to = query.to ? new Date(query.to) : now;
    if (Number.isNaN(from.getTime()) || Number.isNaN(to.getTime())) {
      throw new BadRequestException("Khoảng ngày báo cáo không hợp lệ.");
    }
    // Bao trọn ngày khi frontend gửi YYYY-MM-DD.
    if (query.to && /^\d{4}-\d{2}-\d{2}$/.test(query.to)) {
      to.setHours(23, 59, 59, 999);
    }

    const actionFilter =
      query.assignmentType === "AUTO"
        ? { in: ["ASSIGNED", "REASSIGNED"] }
        : query.assignmentType === "MANUAL"
          ? "MANUAL_ASSIGN"
          : query.assignmentType === "REASSIGNED"
            ? "REASSIGNED"
            : { in: ["ASSIGNED", "REASSIGNED", "MANUAL_ASSIGN"] };

    const where: any = {
      createdAt: { gte: from, lte: to },
      assignedStaffId: { not: null },
      action: actionFilter,
    };
    if (safeText(query.branchId)) where.branchId = safeText(query.branchId);
    if (safeText(query.staffId)) {
      where.assignedStaffId = safeText(query.staffId);
    }
    if (safeText(query.channel) && query.channel !== "ALL") {
      where.channel = safeText(query.channel);
    }

    const [setting, historyRows] = await Promise.all([
      (this.prisma as any).omniAssignmentSetting.findUnique({
        where: { id: "default" },
        include: {
          members: {
            orderBy: [{ sortOrder: "asc" }, { staffName: "asc" }],
          },
        },
      }),
      (this.prisma as any).omniAssignmentHistory.findMany({
        where,
        orderBy: { createdAt: "asc" },
        select: {
          assignedStaffId: true,
          assignedStaffName: true,
          action: true,
          triggerType: true,
          conversationId: true,
          createdAt: true,
        },
      }),
    ]);

    const memberById = new Map(
      (setting?.members || []).map((member: any) => [member.staffId, member]),
    );
    const stats = new Map<string, any>();

    for (const row of historyRows) {
      const staffId = safeText(row.assignedStaffId);
      if (!staffId) continue;
      const member: any = memberById.get(staffId);
      const current = stats.get(staffId) || {
        staffId,
        staffName:
          safeText(row.assignedStaffName) ||
          safeText(member?.staffName) ||
          staffId,
        branchId: safeText(member?.branchId) || null,
        branchName: safeText(member?.branchName) || null,
        weight: Math.max(1, Number(member?.weight || 1)),
        assignedCount: 0,
        autoAssignedCount: 0,
        manualAssignedCount: 0,
        reassignedCount: 0,
        uniqueConversationIds: new Set<string>(),
      };
      current.assignedCount += 1;
      if (row.action === "MANUAL_ASSIGN") current.manualAssignedCount += 1;
      else current.autoAssignedCount += 1;
      if (row.action === "REASSIGNED") current.reassignedCount += 1;
      if (row.conversationId) current.uniqueConversationIds.add(row.conversationId);
      stats.set(staffId, current);
    }

    // Hiện cả nhân viên đã cấu hình dù kỳ này chưa được chia hội thoại.
    for (const member of setting?.members || []) {
      if (query.staffId && member.staffId !== query.staffId) continue;
      if (query.branchId && member.branchId !== query.branchId) continue;
      if (!stats.has(member.staffId)) {
        stats.set(member.staffId, {
          staffId: member.staffId,
          staffName: member.staffName,
          branchId: member.branchId || null,
          branchName: member.branchName || null,
          weight: Math.max(1, Number(member.weight || 1)),
          assignedCount: 0,
          autoAssignedCount: 0,
          manualAssignedCount: 0,
          reassignedCount: 0,
          uniqueConversationIds: new Set<string>(),
        });
      }
    }

    const rows = Array.from(stats.values());
    const totalAssigned = rows.reduce(
      (sum: number, row: any) => sum + row.assignedCount,
      0,
    );
    const activeRows = rows.filter((row: any) => row.weight > 0);
    const totalWeight = activeRows.reduce(
      (sum: number, row: any) => sum + row.weight,
      0,
    );

    const normalizedRows = rows
      .map((row: any) => {
        const targetPercent = totalWeight
          ? (row.weight / totalWeight) * 100
          : 0;
        const actualPercent = totalAssigned
          ? (row.assignedCount / totalAssigned) * 100
          : 0;
        return {
          ...row,
          uniqueConversationCount: row.uniqueConversationIds.size,
          uniqueConversationIds: undefined,
          targetPercent,
          actualPercent,
          differencePercent: actualPercent - targetPercent,
        };
      })
      .sort(
        (a: any, b: any) =>
          b.assignedCount - a.assignedCount ||
          b.weight - a.weight ||
          safeText(a.staffName).localeCompare(safeText(b.staffName)),
      );

    return {
      from: from.toISOString(),
      to: to.toISOString(),
      totalAssigned,
      totalAutoAssigned: normalizedRows.reduce(
        (sum: number, row: any) => sum + row.autoAssignedCount,
        0,
      ),
      totalManualAssigned: normalizedRows.reduce(
        (sum: number, row: any) => sum + row.manualAssignedCount,
        0,
      ),
      totalReassigned: normalizedRows.reduce(
        (sum: number, row: any) => sum + row.reassignedCount,
        0,
      ),
      rows: normalizedRows,
    };
  }

  async listQuickReplyTemplates(includeInactive = false) {
    return (this.prisma as any).omniQuickReplyTemplate.findMany({
      where: includeInactive ? {} : { isActive: true },
      orderBy: [{ sortOrder: "asc" }, { createdAt: "asc" }],
    });
  }

  async createQuickReplyTemplate(dto: any, staff?: any) {
    const content = safeText(dto.content);
    if (!content) throw new BadRequestException("Nội dung mẫu trả lời trống.");
    const normalizedText = this.normalizeSharedText(content);
    const existed = await (this.prisma as any).omniQuickReplyTemplate.findUnique({ where: { normalizedText } });
    if (existed) throw new BadRequestException("Mẫu trả lời này đã tồn tại.");
    return (this.prisma as any).omniQuickReplyTemplate.create({
      data: {
        title: safeText(dto.title) || null,
        content,
        normalizedText,
        category: safeText(dto.category) || null,
        sortOrder: Number(dto.sortOrder || 0),
        createdById: safeText(staff?.id || staff?.sub) || null,
        createdByName: safeText(staff?.name || staff?.username) || null,
      },
    });
  }

  async updateQuickReplyTemplate(id: string, dto: any) {
    const current = await (this.prisma as any).omniQuickReplyTemplate.findUnique({ where: { id } });
    if (!current) throw new NotFoundException("Không tìm thấy mẫu trả lời.");
    const content = dto.content === undefined ? current.content : safeText(dto.content);
    if (!content) throw new BadRequestException("Nội dung mẫu trả lời trống.");
    const normalizedText = this.normalizeSharedText(content);
    const duplicate = await (this.prisma as any).omniQuickReplyTemplate.findFirst({
      where: { normalizedText, id: { not: id } },
    });
    if (duplicate) throw new BadRequestException("Mẫu trả lời này đã tồn tại.");
    return (this.prisma as any).omniQuickReplyTemplate.update({
      where: { id },
      data: {
        title: dto.title === undefined ? current.title : safeText(dto.title) || null,
        content,
        normalizedText,
        category: dto.category === undefined ? current.category : safeText(dto.category) || null,
        sortOrder: dto.sortOrder === undefined ? current.sortOrder : Number(dto.sortOrder || 0),
        isActive: dto.isActive === undefined ? current.isActive : Boolean(dto.isActive),
      },
    });
  }

  async deleteQuickReplyTemplate(id: string) {
    const current = await (this.prisma as any).omniQuickReplyTemplate.findUnique({
      where: { id },
    });
    if (!current) throw new NotFoundException("Không tìm thấy mẫu trả lời.");

    await (this.prisma as any).omniQuickReplyTemplate.delete({
      where: { id },
    });

    return { success: true, id };
  }

  async deleteAllQuickReplyTemplates() {
    const result = await (this.prisma as any).omniQuickReplyTemplate.deleteMany({});
    return {
      success: true,
      deletedCount: Number(result?.count || 0),
    };
  }

  private async getAssignmentAccessRule(staff?: any) {
    if (!staff || this.isAdminUser(staff)) return { unrestricted: true };
    const setting: any = await (this.prisma as any).omniAssignmentSetting.findUnique({ where: { id: "default" } });
    const roles = [staff?.role, ...(Array.isArray(staff?.roles) ? staff.roles : [])]
      .map((value) => safeText(value).toUpperCase());
    const isManager = roles.includes("MANAGER");
    return {
      unrestricted: !setting?.onlyAssignedCanView,
      onlyAssigned: Boolean(setting?.onlyAssignedCanView) && !(isManager && setting?.managerCanViewBranch),
      branchOnly: Boolean(setting?.onlyAssignedCanView) && isManager && setting?.managerCanViewBranch,
      onlyAssignedCanReply: Boolean(setting?.onlyAssignedCanReply),
      staffId: safeText(staff?.id || staff?.sub),
      branchId: safeText(staff?.branchId || staff?.activeBranchId),
    };
  }

  private async assertCanAccessConversation(id: string, staff?: any, reply = false) {
    const access: any = await this.getAssignmentAccessRule(staff);
    if (access.unrestricted) return;
    const conversation: any = await this.prisma.omniConversation.findUnique({ where: { id }, select: { assigneeId: true, branchId: true } });
    if (!conversation) throw new NotFoundException("Không tìm thấy hội thoại.");
    if (reply && access.onlyAssignedCanReply && conversation.assigneeId !== access.staffId) {
      throw new BadRequestException("Hội thoại này đang được phân công cho nhân viên khác.");
    }
    if (access.onlyAssigned && conversation.assigneeId !== access.staffId) {
      throw new NotFoundException("Không tìm thấy hội thoại trong phạm vi được phân công.");
    }
    if (access.branchOnly && access.branchId && conversation.branchId !== access.branchId) {
      throw new NotFoundException("Hội thoại không thuộc chi nhánh của bạn.");
    }
  }

  private isInsideWorkingHours(setting: any, now = new Date()) {
    if (!setting?.workingHoursOnly) return true;
    const vnNow = new Date(now.toLocaleString("en-US", { timeZone: "Asia/Ho_Chi_Minh" }));
    const workDays = Array.isArray(setting.workDays) ? setting.workDays.map(Number) : [1, 2, 3, 4, 5, 6, 0];
    if (!workDays.includes(vnNow.getDay())) return false;
    const minute = vnNow.getHours() * 60 + vnNow.getMinutes();
    return minute >= Number(setting.workStartMinute || 480) && minute <= Number(setting.workEndMinute || 1320);
  }

  private getVietnamDateKey(now = new Date()) {
    return new Intl.DateTimeFormat("en-CA", {
      timeZone: "Asia/Ho_Chi_Minh",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).format(now);
  }

  private getVietnamShiftStart(workStartMinute: number, now = new Date()) {
    const [year, month, day] = this.getVietnamDateKey(now).split("-").map(Number);
    const minute = Math.max(0, Math.min(1439, Number(workStartMinute || 480)));
    const hour = Math.floor(minute / 60);
    const minuteOfHour = minute % 60;
    // Việt Nam UTC+7: giờ địa phương trừ 7 giờ để ra UTC.
    return new Date(Date.UTC(year, month - 1, day, hour - 7, minuteOfHour, 0, 0));
  }

  /**
   * Chia hàng chờ ngoài giờ theo từng đợt:
   * - Người/nhóm online đầu tiên nhận tối đa lô đầu (mặc định 20).
   * - Sau mỗi khoảng cấu hình (mặc định 2 phút), chia tiếp lô nhỏ (mặc định 3).
   * - Nếu có thêm nhân viên online trước khi đủ 2 phút, chạy ngay một lô nhỏ để
   *   người mới vào ca được ưu tiên nhờ quy tắc tải hôm nay thấp nhất.
   */
  private async processMorningQueue() {
    const setting: any = await (this.prisma as any).omniAssignmentSetting.findUnique({
      where: { id: "default" },
      include: { members: { where: { isActive: true, receiveMessages: true } } },
    });
    if (!setting?.isActive || setting.mode !== "AUTO" || !setting.morningQueueEnabled) return;
    if (!this.isInsideWorkingHours(setting)) return;

    const now = new Date();
    const dateKey = this.getVietnamDateKey(now);
    const onlineCutoff = new Date(
      now.getTime() - Number(setting.onlineWindowSeconds || 180) * 1000,
    );
    const staffIds = (setting.members || []).map((item: any) => item.staffId).filter(Boolean);
    if (!staffIds.length) return;

    const onlinePresences: any[] = await (this.prisma as any).omniStaffPresence.findMany({
      where: {
        staffId: { in: staffIds },
        manualAway: false,
        lastHeartbeatAt: { gte: onlineCutoff },
      },
      select: { staffId: true },
    });
    const onlineCount = onlinePresences.length;
    if (!onlineCount) return;

    const sameDay = safeText(setting.morningQueueRunDate) === dateKey;
    const lastRunAt = sameDay && setting.morningQueueLastRunAt
      ? new Date(setting.morningQueueLastRunAt)
      : null;
    const previousOnlineCount = sameDay
      ? Number(setting.morningQueueLastOnlineCount || 0)
      : 0;
    const initialDone = sameDay && Boolean(setting.morningQueueInitialDone);
    const repeatIntervalMs = Math.max(
      1,
      Number(setting.morningQueueRepeatIntervalMinutes || 2),
    ) * 60_000;
    const hasNewOnlineStaff = initialDone && onlineCount > previousOnlineCount;
    const repeatDue = Boolean(
      initialDone && (!lastRunAt || now.getTime() - lastRunAt.getTime() >= repeatIntervalMs),
    );

    if (initialDone && !hasNewOnlineStaff && !repeatDue) return;

    const batchSize = initialDone
      ? Math.max(1, Number(setting.morningQueueRepeatBatchSize || 3))
      : Math.max(1, Number(setting.morningQueueInitialBatchSize || 20));
    const shiftStart = this.getVietnamShiftStart(setting.workStartMinute || 480, now);

    const queued: any[] = await this.prisma.omniConversation.findMany({
      where: {
        assigneeId: null,
        // Hàng chờ đầu ca phải dựa vào việc KHÁCH ĐANG CHỜ TRẢ LỜI,
        // không dựa vào unreadCount. Tin đã được ai đó đọc trên Facebook/Omni
        // nhưng chưa trả lời vẫn phải được chia cho nhân viên.
        status: "OPEN" as any,
        lastMessageAt: { lt: shiftStart },
      },
      orderBy: [{ lastMessageAt: "asc" }, { createdAt: "asc" }],
      take: batchSize,
      select: { id: true },
    });

    let assignedCount = 0;
    for (const row of queued) {
      const assigned = await this.autoAssignConversation(row.id, "MORNING_QUEUE");
      if (assigned?.assigneeId) assignedCount += 1;
    }

    await (this.prisma as any).omniAssignmentSetting.update({
      where: { id: "default" },
      data: {
        morningQueueRunDate: dateKey,
        morningQueueInitialDone: true,
        morningQueueLastRunAt: now,
        morningQueueLastOnlineCount: onlineCount,
      },
    });

    if (queued.length) {
      this.logger.log(
        `[OMNI_MORNING_QUEUE] date=${dateKey} online=${onlineCount} requested=${batchSize} assigned=${assignedCount}`,
      );
    }
  }

  /**
   * Tự động xử lý các hội thoại chưa có người phụ trách.
   *
   * Chạy khi:
   * - Backend khởi động.
   * - Mỗi phút.
   * - Có nhân viên heartbeat/online.
   * - Admin lưu lại cấu hình chia tin.
   *
   * Chỉ lấy hội thoại còn cần xử lý và có tin chưa đọc. Mỗi lượt tối đa 100
   * hội thoại để không làm nghẽn webhook; lượt sau tiếp tục phần còn lại.
   */
  private async sweepUnassignedConversations(triggerType: string) {
    if (this.unassignedSweepRunning) return {
      skipped: true,
      reason: "SWEEP_ALREADY_RUNNING",
    };

    this.unassignedSweepRunning = true;
    this.lastUnassignedSweepAt = Date.now();

    try {
      const setting: any = await (this.prisma as any).omniAssignmentSetting.findUnique({
        where: { id: "default" },
        select: {
          isActive: true,
          mode: true,
          outsideHoursMode: true,
          workingHoursOnly: true,
          workStartMinute: true,
          workEndMinute: true,
          workDays: true,
        },
      });

      if (!setting?.isActive || setting.mode !== "AUTO") {
        return { skipped: true, reason: "AUTO_ASSIGNMENT_DISABLED" };
      }

      if (
        !this.isInsideWorkingHours(setting) &&
        setting.outsideHoursMode === "QUEUE"
      ) {
        return { skipped: true, reason: "OUTSIDE_WORKING_HOURS" };
      }

      const rows = await this.prisma.omniConversation.findMany({
        where: {
          assigneeId: null,
          // OPEN = khách đã nhắn và chưa có phản hồi của shop. Không dùng
          // unreadCount ở đây vì "đã đọc" và "đã trả lời" là hai việc khác nhau.
          status: "OPEN" as any,
        },
        orderBy: [
          { lastMessageAt: "asc" },
          { createdAt: "asc" },
        ],
        take: 100,
        select: { id: true },
      });

      let assignedCount = 0;
      let noCandidateCount = 0;

      for (const row of rows) {
        const assigned = await this.autoAssignConversation(
          row.id,
          triggerType,
        );
        if (assigned?.assigneeId) assignedCount += 1;
        else noCandidateCount += 1;
      }

      if (rows.length) {
        this.logger.log(
          `[OMNI_UNASSIGNED_SWEEP] trigger=${triggerType} checked=${rows.length} assigned=${assignedCount} pending=${noCandidateCount}`,
        );
      }

      return {
        checked: rows.length,
        assigned: assignedCount,
        pending: noCandidateCount,
      };
    } finally {
      this.unassignedSweepRunning = false;
    }
  }

  private async reassignStaleUnreadConversations() {
    const setting: any = await (this.prisma as any).omniAssignmentSetting.findUnique({ where: { id: "default" } });
    if (!setting?.isActive || !setting.reassignUnreadEnabled) return;
    const cutoff = new Date(Date.now() - Number(setting.reassignAfterMinutes || 10) * 60_000);
    const rows = await this.prisma.omniConversation.findMany({
      where: { unreadCount: { gt: 0 }, lastMessageAt: { lt: cutoff }, status: { in: ["OPEN", "PROCESSING"] as any } },
      orderBy: { lastMessageAt: "asc" },
      take: 20,
      select: { id: true },
    });
    for (const row of rows) await this.autoAssignConversation(row.id, "STALE_UNREAD");
  }

  private async autoAssignConversation(conversationId: string, triggerType: string) {
    const setting: any = await (this.prisma as any).omniAssignmentSetting.findUnique({
      where: { id: "default" },
      include: { members: { where: { isActive: true }, orderBy: [{ sortOrder: "asc" }, { staffName: "asc" }] } },
    });
    if (!setting?.isActive || setting.mode !== "AUTO" || !setting.members?.length) return null;

    const conversation: any = await this.prisma.omniConversation.findUnique({
      where: { id: conversationId },
      include: { customer: true },
    });
    if (!conversation) return null;
    if (!this.isInsideWorkingHours(setting) && setting.outsideHoursMode === "QUEUE") return null;

    const isComment = safeText(conversation.providerThreadId).startsWith("FACEBOOK_COMMENT:");
    let candidates = setting.members.filter((member: any) =>
      isComment ? member.receiveComments : member.receiveMessages,
    );
    if (!candidates.length) return null;

    const presenceRows = await (this.prisma as any).omniStaffPresence.findMany({
      where: { staffId: { in: candidates.map((item: any) => item.staffId) } },
    });
    const presenceMap = new Map(presenceRows.map((item: any) => [item.staffId, item]));
    const cutoff = Date.now() - Number(setting.onlineWindowSeconds || 90) * 1000;
    const isOnline = (member: any) => {
      const presence: any = presenceMap.get(member.staffId);
      return Boolean(presence && !presence.manualAway && new Date(presence.lastHeartbeatAt).getTime() >= cutoff);
    };

    if (conversation.assigneeId) {
      const currentMember = candidates.find((item: any) => item.staffId === conversation.assigneeId);
      if (currentMember && (!setting.requireOnline || isOnline(currentMember))) return conversation;

      // requireOnline=true là luật cứng: assignee cũ đã offline không được giữ lại
      // chỉ vì reassignIfAssigneeOffline=false. Tiếp tục xuống dưới để chọn một
      // nhân viên đang online; nếu không có ai online thì để nguyên chưa gán/reassign.
      if (!setting.requireOnline && !setting.reassignIfAssigneeOffline) return conversation;
    }

    const draftOrder: any = await this.prisma.order.findFirst({
      where: { omniConversationId: conversationId, status: "NEW" as any },
      orderBy: { createdAt: "desc" },
      select: { assignedStaffId: true, assignedStaffName: true, createdByStaffId: true, createdByStaffName: true, branchId: true },
    });
    const draftOwnerId = safeText(draftOrder?.assignedStaffId || draftOrder?.createdByStaffId);
    const targetBranchId = safeText(conversation.branchId || draftOrder?.branchId || setting.fallbackBranchId);

    // requireOnline là điều kiện CỨNG, không phụ thuộc priorityOrder.
    // Nếu bật requireOnline thì loại nhân viên offline ngay từ đầu để mọi nhánh
    // capacity/branch/weighted queue phía sau không thể vô tình chọn lại họ.
    if (setting.requireOnline) {
      candidates = candidates.filter(isOnline);
      if (!candidates.length) {
        await (this.prisma as any).omniAssignmentHistory.create({
          data: {
            conversationId,
            customerName: conversation.customer?.name || null,
            channel: conversation.channel,
            branchId: conversation.branchId || null,
            action: "NO_CANDIDATE",
            reason: "Không có nhân viên online đủ điều kiện.",
            decisionDetail: { triggerType, hardRequirement: "ONLINE" },
            triggerType,
          },
        });
        return null;
      }
    }

    const staffIds = candidates.map((item: any) => item.staffId);
    const candidatesBeforeCapacity = [...candidates];

    // Giới hạn tải được tính theo ngày làm việc tại Việt Nam, không cộng dồn
    // toàn bộ hội thoại OPEN/PROCESSING/PENDING từ các ngày trước.
    // 00:00 Asia/Ho_Chi_Minh tương ứng 17:00 UTC của ngày hôm trước.
    const now = new Date();
    const vietnamDateParts = new Intl.DateTimeFormat("en-CA", {
      timeZone: "Asia/Ho_Chi_Minh",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).formatToParts(now);
    const vietnamYear = Number(
      vietnamDateParts.find((part) => part.type === "year")?.value,
    );
    const vietnamMonth = Number(
      vietnamDateParts.find((part) => part.type === "month")?.value,
    );
    const vietnamDay = Number(
      vietnamDateParts.find((part) => part.type === "day")?.value,
    );
    const startOfTodayVn = new Date(
      Date.UTC(vietnamYear, vietnamMonth - 1, vietnamDay, -7, 0, 0, 0),
    );

    const todayLoadWhere = {
      assigneeId: { in: staffIds },
      status: { in: ["OPEN", "PROCESSING", "PENDING"] as any },
      lastMessageAt: { gte: startOfTodayVn },
    };

    const groupedLoads: any[] = await (this.prisma.omniConversation as any).groupBy({
      by: ["assigneeId"],
      where: todayLoadWhere,
      _count: { _all: true },
    });
    const unreadLoads: any[] = await (this.prisma.omniConversation as any).groupBy({
      by: ["assigneeId"],
      where: {
        ...todayLoadWhere,
        unreadCount: { gt: 0 },
      },
      _count: { _all: true },
    });
    const activeMap = new Map(groupedLoads.map((item: any) => [item.assigneeId, item._count._all]));
    const unreadMap = new Map(unreadLoads.map((item: any) => [item.assigneeId, item._count._all]));
    candidates = candidates.filter((member: any) => {
      const active = Number(activeMap.get(member.staffId) || 0);
      const unread = Number(unreadMap.get(member.staffId) || 0);
      const maxActive = Number(member.maxActiveConversations || setting.maxActiveConversations || 20);
      const maxUnread = Number(member.maxUnreadConversations || setting.maxUnreadConversations || 10);
      if (setting.maxActiveEnabled && active >= maxActive) return false;
      if (setting.maxUnreadEnabled && unread >= maxUnread) return false;
      return true;
    });

    // Tin mới không được nằm "Chưa gán" chỉ vì toàn bộ nhân viên đang chạm
    // ngưỡng tải. Với INCOMING_MESSAGE, quay lại tập ứng viên hợp lệ ban đầu rồi
    // tiếp tục xét ONLINE/BRANCH/LOWEST_LOAD. Các lượt sweep cũ vẫn tôn trọng cap.
    if (!candidates.length && triggerType === "INCOMING_MESSAGE") {
      candidates = [...candidatesBeforeCapacity];
      this.logger.warn(
        `[OMNI_ASSIGN_CAPACITY_FALLBACK] conversation=${conversationId} candidates=${candidates.length}`,
      );
    }
    if (!candidates.length) return null;

    const priorities = Array.isArray(setting.priorityOrder)
      ? setting.priorityOrder
      : this.defaultAssignmentPriorities;
    const decision: any = { triggerType, priorities, targetBranchId, draftOwnerId, considered: [] };
    const narrow = (matching: any[], reason: string) => {
      if (matching.length) {
        candidates = matching;
        decision.considered.push({ reason, remaining: matching.map((item: any) => item.staffId) });
      }
    };

    for (const priority of priorities) {
      if (priority === "ONLINE" && setting.requireOnline) {
        const online = candidates.filter(isOnline);
        candidates = online;
        decision.considered.push({ reason: "ONLINE", remaining: online.map((item: any) => item.staffId) });
      }
      if (priority === "BRANCH" && setting.branchPriorityEnabled && setting.branchRoutingEnabled && targetBranchId) {
        narrow(candidates.filter((item: any) => safeText(item.branchId) === targetBranchId), "BRANCH");
      }
      if (priority === "LOWEST_LOAD" && setting.lowestLoadEnabled && candidates.length > 1) {
        const minimum = Math.min(...candidates.map((item: any) => Number(activeMap.get(item.staffId) || 0)));
        narrow(candidates.filter((item: any) => Number(activeMap.get(item.staffId) || 0) === minimum), "LOWEST_LOAD");
      }
      if (priority === "DRAFT_OWNER" && setting.draftOwnerPriorityEnabled && draftOwnerId) {
        narrow(candidates.filter((item: any) => item.staffId === draftOwnerId), "DRAFT_OWNER");
      }
    }

    if (!candidates.length) return null;

    // Chia theo trọng số bằng weighted fair queue:
    // nhân viên có weight 1/2/3 sẽ tiến dần tới tỷ lệ 1:2:3.
    // Mốc đếm bắt đầu từ lần lưu cấu hình gần nhất để thay đổi trọng số có hiệu lực ngay.
    const weightWindowStart = setting.updatedAt || new Date(0);
    const weightedHistory: any[] = await (this.prisma as any).omniAssignmentHistory.groupBy({
      by: ["assignedStaffId"],
      where: {
        assignedStaffId: { in: candidates.map((item: any) => item.staffId) },
        createdAt: { gte: weightWindowStart },
        action: { in: ["ASSIGNED", "REASSIGNED", "MANUAL_ASSIGN"] },
      },
      _count: { _all: true },
    });
    const assignedCountMap = new Map(
      weightedHistory.map((item: any) => [
        item.assignedStaffId,
        Number(item?._count?._all || 0),
      ]),
    );

    const candidateScores = candidates.map((member: any, index: number) => {
      const weight = Math.max(1, Number(member.weight || 1));
      const assignedCount = Number(assignedCountMap.get(member.staffId) || 0);
      return {
        member,
        weight,
        assignedCount,
        // Điểm nhỏ hơn sẽ được nhận trước.
        score: (assignedCount + 1) / weight,
        roundRobinDistance:
          setting.lastAssignedStaffId && candidates.length > 1
            ? (index -
                candidates.findIndex(
                  (item: any) => item.staffId === setting.lastAssignedStaffId,
                ) +
                candidates.length) %
              candidates.length
            : index,
      };
    });

    candidateScores.sort(
      (a: any, b: any) =>
        a.score - b.score ||
        a.roundRobinDistance - b.roundRobinDistance ||
        Number(a.member.sortOrder || 0) - Number(b.member.sortOrder || 0) ||
        safeText(a.member.staffName).localeCompare(safeText(b.member.staffName)),
    );

    const selectedScore = candidateScores[0];
    const selected = selectedScore.member;
    decision.weightedCandidates = candidateScores.map((item: any) => ({
      staffId: item.member.staffId,
      staffName: item.member.staffName,
      weight: item.weight,
      assignedCount: item.assignedCount,
      score: item.score,
    }));

    const previousStaffId = conversation.assigneeId;
    const updated = await this.prisma.$transaction(async (tx: any) => {
      const row = await tx.omniConversation.update({
        where: { id: conversationId },
        data: {
          assigneeId: selected.staffId,
          assigneeName: selected.staffName,
          // Phân công KHÔNG đồng nghĩa đã trả lời. Giữ OPEN cho tới khi
          // nhân viên thực sự gửi tin; sendMessage() mới chuyển OPEN -> PROCESSING.
        },
        include: { customer: true, page: true, tags: true },
      });
      await tx.omniAssignmentSetting.update({ where: { id: "default" }, data: { lastAssignedStaffId: selected.staffId } });
      await tx.omniAssignmentHistory.create({
        data: {
          conversationId,
          customerName: conversation.customer?.name || null,
          channel: conversation.channel,
          branchId: targetBranchId || null,
          previousStaffId: previousStaffId || null,
          previousStaffName: conversation.assigneeName || null,
          assignedStaffId: selected.staffId,
          assignedStaffName: selected.staffName,
          action: previousStaffId ? "REASSIGNED" : "ASSIGNED",
          reason: `Phân công tự động theo thứ tự: ${priorities.join(" → ")}`,
          decisionDetail: {
            ...decision,
            selected: selected.staffId,
            selectedWeight: Math.max(1, Number(selected.weight || 1)),
            assignedCountInWeightWindow: Number(
              assignedCountMap.get(selected.staffId) || 0,
            ),
            activeLoad: Number(activeMap.get(selected.staffId) || 0),
            unreadLoad: Number(unreadMap.get(selected.staffId) || 0),
          },
          triggerType,
        },
      });
      return row;
    });
    this.realtime.emit({ type: "conversation.assigned", payload: updated });
    // Một số client chỉ lắng nghe conversation.updated. Emit cả hai để tên người
    // phụ trách xuất hiện ngay sau webhook, không cần F5/load list lại.
    this.realtime.emit({ type: "conversation.updated", payload: updated });
    return updated;
  }

  private findMetaReferralCandidate(event: any) {
    const direct = [
      event?.message?.referral,
      event?.referral,
      event?.postback?.referral,
      event?.messaging_referral,
      event?.message?.messaging_referral,
      event?.message?.quick_reply?.referral,
    ].find((item) => item && typeof item === "object");
    if (direct) return direct;

    // Meta thỉnh thoảng thay đổi vị trí referral/ads_context_data giữa các loại
    // Click-to-Messenger. Quét nông payload để không bỏ mất nguồn quảng cáo.
    const queue: Array<{ value: any; depth: number }> = [{ value: event, depth: 0 }];
    const seen = new Set<any>();
    while (queue.length) {
      const current = queue.shift()!;
      const value = current.value;
      if (!value || typeof value !== "object" || seen.has(value)) continue;
      seen.add(value);

      const ads = value?.ads_context_data;
      const source = safeText(value?.source).toLowerCase();
      const looksLikeAdReferral = Boolean(
        value?.ad_id ||
          value?.advertisement_id ||
          (ads && typeof ads === "object") ||
          source.includes("ad") ||
          source.includes("ads"),
      );
      if (looksLikeAdReferral) return value;

      if (current.depth >= 4) continue;
      for (const child of Object.values(value)) {
        if (child && typeof child === "object") {
          queue.push({ value: child, depth: current.depth + 1 });
        }
      }
    }
    return null;
  }

  private extractMetaAdReferral(event: any) {
    const referral = this.findMetaReferralCandidate(event);

    if (!referral || typeof referral !== "object") return null;

    const ads =
      referral?.ads_context_data && typeof referral.ads_context_data === "object"
        ? referral.ads_context_data
        : referral?.ad_context_data && typeof referral.ad_context_data === "object"
          ? referral.ad_context_data
          : referral?.ads_context && typeof referral.ads_context === "object"
            ? referral.ads_context
            : {};

    const adId = safeText(
      referral?.ad_id ||
        ads?.ad_id ||
        ads?.advertisement_id ||
        referral?.advertisement_id ||
        referral?.adId,
    );
    const postId = safeText(
      ads?.post_id ||
        referral?.post_id ||
        ads?.page_post_id,
    );
    const productId = safeText(
      ads?.product_id ||
        referral?.product_id,
    );
    const title = safeText(
      ads?.ad_title ||
        ads?.title ||
        referral?.ad_title ||
        referral?.title,
    );
    const body = safeText(
      ads?.ad_text ||
        ads?.body ||
        ads?.description ||
        referral?.ad_text ||
        referral?.body,
    );
    const imageUrl = safeText(
      ads?.photo_url ||
        ads?.image_url ||
        referral?.photo_url ||
        referral?.image_url,
    );
    const videoUrl = safeText(
      ads?.video_url ||
        referral?.video_url,
    );
    const directUrl = safeText(
      referral?.source_url ||
        referral?.url ||
        ads?.source_url ||
        ads?.url ||
        ads?.post_url,
    );
    const adUrl =
      directUrl ||
      (postId ? `https://www.facebook.com/${postId}` : "");

    const normalized = {
      source: safeText(referral?.source) || null,
      type: safeText(referral?.type) || null,
      ref: safeText(referral?.ref) || null,
      identifier: safeText(referral?.identifier) || null,
      adId: adId || null,
      postId: postId || null,
      productId: productId || null,
      title: title || null,
      body: body || null,
      imageUrl: imageUrl || null,
      videoUrl: videoUrl || null,
      url: adUrl || null,
      raw: referral,
    };

    const hasUsefulData = Object.entries(normalized).some(
      ([key, value]) =>
        key !== "raw" &&
        value !== null &&
        value !== "",
    );

    return hasUsefulData ? normalized : null;
  }

  private buildAdReferralUpdate(referral: any, timestamp: number) {
    if (!referral) return {};

    return {
      referralSource: referral.source,
      referralType: referral.type,
      referralRef: referral.ref,
      referralIdentifier: referral.identifier,
      adId: referral.adId,
      adPostId: referral.postId,
      adProductId: referral.productId,
      adTitle: referral.title,
      adBody: referral.body,
      adImageUrl: referral.imageUrl,
      adVideoUrl: referral.videoUrl,
      adUrl: referral.url,
      adReferral: referral.raw,
      adFirstSeenAt: new Date(timestamp || Date.now()),
    };
  }

  async getMetaConnectionStatus() {
    const pageId = this.configuredPageId;
    const subscribedFields = this.defaultSubscribedFields;

    const dbPage = pageId
      ? await this.prisma.omniInboxPage.findUnique({
          where: { providerPageId: pageId },
        })
      : await this.prisma.omniInboxPage.findFirst({
          where: { channel: "FACEBOOK", isActive: true },
          orderBy: { updatedAt: "desc" },
        });

    let graphVerified = false;
    let subscriptionVerified = false;
    let graphError = "";
    let subscriptionError = "";
    let graphPageName = "";

    if (!pageId) {
      graphError = "Thiếu META_INBOX_PAGE_ID.";
    } else if (!this.pageAccessToken) {
      graphError = "Thiếu META_INBOX_PAGE_ACCESS_TOKEN.";
    } else {
      try {
        const graphPage = await this.metaFetch<{ id?: string; name?: string }>(
          pageId,
          {
            fields: "id,name",
          },
        );
        graphVerified = Boolean(graphPage?.id);
        graphPageName = safeText(graphPage?.name);
      } catch (error: any) {
        graphError = error?.message || String(error);
      }

      try {
        const subscription = await this.metaFetch<{ data?: any[] }>(
          `${pageId}/subscribed_apps`,
        );
        subscriptionVerified = Array.isArray(subscription?.data)
          ? subscription.data.length > 0
          : false;
      } catch (error: any) {
        subscriptionError = error?.message || String(error);
      }
    }

    return {
      pageId: pageId || dbPage?.providerPageId || "",
      pageName: graphPageName || dbPage?.pageName || (pageId ? "The 1970" : ""),
      channel: "FACEBOOK",
      webhookPath: this.webhookPath,
      subscribedFields,
      tokenConfigured: Boolean(this.pageAccessToken),
      graphVerified,
      subscriptionVerified,
      lastWebhookAt: dbPage?.lastWebhookAt || null,
      graphError,
      subscriptionError,
    };
  }

  async subscribeConfiguredPage() {
    const pageId = this.configuredPageId;
    if (!pageId) throw new BadRequestException("Thiếu META_INBOX_PAGE_ID.");
    if (!this.pageAccessToken)
      throw new BadRequestException("Thiếu META_INBOX_PAGE_ACCESS_TOKEN.");

    const subscribedFields = this.defaultSubscribedFields;

    await this.metaFormPost(`${pageId}/subscribed_apps`, {
      subscribed_fields: subscribedFields.join(","),
    });

    let pageName = "The 1970";
    try {
      const graphPage = await this.metaFetch<{ id?: string; name?: string }>(
        pageId,
        {
          fields: "id,name",
        },
      );
      pageName = safeText(graphPage?.name) || pageName;
    } catch {
      // subscription succeeded; keep configured display name
    }

    await this.prisma.omniInboxPage.upsert({
      where: { providerPageId: pageId },
      update: {
        pageName,
        channel: "FACEBOOK",
        isActive: true,
      },
      create: {
        providerPageId: pageId,
        pageName,
        channel: "FACEBOOK",
        isActive: true,
      },
    });

    return this.getMetaConnectionStatus();
  }

  private async getMessengerProfile(
    psid: string,
  ): Promise<{ name: string; avatarUrl?: string | null; isFallback: boolean }> {
    const fallbackName = `Khách ${last6(psid)}`;

    if (!this.pageAccessToken) {
      this.logger.warn(
        `[META_PROFILE_SKIP] missing page access token | psid=${last6(psid)}`,
      );
      return { name: fallbackName, avatarUrl: null, isFallback: true };
    }

    try {
      const profile = await this.metaFetch<MetaProfile>(psid, {
        fields: "name,first_name,last_name,profile_pic",
      });

      const fullName =
        safeText(profile.name) ||
        [safeText(profile.first_name), safeText(profile.last_name)]
          .filter(Boolean)
          .join(" ")
          .trim();

      const name = fullName || fallbackName;

      return {
        name,
        avatarUrl: safeText(profile.profile_pic) || null,
        isFallback: !isUsableProfileName(name),
      };
    } catch (error: any) {
      // Không để lỗi gọi profile làm rơi webhook. Khi token Page hết hạn hoặc app
      // chưa đủ quyền, hệ thống vẫn lưu hội thoại bằng tên tạm và sẽ enrich lại
      // khi token được thay mới.
      this.logger.warn(
        `[META_PROFILE_FALLBACK] psid=${last6(psid)} | ${error?.message || error}`,
      );
      return { name: fallbackName, avatarUrl: null, isFallback: true };
    }
  }

  private async getFacebookCommentProfile(
    userId: string,
    fallbackNameFromWebhook?: string | null,
  ): Promise<{ name: string; avatarUrl?: string | null; isFallback: boolean }> {
    const fallbackName = safeText(fallbackNameFromWebhook) || `Khách ${last6(userId)}`;

    if (!this.pageAccessToken) {
      this.logMetaDebug(
        `[META_COMMENT_PROFILE_SKIP] missing page access token | user=${last6(userId)}`,
      );
      return { name: fallbackName, avatarUrl: null, isFallback: isFallbackCustomerName(fallbackName) };
    }

    try {
      const profile = await this.metaFetch<MetaProfile>(userId, {
        fields: "id,name,picture.width(240).height(240)",
      });

      const name = safeText(profile.name) || fallbackName;
      const avatarUrl = safeText(profile.picture?.data?.url) || null;

      return {
        name,
        avatarUrl,
        isFallback: !isUsableProfileName(name),
      };
    } catch (error: any) {
      this.logMetaDebug(
        `[META_COMMENT_PROFILE_FALLBACK] user=${last6(userId)} | ${error?.message || error}`,
      );
      return { name: fallbackName, avatarUrl: null, isFallback: isFallbackCustomerName(fallbackName) };
    }
  }

  /**
   * Map Facebook PSID/providerUserId -> tên khách đã từng biết trong lịch sử tin nhắn.
   * Dùng như nguồn phục hồi trước khi gọi Graph API, để rollback/code deploy không phụ
   * thuộc cache phía frontend và không mất tên nếu OmniCustomer đang bị fallback.
   */
  private async getMappedCustomerNameFromMessages(providerUserId?: string | null) {
    const psid = safeText(providerUserId);
    if (!psid) return "";

    const rows = await this.prisma.omniMessage.findMany({
      where: {
        senderId: psid,
        direction: "IN" as any,
        senderName: { not: null },
      },
      orderBy: { sentAt: "desc" },
      take: 30,
      select: { senderName: true },
    });

    for (const row of rows) {
      const name = safeText(row.senderName);
      if (isUsableProfileName(name)) return name;
    }

    return "";
  }

  private async refreshCustomerProfileIfNeeded(customer?: any | null) {
    if (!customer?.providerUserId) return customer;

    const needsRefresh =
      isFallbackCustomerName(customer.name) || !safeText(customer.avatarUrl);

    if (!needsRefresh) return customer;

    // 1) Ưu tiên map lại PSID -> tên thật đã từng lưu trong OmniMessage.
    // Cách này phục hồi được tên ngay cả khi Graph API đang lỗi/tạm thiếu quyền.
    const mappedName = isFallbackCustomerName(customer.name)
      ? await this.getMappedCustomerNameFromMessages(customer.providerUserId)
      : "";

    if (mappedName) {
      const repaired = await this.prisma.omniCustomer.update({
        where: { id: customer.id },
        data: { name: mappedName },
      });
      customer = repaired;
      this.logger.log(
        `[META_PROFILE_NAME_MAPPED] psid=${last6(customer.providerUserId)} name="${mappedName}"`,
      );
    }

    // 2) Sau đó lấy profile mới từ Meta để bổ sung/cập nhật name + avatar.
    const profile = await this.getMessengerProfile(customer.providerUserId);
    if (profile.isFallback && !profile.avatarUrl) return customer;

    const nextName = profile.isFallback ? customer.name : profile.name;
    const nextAvatar = profile.avatarUrl || customer.avatarUrl || null;

    if (nextName === customer.name && nextAvatar === customer.avatarUrl) {
      return customer;
    }

    const updated = await this.prisma.omniCustomer.update({
      where: { id: customer.id },
      data: {
        name: nextName,
        avatarUrl: nextAvatar,
      },
    });

    this.logger.log(
      `[META_PROFILE_REFRESHED] psid=${last6(customer.providerUserId)} name="${updated.name}" avatar=${updated.avatarUrl ? "yes" : "no"}`,
    );

    return updated;
  }

  private async enrichConversationCustomers<T extends Array<any>>(items: T): Promise<T> {
    const targets = items
      .filter((item) => item?.customer?.providerUserId)
      .filter(
        (item) =>
          isFallbackCustomerName(item.customer?.name) ||
          !safeText(item.customer?.avatarUrl),
      )
      .slice(0, 10);

    if (!targets.length) return items;

    await Promise.all(
      targets.map(async (item) => {
        try {
          const updatedCustomer = await this.refreshCustomerProfileIfNeeded(
            item.customer,
          );
          item.customer = updatedCustomer;
        } catch (error: any) {
          this.logger.warn(
            `[META_PROFILE_REFRESH_SKIP] conversation=${item?.id || "-"} | ${error?.message || error}`,
          );
        }
      }),
    );

    return items;
  }

  async listConversations(query: ListConversationsDto & { tag?: string; unread?: string | boolean }, staff?: any) {
    const page = Number(query.page || 1);
    const limit = Math.min(Math.max(Number(query.limit || 30), 10), 100);
    const skip = (page - 1) * limit;

    const where: any = {};
    const configuredPageId = this.configuredPageId;
    if (configuredPageId) {
      // Các bản cũ từng hiểu nhầm reply của chính Page là một "khách The 1970"
      // và tạo conversation riêng. Ẩn các conversation rác này khỏi danh sách;
      // khi mở thread khách thật, syncFacebookCommentReplies() sẽ chuyển message
      // lịch sử về đúng thread.
      where.NOT = {
        providerThreadId: {
          startsWith: `FACEBOOK_COMMENT:${configuredPageId}:`,
        },
        customer: {
          is: { providerUserId: configuredPageId },
        },
      };
    }

    const access: any = await this.getAssignmentAccessRule(staff);
    if (!access.unrestricted) {
      if (access.onlyAssigned) where.assigneeId = access.staffId || "__NO_STAFF__";
      if (access.branchOnly && access.branchId) where.branchId = access.branchId;
    }

    if (query.status && query.status !== "ALL") where.status = query.status;
    if (query.channel && query.channel !== "ALL") where.channel = query.channel;
    if (query.assigneeId && access.unrestricted) where.assigneeId = query.assigneeId;
    if (query.branchId) where.branchId = query.branchId;

    const tag = safeText(query.tag);
    if (tag) {
      where.tags = {
        some: {
          tag: { equals: tag, mode: "insensitive" },
        },
      };
    }

    const unreadRaw = safeText(query.unread).toLowerCase();
    if (["1", "true", "yes"].includes(unreadRaw)) {
      where.unreadCount = { gt: 0 };
    }

    const q = safeText(query.q);
    if (q) {
      const qDigits = q.replace(/\D/g, "");
      const compactQuery = q.replace(/[\s.+()-]/g, "");
      const isPhoneSearch = qDigits.length >= 8 && qDigits === compactQuery;

      // Luôn tìm trong TOÀN BỘ lịch sử OmniMessage, không chỉ lastMessageText.
      // Đây là nguồn chính cho khách cũ chưa từng tạo đơn qua Omni Inbox.
      const messageConversationIds = new Set<string>();

      if (isPhoneSearch) {
        const normalizedPhone = qDigits.startsWith("84")
          ? `0${qDigits.slice(2)}`
          : qDigits;
        const phoneSuffix = normalizedPhone.slice(-9);

        // Chuẩn hoá cả nội dung tin trong PostgreSQL để khớp các dạng:
        // 098..., +84 98..., 098...., 098-... hoặc có khoảng trắng.
        const rows = await (this.prisma as any).$queryRawUnsafe(
          `SELECT DISTINCT "conversationId"
             FROM "OmniMessage"
            WHERE regexp_replace(coalesce("text", ''), '\\D', '', 'g') LIKE $1
            ORDER BY "conversationId"
            LIMIT 2000`,
          `%${phoneSuffix}%`,
        );
        for (const row of Array.isArray(rows) ? rows : []) {
          const id = safeText(row?.conversationId);
          if (id) messageConversationIds.add(id);
        }

        const customerRows = await (this.prisma as any).$queryRawUnsafe(
          `SELECT DISTINCT c."id"
             FROM "OmniCustomer" c
            WHERE regexp_replace(coalesce(c."phone", ''), '\\D', '', 'g') LIKE $1
            LIMIT 1000`,
          `%${phoneSuffix}%`,
        );
        const customerIds = (Array.isArray(customerRows) ? customerRows : [])
          .map((row: any) => safeText(row?.id))
          .filter(Boolean);

        // Đơn mới có omniConversationId sẽ được tìm thêm qua liên kết đơn.
        // Đơn cũ không có liên kết vẫn không ảnh hưởng vì đã tìm trực tiếp trong tin nhắn.
        const orderRows = await (this.prisma as any).$queryRawUnsafe(
          `SELECT DISTINCT "omniConversationId"
             FROM "Order"
            WHERE "omniConversationId" IS NOT NULL
              AND (
                regexp_replace(coalesce("customerPhone", ''), '\\D', '', 'g') LIKE $1
                OR regexp_replace(coalesce("shippingPhone", ''), '\\D', '', 'g') LIKE $1
              )
            LIMIT 1000`,
          `%${phoneSuffix}%`,
        );
        const orderConversationIds = (Array.isArray(orderRows) ? orderRows : [])
          .map((row: any) => safeText(row?.omniConversationId))
          .filter(Boolean);

        const phoneConditions: any[] = [];
        if (messageConversationIds.size) {
          phoneConditions.push({ id: { in: Array.from(messageConversationIds) } });
        }
        if (customerIds.length) {
          phoneConditions.push({ customerId: { in: customerIds } });
        }
        if (orderConversationIds.length) {
          phoneConditions.push({ id: { in: orderConversationIds } });
        }

        where.AND = [
          ...(Array.isArray(where.AND) ? where.AND : []),
          phoneConditions.length
            ? { OR: phoneConditions }
            : { id: "__PHONE_NOT_FOUND__" },
        ];
      } else {
        const matchingMessages = await this.prisma.omniMessage.findMany({
          where: {
            OR: [
              { text: { contains: q, mode: "insensitive" } },
              { senderName: { contains: q, mode: "insensitive" } },
              { providerMessageId: { contains: q, mode: "insensitive" } },
            ],
          },
          select: { conversationId: true },
          distinct: ["conversationId"],
          take: 2000,
        });
        for (const row of matchingMessages) {
          const id = safeText(row.conversationId);
          if (id) messageConversationIds.add(id);
        }

        where.AND = [
          ...(Array.isArray(where.AND) ? where.AND : []),
          {
            OR: [
              ...(messageConversationIds.size
                ? [{ id: { in: Array.from(messageConversationIds) } }]
                : []),
              { id: { contains: q, mode: "insensitive" } },
              { providerThreadId: { contains: q, mode: "insensitive" } },
              { lastMessageText: { contains: q, mode: "insensitive" } },
              { assigneeName: { contains: q, mode: "insensitive" } },
              { adId: { contains: q, mode: "insensitive" } },
              { adPostId: { contains: q, mode: "insensitive" } },
              { adTitle: { contains: q, mode: "insensitive" } },
              { adBody: { contains: q, mode: "insensitive" } },
              { referralRef: { contains: q, mode: "insensitive" } },
              { customer: { is: { name: { contains: q, mode: "insensitive" } } } },
              { customer: { is: { address: { contains: q, mode: "insensitive" } } } },
              { customer: { is: { providerUserId: { contains: q, mode: "insensitive" } } } },
              { notes: { some: { OR: [
                { note: { contains: q, mode: "insensitive" } },
                { staffName: { contains: q, mode: "insensitive" } },
              ] } } },
              { tags: { some: { tag: { contains: q, mode: "insensitive" } } } },
            ],
          },
        ];
      }
    }

    const [items, total] = await this.prisma.$transaction([
      this.prisma.omniConversation.findMany({
        where,
        orderBy: [{ lastMessageAt: "desc" }, { updatedAt: "desc" }],
        skip,
        take: limit,
        include: {
          customer: true,
          page: true,
          tags: true,
          _count: { select: { messages: true, notes: true } },
        },
      }),
      this.prisma.omniConversation.count({ where }),
    ]);

    await this.enrichConversationCustomers(items as any);

    return {
      items,
      page,
      limit,
      total,
      hasNext: skip + items.length < total,
    };
  }

  private parseFacebookCommentThreadId(providerThreadId?: string | null) {
    const raw = safeText(providerThreadId);
    if (!raw.startsWith("FACEBOOK_COMMENT:")) return null;
    const parts = raw.split(":");
    const pageId = safeText(parts[1]);
    const postId = safeText(parts[2]);
    const commentId = safeText(parts.slice(3).join(":"));
    if (!pageId || !postId || postId === "post" || !commentId) return null;
    return { pageId, postId, commentId };
  }

  private async resolveFacebookCommentPostId(
    commentId: string,
    fallbackPostId?: string | null,
  ) {
    const fallback = safeText(fallbackPostId);

    // Webhook đôi khi trả post_id không phải object bài viết có thể GET trực tiếp
    // (đặc biệt photo/dark-post). Comment object lại giữ quan hệ parent chuẩn hơn.
    // Đi ngược tối đa 2 cấp: reply -> comment gốc -> post.
    let currentId = safeText(commentId);
    for (let depth = 0; depth < 2 && currentId; depth += 1) {
      try {
        const node: any = await this.metaFetch(currentId, {
          fields: "id,parent{id}",
        });
        const parentId = safeText(node?.parent?.id);
        if (!parentId) break;

        // Page post id thường có dạng PAGEID_POSTID. Nếu gặp thì dùng ngay.
        if (parentId.includes("_")) return parentId;
        currentId = parentId;
      } catch (error: any) {
        this.logMetaDebug(
          `[META_COMMENT_POST_RESOLVE_SKIP] comment=${commentId} node=${currentId} | ${error?.message || error}`,
        );
        break;
      }
    }

    return fallback && fallback !== "post" ? fallback : "";
  }

  private async getFacebookCommentSource(providerThreadId?: string | null) {
    const raw = safeText(providerThreadId);
    if (!raw.startsWith("FACEBOOK_COMMENT:")) return null;

    const parts = raw.split(":");
    const pageId = safeText(parts[1]);
    const storedPostId = safeText(parts[2]);
    const commentId = safeText(parts.slice(3).join(":"));
    if (!pageId || !commentId) return null;

    const buildSource = async (postId: string) => {
      const cached = this.facebookPostSourceCache.get(postId);
      if (cached && cached.expiresAt > Date.now()) {
        return { ...cached.data, commentId };
      }

      const post: any = await this.metaFetch(postId, {
        fields: [
          "id",
          "message",
          "permalink_url",
          "full_picture",
          "created_time",
          "attachments{media_type,media,url,target,subattachments}",
          "from{id,name}",
        ].join(","),
      });

      const attachment = Array.isArray(post?.attachments?.data)
        ? post.attachments.data[0]
        : null;
      const subAttachment = Array.isArray(attachment?.subattachments?.data)
        ? attachment.subattachments.data[0]
        : null;
      const imageUrl = safeText(
        post?.full_picture ||
          attachment?.media?.image?.src ||
          subAttachment?.media?.image?.src,
      );
      const videoUrl = safeText(
        attachment?.media_type === "video"
          ? attachment?.url || attachment?.target?.url
          : "",
      );

      const data = {
        postId,
        pageId,
        pageName: safeText(post?.from?.name) || "The 1970",
        message: safeText(post?.message),
        permalinkUrl: safeText(post?.permalink_url),
        imageUrl: imageUrl || null,
        videoUrl: videoUrl || null,
        mediaType: safeText(attachment?.media_type) || null,
        createdTime: safeText(post?.created_time) || null,
      };

      this.facebookPostSourceCache.set(postId, {
        expiresAt: Date.now() + 10 * 60_000,
        data,
      });
      return { ...data, commentId };
    };

    // 1) Giữ hành vi cũ: ưu tiên post id đã lưu trong providerThreadId.
    if (storedPostId && storedPostId !== "post") {
      try {
        return await buildSource(storedPostId);
      } catch (error: any) {
        this.logMetaDebug(
          `[META_COMMENT_SOURCE_DIRECT_SKIP] post=${storedPostId} comment=${commentId} | ${error?.message || error}`,
        );
      }
    }

    // 2) Nếu post id cũ sai/thiếu, tự lần parent từ comment để tìm post thật.
    const resolvedPostId = await this.resolveFacebookCommentPostId(
      commentId,
      storedPostId,
    );
    if (resolvedPostId && resolvedPostId !== storedPostId) {
      try {
        return await buildSource(resolvedPostId);
      } catch (error: any) {
        this.logMetaDebug(
          `[META_COMMENT_SOURCE_RESOLVED_SKIP] post=${resolvedPostId} comment=${commentId} | ${error?.message || error}`,
        );
      }
    }

    // Không làm mất card nguồn nếu Meta tạm thời không cho load chi tiết.
    return {
      postId: resolvedPostId || (storedPostId !== "post" ? storedPostId : ""),
      pageId,
      commentId,
      pageName: "The 1970",
      message: "",
      permalinkUrl: "",
      imageUrl: null,
      videoUrl: null,
      mediaType: null,
      createdTime: null,
    };
  }

  private async syncFacebookCommentReplies(conversation: any) {
    const parsed = this.parseFacebookCommentThreadId(
      conversation?.providerThreadId,
    );
    if (!parsed) return null;

    let response: any = null;
    try {
      response = await this.metaFetch(`${parsed.commentId}/comments`, {
        fields: "id,message,from{id,name},created_time",
        limit: "100",
      });
    } catch (error: any) {
      this.logMetaDebug(
        `[META_COMMENT_THREAD_SYNC_SKIP] conversation=${conversation?.id || "-"} comment=${parsed.commentId} | ${error?.message || error}`,
      );
      return null;
    }

    const replies = Array.isArray(response?.data) ? response.data : [];
    const pageReplies = replies.filter((reply: any) => {
      const senderId = safeText(reply?.from?.id);
      return (
        senderId === parsed.pageId ||
        (Boolean(this.configuredPageId) &&
          senderId === this.configuredPageId)
      );
    });

    if (!pageReplies.length) return null;

    let newestReplyAt: Date | null = null;
    let newestReplyText = "";

    for (const reply of pageReplies) {
      const replyId = safeText(reply?.id);
      if (!replyId) continue;

      const replyText = safeText(reply?.message) || "[Bình luận]";
      const parsedSentAt = reply?.created_time
        ? new Date(reply.created_time)
        : new Date();
      const replySentAt = Number.isNaN(parsedSentAt.getTime())
        ? new Date()
        : parsedSentAt;

      const existing = await this.prisma.omniMessage.findUnique({
        where: { providerMessageId: replyId },
      });

      if (existing) {
        // Nếu reply này từng bị ingest sai thành conversation "The 1970",
        // chuyển thẳng message về thread khách và sửa direction thành OUT.
        if (
          existing.conversationId !== conversation.id ||
          existing.direction !== "OUT"
        ) {
          await this.prisma.omniMessage.update({
            where: { id: existing.id },
            data: {
              conversationId: conversation.id,
              direction: "OUT",
              text: replyText,
              senderId: parsed.pageId,
              senderName:
                safeText(reply?.from?.name) ||
                safeText(conversation?.page?.pageName) ||
                "The 1970",
              sentAt: replySentAt,
            },
          });
        }
      } else {
        await this.prisma.omniMessage.create({
          data: {
            conversationId: conversation.id,
            providerMessageId: replyId,
            direction: "OUT",
            type: "TEXT",
            text: replyText,
            attachmentUrl: null,
            senderId: parsed.pageId,
            senderName:
              safeText(reply?.from?.name) ||
              safeText(conversation?.page?.pageName) ||
              "The 1970",
            sentAt: replySentAt,
          },
        });
      }

      if (!newestReplyAt || replySentAt.getTime() > newestReplyAt.getTime()) {
        newestReplyAt = replySentAt;
        newestReplyText = replyText;
      }
    }

    if (!newestReplyAt) return null;

    const currentLastAt = conversation?.lastMessageAt
      ? new Date(conversation.lastMessageAt)
      : null;
    const replyIsNewest =
      !currentLastAt ||
      Number.isNaN(currentLastAt.getTime()) ||
      newestReplyAt.getTime() >= currentLastAt.getTime();

    const updated = await this.prisma.omniConversation.update({
      where: { id: conversation.id },
      data: {
        ...(replyIsNewest
          ? {
              lastMessageText: `[Trả lời bình luận] ${newestReplyText}`,
              lastMessageAt: newestReplyAt,
            }
          : {}),
        status:
          conversation.status === "OPEN"
            ? "PROCESSING"
            : conversation.status,
      },
      include: { customer: true, page: true, tags: true },
    });

    this.realtime.emit({ type: "conversation.updated", payload: updated });
    return updated;
  }

  async getConversation(id: string, staff?: any) {
    await this.assertCanAccessConversation(id, staff);
    const item = await this.prisma.omniConversation.findUnique({
      where: { id },
      include: {
        customer: true,
        page: true,
        tags: true,
        notes: { orderBy: { createdAt: "desc" }, take: 20 },
        orders: {
          where: { source: "OMNI_INBOX_QUICK_ORDER" },
          orderBy: { createdAt: "desc" },
          take: 10,
          include: { items: true },
        },
        messages: { orderBy: { sentAt: "desc" }, take: 200 },
      },
    });

    if (!item) throw new NotFoundException("Không tìm thấy hội thoại.");

    try {
      const updatedCustomer = await this.refreshCustomerProfileIfNeeded(
        item.customer,
      );
      (item as any).customer = updatedCustomer;
    } catch (error: any) {
      this.logger.warn(
        `[META_PROFILE_REFRESH_SKIP] conversation=${id} | ${error?.message || error}`,
      );
    }

    // Nếu nhân viên mở thread ngay khi webhook vừa tới, chờ đúng job lazy backfill
    // của khách này hoàn tất để lịch sử cũ xuất hiện ngay trong response đầu tiên.
    if (
      (item as any).channel === "FACEBOOK" &&
      !safeText((item as any).providerThreadId).startsWith("FACEBOOK_COMMENT:")
    ) {
      const parts = safeText((item as any).providerThreadId).split(":");
      const pageProviderId = safeText(parts[1] || (item as any).page?.providerPageId);
      const customerPsid = safeText(
        (item as any).customer?.providerUserId || parts.slice(2).join(":"),
      );
      if (pageProviderId && customerPsid) {
        await this.backfillMessengerHistoryForCustomer({
          pageId: pageProviderId,
          customerPsid,
          conversationId: id,
          customerName: (item as any).customer?.name,
        });
        // Backfill có thể vừa thêm message cũ nên lấy lại list trước khi trả frontend.
        (item as any).messages = await this.prisma.omniMessage.findMany({
          where: { conversationId: id },
          orderBy: { sentAt: "desc" },
          take: 200,
        });
      }
    }

    if (safeText((item as any).providerThreadId).startsWith("FACEBOOK_COMMENT:")) {
      try {
        const synced = await this.syncFacebookCommentReplies(item as any);
        if (synced) {
          Object.assign(item as any, synced, {
            messages: (item as any).messages,
            notes: (item as any).notes,
            orders: (item as any).orders,
          });
        }

        // Sync có thể vừa tạo hoặc chuyển các reply lịch sử từ Pancake/Facebook
        // về đúng conversation, nên lấy lại message trước khi trả frontend.
        (item as any).messages = await this.prisma.omniMessage.findMany({
          where: { conversationId: id },
          orderBy: { sentAt: "desc" },
          take: 200,
        });
      } catch (error: any) {
        this.logger.warn(
          `[META_COMMENT_THREAD_SYNC_FAILED] conversation=${id} | ${error?.message || error}`,
        );
      }

      (item as any).commentSource = await this.getFacebookCommentSource(
        (item as any).providerThreadId,
      );
    }

    (item as any).messages = Array.isArray((item as any).messages)
      ? [...(item as any).messages].reverse()
      : [];

    return item;
  }

  async assignConversation(
    id: string,
    dto: { assigneeId: string; assigneeName: string },
    staff?: any,
  ) {
    const current = await this.prisma.omniConversation.findUnique({ where: { id }, include: { customer: true } });
    const item = await this.prisma.omniConversation.update({
      where: { id },
      data: {
        assigneeId: dto.assigneeId,
        assigneeName: dto.assigneeName,
        status: "PROCESSING",
      },
      include: { customer: true, page: true, tags: true },
    });

    await (this.prisma as any).omniAssignmentHistory.create({ data: {
      conversationId: id,
      customerName: current?.customer?.name || null,
      channel: item.channel,
      branchId: item.branchId || null,
      previousStaffId: current?.assigneeId || null,
      previousStaffName: current?.assigneeName || null,
      assignedStaffId: dto.assigneeId,
      assignedStaffName: dto.assigneeName,
      action: "MANUAL_ASSIGN",
      reason: "Phân công thủ công",
      triggerType: "MANUAL",
      createdById: safeText(staff?.id || staff?.sub) || null,
      createdByName: safeText(staff?.name || staff?.username) || null,
    }});
    this.realtime.emit({ type: "conversation.assigned", payload: item });
    return item;
  }

  async updateStatus(id: string, status: any) {
    const item = await this.prisma.omniConversation.update({
      where: { id },
      data: {
        status,
        closedAt: status === "CLOSED" ? new Date() : null,
      },
      include: { customer: true, page: true, tags: true },
    });

    this.realtime.emit({ type: "conversation.updated", payload: item });
    return item;
  }

  async updateTags(id: string, tags: string[]) {
    const cleanTags = Array.from(
      new Set(
        tags
          .map((tag) => safeText(tag))
          .filter(Boolean)
          .slice(0, 20),
      ),
    );

    await this.prisma.$transaction([
      this.prisma.omniConversationTag.deleteMany({
        where: { conversationId: id },
      }),
      ...cleanTags.map((tag) =>
        this.prisma.omniConversationTag.create({
          data: { conversationId: id, tag },
        }),
      ),
    ]);

    const item = await this.getConversation(id);
    this.realtime.emit({ type: "conversation.tagged", payload: item });
    return item;
  }

  private normalizeTagTemplateName(value: string) {
    return safeText(value).toLocaleLowerCase("vi-VN").replace(/\s+/g, " ");
  }

  async listTagTemplates(includeInactive = false) {
    const templates = await (this.prisma as any).omniTagTemplate.findMany({
      where: includeInactive ? {} : { isActive: true },
      orderBy: [{ sortOrder: "asc" }, { name: "asc" }],
    });

    const usageRows: any[] = await (this.prisma.omniConversationTag as any).groupBy({
      by: ["tag"],
      where: {
        tag: { in: templates.map((item: any) => item.name) },
      },
      _count: { _all: true },
    });
    const usageMap = new Map(
      usageRows.map((item: any) => [item.tag, Number(item?._count?._all || 0)]),
    );

    return templates.map((item: any) => ({
      ...item,
      conversationCount: Number(usageMap.get(item.name) || 0),
    }));
  }

  async createTagTemplate(dto: any, staff?: any) {
    const name = safeText(dto.name);
    if (!name) throw new BadRequestException("Tên nhãn trống.");

    const normalizedName = this.normalizeTagTemplateName(name);
    const existed = await (this.prisma as any).omniTagTemplate.findUnique({
      where: { normalizedName },
    });
    if (existed) {
      if (!existed.isActive) {
        return (this.prisma as any).omniTagTemplate.update({
          where: { id: existed.id },
          data: {
            name,
            color: safeText(dto.color) || existed.color || null,
            sortOrder:
              dto.sortOrder === undefined
                ? existed.sortOrder
                : Number(dto.sortOrder || 0),
            isActive: true,
          },
        });
      }
      throw new BadRequestException("Nhãn hội thoại này đã tồn tại.");
    }

    return (this.prisma as any).omniTagTemplate.create({
      data: {
        name,
        normalizedName,
        color: safeText(dto.color) || null,
        sortOrder: Number(dto.sortOrder || 0),
        createdById: safeText(staff?.id || staff?.sub) || null,
        createdByName: safeText(staff?.name || staff?.username) || null,
      },
    });
  }

  async updateTagTemplate(id: string, dto: any) {
    const current = await (this.prisma as any).omniTagTemplate.findUnique({
      where: { id },
    });
    if (!current) throw new NotFoundException("Không tìm thấy nhãn hội thoại.");

    const name = dto.name === undefined ? current.name : safeText(dto.name);
    if (!name) throw new BadRequestException("Tên nhãn trống.");

    const normalizedName = this.normalizeTagTemplateName(name);
    const duplicate = await (this.prisma as any).omniTagTemplate.findFirst({
      where: {
        normalizedName,
        id: { not: id },
      },
    });
    if (duplicate) throw new BadRequestException("Nhãn hội thoại này đã tồn tại.");

    return (this.prisma as any).$transaction(async (tx: any) => {
      if (name !== current.name) {
        await tx.omniConversationTag.updateMany({
          where: { tag: current.name },
          data: { tag: name },
        });
      }

      const updated = await tx.omniTagTemplate.update({
        where: { id },
        data: {
          name,
          normalizedName,
          color:
            dto.color === undefined
              ? current.color
              : safeText(dto.color) || null,
          sortOrder:
            dto.sortOrder === undefined
              ? current.sortOrder
              : Number(dto.sortOrder || 0),
          isActive:
            dto.isActive === undefined
              ? current.isActive
              : Boolean(dto.isActive),
        },
      });

      const conversationCount = await tx.omniConversationTag.count({
        where: { tag: name },
      });

      return { ...updated, conversationCount };
    });
  }

  async deleteTagTemplate(id: string) {
    const current = await (this.prisma as any).omniTagTemplate.findUnique({
      where: { id },
    });
    if (!current) throw new NotFoundException("Không tìm thấy nhãn hội thoại.");

    return (this.prisma as any).$transaction(async (tx: any) => {
      const removed = await tx.omniConversationTag.deleteMany({
        where: { tag: current.name },
      });
      await tx.omniTagTemplate.delete({ where: { id } });
      return {
        success: true,
        id,
        removedConversationTags: Number(removed?.count || 0),
      };
    });
  }

  private normalizeNoteTemplateName(value: string) {
    return safeText(value).toLocaleLowerCase("vi-VN").replace(/\s+/g, " ");
  }

  async listNoteTemplates(includeInactive = false) {
    return this.prisma.omniNoteTemplate.findMany({
      where: includeInactive ? {} : { isActive: true },
      orderBy: [{ sortOrder: "asc" }, { name: "asc" }],
    });
  }

  async createNoteTemplate(dto: any, staff?: any) {
    const name = safeText(dto.name);
    if (!name) throw new BadRequestException("Tên ghi chú trống.");
    const normalizedName = this.normalizeNoteTemplateName(name);
    const existed = await this.prisma.omniNoteTemplate.findUnique({ where: { normalizedName } });
    if (existed) throw new BadRequestException("Tên ghi chú này đã tồn tại.");
    return this.prisma.omniNoteTemplate.create({
      data: {
        name, normalizedName, color: safeText(dto.color) || null,
        sortOrder: Number(dto.sortOrder || 0),
        createdById: staff?.id || staff?.sub || null,
        createdByName: staff?.name || staff?.username || null,
      },
    });
  }

  async updateNoteTemplate(id: string, dto: any) {
    const current = await this.prisma.omniNoteTemplate.findUnique({ where: { id } });
    if (!current) throw new NotFoundException("Không tìm thấy mẫu ghi chú.");
    const name = dto.name === undefined ? current.name : safeText(dto.name);
    if (!name) throw new BadRequestException("Tên ghi chú trống.");
    const normalizedName = this.normalizeNoteTemplateName(name);
    const existed = await this.prisma.omniNoteTemplate.findFirst({ where: { normalizedName, id: { not: id } } });
    if (existed) throw new BadRequestException("Tên ghi chú này đã tồn tại.");
    return this.prisma.omniNoteTemplate.update({
      where: { id },
      data: {
        name, normalizedName,
        color: dto.color === undefined ? current.color : safeText(dto.color) || null,
        sortOrder: dto.sortOrder === undefined ? current.sortOrder : Number(dto.sortOrder || 0),
        isActive: dto.isActive === undefined ? current.isActive : Boolean(dto.isActive),
      },
    });
  }

  async deleteNoteTemplate(id: string) {
    return this.prisma.omniNoteTemplate.update({ where: { id }, data: { isActive: false } });
  }

  async createNote(id: string, dto: { note: string; templateId?: string }, staff?: any) {
    const note = safeText(dto.note);
    if (!note) throw new BadRequestException("Ghi chú trống.");

    let template: any = null;
    if (dto.templateId) {
      template = await this.prisma.omniNoteTemplate.findUnique({ where: { id: dto.templateId } });
      if (!template || !template.isActive) throw new BadRequestException("Mẫu ghi chú không còn hoạt động.");
    }

    const item = await this.prisma.omniConversationNote.create({
      data: {
        conversationId: id,
        templateId: template?.id || null,
        note,
        staffId: staff?.id || staff?.sub || null,
        staffName: staff?.name || staff?.username || null,
      },
    });

    this.realtime.emit({ type: "conversation.note_created", payload: item });
    return item;
  }

  async markRead(id: string) {
    const item = await this.prisma.omniConversation.update({
      where: { id },
      data: { unreadCount: 0 },
      include: { customer: true, page: true, tags: true },
    });

    this.realtime.emit({ type: "conversation.updated", payload: item });
    return item;
  }

  async markUnread(id: string) {
    const current = await this.prisma.omniConversation.findUnique({
      where: { id },
      select: { unreadCount: true },
    });

    if (!current) {
      throw new NotFoundException("Không tìm thấy hội thoại.");
    }

    const item = await this.prisma.omniConversation.update({
      where: { id },
      data: {
        unreadCount: Math.max(1, Number(current.unreadCount || 0)),
      },
      include: { customer: true, page: true, tags: true },
    });

    this.realtime.emit({ type: "conversation.updated", payload: item });
    return item;
  }

  async refreshConversationProfile(id: string) {
    const conversation = await this.prisma.omniConversation.findUnique({
      where: { id },
      include: { customer: true, page: true, tags: true },
    });

    if (!conversation) throw new NotFoundException("Không tìm thấy hội thoại.");

    const customer = await this.refreshCustomerProfileIfNeeded(
      conversation.customer,
    );

    const updated = { ...conversation, customer };
    this.realtime.emit({ type: "conversation.updated", payload: updated });
    return updated;
  }

  async refreshMissingCustomerProfiles(limit = 50) {
    const take = Math.min(Math.max(Number(limit || 50), 1), 100);
    const customers = await this.prisma.omniCustomer.findMany({
      where: {
        providerUserId: { not: null },
        OR: [
          { avatarUrl: null },
          { name: { startsWith: "Khách " } },
        ],
      },
      orderBy: { updatedAt: "desc" },
      take,
    });

    let refreshed = 0;
    let skipped = 0;

    for (const customer of customers) {
      try {
        const beforeName = customer.name;
        const beforeAvatar = customer.avatarUrl;
        const updated = await this.refreshCustomerProfileIfNeeded(customer);
        if (
          updated?.name !== beforeName ||
          updated?.avatarUrl !== beforeAvatar
        ) {
          refreshed += 1;
        } else {
          skipped += 1;
        }
      } catch (error: any) {
        skipped += 1;
        this.logger.warn(
          `[META_PROFILE_BACKFILL_SKIP] customer=${customer.id} | ${error?.message || error}`,
        );
      }
    }

    return { total: customers.length, refreshed, skipped };
  }

  async uploadAttachment(file: Express.Multer.File, staff?: any) {
    if (!file?.buffer?.length) {
      throw new BadRequestException("Thiếu file đính kèm.");
    }

    const maxBytes = 25 * 1024 * 1024;
    if (file.size > maxBytes) {
      throw new BadRequestException("File đính kèm tối đa 25MB.");
    }

    const mimeType = safeText(file.mimetype).toLowerCase();
    const isImage = mimeType.startsWith("image/");
    const resourceType = isImage ? "image" : "raw";

    const result: any = await new Promise((resolve, reject) => {
      const stream = cloudinary.uploader.upload_stream(
        {
          folder: "the1970/omni-inbox",
          resource_type: resourceType as any,
          use_filename: true,
          unique_filename: true,
          filename_override: safeText(file.originalname) || undefined,
        },
        (error, uploaded) => {
          if (uploaded) resolve(uploaded);
          else reject(error || new Error("Upload file thất bại."));
        },
      );
      Readable.from(file.buffer).pipe(stream);
    });

    return {
      success: true,
      url: safeText(result?.secure_url || result?.url),
      publicId: safeText(result?.public_id) || null,
      fileName: safeText(file.originalname) || null,
      mimeType: mimeType || null,
      type: isImage ? "image" : "file",
      size: Number(file.size || 0),
    };
  }

  async sendMessage(
    id: string,
    dto: { text: string; attachmentUrl?: string; attachmentType?: "image" | "file"; fileName?: string },
    staff?: any,
  ) {
    await this.assertCanAccessConversation(id, staff, true);
    const conversation = await this.prisma.omniConversation.findUnique({
      where: { id },
      include: { customer: true },
    });
    if (!conversation) throw new NotFoundException("Không tìm thấy hội thoại.");

    const text = safeText(dto.text);
    if (!text && !dto.attachmentUrl)
      throw new BadRequestException("Tin nhắn trống.");

    const now = new Date();
    const recipientPsid = safeText(conversation.customer?.providerUserId);
    const providerThreadId = safeText(conversation.providerThreadId);
    const isFacebookComment = providerThreadId.startsWith("FACEBOOK_COMMENT:");
    let metaProviderMessageId: string | null = null;

    if (conversation.channel === "FACEBOOK") {
      if (isFacebookComment) {
        if (!text)
          throw new BadRequestException("Nội dung trả lời bình luận trống.");
        if (safeText(dto.attachmentUrl))
          throw new BadRequestException("Trả lời bình luận công khai hiện chỉ hỗ trợ nội dung chữ.");

        const parts = providerThreadId.split(":");
        const commentId = safeText(parts.slice(3).join(":"));
        if (!commentId)
          throw new BadRequestException("Không xác định được ID bình luận Facebook.");

        this.logger.log(
          `[META_COMMENT_REPLY] conversation=${id} comment=${commentId} text="${text.slice(0, 160)}"`,
        );

        try {
          const metaResult: any = await this.metaFormPost(`${commentId}/comments`, {
            message: text,
          });
          metaProviderMessageId =
            safeText(metaResult?.id || metaResult?.comment_id || metaResult?.message_id) || null;
          this.logger.log(
            `[META_COMMENT_REPLY_OK] conversation=${id} comment=${commentId} result=${JSON.stringify(metaResult)}`,
          );
        } catch (error: any) {
          this.logger.error(
            `[META_COMMENT_REPLY_FAILED] conversation=${id} comment=${commentId} error=${error?.message || error}`,
          );
          throw error;
        }
      } else {
        if (!recipientPsid)
          throw new BadRequestException("Hội thoại chưa có PSID khách Facebook.");

        const attachmentUrl = safeText(dto.attachmentUrl);
        const attachmentType = dto.attachmentType === "file" ? "file" : "image";
        const fileName = safeText(dto.fileName);
        const metaMessage = attachmentUrl
          ? {
              attachment: {
                type: attachmentType,
                payload: {
                  url: attachmentUrl,
                  is_reusable: true,
                },
              },
            }
          : { text };

        this.logger.log(
          `[META_SEND] conversation=${id} psid=${last6(recipientPsid)} type=${attachmentUrl ? attachmentType.toUpperCase() : "TEXT"} ${attachmentUrl ? `url="${attachmentUrl.slice(0, 160)}"` : `text="${text.slice(0, 120)}"`}`,
        );

        const latestInbound = await this.prisma.omniMessage.findFirst({
          where: { conversationId: id, direction: "IN" },
          orderBy: { sentAt: "desc" },
          select: { sentAt: true },
        });
        const latestInboundAt = latestInbound?.sentAt
          ? new Date(latestInbound.sentAt).getTime()
          : 0;
        const inboundAgeMs = latestInboundAt > 0
          ? Math.max(0, now.getTime() - latestInboundAt)
          : 0;
        const standardWindowMs = 24 * 60 * 60 * 1000;
        const humanAgentWindowMs = 7 * 24 * 60 * 60 * 1000;
        const shouldUseHumanAgent =
          latestInboundAt > 0 &&
          inboundAgeMs > standardWindowMs &&
          inboundAgeMs <= humanAgentWindowMs;

        try {
          const metaResult: any = shouldUseHumanAgent
            ? await this.sendMetaHumanAgentMessage(recipientPsid, metaMessage)
            : await this.metaPost("me/messages", {
                recipient: { id: recipientPsid },
                messaging_type: "RESPONSE",
                message: metaMessage,
              });
          metaProviderMessageId =
            safeText(metaResult?.message_id || metaResult?.messageId) || null;

          this.logger.log(
            `[META_SEND_OK] conversation=${id} psid=${last6(recipientPsid)} mode=${shouldUseHumanAgent ? "HUMAN_AGENT" : "RESPONSE"} result=${JSON.stringify(metaResult)}`,
          );
        } catch (error: any) {
          this.logger.error(
            `[META_SEND_FAILED] conversation=${id} psid=${last6(recipientPsid)} error=${error?.message || error}`,
          );

          // Meta chỉ cho RESPONSE trong standard messaging window. Vì thao tác này
          // được thực hiện trực tiếp bởi nhân viên trong Omni Inbox, nếu Meta báo
          // đã ra ngoài window thì thử lại bằng HUMAN_AGENT (tối đa 7 ngày kể từ
          // tin nhắn gần nhất của khách, và app phải được Meta cấp Human Agent).
          if (
            !shouldUseHumanAgent &&
            this.isMetaOutsideStandardMessagingWindow(error)
          ) {
            const insideHumanAgentWindow =
              latestInboundAt > 0 && inboundAgeMs <= humanAgentWindowMs;

            if (!insideHumanAgentWindow) {
              throw new BadRequestException(
                "Meta đã đóng cửa sổ trả lời. HUMAN_AGENT chỉ dùng được trong 7 ngày kể từ tin nhắn gần nhất của khách.",
              );
            }

            this.logger.warn(
              `[META_SEND_HUMAN_AGENT_RETRY] conversation=${id} psid=${last6(recipientPsid)} latestInbound=${latestInbound?.sentAt?.toISOString?.() || "-"}`,
            );

            try {
              const humanResult: any = await this.sendMetaHumanAgentMessage(
                recipientPsid,
                metaMessage,
              );
              metaProviderMessageId =
                safeText(humanResult?.message_id || humanResult?.messageId) || null;

              this.logger.log(
                `[META_SEND_HUMAN_AGENT_OK] conversation=${id} psid=${last6(recipientPsid)} result=${JSON.stringify(humanResult)}`,
              );
            } catch (humanError: any) {
              this.logger.error(
                `[META_SEND_HUMAN_AGENT_FAILED] conversation=${id} psid=${last6(recipientPsid)} error=${humanError?.message || humanError}`,
              );
              throw new BadRequestException(
                `Meta không gửi được bằng HUMAN_AGENT. Kiểm tra Human Agent feature/Advanced Access của app. ${safeText(humanError?.message)}`.trim(),
              );
            }
          } else if (shouldUseHumanAgent) {
            throw new BadRequestException(
              `Meta không gửi được bằng HUMAN_AGENT. Kiểm tra Human Agent feature/Advanced Access của app. ${safeText(error?.message)}`.trim(),
            );
          } else {
            throw error;
          }
        }
      }
    }

    const actualSenderId = staff?.id || staff?.sub || null;
    const actualSenderName = staff?.name || staff?.username || "Admin";
    const messageData: any = {
      conversationId: id,
      providerMessageId:
        conversation.channel === "FACEBOOK"
          ? metaProviderMessageId
          : null,
      direction: "OUT",
      type: safeText(dto.attachmentUrl)
        ? dto.attachmentType === "file"
          ? "FILE"
          : "IMAGE"
        : "TEXT",
      text: safeText(dto.attachmentUrl) && dto.attachmentType === "file"
        ? safeText(dto.fileName) || text || "Tệp đính kèm"
        : text,
      attachmentUrl: safeText(dto.attachmentUrl) || null,
      senderId: actualSenderId,
      senderName: actualSenderName,
      sentAt: now,
    };

    // Echo của Meta có thể tới trước response POST /messages và tạo row OUT với
    // senderName=The 1970. Khi API gửi xong, overwrite row đó bằng đúng nhân viên
    // đang đăng nhập thay vì để tên Page cố định hoặc vướng unique providerMessageId.
    let message: any;
    if (conversation.channel === "FACEBOOK" && metaProviderMessageId) {
      const echoed = await this.prisma.omniMessage.findUnique({
        where: { providerMessageId: metaProviderMessageId },
        select: { id: true },
      });
      message = echoed
        ? await this.prisma.omniMessage.update({
            where: { id: echoed.id },
            data: messageData,
          })
        : await this.prisma.omniMessage.create({ data: messageData });
    } else {
      message = await this.prisma.omniMessage.create({ data: messageData });
    }

    const updated = await this.prisma.omniConversation.update({
      where: { id },
      data: {
        lastMessageText: isFacebookComment
          ? `[Trả lời bình luận] ${text}`
          : safeText(dto.attachmentUrl)
            ? dto.attachmentType === "file"
              ? `[Tệp] ${safeText(dto.fileName) || "Đính kèm"}`
              : "[Ảnh]"
            : text,
        lastMessageAt: now,
        status:
          conversation.status === "OPEN" ? "PROCESSING" : conversation.status,
      },
      include: { customer: true, page: true, tags: true },
    });

    this.realtime.emit({ type: "message.created", payload: message });
    this.realtime.emit({ type: "conversation.updated", payload: updated });

    return message;
  }

  async createQuickOrder(conversationId: string, dto: any, staff?: any) {
    const conversation = await this.prisma.omniConversation.findUnique({
      where: { id: conversationId },
      include: { customer: true },
    });
    if (!conversation) throw new NotFoundException("Không tìm thấy hội thoại.");

    const requestId = safeText(dto.requestId);
    if (requestId) {
      const existed = await this.prisma.order.findUnique({ where: { quickOrderRequestId: requestId }, include: { items: true } });
      if (existed) return existed;
    }

    const currentDraft = await this.prisma.order.findFirst({
      where: { omniConversationId: conversationId, source: "OMNI_INBOX_QUICK_ORDER", status: "NEW" },
      include: { items: true },
      orderBy: { createdAt: "desc" },
    });
    if (currentDraft) throw new BadRequestException(`Hội thoại đã có đơn nháp ${currentDraft.orderCode}. Hãy sửa đơn hiện có.`);

    const phone = safeText(dto.phone).replace(/\D/g, "");
    if (!phone) throw new BadRequestException("Thiếu số điện thoại khách hàng.");
    const customerName =
      safeText(dto.customerName) ||
      conversation.customer?.name ||
      "Khách hàng";

    const addressLine1 = safeText(dto.addressLine1) || safeText(dto.address);
    const addressLine2 = safeText(dto.addressLine2);
    const province = safeText(dto.province);
    const district = safeText(dto.district);
    const ward = safeText(dto.ward);
    const postalCode = safeText(dto.postalCode);
    const ghnDistrictId = Number(dto.ghnDistrictId || 0) || undefined;
    const ghnWardCode = safeText(dto.ghnWardCode) || undefined;

    if (!addressLine1)
      throw new BadRequestException("Thiếu địa chỉ giao hàng.");

    const fullAddress = [
      addressLine1,
      addressLine2,
      ward,
      district,
      province,
    ]
      .filter(Boolean)
      .join(", ");

    let order: any = await this.orderService.createOrder(
      {
        customerName,
        customerPhone: phone,
        branchId: dto.branchId,

        // Bắt buộc coi đơn nhanh là đơn giao hàng Facebook,
        // không được rơi vào flow bán tại quầy / nhận tại cửa hàng.
        salesChannel: "FACEBOOK_MANUAL",
        isPosSale: false,
        deliveryMethod: "DELIVERY",
        shippingMethod: "GHN",
        fulfillmentType: "DELIVERY",
        shippingPartner: "ghn",

        shippingFee: 30000,
        note:
          safeText(dto.note) ||
          `Đơn chốt nhanh từ hội thoại ${conversationId}`,
        mode: "draft",
        source: "OMNI_INBOX_QUICK_ORDER",
        omniConversationId: conversationId,
        quickOrderRequestId: requestId || null,
        shippingSnapshot: {
          shippingRecipientName: customerName,
          shippingPhone: phone,
          shippingAddressLine1: addressLine1,
          shippingAddressLine2: addressLine2 || undefined,
          shippingCity: province || undefined,
          shippingProvince: province || undefined,
          shippingDistrict: district || undefined,
          shippingWard: ward || undefined,
          shippingPostalCode: postalCode || undefined,
          shippingPartner: "ghn",
          shippingMethod: "GHN",
          fulfillmentType: "DELIVERY",

          // Gửi cả hai bộ key để tương thích các phiên bản order.service.
          ghnDistrictId,
          ghnWardCode,
          shippingGhnDistrictId: ghnDistrictId,
          shippingGhnWardCode: ghnWardCode,

          skipAutoShipment: true,
        },
        items: dto.items,
      },
      staff,
    );

    // Bảo đảm đơn nháp luôn có địa chỉ dạng cấu trúc.
    // Không phụ thuộc việc order.service đang đọc key snapshot theo phiên bản nào.
    order = await this.prisma.order.update({
      where: { id: order.id },
      data: {
        // Ép lưu liên kết để các đơn tạo từ thời điểm này có thể tìm ngược về hội thoại.
        omniConversationId: conversationId,
        customerPhone: phone,
        shippingRecipientName: customerName,
        shippingPhone: phone,
        shippingAddressLine1: addressLine1,
        shippingAddressLine2: addressLine2 || null,
        shippingCity: province || null,
        shippingProvince: province || null,
        shippingDistrict: district || null,
        shippingWard: ward || null,
        shippingPostalCode: postalCode || null,
        shippingGhnDistrictId: ghnDistrictId || null,
        shippingGhnWardCode: ghnWardCode || null,
      },
      include: { items: true },
    });

    const persistedOrder = await this.prisma.order.findUnique({
      where: { id: order.id },
      select: {
        id: true,
        shippingProvince: true,
        shippingDistrict: true,
        shippingWard: true,
        shippingGhnDistrictId: true,
        shippingGhnWardCode: true,
      },
    });

    if (
      !persistedOrder?.shippingProvince ||
      !persistedOrder?.shippingDistrict ||
      !persistedOrder?.shippingWard
    ) {
      throw new BadRequestException(
        `Đơn đã tạo nhưng chưa lưu đủ địa chỉ cấu trúc: province="${persistedOrder?.shippingProvince || ""}", district="${persistedOrder?.shippingDistrict || ""}", ward="${persistedOrder?.shippingWard || ""}".`,
      );
    }

    await this.prisma.omniCustomer.updateMany({
      where: { id: conversation.customerId || "" },
      data: { phone, address: fullAddress || addressLine1 },
    });
    const note = await this.prisma.omniConversationNote.create({
      data: { conversationId, staffId: staff?.id || staff?.sub || null, staffName: staff?.name || staff?.username || null, note: `Đã tạo đơn nháp ${order.orderCode}.` },
    });
    this.realtime.emit({ type: "conversation.note_created", payload: note });
    this.realtime.emit({
      type: "conversation.quick_order_created",
      payload: order,
    } as any);
    return order;
  }

  async cancelQuickOrder(conversationId: string, orderId: string, staff?: any) {
    const order = await this.prisma.order.findFirst({ where: { id: orderId, omniConversationId: conversationId, source: "OMNI_INBOX_QUICK_ORDER" } });
    if (!order) throw new NotFoundException("Không tìm thấy đơn chốt nhanh.");
    const updated = await this.orderService.updateOrderStatus(orderId, "CANCELLED" as any, staff);
    this.realtime.emit({
      type: "conversation.quick_order_cancelled",
      payload: updated,
    } as any);
    return updated;
  }

  async deleteQuickOrder(conversationId: string, orderId: string, staff?: any) {
    const order = await this.prisma.order.findFirst({ where: { id: orderId, omniConversationId: conversationId, source: "OMNI_INBOX_QUICK_ORDER" } });
    if (!order) throw new NotFoundException("Không tìm thấy đơn chốt nhanh.");
    if (String(order.status) !== "NEW") throw new BadRequestException("Chỉ được xoá đơn nháp chưa duyệt.");
    const result = await this.orderService.deleteOrder(orderId, staff);
    this.realtime.emit({
      type: "conversation.quick_order_deleted",
      payload: { id: orderId, conversationId },
    } as any);
    return result;
  }

  private async findFacebookCommentConversation(
    pageId: string,
    commentId: string,
  ) {
    const normalizedPageId = safeText(pageId);
    const normalizedCommentId = safeText(commentId);
    if (!normalizedPageId || !normalizedCommentId) return null;

    const message = await this.prisma.omniMessage.findUnique({
      where: { providerMessageId: normalizedCommentId },
      include: {
        conversation: {
          include: { customer: true, page: true, tags: true },
        },
      },
    });
    if (
      message?.conversation &&
      safeText(message.conversation.providerThreadId).startsWith(
        `FACEBOOK_COMMENT:${normalizedPageId}:`,
      )
    ) {
      return message.conversation;
    }

    return this.prisma.omniConversation.findFirst({
      where: {
        providerThreadId: {
          startsWith: `FACEBOOK_COMMENT:${normalizedPageId}:`,
          endsWith: `:${normalizedCommentId}`,
        },
      },
      include: { customer: true, page: true, tags: true },
    });
  }

  private async resolveFacebookReplyParentId(
    replyCommentId: string,
    webhookParentId: string,
    postId: string,
  ) {
    const normalizedParentId = safeText(webhookParentId);
    const normalizedPostId = safeText(postId);

    if (
      normalizedParentId &&
      (!normalizedPostId || normalizedParentId !== normalizedPostId)
    ) {
      return normalizedParentId;
    }

    try {
      const detail: any = await this.metaFetch(replyCommentId, {
        fields: "id,parent{id}",
      });
      return safeText(detail?.parent?.id) || normalizedParentId;
    } catch (error: any) {
      this.logMetaDebug(
        `[META_COMMENT_PARENT_LOOKUP_SKIP] comment=${replyCommentId} | ${error?.message || error}`,
      );
      return normalizedParentId;
    }
  }

  private async ensureFacebookParentCommentConversation(params: {
    page: any;
    pageId: string;
    postId: string;
    parentCommentId: string;
    fallbackSentAt: Date;
  }) {
    const existing = await this.findFacebookCommentConversation(
      params.pageId,
      params.parentCommentId,
    );
    if (existing) return existing;

    let parent: any = null;
    try {
      parent = await this.metaFetch(params.parentCommentId, {
        fields: "id,message,from{id,name},created_time",
      });
    } catch (error: any) {
      this.logMetaDebug(
        `[META_COMMENT_PARENT_FETCH_SKIP] comment=${params.parentCommentId} | ${error?.message || error}`,
      );
      return null;
    }

    const parentSenderId = safeText(parent?.from?.id);
    if (!parentSenderId || parentSenderId === params.pageId) return null;

    const parentText = safeText(parent?.message) || "[Bình luận]";
    const parentNameFromWebhook = safeText(parent?.from?.name);
    const profile = await this.getFacebookCommentProfile(
      parentSenderId,
      parentNameFromWebhook,
    );

    const existingCustomer = await this.prisma.omniCustomer.findUnique({
      where: { providerUserId: parentSenderId },
    });
    const nextCustomerName = profile.isFallback
      ? existingCustomer?.name || profile.name
      : profile.name;
    const nextAvatarUrl =
      profile.avatarUrl || existingCustomer?.avatarUrl || null;

    const customer = await this.prisma.omniCustomer.upsert({
      where: { providerUserId: parentSenderId },
      update: {
        name: nextCustomerName,
        avatarUrl: nextAvatarUrl,
      },
      create: {
        providerUserId: parentSenderId,
        name: nextCustomerName,
        avatarUrl: nextAvatarUrl,
      },
    });

    const parentSentAt = parent?.created_time
      ? new Date(parent.created_time)
      : params.fallbackSentAt;
    const validParentSentAt = Number.isNaN(parentSentAt.getTime())
      ? params.fallbackSentAt
      : parentSentAt;

    const providerThreadId = `FACEBOOK_COMMENT:${params.pageId}:${params.postId || "post"}:${params.parentCommentId}`;
    const conversation = await this.prisma.omniConversation.upsert({
      where: { providerThreadId },
      update: {
        pageId: params.page.id,
        customerId: customer.id,
      },
      create: {
        providerThreadId,
        channel: "FACEBOOK",
        pageId: params.page.id,
        customerId: customer.id,
        lastMessageText: `[Bình luận] ${parentText}`,
        lastMessageAt: validParentSentAt,
        unreadCount: 0,
        status: "PROCESSING",
      },
      include: { customer: true, page: true, tags: true },
    });

    const existedParentMessage = await this.prisma.omniMessage.findUnique({
      where: { providerMessageId: params.parentCommentId },
    });
    if (!existedParentMessage) {
      await this.prisma.omniMessage.create({
        data: {
          conversationId: conversation.id,
          providerMessageId: params.parentCommentId,
          direction: "IN",
          type: "TEXT",
          text: parentText,
          attachmentUrl: null,
          senderId: parentSenderId,
          senderName: customer.name,
          sentAt: validParentSentAt,
        },
      });
    }

    return conversation;
  }

  async ingestMetaFeedChange(change: MetaFeedChange, entry?: any) {
    const field = safeText(change?.field);
    const value = change?.value || {};
    const item = safeText(value?.item);
    const verb = safeText(value?.verb);

    if (field !== "feed") return { skipped: true, reason: "not_feed_change" };
    if (item !== "comment")
      return { skipped: true, reason: `not_comment_${item || "unknown"}` };
    if (verb && !["add", "edited"].includes(verb)) {
      return { skipped: true, reason: `comment_${verb}` };
    }

    const pageId =
      safeText(value?.recipient_id) ||
      safeText(value?.page_id) ||
      safeText(entry?.id) ||
      this.configuredPageId;
    const rawPostId = safeText(value?.post_id);
    const webhookParentId =
      safeText(value?.parent_id) ||
      safeText(value?.parent?.id) ||
      safeText(value?.comment?.parent?.id);
    const commentId =
      safeText(value?.comment_id) ||
      safeText(value?.id) ||
      safeText(value?.comment?.id);
    // Với comment gốc, một số payload chỉ có parent_id là post id.
    // Giữ fallback cũ để không làm mất Nguồn bình luận. Với reply của Page,
    // parent_id vẫn được xử lý riêng ở webhookParentId phía dưới.
    const postId = rawPostId ||
      (webhookParentId && webhookParentId !== commentId ? webhookParentId : "");
    const senderId =
      safeText(value?.from?.id) ||
      safeText(value?.sender_id) ||
      safeText(value?.user_id);
    const senderNameFromWebhook =
      safeText(value?.from?.name) || safeText(value?.sender_name);
    const text = safeText(value?.message) || safeText(value?.comment?.message);
    const attachmentUrl =
      safeText(value?.photo) ||
      safeText(value?.photo_url) ||
      safeText(value?.attachment?.media?.image?.src) ||
      safeText(value?.attachment?.url);
    const createdTime = Number(
      value?.created_time || value?.timestamp || Date.now(),
    );
    const sentAt = new Date(
      createdTime > 10_000_000_000 ? createdTime : createdTime * 1000,
    );

    if (!pageId || !senderId || !commentId) {
      this.logMetaDebug(
        `[META_FEED_COMMENT_SKIP] missing_required page=${pageId || "-"} sender=${senderId ? last6(senderId) : "-"} comment=${commentId || "-"}`,
      );
      return { skipped: true, reason: "missing_page_sender_or_comment" };
    }

    if (!text && !attachmentUrl) {
      return { skipped: true, reason: "empty_comment" };
    }

    // Meta dùng comment id làm object id. Dedupe ở đây xử lý cả reply gửi từ Omni:
    // sendMessage() đã lưu providerMessageId trước khi webhook feed quay về.
    const existed = await this.prisma.omniMessage.findUnique({
      where: { providerMessageId: commentId },
    });
    if (existed) return { duplicated: true };

    const page = await this.prisma.omniInboxPage.upsert({
      where: { providerPageId: pageId },
      update: {
        lastWebhookAt: new Date(),
        pageName:
          pageId === this.configuredPageId ? "The 1970" : `Page ${pageId}`,
      },
      create: {
        providerPageId: pageId,
        pageName:
          pageId === this.configuredPageId ? "The 1970" : `Page ${pageId}`,
        channel: "FACEBOOK",
        lastWebhookAt: new Date(),
      },
    });

    const isPageAuthored =
      senderId === pageId ||
      (Boolean(this.configuredPageId) && senderId === this.configuredPageId);

    if (isPageAuthored) {
      // Reply được gửi từ Facebook/Meta Business Suite/Pancake phải quay về
      // đúng thread comment của khách và được lưu là OUT, không tạo một
      // "khách hàng The 1970" / conversation giả.
      const parentCommentId = await this.resolveFacebookReplyParentId(
        commentId,
        webhookParentId,
        postId,
      );

      if (!parentCommentId || parentCommentId === postId) {
        this.logger.warn(
          `[META_FEED_PAGE_COMMENT_SKIP] Không xác định được parent comment. page=${pageId} post=${postId || "-"} comment=${commentId}`,
        );
        return { skipped: true, reason: "page_comment_without_customer_parent" };
      }

      let conversation = await this.findFacebookCommentConversation(
        pageId,
        parentCommentId,
      );

      if (!conversation) {
        conversation = await this.ensureFacebookParentCommentConversation({
          page,
          pageId,
          postId,
          parentCommentId,
          fallbackSentAt: sentAt,
        });
      }

      if (!conversation) {
        this.logger.warn(
          `[META_FEED_PAGE_REPLY_ORPHAN] page=${pageId} post=${postId || "-"} parent=${parentCommentId} comment=${commentId}`,
        );
        return { skipped: true, reason: "page_reply_parent_conversation_missing" };
      }

      const messageText = text || "[Bình luận có tệp đính kèm]";
      const message = await this.prisma.omniMessage.create({
        data: {
          conversationId: conversation.id,
          providerMessageId: commentId,
          direction: "OUT",
          type: attachmentUrl ? "IMAGE" : "TEXT",
          text: messageText,
          attachmentUrl: attachmentUrl || null,
          senderId: pageId,
          senderName: page.pageName || "The 1970",
          sentAt,
        },
      });

      const updated = await this.prisma.omniConversation.update({
        where: { id: conversation.id },
        data: {
          lastMessageText: `[Trả lời bình luận] ${messageText}`,
          lastMessageAt: sentAt,
          status:
            conversation.status === "OPEN"
              ? "PROCESSING"
              : conversation.status,
        },
        include: { customer: true, page: true, tags: true },
      });

      this.logger.log(
        `[META_FEED_COMMENT_OUT] page=${pageId} post=${postId || "-"} parent=${parentCommentId} comment=${commentId} text="${messageText.slice(0, 80)}"`,
      );
      this.realtime.emit({ type: "message.created", payload: message });
      this.realtime.emit({ type: "conversation.updated", payload: updated });

      return { ok: true, direction: "OUT", conversationId: conversation.id };
    }

    // Comment của khách: tạo/đưa vào thread riêng của chính comment gốc.
    const profile = await this.getFacebookCommentProfile(
      senderId,
      senderNameFromWebhook,
    );

    const existingCustomer = await this.prisma.omniCustomer.findUnique({
      where: { providerUserId: senderId },
    });

    const fallbackCustomerName =
      safeText(existingCustomer?.name) || `Khách ${last6(senderId)}`;
    const customer = await this.prisma.omniCustomer.upsert({
      where: { providerUserId: senderId },
      update: {},
      create: {
        providerUserId: senderId,
        name: fallbackCustomerName,
        avatarUrl: existingCustomer?.avatarUrl || null,
      },
    });

    const providerThreadId = `FACEBOOK_COMMENT:${pageId}:${postId || "post"}:${commentId}`;
    const messageText = text || "[Bình luận có tệp đính kèm]";
    const lastMessageText = `[Bình luận] ${messageText}`;

    const conversation = await this.prisma.omniConversation.upsert({
      where: { providerThreadId },
      update: {
        pageId: page.id,
        customerId: customer.id,
        lastMessageText,
        lastMessageAt: sentAt,
        unreadCount: { increment: 1 },
        status: "OPEN",
      },
      create: {
        providerThreadId,
        channel: "FACEBOOK",
        pageId: page.id,
        customerId: customer.id,
        lastMessageText,
        lastMessageAt: sentAt,
        unreadCount: 1,
        status: "OPEN",
      },
      include: { customer: true, page: true, tags: true },
    });

    const message = await this.prisma.omniMessage.create({
      data: {
        conversationId: conversation.id,
        providerMessageId: commentId,
        direction: "IN",
        type: attachmentUrl ? "IMAGE" : "TEXT",
        text: messageText,
        attachmentUrl: attachmentUrl || null,
        senderId,
        senderName: customer.name,
        sentAt,
      },
    });

    this.logger.log(
      `[META_FEED_COMMENT_IN] page=${pageId} post=${postId || "-"} comment=${commentId} sender=${last6(senderId)} customer="${customer.name}" text="${messageText.slice(0, 80)}"`,
    );

    this.realtime.emit({ type: "message.created", payload: message });
    this.realtime.emit({ type: "conversation.updated", payload: conversation });
    await this.autoAssignConversation(conversation.id, "INCOMING_MESSAGE");

    return { ok: true, direction: "IN" };
  }

  async ingestMetaWebhookEvent(event: any) {
    const senderId = safeText(event?.sender?.id);
    const recipientId = safeText(event?.recipient?.id);
    const messageId = safeText(event?.message?.mid);
    const text = safeText(event?.message?.text);
    const timestamp = Number(event?.timestamp || Date.now());
    const attachments = Array.isArray(event?.message?.attachments)
      ? event.message.attachments.filter((item: any) => safeText(item?.payload?.url))
      : [];
    const attachment = attachments[0];
    const adReferral = this.extractMetaAdReferral(event);
    const adReferralData = this.buildAdReferralUpdate(adReferral, timestamp);
    if (!adReferral && (event?.referral || event?.messaging_referral || event?.message?.referral)) {
      this.logger.warn(`[META_AD_REFERRAL_UNPARSED] ${JSON.stringify({ referral: event?.referral, messaging_referral: event?.messaging_referral, message_referral: event?.message?.referral })}`);
    }

    if (!senderId || !recipientId)
      return { skipped: true, reason: "missing_sender_or_recipient" };

    if (event?.message?.is_echo) {
      const pageId = senderId;
      const customerPsid = recipientId;
      const providerThreadId = `FACEBOOK:${pageId}:${customerPsid}`;
      const sentAt = new Date(timestamp);
      const messageText = text ||
        (attachments.length > 1
          ? `[${attachments.length} ảnh đính kèm]`
          : attachments.length === 1
            ? "[Ảnh đính kèm]"
            : "[Tệp đính kèm]");

      if (messageId) {
        const existedEcho = await this.prisma.omniMessage.findUnique({
          where: { providerMessageId: messageId },
        });
        if (existedEcho) return { duplicated: true, echo: true };
      }

      const page = await this.prisma.omniInboxPage.upsert({
        where: { providerPageId: pageId },
        update: {
          lastWebhookAt: new Date(),
          pageName:
            pageId === this.configuredPageId
              ? "The 1970"
              : `Page ${pageId}`,
        },
        create: {
          providerPageId: pageId,
          pageName:
            pageId === this.configuredPageId
              ? "The 1970"
              : `Page ${pageId}`,
          channel: "FACEBOOK",
          lastWebhookAt: new Date(),
        },
      });

      const existingCustomer = await this.prisma.omniCustomer.findUnique({
        where: { providerUserId: customerPsid },
      });
      const profile = existingCustomer
        ? null
        : await this.getMessengerProfile(customerPsid);
      const customer = await this.prisma.omniCustomer.upsert({
        where: { providerUserId: customerPsid },
        update: {},
        create: {
          providerUserId: customerPsid,
          name: profile?.name || `Khách ${last6(customerPsid)}`,
          avatarUrl: profile?.avatarUrl || null,
        },
      });

      const conversation = await this.prisma.omniConversation.upsert({
        where: { providerThreadId },
        update: {
          pageId: page.id,
          customerId: customer.id,
          lastMessageText: messageText,
          lastMessageAt: sentAt,
        },
        create: {
          providerThreadId,
          channel: "FACEBOOK",
          pageId: page.id,
          customerId: customer.id,
          lastMessageText: messageText,
          lastMessageAt: sentAt,
          unreadCount: 0,
          status: "PROCESSING",
        },
        include: { customer: true, page: true, tags: true },
      });

      const createdMessages: any[] = [];

      if (text) {
        createdMessages.push(
          await this.prisma.omniMessage.create({
            data: {
              conversationId: conversation.id,
              providerMessageId: messageId || null,
              direction: "OUT",
              type: "TEXT",
              text,
              attachmentUrl: null,
              senderId: pageId,
              senderName: page.pageName || "The 1970",
              sentAt,
            },
          }),
        );
      }

      for (let index = 0; index < attachments.length; index += 1) {
        const item = attachments[index];
        createdMessages.push(
          await this.prisma.omniMessage.create({
            data: {
              conversationId: conversation.id,
              providerMessageId: messageId
                ? text || index > 0
                  ? `${messageId}:attachment:${index}`
                  : messageId
                : null,
              direction: "OUT",
              type: "IMAGE",
              text: null,
              attachmentUrl: safeText(item?.payload?.url) || null,
              senderId: pageId,
              senderName: page.pageName || "The 1970",
              sentAt: new Date(sentAt.getTime() + index + (text ? 1 : 0)),
            },
          }),
        );
      }

      // Trường hợp event không có text/attachment nhưng vẫn lọt vào echo.
      if (!createdMessages.length) {
        createdMessages.push(
          await this.prisma.omniMessage.create({
            data: {
              conversationId: conversation.id,
              providerMessageId: messageId || null,
              direction: "OUT",
              type: "TEXT",
              text: messageText,
              attachmentUrl: null,
              senderId: pageId,
              senderName: page.pageName || "The 1970",
              sentAt,
            },
          }),
        );
      }

      this.logMetaDebug(
        `[META_WEBHOOK_ECHO] page=${pageId} recipient=${last6(customerPsid)} text="${messageText.slice(0, 80)}" attachments=${attachments.length}`,
      );
      createdMessages.forEach((message) =>
        this.realtime.emit({ type: "message.created", payload: message }),
      );
      this.realtime.emit({ type: "conversation.updated", payload: conversation });
      return { ok: true, echo: true };
    }

    if (
      event?.delivery ||
      event?.read ||
      event?.reaction ||
      (event?.postback && !adReferral)
    ) {
      this.logMetaDebug(
        `[META_WEBHOOK_EVENT] non-message event | sender=${last6(senderId)} recipient=${last6(recipientId)}`,
      );
      return { skipped: true, reason: "non_message_event" };
    }

    if (!text && !event?.message?.attachments?.length && !adReferral) {
      return { skipped: true, reason: "empty_message" };
    }

    if (this.configuredPageId && recipientId !== this.configuredPageId) {
      this.logger.warn(
        `[META_WEBHOOK_PAGE_MISMATCH] expected=${this.configuredPageId} actual=${recipientId} sender=${last6(senderId)}`,
      );
    }

    if (messageId) {
      const existed = await this.prisma.omniMessage.findUnique({
        where: { providerMessageId: messageId },
      });
      if (existed) return { duplicated: true };
    }

    // Webhook phải ghi tin vào DB/SSE trước, không chờ Graph profile.
    // Profile sẽ refresh nền sau để tin mới xuất hiện gần như ngay lập tức.
    const page = await this.prisma.omniInboxPage.upsert({
      where: { providerPageId: recipientId },
      update: {
        lastWebhookAt: new Date(),
        pageName:
          recipientId === this.configuredPageId
            ? "The 1970"
            : `Page ${recipientId}`,
      },
      create: {
        providerPageId: recipientId,
        pageName:
          recipientId === this.configuredPageId
            ? "The 1970"
            : `Page ${recipientId}`,
        channel: "FACEBOOK",
        lastWebhookAt: new Date(),
      },
    });

    const existingCustomer = await this.prisma.omniCustomer.findUnique({
      where: { providerUserId: senderId },
    });

    // Messenger inbound: không chờ Graph profile ở webhook. Dùng dữ liệu DB hiện có
    // hoặc tên tạm; sau khi emit realtime sẽ refresh tên/avatar ở background.
    const nextCustomerName =
      safeText(existingCustomer?.name) || `Khách ${last6(senderId)}`;
    const nextAvatarUrl = existingCustomer?.avatarUrl || null;

    const customer = await this.prisma.omniCustomer.upsert({
      where: { providerUserId: senderId },
      update: {
        name: nextCustomerName,
        avatarUrl: nextAvatarUrl,
      },
      create: {
        providerUserId: senderId,
        name: nextCustomerName,
        avatarUrl: nextAvatarUrl,
      },
    });

    const providerThreadId = `FACEBOOK:${recipientId}:${senderId}`;
    const sentAt = new Date(timestamp);
    const hasMessage = Boolean(
      text || event?.message?.attachments?.length,
    );
    const messageText = text ||
      (attachments.length > 1
        ? `[${attachments.length} ảnh đính kèm]`
        : attachments.length === 1
          ? "[Ảnh đính kèm]"
          : "");

    const updateData: any = {
      pageId: page.id,
      customerId: customer.id,
      ...adReferralData,
    };
    if (hasMessage) {
      updateData.lastMessageText = messageText;
      updateData.lastMessageAt = sentAt;
      updateData.unreadCount = { increment: 1 };
      updateData.status = "OPEN";
    }

    const createData: any = {
      providerThreadId,
      channel: "FACEBOOK",
      pageId: page.id,
      customerId: customer.id,
      unreadCount: hasMessage ? 1 : 0,
      status: hasMessage ? "OPEN" : "PENDING",
      ...adReferralData,
    };
    if (hasMessage) {
      createData.lastMessageText = messageText;
      createData.lastMessageAt = sentAt;
    }

    const conversation = await this.prisma.omniConversation.upsert({
      where: { providerThreadId },
      update: updateData,
      create: createData,
      include: { customer: true, page: true, tags: true },
    });

    const createdMessages: any[] = [];
    if (hasMessage) {
      if (text) {
        createdMessages.push(
          await this.prisma.omniMessage.create({
            data: {
              conversationId: conversation.id,
              providerMessageId: messageId || null,
              direction: "IN",
              type: "TEXT",
              text,
              attachmentUrl: null,
              senderId,
              senderName: customer.name,
              sentAt,
            },
          }),
        );
      }

      for (let index = 0; index < attachments.length; index += 1) {
        const item = attachments[index];
        createdMessages.push(
          await this.prisma.omniMessage.create({
            data: {
              conversationId: conversation.id,
              providerMessageId: messageId
                ? text || index > 0
                  ? `${messageId}:attachment:${index}`
                  : messageId
                : null,
              direction: "IN",
              type: "IMAGE",
              text: null,
              attachmentUrl: safeText(item?.payload?.url) || null,
              senderId,
              senderName: customer.name,
              sentAt: new Date(sentAt.getTime() + index + (text ? 1 : 0)),
            },
          }),
        );
      }
    }

    // Khách vừa gửi tin mới: chỉ backfill lịch sử của CHÍNH khách này.
    // Chạy nền để webhook trả 200 nhanh; getConversation() sẽ await cùng job nếu
    // nhân viên mở thread ngay lập tức. Không có vòng lặp toàn bộ conversations Page.
    if (hasMessage) {
      void this.backfillMessengerHistoryForCustomer({
        pageId: recipientId,
        customerPsid: senderId,
        conversationId: conversation.id,
        customerName: customer.name,
      });
    }

    if (adReferral) {
      this.logger.log(
        `[META_AD_REFERRAL] conversation=${conversation.id} ad=${adReferral.adId || "-"} post=${adReferral.postId || "-"} source=${adReferral.source || "-"}`,
      );
    }

    this.logMetaDebug(
      `[META_WEBHOOK_MESSAGE] page=${recipientId} sender=${last6(senderId)} customer="${customer.name}" text="${messageText.slice(0, 80)}"`,
    );

    createdMessages.forEach((message) =>
      this.realtime.emit({ type: "message.created", payload: message }),
    );
    this.realtime.emit({
      type: "conversation.updated",
      payload: conversation,
    });

    // Refresh tên/avatar nền; không làm chậm việc hiện tin mới.
    void this.refreshCustomerProfileIfNeeded(customer)
      .then(async (refreshed: any) => {
        if (!refreshed) return;
        const refreshedConversation = await this.prisma.omniConversation.findUnique({
          where: { id: conversation.id },
          include: { customer: true, page: true, tags: true },
        });
        if (refreshedConversation) {
          this.realtime.emit({ type: "conversation.updated", payload: refreshedConversation });
        }
      })
      .catch((error: any) =>
        this.logMetaDebug(`[META_PROFILE_ASYNC_SKIP] psid=${last6(senderId)} | ${error?.message || error}`),
      );

    if (hasMessage) {
      const assignedConversation = await this.autoAssignConversation(
        conversation.id,
        "INCOMING_MESSAGE",
      );
      if (!assignedConversation?.assigneeId) {
        this.logger.warn(
          `[OMNI_INCOMING_UNASSIGNED] conversation=${conversation.id} customer=${last6(senderId)}`,
        );
      }
    }

    return {
      ok: true,
      referral: Boolean(adReferral),
      message: hasMessage,
    };
  }
}
