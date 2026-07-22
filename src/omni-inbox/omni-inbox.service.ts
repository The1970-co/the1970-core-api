import {
  BadRequestException,
  Injectable,
  Logger,
  NotFoundException,
} from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";
import { OmniInboxRealtimeService } from "./omni-inbox.realtime";
import { ListConversationsDto } from "./dto/list-conversations.dto";

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
export class OmniInboxService {
  private readonly logger = new Logger(OmniInboxService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly realtime: OmniInboxRealtimeService,
  ) {}

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
    return (
      process.env.META_INBOX_VERBOSE_LOGS === "true" ||
      process.env.NODE_ENV !== "production"
    );
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
      "message_reads",
      "message_deliveries",
      "message_reactions",
      "messaging_postbacks",
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
      this.logMetaDebug(
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
      this.logger.error(
        `[META_PROFILE_ERROR] psid=${psid} | ${error?.message || error}`,
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

  private async refreshCustomerProfileIfNeeded(customer?: any | null) {
    if (!customer?.providerUserId) return customer;

    const needsRefresh =
      isFallbackCustomerName(customer.name) || !safeText(customer.avatarUrl);

    if (!needsRefresh) return customer;

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
      .slice(0, 50);

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

  async listConversations(query: ListConversationsDto) {
    const page = Number(query.page || 1);
    const limit = Math.min(Math.max(Number(query.limit || 30), 10), 100);
    const skip = (page - 1) * limit;

    const where: any = {};

    if (query.status && query.status !== "ALL") where.status = query.status;
    if (query.channel && query.channel !== "ALL") where.channel = query.channel;
    if (query.assigneeId) where.assigneeId = query.assigneeId;
    if (query.branchId) where.branchId = query.branchId;

    const q = safeText(query.q);
    if (q) {
      where.OR = [
        { lastMessageText: { contains: q, mode: "insensitive" } },
        { customer: { name: { contains: q, mode: "insensitive" } } },
        { customer: { phone: { contains: q, mode: "insensitive" } } },
      ];
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

  async getConversation(id: string) {
    const item = await this.prisma.omniConversation.findUnique({
      where: { id },
      include: {
        customer: true,
        page: true,
        tags: true,
        notes: { orderBy: { createdAt: "desc" }, take: 20 },
        messages: { orderBy: { sentAt: "asc" }, take: 100 },
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

    return item;
  }

  async assignConversation(
    id: string,
    dto: { assigneeId: string; assigneeName: string },
  ) {
    const item = await this.prisma.omniConversation.update({
      where: { id },
      data: {
        assigneeId: dto.assigneeId,
        assigneeName: dto.assigneeName,
        status: "PROCESSING",
      },
      include: { customer: true, page: true, tags: true },
    });

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

  async createNote(id: string, dto: { note: string }, staff?: any) {
    const note = safeText(dto.note);
    if (!note) throw new BadRequestException("Ghi chú trống.");

    const item = await this.prisma.omniConversationNote.create({
      data: {
        conversationId: id,
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

  async sendMessage(
    id: string,
    dto: { text: string; attachmentUrl?: string },
    staff?: any,
  ) {
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

    if (conversation.channel === "FACEBOOK") {
      if (!recipientPsid)
        throw new BadRequestException("Hội thoại chưa có PSID khách Facebook.");
      if (!text)
        throw new BadRequestException(
          "Hiện tại Messenger chỉ hỗ trợ gửi text trong màn này.",
        );

      this.logger.log(
        `[META_SEND] conversation=${id} psid=${last6(recipientPsid)} text="${text.slice(0, 120)}"`,
      );

      try {
        const metaResult = await this.metaPost("me/messages", {
          recipient: { id: recipientPsid },
          messaging_type: "RESPONSE",
          message: { text },
        });

        this.logger.log(
          `[META_SEND_OK] conversation=${id} psid=${last6(recipientPsid)} result=${JSON.stringify(metaResult)}`,
        );
      } catch (error: any) {
        this.logger.error(
          `[META_SEND_FAILED] conversation=${id} psid=${last6(recipientPsid)} error=${error?.message || error}`,
        );
        throw error;
      }
    }

    const message = await this.prisma.omniMessage.create({
      data: {
        conversationId: id,
        direction: "OUT",
        type: dto.attachmentUrl ? "IMAGE" : "TEXT",
        text,
        attachmentUrl: dto.attachmentUrl || null,
        senderId: staff?.id || staff?.sub || null,
        senderName: staff?.name || staff?.username || "Admin",
        sentAt: now,
      },
    });

    const updated = await this.prisma.omniConversation.update({
      where: { id },
      data: {
        lastMessageText: text || "[Ảnh]",
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

  async ingestMetaFeedChange(change: MetaFeedChange, entry?: any) {
    const field = safeText(change?.field);
    const value = change?.value || {};
    const item = safeText(value?.item);
    const verb = safeText(value?.verb);

    if (field !== "feed") return { skipped: true, reason: "not_feed_change" };
    if (item !== "comment") return { skipped: true, reason: `not_comment_${item || "unknown"}` };
    if (verb && !["add", "edited"].includes(verb)) {
      return { skipped: true, reason: `comment_${verb}` };
    }

    const pageId =
      safeText(value?.recipient_id) ||
      safeText(value?.page_id) ||
      safeText(entry?.id) ||
      this.configuredPageId;
    const postId = safeText(value?.post_id) || safeText(value?.parent_id);
    const commentId =
      safeText(value?.comment_id) ||
      safeText(value?.id) ||
      safeText(value?.comment?.id);
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
    const createdTime = Number(value?.created_time || value?.timestamp || Date.now());
    const sentAt = new Date(createdTime > 10_000_000_000 ? createdTime : createdTime * 1000);

    if (!pageId || !senderId || !commentId) {
      this.logMetaDebug(
        `[META_FEED_COMMENT_SKIP] missing_required page=${pageId || "-"} sender=${senderId ? last6(senderId) : "-"} comment=${commentId || "-"}`,
      );
      return { skipped: true, reason: "missing_page_sender_or_comment" };
    }

    if (!text && !attachmentUrl) {
      return { skipped: true, reason: "empty_comment" };
    }

    const existed = await this.prisma.omniMessage.findUnique({
      where: { providerMessageId: commentId },
    });
    if (existed) return { duplicated: true };

    const profile = await this.getFacebookCommentProfile(
      senderId,
      senderNameFromWebhook,
    );

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
      where: { providerUserId: senderId },
    });

    const nextCustomerName = profile.isFallback
      ? existingCustomer?.name || profile.name
      : profile.name;
    const nextAvatarUrl =
      profile.avatarUrl || existingCustomer?.avatarUrl || null;

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
      `[META_FEED_COMMENT] page=${pageId} post=${postId || "-"} comment=${commentId} sender=${last6(senderId)} customer="${customer.name}" text="${messageText.slice(0, 80)}"`,
    );

    this.realtime.emit({ type: "message.created", payload: message });
    this.realtime.emit({ type: "conversation.updated", payload: conversation });

    return { ok: true };
  }

  async ingestMetaWebhookEvent(event: any) {
    const senderId = safeText(event?.sender?.id);
    const recipientId = safeText(event?.recipient?.id);
    const messageId = safeText(event?.message?.mid);
    const text = safeText(event?.message?.text);
    const timestamp = Number(event?.timestamp || Date.now());
    const attachment = event?.message?.attachments?.[0];

    if (!senderId || !recipientId)
      return { skipped: true, reason: "missing_sender_or_recipient" };

    if (event?.message?.is_echo) {
      // Tin do Page/shop gửi từ Facebook, Meta Business Suite hoặc Pancake.
      // Với echo: sender là Page, recipient là khách.
      const pageId = senderId;
      const customerPsid = recipientId;
      const providerThreadId = `FACEBOOK:${pageId}:${customerPsid}`;
      const sentAt = new Date(timestamp);
      const messageText = text || "[Tệp đính kèm]";

      if (messageId) {
        const existed = await this.prisma.omniMessage.findUnique({
          where: { providerMessageId: messageId },
        });
        if (existed) return { duplicated: true, echo: true };
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

      let customer = await this.prisma.omniCustomer.findUnique({
        where: { providerUserId: customerPsid },
      });

      if (!customer) {
        const profile = await this.getMessengerProfile(customerPsid);
        customer = await this.prisma.omniCustomer.create({
          data: {
            providerUserId: customerPsid,
            name: profile.name,
            avatarUrl: profile.avatarUrl || null,
          },
        });
      }

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

      const message = await this.prisma.omniMessage.create({
        data: {
          conversationId: conversation.id,
          providerMessageId: messageId || null,
          direction: "OUT",
          type: attachment ? "IMAGE" : "TEXT",
          text,
          attachmentUrl: attachment?.payload?.url || null,
          senderId: pageId,
          senderName: page.pageName || "The 1970",
          sentAt,
        },
      });

      this.realtime.emit({ type: "message.created", payload: message });
      this.realtime.emit({
        type: "conversation.updated",
        payload: conversation,
      });

      return { ok: true, echo: true };
    }

    if (event?.delivery || event?.read || event?.reaction || event?.postback) {
      this.logMetaDebug(
        `[META_WEBHOOK_EVENT] non-message event | sender=${last6(senderId)} recipient=${last6(recipientId)}`,
      );
      return { skipped: true, reason: "non_message_event" };
    }

    if (!text && !event?.message?.attachments?.length) {
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

    const profile = await this.getMessengerProfile(senderId);

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

    const nextCustomerName = profile.isFallback
      ? existingCustomer?.name || profile.name
      : profile.name;
    const nextAvatarUrl =
      profile.avatarUrl || existingCustomer?.avatarUrl || null;

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
    const messageText = text || "[Tệp đính kèm]";

    const conversation = await this.prisma.omniConversation.upsert({
      where: { providerThreadId },
      update: {
        pageId: page.id,
        customerId: customer.id,
        lastMessageText: messageText,
        lastMessageAt: sentAt,
        unreadCount: { increment: 1 },
        status: "OPEN",
      },
      create: {
        providerThreadId,
        channel: "FACEBOOK",
        pageId: page.id,
        customerId: customer.id,
        lastMessageText: messageText,
        lastMessageAt: sentAt,
        unreadCount: 1,
        status: "OPEN",
      },
      include: { customer: true, page: true, tags: true },
    });

    const message = await this.prisma.omniMessage.create({
      data: {
        conversationId: conversation.id,
        providerMessageId: messageId || null,
        direction: "IN",
        type: attachment ? "IMAGE" : "TEXT",
        text,
        attachmentUrl: attachment?.payload?.url || null,
        senderId,
        senderName: customer.name,
        sentAt,
      },
    });

    this.logMetaDebug(
      `[META_WEBHOOK_MESSAGE] page=${recipientId} sender=${last6(senderId)} customer="${customer.name}" text="${messageText.slice(0, 80)}"`,
    );

    this.realtime.emit({ type: "message.created", payload: message });
    this.realtime.emit({ type: "conversation.updated", payload: conversation });

    return { ok: true };
  }
}
