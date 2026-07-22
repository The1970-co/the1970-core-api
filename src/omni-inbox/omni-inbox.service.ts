import {
  BadRequestException,
  Injectable,
  Logger,
  NotFoundException,
} from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";
import { OmniInboxRealtimeService } from "./omni-inbox.realtime";
import { ListConversationsDto } from "./dto/list-conversations.dto";
import { OrderService } from "../order/order.service";

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
  private lastStaleAssignmentSweepAt = 0;
  private readonly logger = new Logger(OmniInboxService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly realtime: OmniInboxRealtimeService,
    private readonly orderService: OrderService,
  ) {}

  private get pageAccessToken() {
    return safeText(process.env.META_INBOX_PAGE_ACCESS_TOKEN);
  }

  private get configuredPageId() {
    return safeText(process.env.META_INBOX_PAGE_ID);
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

    return this.getAssignmentSettings();
  }

  async listAssignmentHistory(limit = 100) {
    return (this.prisma as any).omniAssignmentHistory.findMany({
      orderBy: { createdAt: "desc" },
      take: Math.min(Math.max(Number(limit || 100), 10), 500),
    });
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
    const current = await (this.prisma as any).omniQuickReplyTemplate.findUnique({ where: { id } });
    if (!current) throw new NotFoundException("Không tìm thấy mẫu trả lời.");
    return (this.prisma as any).omniQuickReplyTemplate.update({ where: { id }, data: { isActive: false } });
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
      if (!setting.reassignIfAssigneeOffline) return conversation;
    }

    const draftOrder: any = await this.prisma.order.findFirst({
      where: { omniConversationId: conversationId, status: "NEW" as any },
      orderBy: { createdAt: "desc" },
      select: { assignedStaffId: true, assignedStaffName: true, createdByStaffId: true, createdByStaffName: true, branchId: true },
    });
    const draftOwnerId = safeText(draftOrder?.assignedStaffId || draftOrder?.createdByStaffId);
    const targetBranchId = safeText(conversation.branchId || draftOrder?.branchId || setting.fallbackBranchId);

    const staffIds = candidates.map((item: any) => item.staffId);
    const groupedLoads: any[] = await (this.prisma.omniConversation as any).groupBy({
      by: ["assigneeId"],
      where: { assigneeId: { in: staffIds }, status: { in: ["OPEN", "PROCESSING", "PENDING"] as any } },
      _count: { _all: true },
    });
    const unreadLoads: any[] = await (this.prisma.omniConversation as any).groupBy({
      by: ["assigneeId"],
      where: { assigneeId: { in: staffIds }, unreadCount: { gt: 0 }, status: { in: ["OPEN", "PROCESSING", "PENDING"] as any } },
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
        if (!online.length) {
          if (setting.noCandidateMode === "ASSIGN_ANYWAY") continue;
          await (this.prisma as any).omniAssignmentHistory.create({
            data: {
              conversationId,
              customerName: conversation.customer?.name || null,
              channel: conversation.channel,
              branchId: targetBranchId || null,
              action: "NO_CANDIDATE",
              reason: "Không có nhân viên online đủ điều kiện.",
              decisionDetail: decision,
              triggerType,
            },
          });
          return null;
        }
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
    let selected = candidates[0];
    if (candidates.length > 1) {
      const lastIndex = candidates.findIndex((item: any) => item.staffId === setting.lastAssignedStaffId);
      selected = candidates[(lastIndex + 1 + candidates.length) % candidates.length];
    }

    const previousStaffId = conversation.assigneeId;
    const updated = await this.prisma.$transaction(async (tx: any) => {
      const row = await tx.omniConversation.update({
        where: { id: conversationId },
        data: { assigneeId: selected.staffId, assigneeName: selected.staffName, status: "PROCESSING" },
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
          decisionDetail: { ...decision, selected: selected.staffId, activeLoad: Number(activeMap.get(selected.staffId) || 0), unreadLoad: Number(unreadMap.get(selected.staffId) || 0) },
          triggerType,
        },
      });
      return row;
    });
    this.realtime.emit({ type: "conversation.assigned", payload: updated });
    return updated;
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
      this.logMetaDebug(
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

  async listConversations(query: ListConversationsDto, staff?: any) {
    const page = Number(query.page || 1);
    const limit = Math.min(Math.max(Number(query.limit || 30), 10), 100);
    const skip = (page - 1) * limit;

    const where: any = {};
    const access: any = await this.getAssignmentAccessRule(staff);
    if (!access.unrestricted) {
      if (access.onlyAssigned) where.assigneeId = access.staffId || "__NO_STAFF__";
      if (access.branchOnly && access.branchId) where.branchId = access.branchId;
    }

    if (query.status && query.status !== "ALL") where.status = query.status;
    if (query.channel && query.channel !== "ALL") where.channel = query.channel;
    if (query.assigneeId && access.unrestricted) where.assigneeId = query.assigneeId;
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
    const customerName = safeText(dto.customerName) || conversation.customer?.name || "Khách hàng";
    const address = safeText(dto.address);
    if (!address) throw new BadRequestException("Thiếu địa chỉ giao hàng.");

    const order: any = await this.orderService.createOrder({
      salesChannel: "FACEBOOK_MANUAL",
      customerName, customerPhone: phone, branchId: dto.branchId,
      note: safeText(dto.note) || `Đơn chốt nhanh từ hội thoại ${conversationId}`,
      mode: "draft", source: "OMNI_INBOX_QUICK_ORDER",
      omniConversationId: conversationId, quickOrderRequestId: requestId || null,
      shippingSnapshot: {
        shippingRecipientName: customerName, shippingPhone: phone,
        shippingAddressLine1: address, skipAutoShipment: true,
      },
      items: dto.items,
    }, staff);

    await this.prisma.omniCustomer.updateMany({
      where: { id: conversation.customerId || "" },
      data: { phone, address },
    });
    const note = await this.prisma.omniConversationNote.create({
      data: { conversationId, staffId: staff?.id || staff?.sub || null, staffName: staff?.name || staff?.username || null, note: `Đã tạo đơn nháp ${order.orderCode}.` },
    });
    this.realtime.emit({ type: "conversation.note_created", payload: note });
    this.realtime.emit({ type: "conversation.quick_order_created", payload: order });
    return order;
  }

  async cancelQuickOrder(conversationId: string, orderId: string, staff?: any) {
    const order = await this.prisma.order.findFirst({ where: { id: orderId, omniConversationId: conversationId, source: "OMNI_INBOX_QUICK_ORDER" } });
    if (!order) throw new NotFoundException("Không tìm thấy đơn chốt nhanh.");
    const updated = await this.orderService.updateOrderStatus(orderId, "CANCELLED" as any, staff);
    this.realtime.emit({ type: "conversation.quick_order_cancelled", payload: updated });
    return updated;
  }

  async deleteQuickOrder(conversationId: string, orderId: string, staff?: any) {
    const order = await this.prisma.order.findFirst({ where: { id: orderId, omniConversationId: conversationId, source: "OMNI_INBOX_QUICK_ORDER" } });
    if (!order) throw new NotFoundException("Không tìm thấy đơn chốt nhanh.");
    if (String(order.status) !== "NEW") throw new BadRequestException("Chỉ được xoá đơn nháp chưa duyệt.");
    const result = await this.orderService.deleteOrder(orderId, staff);
    this.realtime.emit({ type: "conversation.quick_order_deleted", payload: { id: orderId, conversationId } });
    return result;
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
    await this.autoAssignConversation(conversation.id, "INCOMING_MESSAGE");

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
      return { skipped: true, reason: "echo_message" };
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
    await this.autoAssignConversation(conversation.id, "INCOMING_MESSAGE");

    return { ok: true };
  }
}
