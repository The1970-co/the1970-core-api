import { BadRequestException, Injectable, NotFoundException } from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";
import { OmniInboxRealtimeService } from "./omni-inbox.realtime";
import { ListConversationsDto } from "./dto/list-conversations.dto";

function safeText(value: any) {
  return String(value || "").trim();
}

@Injectable()
export class OmniInboxService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly realtime: OmniInboxRealtimeService,
  ) {}

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
          tags: true,
          _count: { select: { messages: true, notes: true } },
        },
      }),
      this.prisma.omniConversation.count({ where }),
    ]);

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
        tags: true,
        notes: { orderBy: { createdAt: "desc" }, take: 20 },
        messages: { orderBy: { sentAt: "asc" }, take: 100 },
      },
    });

    if (!item) throw new NotFoundException("Không tìm thấy hội thoại.");
    return item;
  }

  async assignConversation(id: string, dto: { assigneeId: string; assigneeName: string }) {
    const item = await this.prisma.omniConversation.update({
      where: { id },
      data: {
        assigneeId: dto.assigneeId,
        assigneeName: dto.assigneeName,
        status: "PROCESSING",
      },
      include: { customer: true, tags: true },
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
      include: { customer: true, tags: true },
    });

    this.realtime.emit({ type: "conversation.updated", payload: item });
    return item;
  }

  async updateTags(id: string, tags: string[]) {
    const cleanTags = Array.from(
      new Set(tags.map((tag) => safeText(tag)).filter(Boolean).slice(0, 20)),
    );

    await this.prisma.$transaction([
      this.prisma.omniConversationTag.deleteMany({ where: { conversationId: id } }),
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
      include: { customer: true, tags: true },
    });

    this.realtime.emit({ type: "conversation.updated", payload: item });
    return item;
  }

  /**
   * Chưa gọi Meta API thật ở Phase 1.
   * Khi nối Meta thật, hàm này sẽ gửi Graph API bằng Page token rồi lưu OUT message.
   */
  async sendMessage(id: string, dto: { text: string; attachmentUrl?: string }, staff?: any) {
    const conversation = await this.prisma.omniConversation.findUnique({ where: { id } });
    if (!conversation) throw new NotFoundException("Không tìm thấy hội thoại.");

    const text = safeText(dto.text);
    if (!text && !dto.attachmentUrl) throw new BadRequestException("Tin nhắn trống.");

    const now = new Date();

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
        status: conversation.status === "OPEN" ? "PROCESSING" : conversation.status,
      },
      include: { customer: true, tags: true },
    });

    this.realtime.emit({ type: "message.created", payload: message });
    this.realtime.emit({ type: "conversation.updated", payload: updated });

    return message;
  }

  async ingestMetaWebhookEvent(event: any) {
    const senderId = safeText(event?.sender?.id);
    const recipientId = safeText(event?.recipient?.id);
    const messageId = safeText(event?.message?.mid);
    const text = safeText(event?.message?.text);
    const timestamp = Number(event?.timestamp || Date.now());

    if (!senderId || !recipientId || (!text && !event?.message?.attachments?.length)) {
      return { skipped: true };
    }

    const page = await this.prisma.omniInboxPage.upsert({
      where: { providerPageId: recipientId },
      update: { lastWebhookAt: new Date() },
      create: {
        providerPageId: recipientId,
        pageName: `Page ${recipientId}`,
        channel: "FACEBOOK",
        lastWebhookAt: new Date(),
      },
    });

    const customer = await this.prisma.omniCustomer.upsert({
      where: { providerUserId: senderId },
      update: {},
      create: {
        providerUserId: senderId,
        name: `Khách ${senderId.slice(-6)}`,
      },
    });

    const providerThreadId = `FACEBOOK:${recipientId}:${senderId}`;
    const sentAt = new Date(timestamp);

    const conversation = await this.prisma.omniConversation.upsert({
      where: { providerThreadId },
      update: {
        lastMessageText: text || "[Tệp đính kèm]",
        lastMessageAt: sentAt,
        unreadCount: { increment: 1 },
        status: "OPEN",
      },
      create: {
        providerThreadId,
        channel: "FACEBOOK",
        pageId: page.id,
        customerId: customer.id,
        lastMessageText: text || "[Tệp đính kèm]",
        lastMessageAt: sentAt,
        unreadCount: 1,
        status: "OPEN",
      },
      include: { customer: true, tags: true },
    });

    if (messageId) {
      const existed = await this.prisma.omniMessage.findUnique({
        where: { providerMessageId: messageId },
      });
      if (existed) return { duplicated: true };
    }

    const attachment = event?.message?.attachments?.[0];
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

    this.realtime.emit({ type: "message.created", payload: message });
    this.realtime.emit({ type: "conversation.updated", payload: conversation });

    return { ok: true };
  }
}
