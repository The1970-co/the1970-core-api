import { Body, Controller, Get, Post, Query } from "@nestjs/common";
import { OmniInboxService } from "./omni-inbox.service";

@Controller("webhooks/meta/inbox")
export class OmniInboxMetaWebhookController {
  constructor(private readonly service: OmniInboxService) {}

  @Get()
  verifyWebhook(@Query() query: any) {
    const verifyToken = process.env.META_INBOX_WEBHOOK_VERIFY_TOKEN || "";
    const mode = query["hub.mode"];
    const token = query["hub.verify_token"];
    const challenge = query["hub.challenge"];

    if (mode === "subscribe" && token === verifyToken) {
      return challenge;
    }

    return "Invalid verify token";
  }

  @Post()
  async receiveWebhook(@Body() body: any) {
    const entries = Array.isArray(body?.entry) ? body.entry : [];

    for (const entry of entries) {
      const messaging = Array.isArray(entry?.messaging) ? entry.messaging : [];
      for (const event of messaging) {
        await this.service.ingestMetaWebhookEvent(event);
      }
    }

    // Meta cần trả 200 nhanh. Không xử lý nặng trong webhook.
    return { ok: true };
  }
}
