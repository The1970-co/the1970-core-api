import { Body, Controller, Get, HttpCode, Logger, Post, Query } from "@nestjs/common";
import { OmniInboxService } from "./omni-inbox.service";

@Controller("webhooks/meta/inbox")
export class OmniInboxMetaWebhookController {
  private readonly logger = new Logger(OmniInboxMetaWebhookController.name);

  constructor(private readonly service: OmniInboxService) {}

  @Get()
  verifyWebhook(@Query() query: any) {
    const verifyToken = process.env.META_INBOX_WEBHOOK_VERIFY_TOKEN || "";
    const mode = query["hub.mode"];
    const token = query["hub.verify_token"];
    const challenge = query["hub.challenge"];

    if (mode === "subscribe" && token === verifyToken) {
      this.logger.log("[META_WEBHOOK_VERIFY_OK]");
      return challenge;
    }

    this.logger.warn(
      `[META_WEBHOOK_VERIFY_FAILED] mode=${mode || "-"} token=${token ? "provided" : "missing"}`,
    );
    return "Invalid verify token";
  }

  @Post()
  @HttpCode(200)
  async receiveWebhook(@Body() body: any) {
    const entries = Array.isArray(body?.entry) ? body.entry : [];
    let handled = 0;
    let skipped = 0;

    this.logger.log(`[META_WEBHOOK_RECEIVED] entries=${entries.length}`);

    for (const entry of entries) {
      const messaging = Array.isArray(entry?.messaging) ? entry.messaging : [];

      for (const event of messaging) {
        try {
          const result: any = await this.service.ingestMetaWebhookEvent(event);
          if (result?.ok || result?.duplicated) handled += 1;
          else skipped += 1;
        } catch (error: any) {
          skipped += 1;
          // Vẫn trả 200 cho Meta để không bị retry storm, nhưng log rõ lỗi để debug.
          this.logger.error(
            `[META_WEBHOOK_EVENT_FAILED] ${error?.message || error}`,
            error?.stack,
          );
        }
      }
    }

    return { ok: true, handled, skipped };
  }
}
