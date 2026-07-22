import { Module } from "@nestjs/common";
import { OmniInboxController } from "./omni-inbox.controller";
import { OmniInboxMetaWebhookController } from "./omni-inbox.meta-webhook.controller";
import { OmniInboxService } from "./omni-inbox.service";
import { OmniInboxRealtimeService } from "./omni-inbox.realtime";

@Module({
  controllers: [OmniInboxController, OmniInboxMetaWebhookController],
  providers: [OmniInboxService, OmniInboxRealtimeService],
  exports: [OmniInboxService],
})
export class OmniInboxModule {}
