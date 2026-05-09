import { Module } from "@nestjs/common";
import { ShipmentController } from "./shipment.controller";
import { ShipmentService } from "./shipment.service";
import { GhnClient } from "./ghn.client";
import { AhamoveClient } from "./ahamove.client";
import { ViettelPostClient } from "./viettelpost.client";
import { AuthTotpModule } from "../auth-totp/auth-totp.module";
import { AhamoveWebhookController } from "./ahamove-webhook.controller";

@Module({
  imports: [
  AuthTotpModule, // 👈 inject TOTP đúng cách
  ],
  controllers: [ShipmentController, AhamoveWebhookController],
  providers: [ShipmentService, GhnClient, AhamoveClient, ViettelPostClient],
  exports: [ShipmentService],
})
export class ShipmentModule {}