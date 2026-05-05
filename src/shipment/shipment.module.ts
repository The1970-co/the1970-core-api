import { Module } from "@nestjs/common";
import { ShipmentController } from "./shipment.controller";
import { ShipmentService } from "./shipment.service";
import { GhnClient } from "./ghn.client";
import { AhamoveClient } from "./ahamove.client";
import { AuthTotpModule } from "../auth-totp/auth-totp.module";

@Module({
  imports: [
  AuthTotpModule, // 👈 inject TOTP đúng cách
  ],
  controllers: [ShipmentController],
  providers: [ShipmentService, GhnClient, AhamoveClient],
  exports: [ShipmentService],
})
export class ShipmentModule {}