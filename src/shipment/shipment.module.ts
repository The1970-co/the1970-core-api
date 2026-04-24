import { Module } from "@nestjs/common";
import { PrismaModule } from "../prisma/prisma.module";
import { ShipmentController } from "./shipment.controller";
import { ShipmentService } from "./shipment.service";
import { GhnClient } from "./ghn.client";
import { AuthTotpModule } from "../auth-totp/auth-totp.module";

@Module({
  imports: [
    PrismaModule,
    AuthTotpModule, // 👈 inject TOTP đúng cách
  ],
  controllers: [ShipmentController],
  providers: [ShipmentService, GhnClient],
  exports: [ShipmentService],
})
export class ShipmentModule {}