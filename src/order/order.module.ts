import { Module } from "@nestjs/common";
import { PrismaModule } from "../prisma/prisma.module";
import { ShipmentModule } from "../shipment/shipment.module";
import { OrderController } from "./order.controller";
import { OrderService } from "./order.service";

@Module({
  imports: [PrismaModule, ShipmentModule],
  controllers: [OrderController],
  providers: [OrderService],
  exports: [OrderService],
})
export class OrderModule {}