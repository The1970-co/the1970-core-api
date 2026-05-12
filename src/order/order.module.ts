import { Module } from "@nestjs/common";

import { ShipmentModule } from "../shipment/shipment.module";
import { PromotionsModule } from "../promotions/promotions.module";
import { OrderController } from "./order.controller";
import { PartialDeliveryController } from "./partial-delivery.controller";
import { OrderService } from "./order.service";

@Module({
  imports: [ShipmentModule, PromotionsModule],
  controllers: [OrderController, PartialDeliveryController],
  providers: [OrderService],
  exports: [OrderService],
})
export class OrderModule {}