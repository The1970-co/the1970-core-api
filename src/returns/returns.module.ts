import { Module } from "@nestjs/common";
import { ReturnsController } from "./returns.controller";
import { ReturnsService } from "./returns.service";
import { OrderModule } from "../order/order.module";
import { ShipmentModule } from "../shipment/shipment.module";

@Module({
  imports: [OrderModule, ShipmentModule],
  controllers: [ReturnsController],
  providers: [ReturnsService],
  exports: [ReturnsService],
})
export class ReturnsModule {}
