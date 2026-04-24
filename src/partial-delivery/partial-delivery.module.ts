import { Module } from "@nestjs/common";
import { PartialDeliveryController } from "./partial-delivery.controller";
import { PartialDeliveryService } from "./partial-delivery.service";

@Module({
  controllers: [PartialDeliveryController],
  providers: [PartialDeliveryService],
})
export class PartialDeliveryModule {}