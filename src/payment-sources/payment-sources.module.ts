import { Module } from "@nestjs/common";
import { PaymentSourcesController } from "./payment-sources.controller";
import { PaymentSourcesService } from "./payment-sources.service";

@Module({
  controllers: [PaymentSourcesController],
  providers: [PaymentSourcesService],
})
export class PaymentSourcesModule {}