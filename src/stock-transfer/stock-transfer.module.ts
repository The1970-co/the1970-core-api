import { Module } from "@nestjs/common";
import { BranchNotificationsModule } from "../notifications/branch-notifications.module";
import { StockTransferController } from "./stock-transfer.controller";
import { StockTransferService } from "./stock-transfer.service";

@Module({
  imports: [BranchNotificationsModule],
  controllers: [StockTransferController],
  providers: [StockTransferService],
  exports: [StockTransferService],
})
export class StockTransferModule {}