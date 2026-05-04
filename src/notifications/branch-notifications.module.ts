import { Module } from "@nestjs/common";
import { BranchNotificationsController } from "./branch-notifications.controller";
import { BranchNotificationsService } from "./branch-notifications.service";

@Module({
  controllers: [BranchNotificationsController],
  providers: [BranchNotificationsService],
  exports: [BranchNotificationsService],
})
export class BranchNotificationsModule {}
