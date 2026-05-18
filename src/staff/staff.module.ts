import { Module } from "@nestjs/common";
import { StaffController, StaffMeController, StaffTransferController } from "./staff.controller";
import { StaffService } from "./staff.service";

@Module({
  controllers: [StaffController, StaffMeController, StaffTransferController],
  providers: [StaffService],
  exports: [StaffService],
})
export class StaffModule {}
