import { Module } from "@nestjs/common";
import { StaffController, StaffMeController } from "./staff.controller";
import { StaffService } from "./staff.service";

@Module({
  controllers: [StaffController, StaffMeController],
  providers: [StaffService],
  exports: [StaffService],
})
export class StaffModule {}
