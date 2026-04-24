import { Module } from "@nestjs/common";
import { MobileReportsService } from "./mobile-reports.service";
import { MobileReportsController } from "./mobile-reports.controller";

@Module({
  providers: [MobileReportsService],
  controllers: [MobileReportsController],
})
export class MobileReportsModule {}