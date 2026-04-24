import { Module } from "@nestjs/common";
import { MobileDashboardController } from "./dashboard/mobile-dashboard.controller";
import { MobileDashboardService } from "./dashboard/mobile-dashboard.service";
import { MobileOperationsController } from "./operations/mobile-operations.controller";
import { MobileOperationsService } from "./operations/mobile-operations.service";
import { MobileProfileController } from "./profile/mobile-profile.controller";
import { MobileProfileService } from "./profile/mobile-profile.service";
import { MobileHomeController } from "./home/mobile-home.controller";
import { MobileHomeService } from "./home/mobile-home.service";
import { MobileReportsModule } from "./reports/mobile.module";
import { MobileBranchesController } from "./branches/mobile-branches.controller";
import { MobileBranchesService } from "./branches/mobile-branches.service";
import { Controller, Get, Query, UseGuards } from "@nestjs/common";
@Module({
  imports: [MobileReportsModule],
  controllers: [
    MobileDashboardController,
    MobileOperationsController,
    MobileProfileController,
    MobileHomeController,
    MobileBranchesController,
  ],
  providers: [
    MobileDashboardService,
    MobileOperationsService,
    MobileProfileService,
    MobileHomeService,
    MobileBranchesService,
  ],
})
export class MobileModule {}