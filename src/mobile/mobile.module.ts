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
import { MobileProductsController } from "./products/mobile-products.controller";
import { MobileProductsService } from "./products/mobile-products.service";
@Module({
  imports: [MobileReportsModule],
  controllers: [
    MobileDashboardController,
    MobileOperationsController,
    MobileProfileController,
    MobileHomeController,
    MobileBranchesController,
    MobileProductsController,
  ],
  providers: [
    MobileDashboardService,
    MobileOperationsService,
    MobileProfileService,
    MobileHomeService,
    MobileBranchesService,
    MobileProductsService,
  ],
})
export class MobileModule {}