import { Controller, Get, Query, UseGuards } from "@nestjs/common";
import { JwtGuard } from "../../auth/jwt.guard";
import { MobileDashboardService } from "./mobile-dashboard.service";

@Controller("mobile/dashboard")
@UseGuards(JwtGuard)
export class MobileDashboardController {
  constructor(
    private readonly mobileDashboardService: MobileDashboardService
  ) {}

  @Get("summary")
  async getSummary(@Query("branchId") branchId?: string) {
    return this.mobileDashboardService.getSummary(branchId);
  }

  @Get("branch-breakdown")
  async getBranchBreakdown(@Query("branchId") branchId?: string) {
    return this.mobileDashboardService.getBranchBreakdown(branchId);
  }

  @Get("alerts")
  async getAlerts(@Query("branchId") branchId?: string) {
    return this.mobileDashboardService.getAlerts(branchId);
  }
}