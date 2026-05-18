
import { Controller, Get, Query } from '@nestjs/common';
import { DashboardService } from './dashboard.service';

@Controller('dashboard')
export class DashboardController {
  constructor(private readonly dashboardService: DashboardService) {}

  @Get('overview')
  overview(
    @Query('branchId') branchId?: string,
    @Query('range') range?: string,
    @Query('fromDate') fromDate?: string,
    @Query('toDate') toDate?: string,
  ) {
    // range/fromDate/toDate giữ lại để tương thích với frontend dashboard.
    // DashboardService hiện vẫn build bảng tháng, ads được map đủ 30 ngày gần nhất.
    return this.dashboardService.getOverview(branchId);
  }
}
