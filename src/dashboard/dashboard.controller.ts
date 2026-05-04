
import { Controller, Get, Query } from '@nestjs/common';
import { DashboardService } from './dashboard.service';

@Controller('dashboard')
export class DashboardController {
  constructor(private readonly dashboardService: DashboardService) {}

  @Get('overview')
  overview(@Query('branchId') branchId?: string) {
    return this.dashboardService.getOverview(branchId);
  }
}
