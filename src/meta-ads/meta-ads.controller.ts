import { Controller, Get, Query } from '@nestjs/common';
import { MetaAdsService } from './meta-ads.service';

@Controller('meta-ads')
export class MetaAdsController {
  constructor(private readonly metaAdsService: MetaAdsService) {}

  @Get('test')
  test() {
    return this.metaAdsService.testConnection();
  }

  @Get('summary')
  summary(
    @Query('range') range?: any,
    @Query('fromDate') fromDate?: string,
    @Query('toDate') toDate?: string,
  ) {
    return this.metaAdsService.getSummary({ range, fromDate, toDate });
  }

  @Get('insights')
  insights(
    @Query('range') range?: any,
    @Query('fromDate') fromDate?: string,
    @Query('toDate') toDate?: string,
  ) {
    return this.metaAdsService.getCampaignInsights({ range, fromDate, toDate });
  }
}
