import { Body, Controller, Get, Post, Query, Req } from '@nestjs/common';
import { MetaAdsService } from './meta-ads.service';
import { MetaAdsSyncService } from './meta-ads-sync.service';
import { SyncMetaAdsDto } from './dto/sync-meta-ads.dto';
import { MetaAdsQueryDto } from './dto/meta-ads-query.dto';

@Controller('meta-ads')
export class MetaAdsController {
  constructor(
    private readonly metaAdsService: MetaAdsService,
    private readonly metaAdsSyncService: MetaAdsSyncService,
  ) {}

  // ==== FLOW CŨ: Dashboard đang phụ thuộc vào các endpoint/hàm này. Không đổi output. ====
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

  // ==== FLOW MỚI: Ads Brain Center đọc/ghi DB riêng, không ảnh hưởng Dashboard live. ====
  @Post('sync')
  sync(@Body() body: SyncMetaAdsDto, @Req() req: any) {
    return this.metaAdsSyncService.syncAll(body || {}, req?.user);
  }

  @Get('accounts-db')
  accountsDb() {
    return this.metaAdsSyncService.getAccounts();
  }

  @Get('campaigns-db')
  campaignsDb(@Query() query: MetaAdsQueryDto) {
    return this.metaAdsSyncService.getCampaigns(query);
  }

  @Get('adsets-db')
  adSetsDb(@Query() query: MetaAdsQueryDto) {
    return this.metaAdsSyncService.getAdSets(query);
  }

  @Get('ads-db')
  adsDb(@Query() query: MetaAdsQueryDto) {
    return this.metaAdsSyncService.getAds(query);
  }

  @Get('insights-db')
  insightsDb(@Query() query: MetaAdsQueryDto) {
    return this.metaAdsSyncService.getInsights(query);
  }

  @Get('sync-logs')
  syncLogs(@Query() query: MetaAdsQueryDto) {
    return this.metaAdsSyncService.getSyncLogs(query);
  }

  @Get('brain-overview')
  brainOverview(@Query() query: MetaAdsQueryDto) {
    return this.metaAdsSyncService.getBrainOverview(query);
  }

  @Get('entity-detail')
  entityDetail(@Query() query: MetaAdsQueryDto & { type?: string; id?: string; metaId?: string }) {
    return this.metaAdsSyncService.getEntityDetail(query);
  }

}
