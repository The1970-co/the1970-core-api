import { Body, Controller, Get, Post, Query, Req } from '@nestjs/common';
import { MetaAdsService } from './meta-ads.service';
import { MetaAdsSyncService } from './meta-ads-sync.service';
import { MetaAdsOrderAttributionService } from './meta-ads-order-attribution.service';
import { SyncMetaAdsDto } from './dto/sync-meta-ads.dto';

function parseDateRange(query: any) {
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());

  const range = String(query?.range || '7d');
  let since = new Date(today);
  let until = new Date(today);

  if (range === 'today') {
    since = new Date(today);
    until = new Date(today);
  } else if (range === 'yesterday') {
    since = new Date(today);
    since.setDate(since.getDate() - 1);
    until = new Date(since);
  } else if (range === '10d') {
    since = new Date(today);
    since.setDate(since.getDate() - 9);
  } else if (range === '30d') {
    since = new Date(today);
    since.setDate(since.getDate() - 29);
  } else {
    since = new Date(today);
    since.setDate(since.getDate() - 6);
  }

  if (query?.fromDate) since = new Date(`${query.fromDate}T00:00:00.000Z`);
  if (query?.toDate) until = new Date(`${query.toDate}T00:00:00.000Z`);

  return {
    since: new Date(`${since.toISOString().slice(0, 10)}T00:00:00.000Z`),
    until: new Date(`${until.toISOString().slice(0, 10)}T23:59:59.999Z`),
  };
}

@Controller('meta-ads')
export class MetaAdsController {
  constructor(
    private readonly metaAdsService: MetaAdsService,
    private readonly metaAdsSyncService: MetaAdsSyncService,
    private readonly metaAdsOrderAttributionService: MetaAdsOrderAttributionService,
  ) {}

  /**
   * LEGACY LIVE ENDPOINTS
   * Không đổi output để Dashboard/Tổng quan cũ không bị ảnh hưởng.
   */
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

  /**
   * BRAIN CENTER DB ENDPOINTS
   */
  @Get('accounts')
  getAccounts() {
    return this.metaAdsSyncService.getAccounts();
  }

  @Post('sync')
  async sync(@Body() body: SyncMetaAdsDto = {}, @Req() req?: any) {
    const result: any = await this.metaAdsSyncService.syncAll(body, req?.user);

    const structure = result?.structure || {};
    const insights = result?.insights || {};

    return {
      ok: true,
      logId: result?.logId,
      durationMs: result?.durationMs,
      campaigns: structure?.campaignCount || 0,
      adSets: structure?.adSetCount || 0,
      ads: structure?.adCount || 0,
      insights: insights?.insightRows || 0,
      structure,
      insightSummary: insights,
      message: 'Sync Meta Ads Brain Center thành công',
    };
  }

  @Get('campaigns-db')
  getCampaigns(@Query() query: any) {
    return this.metaAdsSyncService.getCampaigns(query);
  }

  @Get('adsets-db')
  getAdSets(@Query() query: any) {
    return this.metaAdsSyncService.getAdSets(query);
  }

  @Get('ads-db')
  getAds(@Query() query: any) {
    return this.metaAdsSyncService.getAds(query);
  }

  @Get('insights-db')
  getInsights(@Query() query: any) {
    return this.metaAdsSyncService.getInsights(query);
  }

  @Get('sync-logs')
  getSyncLogs(@Query() query: any) {
    return this.metaAdsSyncService.getSyncLogs(query);
  }

  @Get('brain-overview')
  async getBrainOverview(@Query() query: any) {
    const result: any = await this.metaAdsSyncService.getBrainOverview(query);

    const includeProductOrders =
      String(query?.includeProductOrders || query?.includeAttribution || '').toLowerCase() === '1' ||
      String(query?.includeProductOrders || query?.includeAttribution || '').toLowerCase() === 'true';

    if (!includeProductOrders) return result;

    const range = parseDateRange(query);

    result.topAds = await this.metaAdsOrderAttributionService.attachProductOrdersToAds(result.topAds || [], range);

    return {
      ...result,
      attribution: {
        enabled: true,
        mode: 'product_order_exact_then_ad_name_match',
        note:
          'Đơn hệ thống là đơn thật theo sản phẩm/SKU. Việc gắn vào ads vẫn dựa tên/SKU, chưa phải attribution chuẩn fbclid/pixel/CAPI.',
      },
    };
  }

  @Get('entity-detail')
  getEntityDetail(@Query() query: any) {
    return this.metaAdsSyncService.getEntityDetail(query);
  }

  @Get('product-performance')
  async getProductPerformance(@Query() query: any) {
    const range = parseDateRange(query);
    return this.metaAdsOrderAttributionService.getProductPerformance({
      since: range.since,
      until: range.until,
      search: query?.search,
      limit: Number(query?.limit || 100),
    });
  }
}
