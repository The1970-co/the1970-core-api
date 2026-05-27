import { Body, Controller, Get, Post, Query, Req } from '@nestjs/common';
import { MetaAdsService } from './meta-ads.service';
import { MetaAdsSyncService } from './meta-ads-sync.service';
import { MetaAdsOrderAttributionService } from './meta-ads-order-attribution.service';
import { SyncMetaAdsDto } from './dto/sync-meta-ads.dto';
import type { MetaInsightLevel } from './dto/sync-meta-ads.dto';

function hcmYmd(date: Date) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Ho_Chi_Minh',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date);
}

function hcmBoundary(ymd: string, end = false) {
  return new Date(`${ymd}${end ? 'T23:59:59.999+07:00' : 'T00:00:00.000+07:00'}`);
}

function addDays(ymd: string, days: number) {
  const d = hcmBoundary(ymd, false);
  d.setUTCDate(d.getUTCDate() + days);
  return hcmYmd(d);
}

function parseDateRange(query: any) {
  const today = hcmYmd(new Date());
  const range = String(query?.range || '7d');

  let sinceYmd = today;
  let untilYmd = today;

  if (range === 'yesterday') {
    sinceYmd = addDays(today, -1);
    untilYmd = sinceYmd;
  } else if (range === '7d') {
    untilYmd = addDays(today, -1);
    sinceYmd = addDays(untilYmd, -6);
  } else if (range === '10d') {
    untilYmd = addDays(today, -1);
    sinceYmd = addDays(untilYmd, -9);
  } else if (range === '30d') {
    untilYmd = addDays(today, -1);
    sinceYmd = addDays(untilYmd, -29);
  } else if (range !== 'today' && range !== 'custom') {
    untilYmd = addDays(today, -1);
    sinceYmd = addDays(untilYmd, -6);
  }

  if (query?.fromDate) sinceYmd = String(query.fromDate).slice(0, 10);
  if (query?.toDate) untilYmd = String(query.toDate).slice(0, 10);

  return {
    since: hcmBoundary(sinceYmd, false),
    until: hcmBoundary(untilYmd, true),
    sinceYmd,
    untilYmd,
  };
}

@Controller('meta-ads')
export class MetaAdsController {
  constructor(
    private readonly metaAdsService: MetaAdsService,
    private readonly metaAdsSyncService: MetaAdsSyncService,
    private readonly metaAdsOrderAttributionService: MetaAdsOrderAttributionService,
  ) {}

  @Get('test')
  test() {
    return this.metaAdsService.testConnection();
  }

  @Get('summary')
  summary(@Query('range') range?: any, @Query('fromDate') fromDate?: string, @Query('toDate') toDate?: string) {
    return this.metaAdsService.getSummary({ range, fromDate, toDate });
  }

  @Get('insights')
  insights(@Query('range') range?: any, @Query('fromDate') fromDate?: string, @Query('toDate') toDate?: string) {
    return this.metaAdsService.getCampaignInsights({ range, fromDate, toDate });
  }

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
    const sourceMode = String(query?.sourceMode || 'facebook').toLowerCase();
    const orderMode = String(query?.orderMode || 'valid').toLowerCase();

    const attachParams = {
      since: range.since,
      until: range.until,
      sourceMode,
      orderMode,
    };

    result.topAds = await this.metaAdsOrderAttributionService.attachProductOrdersToAds(result.topAds || [], attachParams);
    result.topAdSets = await this.metaAdsOrderAttributionService.attachProductOrdersToAds(result.topAdSets || [], attachParams);
    result.topCampaigns = await this.metaAdsOrderAttributionService.attachProductOrdersToAds(result.topCampaigns || [], attachParams);

    return {
      ...result,
      productOrderRange: {
        since: range.sinceYmd,
        until: range.untilYmd,
        timezone: 'Asia/Ho_Chi_Minh',
        sourceMode,
        orderMode,
      },
      attribution: {
        enabled: true,
        mode: 'sku_family_v2',
        note:
          'V16: gom SKU family, bỏ đơn huỷ mặc định, tách DT sản phẩm và DT đơn, không nhân đôi ROAS.',
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
    const sourceMode = String(query?.sourceMode || 'facebook').toLowerCase();
    const orderMode = String(query?.orderMode || 'valid').toLowerCase();

    const result = await this.metaAdsOrderAttributionService.getProductPerformance({
      since: range.since,
      until: range.until,
      search: query?.search,
      limit: Number(query?.limit || 100),
      sourceMode,
      orderMode,
    });

    return {
      ...result,
      range: {
        since: range.sinceYmd,
        until: range.untilYmd,
        timezone: 'Asia/Ho_Chi_Minh',
        sourceMode,
        orderMode,
      },
    };
  }
  @Get('live-insights')
  async getLiveInsights(
    @Query('range') range?: string,
    @Query('fromDate') fromDate?: string,
    @Query('toDate') toDate?: string,
    @Query('level') level?: MetaInsightLevel,
    @Query('limit') limit?: string,
  ) {
    return this.metaAdsSyncService.getLiveInsights({
      range: range || 'today',
      fromDate,
      toDate,
      level: (level || 'ad') as MetaInsightLevel,
      limit: Number(limit || 1000),
    });
  }

}
