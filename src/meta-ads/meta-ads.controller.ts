import { Body, Controller, Get, Post, Query, Req } from '@nestjs/common';
import { MetaAdsService } from './meta-ads.service';
import { MetaAdsSyncService } from './meta-ads-sync.service';
import { MetaAdsOrderAttributionService } from './meta-ads-order-attribution.service';
import { MetaAdsInventoryAutopilotService } from './meta-ads-inventory-autopilot.service';
import { MetaAdsPerformanceAutopilotService } from './meta-ads-performance-autopilot.service';
import { MetaAdsPostLaunchAutopilotService } from './meta-ads-post-launch-autopilot.service';
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
    private readonly metaAdsInventoryAutopilotService: MetaAdsInventoryAutopilotService,
    private readonly metaAdsPerformanceAutopilotService: MetaAdsPerformanceAutopilotService,
    private readonly metaAdsPostLaunchAutopilotService: MetaAdsPostLaunchAutopilotService,
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

  @Get('autopilot/live-ads')
  getAutopilotLiveAds(@Query('limit') limit?: string) {
    return this.metaAdsSyncService.getLiveAdsForAutopilot(Number(limit || 5000));
  }

  @Post('autopilot/budgets')
  getAutopilotBudgets(
    @Body() body: { metaAdSetIds?: string[]; metaCampaignIds?: string[] } = {},
  ) {
    return this.metaAdsSyncService.getBudgetSnapshot({
      metaAdSetIds: Array.isArray(body?.metaAdSetIds) ? body.metaAdSetIds : [],
      metaCampaignIds: Array.isArray(body?.metaCampaignIds) ? body.metaCampaignIds : [],
    });
  }

  @Get('autopilot/control-center')
  getAutopilotControlCenter() {
    return this.metaAdsPerformanceAutopilotService.getControlCenter();
  }

  @Get('autopilot/scale-history')
  getAutopilotScaleHistory(@Query('limit') limit?: string) {
    return this.metaAdsPerformanceAutopilotService.getScaleHistory(Number(limit || 1000));
  }

  @Get('autopilot/performance/status')
  getPerformanceAutopilotStatus() {
    return this.metaAdsPerformanceAutopilotService.getStatus();
  }

  @Post('autopilot/performance/config')
  setPerformanceAutopilotConfig(@Body() body: any = {}) {
    return this.metaAdsPerformanceAutopilotService.setRuntimeConfig(body);
  }

  @Post('autopilot/performance/run')
  runPerformanceAutopilot(@Body() body: { dryRun?: boolean } = {}) {
    return this.metaAdsPerformanceAutopilotService.runNow({ source: 'api', dryRun: body?.dryRun });
  }

  @Post('actions/scale-adset')
  scaleAdSet(
    @Body()
    body: { metaAdSetId?: string; metaAdId?: string; percent?: number; dryRun?: boolean } = {},
  ) {
    return this.metaAdsPerformanceAutopilotService.executeAdSetScale(
      String(body?.metaAdSetId || ''),
      Number(body?.percent || 20),
      Boolean(body?.dryRun),
      {
        source: 'manual',
        metaAdId: String(body?.metaAdId || ''),
      },
    );
  }

  @Post('actions/ad-status')
  setSingleAdStatus(@Body() body: { metaAdId?: string; status?: 'PAUSED' | 'ACTIVE' } = {}) {
    const status = String(body?.status || '').toUpperCase();
    if (status !== 'PAUSED' && status !== 'ACTIVE') {
      throw new Error('status phải là ACTIVE hoặc PAUSED');
    }
    return this.metaAdsSyncService.setAdStatus(String(body?.metaAdId || ''), status as 'PAUSED' | 'ACTIVE');
  }

  @Post('autopilot/inventory/assess')
  assessInventoryForAds(@Body() body: { ads?: any[] } = {}) {
    const ads = Array.isArray(body?.ads) ? body.ads.slice(0, 500) : [];
    return this.metaAdsInventoryAutopilotService.assessAdsForScale(ads);
  }

  @Get('autopilot/inventory/mapping-options')
  getInventoryMappingOptions(@Query('limit') limit?: string) {
    return this.metaAdsInventoryAutopilotService.getManualMappingOptions(Number(limit || 1000));
  }

  @Post('autopilot/ads/map')
  async setAdManualMapping(
    @Body() body: { metaAdId?: string; productCode?: string; color?: string } = {},
  ) {
    const metaAdId = String(body?.metaAdId || '').trim();
    const productCode = String(body?.productCode || '').trim().toUpperCase();
    const color = String(body?.color || '').trim();

    if (!metaAdId) throw new Error('Thiếu metaAdId');
    if (!productCode) throw new Error('Thiếu productCode');

    const options = await this.metaAdsInventoryAutopilotService.getManualMappingOptions(5000);
    const rows = Array.isArray(options) ? options : (options as any)?.items || [];

    const product = rows.find(
      (row: any) => String(row?.productCode || '').trim().toUpperCase() === productCode,
    );
    if (!product) throw new Error(`Không tìm thấy mã sản phẩm ${productCode} trong kho nội bộ`);

    const colors = Array.isArray(product?.colors) ? product.colors : [];
    const finalColor =
      color ||
      (colors.length === 1 ? String(colors[0]?.color || '').trim() : '');

    if (colors.length > 1 && !finalColor) {
      throw new Error('Mã này có nhiều màu, cần chọn màu');
    }

    if (finalColor && colors.length) {
      const normalize = (value: any) =>
        String(value || '')
          .trim()
          .toLowerCase()
          .normalize('NFD')
          .replace(/[\u0300-\u036f]/g, '')
          .replace(/đ/g, 'd');

      if (!colors.some((item: any) => normalize(item?.color) === normalize(finalColor))) {
        throw new Error(`Màu ${finalColor} không thuộc mã ${productCode}`);
      }
    }

    return this.metaAdsSyncService.setAutopilotAdManualMapping({
      metaAdId,
      productCode,
      color: finalColor || undefined,
    });
  }

  @Get('autopilot/inventory/status')
  getInventoryAutopilotStatus() {
    return this.metaAdsInventoryAutopilotService.getStatus();
  }

  @Post('autopilot/inventory/config')
  setInventoryAutopilotConfig(@Body() body: { enabled?: boolean; dryRun?: boolean } = {}) {
    return this.metaAdsInventoryAutopilotService.setRuntimeConfig(body);
  }

  @Post('autopilot/inventory/run')
  runInventoryAutopilot(@Body() body: { dryRun?: boolean } = {}) {
    return this.metaAdsInventoryAutopilotService.runNow({ source: 'api', dryRun: body?.dryRun });
  }

  @Post('actions/pause-ad')
  pauseSingleAd(@Body() body: { metaAdId?: string }) {
    return this.metaAdsSyncService.setAdStatus(String(body?.metaAdId || ''), 'PAUSED');
  }

  @Get('autopilot/launch/status')
  getPostLaunchAutopilotStatus() {
    return this.metaAdsPostLaunchAutopilotService.getStatus();
  }

  @Get('autopilot/launch/posts')
  getPostLaunchAutopilotPosts(@Query('limit') limit?: string) {
    return this.metaAdsPostLaunchAutopilotService.getPosts(Number(limit || 100));
  }

  @Post('autopilot/launch/config')
  setPostLaunchAutopilotConfig(@Body() body: any = {}) {
    return this.metaAdsPostLaunchAutopilotService.setRuntimeConfig(body);
  }

  @Get('autopilot/launch/adset-templates')
  getPostLaunchAdSetTemplates(@Query('q') q?: string, @Query('limit') limit?: string) {
    return this.metaAdsSyncService.getAutoLaunchAdSetTemplates({
      q: String(q || ''),
      limit: Number(limit || 100),
    });
  }

  @Post('autopilot/launch/template-comparison')
  async getPostLaunchTemplateComparison(@Body() body: {
    launchMode?: 'EXISTING_ADSET' | 'CLONE_ADSET' | 'NEW_CAMPAIGN';
    targetAdSetId?: string;
    templateAdSetId?: string;
    targetCampaignId?: string;
    dailyBudget?: number;
    name?: string;
  } = {}) {
    try {
      return await this.metaAdsSyncService.getAutoLaunchTemplateComparison(body);
    } catch (error: any) {
      // Diagnostic mode: không trả 500 mù. Mobile sẽ nhìn thấy stage/message thật.
      return {
        ok: false,
        diagnostic: true,
        stage: 'template-comparison',
        error: String(error?.message || error || 'Unknown error'),
        name: String(error?.name || ''),
        code: error?.code ?? null,
        response: error?.response?.data ?? error?.response ?? null,
        input: {
          launchMode: body?.launchMode || 'NEW_CAMPAIGN',
          templateAdSetId: body?.templateAdSetId || body?.targetAdSetId || null,
          dailyBudget: body?.dailyBudget ?? null,
        },
      };
    }
  }

  @Post('autopilot/launch/run')
  runPostLaunchAutopilot(@Body() body: { dryRun?: boolean; postId?: string; force?: boolean; manualOverride?: boolean; manualProductCode?: string; manualColor?: string; discoverOnly?: boolean; scanLimit?: number } = {}) {
    return this.metaAdsPostLaunchAutopilotService.runNow({
      source: body?.discoverOnly ? 'api-discovery' : body?.manualOverride ? 'api-manual' : 'api',
      dryRun: body?.dryRun,
      postId: body?.postId,
      force: body?.force,
      manualOverride: body?.manualOverride,
      manualProductCode: body?.manualProductCode,
      manualColor: body?.manualColor,
      discoverOnly: body?.discoverOnly,
      scanLimit: body?.scanLimit,
    });
  }

  @Post('autopilot/launch/map')
  setPostManualMapping(@Body() body: { postId?: string; productCode?: string; color?: string } = {}) {
    return this.metaAdsPostLaunchAutopilotService.setManualMapping({
      postId: String(body?.postId || ''),
      productCode: String(body?.productCode || ''),
      color: body?.color ? String(body.color) : undefined,
    });
  }

  @Post('autopilot/launch/skip')
  skipPostLaunchAutopilot(@Body() body: { postId?: string } = {}) {
    return this.metaAdsPostLaunchAutopilotService.skipPost(String(body?.postId || ''));
  }

  @Get('live-insights')
  async getLiveInsights(
    @Query('range') range?: string,
    @Query('fromDate') fromDate?: string,
    @Query('toDate') toDate?: string,
    @Query('level') level?: MetaInsightLevel,
    @Query('limit') limit?: string,
  ) {
    const resolvedLevel = (level || 'ad') as MetaInsightLevel;

    const result: any = await this.metaAdsSyncService.getLiveInsights({
      range: range || 'today',
      fromDate,
      toDate,
      level: resolvedLevel,
      limit: Number(limit || 1000),
    });

    // ROAS nội bộ chỉ có ý nghĩa ở level Ad:
    // Meta metrics lấy live, còn doanh thu/order lấy từ DB nội bộ.
    if (resolvedLevel !== 'ad') return result;

    const date = parseDateRange({
      range: range || 'today',
      fromDate,
      toDate,
    });

    const attributed = await this.metaAdsOrderAttributionService.attachProductOrdersToAds(
      Array.isArray(result?.topAds) ? result.topAds : [],
      {
        since: date.since,
        until: date.until,
        sourceMode: 'all',
        orderMode: 'valid',
      },
    );

    const topAds = (attributed || []).map((row: any) => {
      const attr = row?.productAttribution || {};
      const facebookRevenue = Number(attr?.facebookRevenue || 0) || 0;
      const posRevenue = Number(attr?.posRevenue || 0) || 0;
      const internalRevenue = Number(attr?.totalRevenue ?? attr?.revenue ?? 0) || 0;
      const internalOrderRevenue = internalRevenue;
      const facebookRoas = Number(attr?.facebookRoas || 0) || 0;
      const posRoas = Number(attr?.posRoas || 0) || 0;
      const totalRoas = Number(attr?.totalRoas ?? attr?.realRoasEstimate ?? 0) || 0;

      return {
        ...row,

        // Chuẩn Dashboard theo mã SP.
        facebookRevenue,
        posRevenue,
        internalRevenue,
        internalOrderRevenue,
        facebookRoas,
        posRoas,
        totalRoas,
        internalRoas: totalRoas,

        // Tương thích UI cũ + field mới cho mobile/web.
        metrics: {
          ...(row?.metrics || {}),
          facebookRevenue,
          posRevenue,
          internalRevenue,
          internalOrderRevenue,
          facebookRoas,
          posRoas,
          totalRoas,
          internalRoas: totalRoas,
          roasInternal: totalRoas,
          realRoasEstimate: totalRoas,
        },
      };
    });

    return {
      ...result,
      topAds,
      attribution: {
        ...(result?.attribution || {}),
        enabled: true,
        mode: 'dashboard_channel_roas_v1',
        note:
          'Doanh thu mã SP phân POS / Facebook-COD giống Dashboard; thời gian ưu tiên soldAt; ROAS tổng = (POS + Facebook) / tổng Meta spend của SKU family.',
      },
      internalRevenueSummary: topAds.reduce(
        (sum: number, row: any) => sum + (Number(row?.internalRevenue || 0) || 0),
        0,
      ),
    };
  }

}
