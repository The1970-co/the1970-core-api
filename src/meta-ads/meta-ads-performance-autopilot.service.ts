import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { MetaAdsSyncService } from './meta-ads-sync.service';
import { MetaAdsOrderAttributionService } from './meta-ads-order-attribution.service';
import { MetaAdsInventoryAutopilotService } from './meta-ads-inventory-autopilot.service';

type AutomationLevel = 'manual' | 'semi' | 'auto';
type AnyRow = Record<string, any>;

type PerformanceAction = {
  at: string;
  type: 'SCALE' | 'DRY_RUN_SCALE' | 'SUGGEST_SCALE' | 'SKIP' | 'ERROR';
  metaAdId?: string;
  metaAdSetId?: string;
  adName?: string;
  sku?: string | null;
  roas?: number;
  spend?: number;
  oldBudget?: number;
  newBudget?: number;
  reason: string;
};

function n(value: any) {
  const num = Number(value || 0);
  return Number.isFinite(num) ? num : 0;
}

function isActive(row: any) {
  const status = String(row?.effectiveStatus || row?.status || '').toUpperCase();
  return status === 'ACTIVE';
}

@Injectable()
export class MetaAdsPerformanceAutopilotService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(MetaAdsPerformanceAutopilotService.name);
  private timer: ReturnType<typeof setInterval> | null = null;
  private running = false;

  private runtimeEnabled = this.envBool('META_ADS_PERFORMANCE_AUTOPILOT_ENABLED', false);
  private runtimeDryRun = this.envBool('META_ADS_PERFORMANCE_AUTOPILOT_DRY_RUN', true);
  private runtimeLevel: AutomationLevel = this.envLevel(process.env.META_ADS_PERFORMANCE_AUTOMATION_LEVEL || 'manual');

  // Rule đã chốt: đánh giá rolling 24h, ROAS >= 3, scale +20%, tối đa 1 lần / 24h.
  private scaleRoas = this.envNumber('META_ADS_SCALE_ROAS', 3);
  private scalePercent = this.envNumber('META_ADS_SCALE_PERCENT', 20);
  private minSpend = this.envNumber('META_ADS_SCALE_MIN_SPEND', 200000);
  private readonly minRunHours = 24;
  private readonly scaleWindowHours = 24;

  private lastRunAt: string | null = null;
  private lastSummary: any = null;
  private actions: PerformanceAction[] = [];

  constructor(
    private readonly prisma: PrismaService,
    private readonly metaAdsSyncService: MetaAdsSyncService,
    private readonly attributionService: MetaAdsOrderAttributionService,
    private readonly inventoryAutopilotService: MetaAdsInventoryAutopilotService,
  ) {}

  private envBool(key: string, fallback: boolean) {
    const raw = String(process.env[key] ?? '').trim().toLowerCase();
    if (!raw) return fallback;
    return ['1', 'true', 'yes', 'on'].includes(raw);
  }

  private envNumber(key: string, fallback: number) {
    const value = Number(process.env[key]);
    return Number.isFinite(value) ? value : fallback;
  }

  private envLevel(value: any): AutomationLevel {
    const v = String(value || '').toLowerCase();
    return v === 'auto' || v === 'semi' ? v : 'manual';
  }

  private get intervalMs() {
    // Scan thường xuyên để khi vừa đủ 24h thì không phải đợi lâu; guardrail vẫn chỉ cho scale 1 lần / 24h.
    return Math.max(60_000, this.envNumber('META_ADS_PERFORMANCE_INTERVAL_MS', 300_000));
  }

  private runScheduledSafely(source: 'startup' | 'interval') {
    void this.runNow({ source }).catch((error: any) => {
      this.logger.error(`[$META_PERFORMANCE_AUTOPILOT] scheduled ${source} failed: ${error?.message || error}`);
    });
  }

  async onModuleInit() {
    await this.loadPersistedConfig();
    this.restartTimer();
    if (this.runtimeEnabled) setTimeout(() => this.runScheduledSafely('startup'), 20_000);
  }

  onModuleDestroy() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  private restartTimer() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    if (!this.runtimeEnabled) return;
    this.timer = setInterval(() => this.runScheduledSafely('interval'), this.intervalMs);
  }

  async setRuntimeConfig(input: any = {}) {
    if (typeof input.enabled === 'boolean') this.runtimeEnabled = input.enabled;
    if (typeof input.dryRun === 'boolean') this.runtimeDryRun = input.dryRun;
    if (input.level) this.runtimeLevel = this.envLevel(input.level);
    if (Number.isFinite(Number(input.scaleRoas))) this.scaleRoas = Math.max(0.1, Number(input.scaleRoas));
    if (Number.isFinite(Number(input.scalePercent))) this.scalePercent = Math.min(50, Math.max(1, Number(input.scalePercent)));
    if (Number.isFinite(Number(input.minSpend))) this.minSpend = Math.max(0, Number(input.minSpend));
    this.restartTimer();
    await this.persistRuntimeConfig();
    return this.getStatus();
  }

  private async loadPersistedConfig() {
    try {
      const row = await (this.prisma as any).metaSyncLog.findFirst({
        where: { syncType: 'META_ADS_AUTOPILOT_PERFORMANCE_CONFIG', status: 'SUCCESS' },
        orderBy: { startedAt: 'desc' },
      });
      const config = (row?.errorJson as any)?.config || {};
      if (typeof config.enabled === 'boolean') this.runtimeEnabled = config.enabled;
      if (typeof config.dryRun === 'boolean') this.runtimeDryRun = config.dryRun;
      if (config.level) this.runtimeLevel = this.envLevel(config.level);
      if (Number.isFinite(Number(config.scaleRoas))) this.scaleRoas = Math.max(0.1, Number(config.scaleRoas));
      if (Number.isFinite(Number(config.scalePercent))) this.scalePercent = Math.min(50, Math.max(1, Number(config.scalePercent)));
      if (Number.isFinite(Number(config.minSpend))) this.minSpend = Math.max(0, Number(config.minSpend));
    } catch (error: any) {
      this.logger.warn(`[AUTOPILOT_CONFIG_LOAD] ${error?.message || error}`);
    }
  }

  private async persistRuntimeConfig() {
    try {
      const config = { enabled: this.runtimeEnabled, dryRun: this.runtimeDryRun, level: this.runtimeLevel, scaleRoas: this.scaleRoas, scalePercent: this.scalePercent, minSpend: this.minSpend };
      await (this.prisma as any).metaSyncLog.create({ data: { metaAccountId: null, syncType: 'META_ADS_AUTOPILOT_PERFORMANCE_CONFIG', status: 'SUCCESS', range: 'config', startedAt: new Date(), finishedAt: new Date(), durationMs: 0, scanned: 0, upserted: 1, failed: 0, message: 'Saved Performance Autopilot config', errorJson: { config } } });
    } catch (error: any) {
      this.logger.warn(`[AUTOPILOT_CONFIG_SAVE] ${error?.message || error}`);
    }
  }

  getStatus() {
    return {
      ok: true,
      enabled: this.runtimeEnabled,
      dryRun: this.runtimeDryRun,
      level: this.runtimeLevel,
      running: this.running,
      intervalMs: this.intervalMs,
      scaleRoas: this.scaleRoas,
      scalePercent: this.scalePercent,
      minSpend: this.minSpend,
      minRunHours: this.minRunHours,
      scaleWindowHours: this.scaleWindowHours,
      maxScalePerAdSetPer24h: 1,
      lastRunAt: this.lastRunAt,
      lastSummary: this.lastSummary,
      recentActions: this.actions.slice(0, 100),
      rule: `Chạy >= ${this.minRunHours}h + ROAS rolling 24h >= ${this.scaleRoas} + spend >= ${this.minSpend} + tồn mọi size >= 10; scale Ad Set +${this.scalePercent}%, tối đa 1 lần/24h.`,
    };
  }

  private pushAction(action: PerformanceAction) {
    this.actions = [action, ...this.actions].slice(0, 200);
  }

  private async recentScaleLogs(hours = this.scaleWindowHours) {
    const since = new Date(Date.now() - hours * 60 * 60 * 1000);
    try {
      const rows = await (this.prisma as any).metaSyncLog.findMany({
        where: {
          syncType: 'META_ADS_AUTOPILOT_SCALE',
          status: 'SUCCESS',
          startedAt: { gte: since },
        },
        orderBy: { startedAt: 'desc' },
        take: 1000,
      });
      return (rows || []) as AnyRow[];
    } catch (error: any) {
      this.logger.warn(`[AUTO_SCALE_LOG_READ] ${error?.message || error}`);
      return [];
    }
  }

  private scaleLogAdSetId(log: AnyRow) {
    const payload = (log?.errorJson || {}) as AnyRow;
    return String(payload?.metaAdSetId || '').trim();
  }

  private async lastScaleByAdSet() {
    const logs = await this.recentScaleLogs();
    const map = new Map<string, AnyRow>();
    for (const log of logs) {
      const adSetId = this.scaleLogAdSetId(log);
      if (adSetId && !map.has(adSetId)) map.set(adSetId, log);
    }
    return map;
  }

  async getScaleHistory(limit = 1000) {
    const safeLimit = Math.min(5000, Math.max(1, Number(limit || 1000)));
    try {
      const rows = await (this.prisma as any).metaSyncLog.findMany({
        where: {
          syncType: 'META_ADS_AUTOPILOT_SCALE',
          status: 'SUCCESS',
        },
        orderBy: { startedAt: 'desc' },
        take: safeLimit,
      });

      const items = (rows || []).map((log: AnyRow) => {
        const payload = (log?.errorJson || {}) as AnyRow;
        const metaAdSetId = String(payload?.metaAdSetId || '').trim() || null;
        const metaCampaignId = String(payload?.metaCampaignId || '').trim() || null;
        const budgetLevel = String(payload?.budgetLevel || 'ADSET').toUpperCase() === 'CAMPAIGN' ? 'CAMPAIGN' : 'ADSET';
        const budgetEntityId = String(payload?.budgetEntityId || (budgetLevel === 'CAMPAIGN' ? metaCampaignId : metaAdSetId) || '').trim() || null;
        return {
          id: String(log?.id || `${log?.startedAt || ''}-${budgetEntityId || ''}`),
          at: log?.startedAt ? new Date(log.startedAt).toISOString() : null,
          metaAdId: String(payload?.metaAdId || '').trim() || null,
          metaAdSetId,
          metaCampaignId,
          budgetLevel,
          budgetEntityId,
          source: String(payload?.source || 'manual'),
          percent: n(payload?.percent),
          oldBudget: n(payload?.oldBudget),
          newBudget: n(payload?.newBudget),
          roas: payload?.roas == null ? null : n(payload?.roas),
          spend: payload?.spend == null ? null : n(payload?.spend),
          message: log?.message || null,
        };
      });

      const countByEntity: Record<string, number> = {};
      for (const item of items) {
        if (item.budgetEntityId) countByEntity[item.budgetEntityId] = (countByEntity[item.budgetEntityId] || 0) + 1;
      }

      return { ok: true, items, countByEntity, total: items.length };
    } catch (error: any) {
      this.logger.warn(`[AUTO_SCALE_HISTORY] ${error?.message || error}`);
      return { ok: false, items: [], countByEntity: {}, total: 0, error: error?.message || String(error) };
    }
  }

  private async persistScaleLog(input: {
    metaAdSetId: string;
    metaCampaignId?: string;
    budgetLevel?: 'ADSET' | 'CAMPAIGN';
    budgetEntityId?: string;
    metaAdId?: string;
    source: string;
    percent: number;
    oldBudget: number;
    newBudget: number;
    roas?: number;
    spend?: number;
  }) {
    try {
      await (this.prisma as any).metaSyncLog.create({
        data: {
          metaAccountId: null,
          syncType: 'META_ADS_AUTOPILOT_SCALE',
          status: 'SUCCESS',
          range: 'rolling_24h',
          startedAt: new Date(),
          finishedAt: new Date(),
          durationMs: 0,
          scanned: 1,
          upserted: 1,
          failed: 0,
          message: `Scale Ad Set +${input.percent}%: ${input.oldBudget} -> ${input.newBudget}`,
          errorJson: {
            metaAdSetId: input.metaAdSetId,
            metaCampaignId: input.metaCampaignId || null,
            budgetLevel: input.budgetLevel || 'ADSET',
            budgetEntityId: input.budgetEntityId || input.metaAdSetId,
            metaAdId: input.metaAdId || null,
            source: input.source,
            percent: input.percent,
            oldBudget: input.oldBudget,
            newBudget: input.newBudget,
            roas: input.roas ?? null,
            spend: input.spend ?? null,
          },
        },
      });
    } catch (error: any) {
      // Không rollback lệnh Meta nếu ghi audit log lỗi.
      this.logger.warn(`[AUTO_SCALE_LOG_WRITE] ${error?.message || error}`);
    }
  }

  async executeAdSetScale(
    metaAdSetId: string,
    percent = this.scalePercent,
    dryRun = this.runtimeDryRun,
    context: { source?: string; metaAdId?: string; roas?: number; spend?: number } = {},
  ) {
    const adSetId = String(metaAdSetId || '').trim();
    if (!adSetId) throw new Error('Thiếu metaAdSetId');

    const recent = await this.lastScaleByAdSet();
    const existing = recent.get(adSetId);
    const isAutomaticSource = String(context.source || '').toLowerCase().startsWith('auto') || String(context.source || '').toLowerCase() === 'interval' || String(context.source || '').toLowerCase() === 'startup';
    if (existing && !dryRun && isAutomaticSource) {
      const lastAt = new Date(existing.startedAt).getTime();
      const nextAt = new Date(lastAt + this.scaleWindowHours * 60 * 60 * 1000);
      throw new Error(`Ad Set đã scale trong 24h. Được scale lại sau ${nextAt.toLocaleString('vi-VN')}`);
    }

    const adSet = await this.metaAdsSyncService.getAdSetForAutopilot(adSetId);
    const metaCampaignId = String(adSet?.campaign_id || adSet?.campaignId || '').trim();
    const adSetBudget = n(adSet?.daily_budget ?? adSet?.dailyBudget);

    let budgetLevel: 'ADSET' | 'CAMPAIGN' = 'ADSET';
    let budgetEntityId = adSetId;
    let currentBudget = adSetBudget;

    // Nếu Ad Set không có daily_budget thì account đang dùng Advantage Campaign Budget / CBO.
    // Khi đó phải scale ngân sách Campaign, không thể POST daily_budget vào Ad Set.
    if (!currentBudget && metaCampaignId) {
      const campaign = await this.metaAdsSyncService.getCampaignForAutopilot(metaCampaignId);
      currentBudget = n(campaign?.daily_budget ?? campaign?.dailyBudget);
      if (currentBudget) {
        budgetLevel = 'CAMPAIGN';
        budgetEntityId = metaCampaignId;
      }
    }

    if (!currentBudget) {
      throw new Error('Không tìm thấy daily_budget ở Ad Set hoặc Campaign. Có thể quảng cáo đang dùng lifetime budget.');
    }

    const safePercent = Math.min(50, Math.max(1, Number(percent) || this.scalePercent));
    const nextBudget = Math.round(currentBudget * (1 + safePercent / 100));

    if (!dryRun) {
      if (budgetLevel === 'CAMPAIGN') {
        await this.metaAdsSyncService.setCampaignDailyBudget(budgetEntityId, nextBudget);
      } else {
        await this.metaAdsSyncService.setAdSetDailyBudget(budgetEntityId, nextBudget);
      }
      await this.persistScaleLog({
        metaAdSetId: adSetId,
        metaCampaignId: metaCampaignId || undefined,
        budgetLevel,
        budgetEntityId,
        metaAdId: context.metaAdId,
        source: context.source || 'manual',
        percent: safePercent,
        oldBudget: currentBudget,
        newBudget: nextBudget,
        roas: context.roas,
        spend: context.spend,
      });
    }

    return {
      ok: true,
      dryRun,
      metaAdSetId: adSetId,
      metaCampaignId: metaCampaignId || null,
      budgetLevel,
      budgetEntityId,
      oldBudget: currentBudget,
      newBudget: nextBudget,
      percent: safePercent,
      nextAutoScaleAt: dryRun ? null : new Date(Date.now() + this.scaleWindowHours * 60 * 60 * 1000).toISOString(),
    };
  }

  private runtimeHours(row: AnyRow) {
    const candidates = [row?.createdTime, row?.created_time, row?.updatedTime, row?.updated_time]
      .map((x) => (x ? new Date(x).getTime() : NaN))
      .filter((x) => Number.isFinite(x));
    if (!candidates.length) return 999999;
    // Conservative: sau một thay đổi Meta gần đây cũng chờ đủ 24h trước auto scale.
    const anchor = Math.max(...candidates);
    return Math.max(0, (Date.now() - anchor) / 3_600_000);
  }

  private evaluateCandidate(input: {
    row: AnyRow;
    stock: AnyRow | undefined;
    lastScale: AnyRow | undefined;
    exactRolling24h: boolean;
  }) {
    const { row, stock, lastScale, exactRolling24h } = input;
    const attr = (row?.productAttribution || {}) as AnyRow;
    const roas = n(attr?.realRoasEstimate);
    const spend = n(row?.metrics?.spend);
    const runHours = this.runtimeHours(row);
    const reasons: string[] = [];

    if (!exactRolling24h) reasons.push('Meta chưa trả được rolling 24h chính xác');
    if (attr?.allocationMode !== 'single_ad_family' || n(attr?.confidence) < 80) reasons.push('Attribution chưa đủ chắc');
    if (runHours < this.minRunHours) reasons.push(`Mới chạy ${runHours.toFixed(1)}h < ${this.minRunHours}h`);
    if (roas < this.scaleRoas) reasons.push(`ROAS 24h ${roas.toFixed(2)} < ${this.scaleRoas}`);
    if (spend < this.minSpend) reasons.push(`Spend 24h ${Math.round(spend)} < ${Math.round(this.minSpend)}`);
    if (!stock?.safe) reasons.push(stock?.reason || 'Tồn kho chưa an toàn');

    let nextScaleAt: string | null = null;
    if (lastScale) {
      const lastAt = new Date(lastScale.startedAt).getTime();
      const next = lastAt + this.scaleWindowHours * 60 * 60 * 1000;
      nextScaleAt = new Date(next).toISOString();
      if (Date.now() < next) reasons.push('Đã scale trong 24h');
    }

    return {
      eligible: reasons.length === 0,
      reasons,
      roas,
      spend,
      runHours,
      nextScaleAt,
      attribution: attr,
    };
  }

  async getControlCenter() {
    const now = new Date();
    const since = new Date(now.getTime() - 24 * 60 * 60 * 1000);

    const [rollingRaw, structure, lastScaleMap] = await Promise.all([
      this.metaAdsSyncService.getRolling24hAdInsights(1000),
      this.metaAdsSyncService.getLiveAdsForAutopilot(5000),
      this.lastScaleByAdSet(),
    ]);
    const rolling: any = rollingRaw;

    const attributed = (await this.attributionService.attachProductOrdersToAds(
      ((rolling?.topAds || []) as AnyRow[]),
      {
        since,
        until: now,
        sourceMode: 'facebook',
        orderMode: 'valid',
      },
    )) as AnyRow[];

    const structureByAd = new Map((structure || []).map((row: AnyRow) => [String(row.metaAdId || row.id), row]));
    const merged = attributed.map((row: AnyRow) => ({
      ...row,
      ...(structureByAd.get(String(row.metaAdId || row.id)) || {}),
      metrics: row.metrics || {},
      productAttribution: row.productAttribution || {},
    }));

    // Ads ACTIVE nhưng không có spend 24h vẫn phải xuất hiện để người vận hành bật/tắt được.
    const known = new Set(merged.map((row: AnyRow) => String(row.metaAdId || row.id)));
    for (const row of structure || []) {
      const id = String((row as AnyRow).metaAdId || (row as AnyRow).id || '');
      if (!id || known.has(id)) continue;
      merged.push({ ...(row as AnyRow), metrics: { spend: 0 }, productAttribution: {} });
    }

    const inventoryChecks = await this.inventoryAutopilotService.assessAdsForScale(merged);
    const inventoryByAd = new Map(inventoryChecks.map((x: AnyRow) => [String(x.metaAdId), x]));

    const ads = merged.map((row: AnyRow) => {
      const adId = String(row?.metaAdId || row?.id || '');
      const adSetId = String(row?.metaAdSetId || row?.adSetId || '');
      const stock = inventoryByAd.get(adId) as AnyRow | undefined;
      const evaluation = this.evaluateCandidate({
        row,
        stock,
        lastScale: lastScaleMap.get(adSetId),
        exactRolling24h: rolling?.exactRolling24h === true,
      });
      const attr = evaluation.attribution;
      const internalRevenue = n(attr?.revenue || attr?.orderRevenue);

      return {
        metaAdId: adId,
        adName: row?.name || row?.adName || '',
        metaAdSetId: adSetId,
        adSetName: row?.adSetName || null,
        metaCampaignId: row?.metaCampaignId || row?.campaignId || null,
        campaignName: row?.campaignName || null,
        status: row?.status || null,
        effectiveStatus: row?.effectiveStatus || null,
        thumbnailUrl: row?.thumbnailUrl || row?.imageUrl || null,
        createdTime: row?.createdTime || row?.created_time || null,
        updatedTime: row?.updatedTime || row?.updated_time || null,
        runHours: evaluation.runHours,
        budgetDaily: n(row?.adSetDailyBudget || row?.campaignDailyBudget || row?.dailyBudget || row?.daily_budget),
        budgetLevel: n(row?.adSetDailyBudget) > 0 ? 'ADSET' : n(row?.campaignDailyBudget) > 0 ? 'CAMPAIGN' : null,
        budgetEntityId: n(row?.adSetDailyBudget) > 0 ? adSetId : n(row?.campaignDailyBudget) > 0 ? String(row?.metaCampaignId || row?.campaignId || '') : null,
        spend24h: evaluation.spend,
        revenue24h: internalRevenue,
        roas24h: evaluation.roas,
        orderCount24h: n(attr?.orderCount),
        productId: attr?.productId || null,
        sku: attr?.sku || attr?.familySku || null,
        familySku: attr?.familySku || null,
        productName: attr?.productName || null,
        attributionConfidence: n(attr?.confidence),
        allocationMode: attr?.allocationMode || null,
        sizes: stock?.sizes || [],
        stockSafe: Boolean(stock?.safe),
        stockLevel: stock?.level || 'UNMAPPED',
        stockReason: stock?.reason || 'Chưa match tồn kho',
        autoScaleEligible: evaluation.eligible && isActive(row),
        autoScaleReasons: evaluation.reasons,
        nextScaleAt: evaluation.nextScaleAt,
      };
    });

    return {
      ok: true,
      generatedAt: new Date().toISOString(),
      window: { since: since.toISOString(), until: now.toISOString(), hours: 24 },
      exactRolling24h: rolling?.exactRolling24h === true,
      fallbackReason: rolling?.fallbackReason || null,
      config: this.getStatus(),
      ads,
      summary: {
        total: ads.length,
        active: ads.filter((x: AnyRow) => String(x.effectiveStatus || x.status).toUpperCase() === 'ACTIVE').length,
        paused: ads.filter((x: AnyRow) => String(x.effectiveStatus || x.status).toUpperCase().includes('PAUSED')).length,
        scaleEligible: ads.filter((x: AnyRow) => x.autoScaleEligible).length,
        lowStock: ads.filter((x: AnyRow) => x.stockLevel === 'LOW_STOCK').length,
        criticalStock: ads.filter((x: AnyRow) => x.stockLevel === 'CRITICAL').length,
      },
    };
  }

  async runNow(options: { source?: string; dryRun?: boolean } = {}) {
    if (this.running) return { ok: false, skipped: true, reason: 'Performance autopilot đang chạy' };
    this.running = true;
    const started = Date.now();
    const dryRun = typeof options.dryRun === 'boolean' ? options.dryRun : this.runtimeDryRun;

    try {
      const center = await this.getControlCenter();
      const rows = (center.ads || []).filter((row: AnyRow) => String(row?.effectiveStatus || row?.status || '').toUpperCase() === 'ACTIVE');

      // Một budget target có thể chứa nhiều ads/adsets. Với CBO, nhiều Ad Set dùng chung ngân sách Campaign,
      // nên chỉ được scale Campaign một lần trong mỗi phiên.
      const bestByBudgetTarget = new Map<string, AnyRow>();
      for (const row of rows) {
        const targetId = String(row?.budgetEntityId || row?.metaAdSetId || '');
        if (!targetId) continue;
        const old = bestByBudgetTarget.get(targetId);
        if (!old || n(row?.roas24h) > n(old?.roas24h)) bestByBudgetTarget.set(targetId, row);
      }

      let eligible = 0;
      let suggested = 0;
      let scaled = 0;
      let skipped = 0;
      let failed = 0;
      const results: AnyRow[] = [];

      for (const [, row] of bestByBudgetTarget) {
        const adSetId = String(row?.metaAdSetId || '');
        if (!row.autoScaleEligible) {
          skipped += 1;
          const reason = Array.isArray(row.autoScaleReasons) ? row.autoScaleReasons.join(' · ') : 'Không đủ điều kiện';
          this.pushAction({
            at: new Date().toISOString(),
            type: 'SKIP',
            metaAdId: row.metaAdId,
            metaAdSetId: adSetId,
            adName: row.adName,
            sku: row.sku || null,
            roas: n(row.roas24h),
            spend: n(row.spend24h),
            reason,
          });
          results.push({ metaAdId: row.metaAdId, metaAdSetId: adSetId, action: 'SKIP', reason });
          continue;
        }

        eligible += 1;
        if (this.runtimeLevel !== 'auto') {
          suggested += 1;
          const reason = `Đủ điều kiện: chạy ${n(row.runHours).toFixed(1)}h, ROAS 24h ${n(row.roas24h).toFixed(2)}, tồn an toàn.`;
          this.pushAction({
            at: new Date().toISOString(),
            type: 'SUGGEST_SCALE',
            metaAdId: row.metaAdId,
            metaAdSetId: adSetId,
            adName: row.adName,
            sku: row.sku || null,
            roas: n(row.roas24h),
            spend: n(row.spend24h),
            reason,
          });
          results.push({ metaAdId: row.metaAdId, metaAdSetId: adSetId, action: 'SUGGEST_SCALE' });
          continue;
        }

        try {
          const scaledResult = await this.executeAdSetScale(adSetId, this.scalePercent, dryRun, {
            source: options.source || 'auto',
            metaAdId: row.metaAdId,
            roas: n(row.roas24h),
            spend: n(row.spend24h),
          });
          if (!dryRun) scaled += 1;
          this.pushAction({
            at: new Date().toISOString(),
            type: dryRun ? 'DRY_RUN_SCALE' : 'SCALE',
            metaAdId: row.metaAdId,
            metaAdSetId: adSetId,
            adName: row.adName,
            sku: row.sku || null,
            roas: n(row.roas24h),
            spend: n(row.spend24h),
            oldBudget: scaledResult.oldBudget,
            newBudget: scaledResult.newBudget,
            reason: `ROAS rolling 24h ${n(row.roas24h).toFixed(2)} >= ${this.scaleRoas}; scale +${this.scalePercent}%.`,
          });
          results.push({ ...scaledResult, metaAdId: row.metaAdId, roas24h: row.roas24h });
        } catch (error: any) {
          failed += 1;
          const message = error?.message || String(error);
          this.pushAction({
            at: new Date().toISOString(),
            type: 'ERROR',
            metaAdId: row.metaAdId,
            metaAdSetId: adSetId,
            adName: row.adName,
            sku: row.sku || null,
            roas: n(row.roas24h),
            spend: n(row.spend24h),
            reason: message,
          });
          results.push({ metaAdId: row.metaAdId, metaAdSetId: adSetId, action: 'ERROR', error: message });
        }
      }

      this.lastRunAt = new Date().toISOString();
      this.lastSummary = {
        source: options.source || 'manual',
        dryRun,
        exactRolling24h: center.exactRolling24h,
        scannedBudgetTargets: bestByBudgetTarget.size,
        eligible,
        suggested,
        scaled,
        skipped,
        failed,
        durationMs: Date.now() - started,
      };
      return { ok: true, summary: this.lastSummary, results, status: this.getStatus() };
    } finally {
      this.running = false;
    }
  }
}
