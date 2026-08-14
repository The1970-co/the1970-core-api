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

  onModuleInit() {
    this.restartTimer();
    if (this.runtimeEnabled) setTimeout(() => void this.runNow({ source: 'startup' }), 20_000);
  }

  onModuleDestroy() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  private restartTimer() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    if (!this.runtimeEnabled) return;
    this.timer = setInterval(() => void this.runNow({ source: 'interval' }), this.intervalMs);
  }

  setRuntimeConfig(input: any = {}) {
    if (typeof input.enabled === 'boolean') this.runtimeEnabled = input.enabled;
    if (typeof input.dryRun === 'boolean') this.runtimeDryRun = input.dryRun;
    if (input.level) this.runtimeLevel = this.envLevel(input.level);
    if (Number.isFinite(Number(input.scaleRoas))) this.scaleRoas = Math.max(0.1, Number(input.scaleRoas));
    if (Number.isFinite(Number(input.scalePercent))) this.scalePercent = Math.min(50, Math.max(1, Number(input.scalePercent)));
    if (Number.isFinite(Number(input.minSpend))) this.minSpend = Math.max(0, Number(input.minSpend));
    this.restartTimer();
    return this.getStatus();
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

  private async persistScaleLog(input: {
    metaAdSetId: string;
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
    const currentBudget = n(adSet?.daily_budget ?? adSet?.dailyBudget);
    if (!currentBudget) throw new Error('Ad Set không có daily_budget để scale');

    const safePercent = Math.min(50, Math.max(1, Number(percent) || this.scalePercent));
    const nextBudget = Math.round(currentBudget * (1 + safePercent / 100));

    if (!dryRun) {
      await this.metaAdsSyncService.setAdSetDailyBudget(adSetId, nextBudget);
      await this.persistScaleLog({
        metaAdSetId: adSetId,
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
      oldBudget: currentBudget,
      newBudget: nextBudget,
      percent: safePercent,
      nextAutoScaleAt: dryRun ? null : new Date(Date.now() + this.scaleWindowHours * 60 * 60 * 1000).toISOString(),
    };
  }

  private runtimeHours(row: AnyRow) {
    // Tuổi chạy phải tính từ lúc ad/ad set bắt đầu, không dùng updated_time.
    // updated_time thay đổi khi chỉnh ngân sách/trạng thái và sẽ làm reset giả thời gian chạy.
    const candidates = [row?.createdTime, row?.created_time, row?.adSetStartTime, row?.startTime, row?.start_time]
      .map((x) => (x ? new Date(x).getTime() : NaN))
      .filter((x) => Number.isFinite(x));
    if (!candidates.length) return 999999;
    const anchor = Math.min(...candidates);
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

    // Structure là nguồn chính để dựng danh sách điều khiển.
    // Metrics/attribution/inventory có lỗi thì vẫn phải trả ads để admin bật/tắt thủ công.
    let structure: AnyRow[] = [];
    let structureError: string | null = null;
    try {
      structure = (await this.metaAdsSyncService.getLiveAdsForAutopilot(5000)) as AnyRow[];
    } catch (error: any) {
      structureError = error?.message || String(error);
      this.logger.error(`[AUTOPILOT_CONTROL_STRUCTURE] ${structureError}`);
    }

    let rolling: AnyRow = {
      ok: false,
      topAds: [],
      exactRolling24h: false,
      fallbackReason: 'Chưa tải được rolling 24h',
    };
    try {
      rolling = (await this.metaAdsSyncService.getRolling24hAdInsights(1000)) as AnyRow;
    } catch (error: any) {
      rolling = {
        ok: false,
        topAds: [],
        exactRolling24h: false,
        fallbackReason: error?.message || String(error),
      };
      this.logger.warn(`[AUTOPILOT_CONTROL_ROLLING] ${rolling.fallbackReason}`);
    }

    const lastScaleMap = await this.lastScaleByAdSet();

    let attributed: AnyRow[] = ((rolling?.topAds || []) as AnyRow[]).map((row) => ({
      ...row,
      metrics: row?.metrics || {},
      productAttribution: row?.productAttribution || {},
    }));

    try {
      attributed = (await this.attributionService.attachProductOrdersToAds(
        ((rolling?.topAds || []) as AnyRow[]),
        {
          since,
          until: now,
          sourceMode: 'facebook',
          orderMode: 'valid',
        },
      )) as AnyRow[];
    } catch (error: any) {
      // Attribution lỗi không được làm mất danh sách ads live.
      this.logger.warn(`[AUTOPILOT_CONTROL_ATTRIBUTION] ${error?.message || error}`);
    }

    const structureByAd = new Map(
      (structure || []).map((row: AnyRow) => [String(row.metaAdId || row.id), row]),
    );

    const merged = attributed.map((row: AnyRow) => {
      const live = structureByAd.get(String(row.metaAdId || row.id)) || {};
      return {
        ...row,
        ...live,
        // Giữ metrics + attribution của rolling, không để structure ghi đè.
        metrics: row?.metrics || {},
        productAttribution: row?.productAttribution || {},
      };
    });

    // Ads không có spend 24h vẫn phải xuất hiện để bật/tắt và scale thủ công.
    const known = new Set(merged.map((row: AnyRow) => String(row.metaAdId || row.id)));
    for (const row of structure || []) {
      const id = String((row as AnyRow).metaAdId || (row as AnyRow).id || '');
      if (!id || known.has(id)) continue;
      merged.push({
        ...(row as AnyRow),
        metrics: { spend: 0 },
        productAttribution: {},
      });
    }

    let inventoryChecks: AnyRow[] = [];
    try {
      inventoryChecks = (await this.inventoryAutopilotService.assessAdsForScale(merged)) as AnyRow[];
    } catch (error: any) {
      // Inventory lỗi cũng không được làm biến mất ads khỏi Control Center.
      this.logger.warn(`[AUTOPILOT_CONTROL_INVENTORY] ${error?.message || error}`);
    }
    const inventoryByAd = new Map(
      inventoryChecks.map((x: AnyRow) => [String(x.metaAdId), x]),
    );

    const ads = merged
      .map((row: AnyRow) => {
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
          adSetStartTime: row?.adSetStartTime || null,
          runHours: evaluation.runHours,
          budgetDaily: n(row?.adSetDailyBudget || row?.dailyBudget || row?.daily_budget),
          spend24h: evaluation.spend,
          revenue24h: internalRevenue,
          roas24h: evaluation.roas,
          orderCount24h: n(attr?.orderCount),
          productId: attr?.productId || stock?.productId || null,
          sku: attr?.sku || attr?.familySku || stock?.colorKey || null,
          familySku: attr?.familySku || stock?.colorKey || null,
          productName: attr?.productName || stock?.productName || null,
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
      })
      .filter((row: AnyRow) => Boolean(row.metaAdId));

    return {
      ok: structure.length > 0 || ads.length > 0,
      generatedAt: new Date().toISOString(),
      window: { since: since.toISOString(), until: now.toISOString(), hours: 24 },
      exactRolling24h: rolling?.exactRolling24h === true,
      fallbackReason: rolling?.fallbackReason || null,
      structureError,
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

      // Một Ad Set có thể chứa nhiều ads; chỉ scale một lần, lấy ads có ROAS 24h cao nhất làm đại diện.
      const bestByAdSet = new Map<string, AnyRow>();
      for (const row of rows) {
        const adSetId = String(row?.metaAdSetId || '');
        if (!adSetId) continue;
        const old = bestByAdSet.get(adSetId);
        if (!old || n(row?.roas24h) > n(old?.roas24h)) bestByAdSet.set(adSetId, row);
      }

      let eligible = 0;
      let suggested = 0;
      let scaled = 0;
      let skipped = 0;
      let failed = 0;
      const results: AnyRow[] = [];

      for (const [adSetId, row] of bestByAdSet) {
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
        scannedAdSets: bestByAdSet.size,
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
