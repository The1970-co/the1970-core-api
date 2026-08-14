import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { MetaAdsSyncService } from './meta-ads-sync.service';
import { MetaAdsOrderAttributionService } from './meta-ads-order-attribution.service';
import { MetaAdsInventoryAutopilotService } from './meta-ads-inventory-autopilot.service';

type AutomationLevel = 'manual' | 'semi' | 'auto';

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
  private scaleRoas = this.envNumber('META_ADS_SCALE_ROAS', 3);
  private scalePercent = this.envNumber('META_ADS_SCALE_PERCENT', 20);
  private minSpend = this.envNumber('META_ADS_SCALE_MIN_SPEND', 200000);
  private cooldownMinutes = this.envNumber('META_ADS_SCALE_COOLDOWN_MINUTES', 360);
  private maxScalePerAdSetPerDay = this.envNumber('META_ADS_SCALE_MAX_PER_ADSET_PER_DAY', 2);
  private lastRunAt: string | null = null;
  private lastSummary: any = null;
  private actions: PerformanceAction[] = [];
  private scaleHistory = new Map<string, string[]>();

  constructor(
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
    if (Number.isFinite(Number(input.cooldownMinutes))) this.cooldownMinutes = Math.max(30, Number(input.cooldownMinutes));
    if (Number.isFinite(Number(input.maxScalePerAdSetPerDay))) this.maxScalePerAdSetPerDay = Math.max(1, Math.round(Number(input.maxScalePerAdSetPerDay)));
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
      cooldownMinutes: this.cooldownMinutes,
      maxScalePerAdSetPerDay: this.maxScalePerAdSetPerDay,
      lastRunAt: this.lastRunAt,
      lastSummary: this.lastSummary,
      recentActions: this.actions.slice(0, 100),
      rule: `ROAS >= ${this.scaleRoas}, spend >= ${this.minSpend}, tồn mọi size >= 10; scale ad set +${this.scalePercent}%.`,
    };
  }

  private hcmTodayRange() {
    const ymd = new Intl.DateTimeFormat('en-CA', {
      timeZone: 'Asia/Ho_Chi_Minh', year: 'numeric', month: '2-digit', day: '2-digit',
    }).format(new Date());
    return {
      since: new Date(`${ymd}T00:00:00.000+07:00`),
      until: new Date(`${ymd}T23:59:59.999+07:00`),
    };
  }

  private pushAction(action: PerformanceAction) {
    this.actions = [action, ...this.actions].slice(0, 200);
  }

  private cleanHistory(adSetId: string) {
    const now = Date.now();
    const oneDay = 24 * 60 * 60 * 1000;
    const rows = (this.scaleHistory.get(adSetId) || []).filter((iso) => now - new Date(iso).getTime() < oneDay);
    this.scaleHistory.set(adSetId, rows);
    return rows;
  }

  private cooldownOk(adSetId: string) {
    const rows = this.cleanHistory(adSetId);
    if (!rows.length) return true;
    const latest = Math.max(...rows.map((x) => new Date(x).getTime()));
    return Date.now() - latest >= this.cooldownMinutes * 60_000;
  }

  private recordScale(adSetId: string) {
    const rows = this.cleanHistory(adSetId);
    rows.push(new Date().toISOString());
    this.scaleHistory.set(adSetId, rows);
  }

  async executeAdSetScale(metaAdSetId: string, percent = this.scalePercent, dryRun = this.runtimeDryRun) {
    const adSet = await this.metaAdsSyncService.getAdSetForAutopilot(metaAdSetId);
    const currentBudget = n(adSet?.daily_budget);
    if (!currentBudget) throw new Error('Ad set không có daily_budget để scale');
    const nextBudget = Math.round(currentBudget * (1 + Number(percent) / 100));
    if (!dryRun) await this.metaAdsSyncService.setAdSetDailyBudget(metaAdSetId, nextBudget);
    return { ok: true, dryRun, metaAdSetId, oldBudget: currentBudget, newBudget: nextBudget, percent };
  }

  async runNow(options: { source?: string; dryRun?: boolean } = {}) {
    if (this.running) return { ok: false, skipped: true, reason: 'Performance autopilot đang chạy' };
    this.running = true;
    const started = Date.now();
    const dryRun = typeof options.dryRun === 'boolean' ? options.dryRun : this.runtimeDryRun;

    try {
      const live = await this.metaAdsSyncService.getLiveInsights({ range: 'today', level: 'ad', limit: 1000 });
      const range = this.hcmTodayRange();
      const attributed = (await this.attributionService.attachProductOrdersToAds(
        (live?.topAds || []) as any[],
        {
        since: range.since,
        until: range.until,
        sourceMode: 'facebook',
          orderMode: 'valid',
        },
      )) as any[];

      const activeRows: any[] = attributed.filter((row: any) => isActive(row) && row?.metaAdSetId);
      const inventoryChecks = await this.inventoryAutopilotService.assessAdsForScale(activeRows);
      const inventoryByAd = new Map(inventoryChecks.map((x: any) => [String(x.metaAdId), x]));

      // Một ad set chỉ scale tối đa 1 lần mỗi vòng, lấy ad có ROAS nội bộ tốt nhất.
      const bestByAdSet = new Map<string, any>();
      for (const row of activeRows) {
        const attr = row?.productAttribution || {};
        const roas = n(attr?.realRoasEstimate);
        const spend = n(row?.metrics?.spend);
        const adSetId = String(row?.metaAdSetId || '');
        const candidate = { row, roas, spend, attr };
        const old = bestByAdSet.get(adSetId);
        if (!old || roas > old.roas) bestByAdSet.set(adSetId, candidate);
      }

      let eligible = 0;
      let suggested = 0;
      let scaled = 0;
      let skipped = 0;
      let failed = 0;
      const results: any[] = [];

      for (const [adSetId, candidate] of bestByAdSet) {
        const { row, roas, spend, attr } = candidate;
        const adId = String(row?.metaAdId || row?.id || '');
        const stock = inventoryByAd.get(adId) as any;
        const reasons: string[] = [];

        if (attr?.allocationMode !== 'single_ad_family' || n(attr?.confidence) < 80) reasons.push('Attribution chưa đủ chắc để auto scale');
        if (roas < this.scaleRoas) reasons.push(`ROAS ${roas.toFixed(2)} < ${this.scaleRoas}`);
        if (spend < this.minSpend) reasons.push(`Spend ${Math.round(spend)} < ${Math.round(this.minSpend)}`);
        if (!stock?.safe) reasons.push(stock?.reason || 'Tồn kho chưa an toàn');
        if (!this.cooldownOk(adSetId)) reasons.push(`Ad set đang trong cooldown ${this.cooldownMinutes} phút`);
        if (this.cleanHistory(adSetId).length >= this.maxScalePerAdSetPerDay) reasons.push(`Đã đủ ${this.maxScalePerAdSetPerDay} lần scale/24h`);

        if (reasons.length) {
          skipped += 1;
          results.push({ metaAdId: adId, metaAdSetId: adSetId, roas, spend, action: 'SKIP', reasons });
          continue;
        }

        eligible += 1;
        const reason = `ROAS ${roas.toFixed(2)} >= ${this.scaleRoas}; spend đủ; tồn size an toàn; attribution chắc.`;

        if (this.runtimeLevel === 'manual') {
          suggested += 1;
          this.pushAction({ at: new Date().toISOString(), type: 'SUGGEST_SCALE', metaAdId: adId, metaAdSetId: adSetId, adName: row?.name, sku: attr?.sku || null, roas, spend, reason });
          results.push({ metaAdId: adId, metaAdSetId: adSetId, roas, spend, action: 'SUGGEST' });
          continue;
        }

        if (this.runtimeLevel === 'semi') {
          suggested += 1;
          this.pushAction({ at: new Date().toISOString(), type: 'SUGGEST_SCALE', metaAdId: adId, metaAdSetId: adSetId, adName: row?.name, sku: attr?.sku || null, roas, spend, reason: `${reason} Chờ bấm xác nhận.` });
          results.push({ metaAdId: adId, metaAdSetId: adSetId, roas, spend, action: 'SUGGEST' });
          continue;
        }

        try {
          const execution = await this.executeAdSetScale(adSetId, this.scalePercent, dryRun);
          if (!dryRun) this.recordScale(adSetId);
          scaled += 1;
          this.pushAction({
            at: new Date().toISOString(), type: dryRun ? 'DRY_RUN_SCALE' : 'SCALE', metaAdId: adId, metaAdSetId: adSetId,
            adName: row?.name, sku: attr?.sku || null, roas, spend, oldBudget: execution.oldBudget, newBudget: execution.newBudget,
            reason: `${reason} Scale +${this.scalePercent}%.`,
          });
          results.push({ ...execution, metaAdId: adId, roas, spend, action: dryRun ? 'DRY_RUN_SCALE' : 'SCALE' });
        } catch (error: any) {
          failed += 1;
          const message = error?.message || String(error);
          this.pushAction({ at: new Date().toISOString(), type: 'ERROR', metaAdId: adId, metaAdSetId: adSetId, adName: row?.name, sku: attr?.sku || null, roas, spend, reason: message });
          results.push({ metaAdId: adId, metaAdSetId: adSetId, roas, spend, action: 'ERROR', error: message });
        }
      }

      this.lastRunAt = new Date().toISOString();
      this.lastSummary = { source: options.source || 'manual', dryRun, level: this.runtimeLevel, scannedAds: activeRows.length, scannedAdSets: bestByAdSet.size, eligible, suggested, scaled, skipped, failed };
      this.logger.log(`[META_PERFORMANCE_AUTOPILOT] ${JSON.stringify(this.lastSummary)}`);
      return { ok: true, ...this.lastSummary, durationMs: Date.now() - started, results };
    } finally {
      this.running = false;
    }
  }
}
