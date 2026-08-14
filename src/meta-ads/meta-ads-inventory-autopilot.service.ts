import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { MetaAdsSyncService } from './meta-ads-sync.service';

type AutopilotLevel = 'NORMAL' | 'READY_TO_PAUSE' | 'AUTO_PAUSED' | 'CRITICAL_NO_AD_MATCH';

type SizeStock = {
  size: string;
  qty: number;
  variantIds: string[];
  skus: string[];
};

type ColorStockGroup = {
  key: string;
  productId: string;
  productCode: string;
  productName: string;
  color: string;
  colorKey: string;
  skuAliases: string[];
  sizes: SizeStock[];
  totalQty: number;
  minQty: number;
  lowSizes: string[];
  criticalSizes: string[];
  level: AutopilotLevel;
};

type AutopilotAction = {
  at: string;
  type: 'WARNING' | 'PAUSE' | 'DRY_RUN_PAUSE' | 'NO_MATCH' | 'ERROR';
  colorKey: string;
  productName: string;
  sizes: Array<{ size: string; qty: number }>;
  metaAdId?: string;
  adName?: string;
  campaignName?: string | null;
  reason: string;
};

function n(value: any) {
  const num = Number(value || 0);
  return Number.isFinite(num) ? num : 0;
}

function normalizeText(value: any) {
  return String(value || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/đ/g, 'd')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function compactText(value: any) {
  return normalizeText(value).replace(/\s+/g, '');
}

function uniq<T>(items: T[]) {
  return Array.from(new Set(items));
}

function isMetaAdActive(row: any) {
  const status = String(row?.effectiveStatus || row?.status || '').toUpperCase();
  if (!status) return false;
  if (status.includes('PAUSED') || status.includes('INACTIVE') || status.includes('ARCHIVED') || status.includes('DELETED')) return false;
  return status === 'ACTIVE' || status.includes('ACTIVE');
}

@Injectable()
export class MetaAdsInventoryAutopilotService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(MetaAdsInventoryAutopilotService.name);
  private timer: ReturnType<typeof setInterval> | null = null;
  private running = false;
  private runtimeEnabled = this.envBool('META_ADS_INVENTORY_AUTOPILOT_ENABLED', false);
  private runtimeDryRun = this.envBool('META_ADS_INVENTORY_AUTOPILOT_DRY_RUN', true);
  private lastRunAt: string | null = null;
  private lastRunDurationMs = 0;
  private lastSummary: any = null;
  private recentActions: AutopilotAction[] = [];

  constructor(
    private readonly prisma: PrismaService,
    private readonly metaAdsSyncService: MetaAdsSyncService,
  ) {}

  private envBool(key: string, fallback: boolean) {
    const raw = String(process.env[key] ?? '').trim().toLowerCase();
    if (!raw) return fallback;
    return ['1', 'true', 'yes', 'on'].includes(raw);
  }

  private get warnThreshold() {
    return Math.max(1, Number(process.env.META_ADS_INVENTORY_WARN_THRESHOLD || 10));
  }

  private get pauseThreshold() {
    return Math.max(0, Number(process.env.META_ADS_INVENTORY_PAUSE_THRESHOLD || 5));
  }

  private get intervalMs() {
    return Math.max(60_000, Number(process.env.META_ADS_INVENTORY_INTERVAL_MS || 300_000));
  }

  onModuleInit() {
    this.restartTimer();
    if (this.runtimeEnabled) {
      setTimeout(() => void this.runNow({ source: 'startup' }), 15_000);
    }
  }

  onModuleDestroy() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  private restartTimer() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    if (!this.runtimeEnabled) return;

    this.timer = setInterval(() => {
      void this.runNow({ source: 'interval' });
    }, this.intervalMs);
  }

  setRuntimeConfig(input: { enabled?: boolean; dryRun?: boolean }) {
    if (typeof input?.enabled === 'boolean') this.runtimeEnabled = input.enabled;
    if (typeof input?.dryRun === 'boolean') this.runtimeDryRun = input.dryRun;
    this.restartTimer();
    return this.getStatus();
  }

  getStatus() {
    return {
      ok: true,
      enabled: this.runtimeEnabled,
      dryRun: this.runtimeDryRun,
      running: this.running,
      warnThreshold: this.warnThreshold,
      pauseThreshold: this.pauseThreshold,
      intervalMs: this.intervalMs,
      intervalMinutes: Math.round(this.intervalMs / 60_000),
      rule: `Cảnh báo khi bất kỳ size < ${this.warnThreshold}; pause ad con khi bất kỳ size < ${this.pauseThreshold}.`,
      lastRunAt: this.lastRunAt,
      lastRunDurationMs: this.lastRunDurationMs,
      lastSummary: this.lastSummary,
      recentActions: this.recentActions.slice(0, 50),
    };
  }

  private sizeLabel(value: any) {
    return String(value || '').trim().toUpperCase() || 'ONE';
  }

  private buildColorSkuAlias(sku: string, size: string) {
    const rawSku = String(sku || '').trim().toUpperCase();
    const rawSize = String(size || '').trim().toUpperCase();
    if (!rawSku) return '';
    if (!rawSize) return rawSku;

    const escaped = rawSize.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    return rawSku.replace(new RegExp(`[-_\\s/]+${escaped}$`, 'i'), '');
  }

  private async loadInventoryGroups(): Promise<ColorStockGroup[]> {
    const variants = await (this.prisma as any).productVariant.findMany({
      include: {
        product: true,
        inventoryItems: true,
      },
      take: 30000,
    });

    const groups = new Map<string, any>();

    for (const variant of variants || []) {
      const product = variant?.product || {};
      const productId = String(product?.id || variant?.productId || '').trim();
      const productCode = String(product?.slug || product?.code || '').trim().toUpperCase();
      const productName = String(product?.name || variant?.productName || productCode || 'Sản phẩm').trim();
      const color = String(variant?.color || '').trim();
      const colorNormalized = normalizeText(color);
      const size = this.sizeLabel(variant?.size);
      const sku = String(variant?.sku || '').trim().toUpperCase();

      // Auto-pause chỉ xử lý được khi xác định rõ mã sản phẩm + màu.
      if (!productCode || !colorNormalized) continue;

      const groupKey = `${productId || productCode}::${colorNormalized}`;
      const group = groups.get(groupKey) || {
        key: groupKey,
        productId,
        productCode,
        productName,
        color,
        colorKey: `${productCode}-${String(color).trim().toUpperCase()}`,
        skuAliases: new Set<string>(),
        sizes: new Map<string, any>(),
      };

      if (sku) {
        group.skuAliases.add(sku);
        const colorAlias = this.buildColorSkuAlias(sku, size);
        if (colorAlias) group.skuAliases.add(colorAlias);
      }

      const qty = Array.isArray(variant?.inventoryItems)
        ? variant.inventoryItems.reduce((sum: number, item: any) => sum + n(item?.availableQty), 0)
        : 0;

      const sizeRow = group.sizes.get(size) || { size, qty: 0, variantIds: [], skus: [] };
      sizeRow.qty += qty;
      if (variant?.id) sizeRow.variantIds.push(String(variant.id));
      if (sku) sizeRow.skus.push(sku);
      group.sizes.set(size, sizeRow);
      groups.set(groupKey, group);
    }

    return Array.from(groups.values()).map((group: any) => {
      const sizes: SizeStock[] = Array.from(group.sizes.values()).sort((a: any, b: any) =>
        String(a.size).localeCompare(String(b.size), 'vi', { numeric: true }),
      ) as SizeStock[];
      const totalQty = sizes.reduce((sum: number, row: SizeStock) => sum + row.qty, 0);
      const minQty = sizes.length ? Math.min(...sizes.map((row) => row.qty)) : 0;
      const lowSizes = sizes.filter((row) => row.qty < this.warnThreshold).map((row) => row.size);
      const criticalSizes = sizes.filter((row) => row.qty < this.pauseThreshold).map((row) => row.size);
      const level: AutopilotLevel = criticalSizes.length ? 'AUTO_PAUSED' : lowSizes.length ? 'READY_TO_PAUSE' : 'NORMAL';

      return {
        key: group.key,
        productId: group.productId,
        productCode: group.productCode,
        productName: group.productName,
        color: group.color,
        colorKey: group.colorKey,
        skuAliases: Array.from(group.skuAliases),
        sizes,
        totalQty,
        minQty,
        lowSizes,
        criticalSizes,
        level,
      };
    });
  }

  private matchAdsForGroup(group: ColorStockGroup, ads: any[], productColorCount: number) {
    const productToken = compactText(group.productCode);
    const colorPhrase = normalizeText(group.color);
    const colorCompact = compactText(group.color);
    const skuAliases = group.skuAliases.map(compactText).filter(Boolean);

    return ads.filter((ad) => {
      if (!isMetaAdActive(ad)) return false;

      const raw = [ad?.name, ad?.adSetName, ad?.campaignName].filter(Boolean).join(' ');
      const text = normalizeText(raw);
      const compact = compactText(raw);

      const hasProduct = Boolean(productToken && compact.includes(productToken));
      const hasSkuAlias = skuAliases.some((alias) => alias.length >= 4 && compact.includes(alias));
      if (!hasProduct && !hasSkuAlias) return false;

      const hasColor = Boolean(
        (colorPhrase && text.includes(colorPhrase)) ||
        (colorCompact && compact.includes(colorCompact)) ||
        skuAliases.some((alias) => alias.includes(productToken) && compact.includes(alias)),
      );

      // Nếu một mã chỉ có đúng 1 màu thì mã chính là đủ. Có nhiều màu bắt buộc phải match màu rõ ràng.
      return hasColor || productColorCount === 1;
    });
  }

  async assessAdsForScale(ads: any[]) {
    const groups = await this.loadInventoryGroups();
    const colorCountByProduct = new Map<string, number>();
    for (const group of groups) {
      colorCountByProduct.set(group.productCode, (colorCountByProduct.get(group.productCode) || 0) + 1);
    }

    return (ads || []).map((ad) => {
      const matchedGroups = groups.filter((group) =>
        this.matchAdsForGroup(group, [ad], colorCountByProduct.get(group.productCode) || 1).length > 0,
      );
      if (!matchedGroups.length) {
        return { metaAdId: String(ad?.metaAdId || ad?.id || ''), safe: false, reason: 'Không match chắc chắn được mã + màu để kiểm tồn', groups: [] };
      }
      const unsafe = matchedGroups.filter((group) => group.lowSizes.length > 0);
      return {
        metaAdId: String(ad?.metaAdId || ad?.id || ''),
        safe: unsafe.length === 0,
        reason: unsafe.length
          ? `Không scale: ${unsafe.map((g) => `${g.colorKey} có size ${g.lowSizes.join(', ')} dưới ${this.warnThreshold}`).join('; ')}`
          : 'Tồn size an toàn để scale',
        groups: matchedGroups.map((g) => ({ colorKey: g.colorKey, sizes: g.sizes.map((x) => ({ size: x.size, qty: x.qty })), lowSizes: g.lowSizes, criticalSizes: g.criticalSizes })),
      };
    });
  }

  private pushAction(action: AutopilotAction) {
    this.recentActions = [action, ...this.recentActions].slice(0, 100);
  }

  async runNow(options: { source?: string; dryRun?: boolean } = {}) {
    if (this.running) {
      return { ok: false, skipped: true, reason: 'Autopilot đang chạy một phiên khác', status: this.getStatus() };
    }

    const started = Date.now();
    const dryRun = typeof options.dryRun === 'boolean' ? options.dryRun : this.runtimeDryRun;
    this.running = true;

    try {
      const [groups, ads] = await Promise.all([
        this.loadInventoryGroups(),
        this.metaAdsSyncService.getLiveAdsForAutopilot(5000),
      ]);

      const colorCountByProduct = new Map<string, number>();
      for (const group of groups) {
        colorCountByProduct.set(group.productCode, (colorCountByProduct.get(group.productCode) || 0) + 1);
      }

      const warnings = groups.filter((group) => group.lowSizes.length > 0 && group.criticalSizes.length === 0);
      const critical = groups.filter((group) => group.criticalSizes.length > 0);
      let matchedAds = 0;
      let pausedAds = 0;
      let failedAds = 0;
      let noMatchGroups = 0;
      const results: any[] = [];

      for (const group of warnings) {
        const reason = `Chuẩn bị tắt: size ${group.lowSizes.join(', ')} dưới ${this.warnThreshold}; chưa size nào dưới ${this.pauseThreshold}.`;
        this.pushAction({
          at: new Date().toISOString(),
          type: 'WARNING',
          colorKey: group.colorKey,
          productName: group.productName,
          sizes: group.sizes.map((row) => ({ size: row.size, qty: row.qty })),
          reason,
        });
      }

      for (const group of critical) {
        const matched = this.matchAdsForGroup(group, ads, colorCountByProduct.get(group.productCode) || 1);
        matchedAds += matched.length;

        if (!matched.length) {
          noMatchGroups += 1;
          group.level = 'CRITICAL_NO_AD_MATCH';
          const reason = `Có size ${group.criticalSizes.join(', ')} dưới ${this.pauseThreshold} nhưng không match chắc chắn được ad ACTIVE đúng mã + màu.`;
          this.pushAction({
            at: new Date().toISOString(),
            type: 'NO_MATCH',
            colorKey: group.colorKey,
            productName: group.productName,
            sizes: group.sizes.map((row) => ({ size: row.size, qty: row.qty })),
            reason,
          });
          results.push({ group, matchedAds: [], action: 'NO_MATCH' });
          continue;
        }

        const adResults: any[] = [];
        for (const ad of matched) {
          const reason = `Auto pause vì ${group.colorKey}: size ${group.criticalSizes.join(', ')} dưới ${this.pauseThreshold}. Chỉ pause ad con, không pause adset/campaign.`;
          try {
            if (!dryRun) {
              await this.metaAdsSyncService.setAdStatus(ad.metaAdId || ad.id, 'PAUSED');
              pausedAds += 1;
            }
            this.pushAction({
              at: new Date().toISOString(),
              type: dryRun ? 'DRY_RUN_PAUSE' : 'PAUSE',
              colorKey: group.colorKey,
              productName: group.productName,
              sizes: group.sizes.map((row) => ({ size: row.size, qty: row.qty })),
              metaAdId: ad.metaAdId || ad.id,
              adName: ad.name,
              campaignName: ad.campaignName,
              reason,
            });
            adResults.push({ metaAdId: ad.metaAdId || ad.id, name: ad.name, ok: true, dryRun });
          } catch (error: any) {
            failedAds += 1;
            const message = error?.message || String(error);
            this.pushAction({
              at: new Date().toISOString(),
              type: 'ERROR',
              colorKey: group.colorKey,
              productName: group.productName,
              sizes: group.sizes.map((row) => ({ size: row.size, qty: row.qty })),
              metaAdId: ad.metaAdId || ad.id,
              adName: ad.name,
              campaignName: ad.campaignName,
              reason: message,
            });
            adResults.push({ metaAdId: ad.metaAdId || ad.id, name: ad.name, ok: false, error: message });
          }
        }
        results.push({ group, matchedAds: adResults, action: dryRun ? 'DRY_RUN_PAUSE' : 'PAUSE' });
      }

      const summary = {
        source: options.source || 'manual',
        dryRun,
        scannedColorGroups: groups.length,
        warningGroups: warnings.length,
        criticalGroups: critical.length,
        noMatchGroups,
        activeAds: ads.filter(isMetaAdActive).length,
        matchedAds,
        pausedAds,
        failedAds,
        rule: { warnThreshold: this.warnThreshold, pauseThreshold: this.pauseThreshold },
      };

      this.lastRunAt = new Date().toISOString();
      this.lastRunDurationMs = Date.now() - started;
      this.lastSummary = summary;
      this.logger.log(`[META_INVENTORY_AUTOPILOT] ${JSON.stringify(summary)}`);

      return {
        ok: true,
        ...summary,
        durationMs: this.lastRunDurationMs,
        groups: [...warnings, ...critical].slice(0, 200),
        results: results.slice(0, 200),
      };
    } finally {
      this.running = false;
    }
  }
}
