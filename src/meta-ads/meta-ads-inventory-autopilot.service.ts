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
  productAliases: string[];
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

  private extractProductCode(...values: any[]) {
    for (const value of values) {
      const raw = String(value || '').toUpperCase();
      const compact = raw.replace(/[^A-Z0-9]+/g, ' ');
      const match = compact.match(/\b[A-Z]{1,6}\s*\d{2,6}\b/);
      if (match?.[0]) return match[0].replace(/\s+/g, '');
    }
    return '';
  }

  private meaningfulColorTokens(value: any) {
    const stop = new Set(['mau', 'color', 'xanh']);
    return normalizeText(value)
      .split(/\s+/)
      .map((token) => token.trim())
      .filter((token) => token.length >= 2 && !stop.has(token));
  }

  private colorMatchScore(group: ColorStockGroup, rawAdText: string) {
    const text = normalizeText(rawAdText);
    const compact = compactText(rawAdText);
    const colorPhrase = normalizeText(group.color);
    const colorCompact = compactText(group.color);
    const skuAliases = group.skuAliases.map(compactText).filter(Boolean);

    if (skuAliases.some((alias) => alias.length >= 4 && compact.includes(alias))) return 120;
    if (colorPhrase && text.includes(colorPhrase)) return 110;
    if (colorCompact && compact.includes(colorCompact)) return 105;

    const groupTokens = this.meaningfulColorTokens(group.color);
    if (!groupTokens.length) return 0;
    const adTokens = new Set(normalizeText(rawAdText).split(/\s+/).filter(Boolean));
    const matched = groupTokens.filter((token) => adTokens.has(token));
    if (!matched.length) return 0;

    // Cho phép tên quảng cáo dùng tên màu đời thường khác tên màu kho.
    // Ví dụ kho "RÊU TRÀM" nhưng bài ads ghi "Xanh rêu" -> token "reu" vẫn match chắc.
    const ratio = matched.length / groupTokens.length;
    if (matched.length === groupTokens.length) return 95;
    if (matched.length === 1 && groupTokens.length === 1) return 90;
    return 60 + Math.round(ratio * 25);
  }

  private productMatchesAd(group: ColorStockGroup, rawAdText: string) {
    const compact = compactText(rawAdText);
    const productTokens = uniq([
      compactText(group.productCode),
      ...(group.productAliases || []).map(compactText),
    ]).filter((token) => token.length >= 4);
    const skuAliases = group.skuAliases.map(compactText).filter(Boolean);
    return productTokens.some((token) => compact.includes(token)) || skuAliases.some((alias) => alias.length >= 4 && compact.includes(alias));
  }

  private bestGroupForAd(
    ad: any,
    groups: ColorStockGroup[],
    colorCountByProduct: Map<string, number>,
    options: { activeOnly?: boolean } = {},
  ) {
    if (options.activeOnly !== false && !isMetaAdActive(ad)) return { group: null as ColorStockGroup | null, ambiguous: false, score: 0 };

    const raw = [ad?.name, ad?.adName, ad?.adSetName, ad?.campaignName].filter(Boolean).join(' ');
    const candidates = groups
      .filter((group) => this.productMatchesAd(group, raw))
      .map((group) => {
        let score = this.colorMatchScore(group, raw);
        if (!score && (colorCountByProduct.get(group.productCode) || 0) === 1) score = 20;
        return { group, score };
      })
      .filter((item) => item.score > 0)
      .sort((a, b) => b.score - a.score);

    if (!candidates.length) return { group: null as ColorStockGroup | null, ambiguous: false, score: 0 };
    const best = candidates[0];
    const second = candidates[1];
    const ambiguous = Boolean(second && second.score === best.score && second.group.colorKey !== best.group.colorKey);
    return { group: ambiguous ? null : best.group, ambiguous, score: best.score, candidates: candidates.slice(0, 5) };
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
      const productName = String(product?.name || variant?.productName || product?.slug || product?.code || 'Sản phẩm').trim();
      const sku = String(variant?.sku || '').trim().toUpperCase();
      const productCode = this.extractProductCode(product?.code, product?.slug, productName, sku)
        || String(product?.code || product?.slug || '').trim().toUpperCase();
      const color = String(variant?.color || '').trim();
      const colorNormalized = normalizeText(color);
      const size = this.sizeLabel(variant?.size);

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
        productAliases: new Set<string>(),
        skuAliases: new Set<string>(),
        sizes: new Map<string, any>(),
      };

      [productCode, product?.code, product?.slug, this.extractProductCode(productName, sku)]
        .map((value) => String(value || '').trim())
        .filter(Boolean)
        .forEach((value) => group.productAliases.add(value));

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
        productAliases: Array.from(group.productAliases),
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


  private pushAction(action: AutopilotAction) {
    this.recentActions = [action, ...this.recentActions].slice(0, 100);
  }


  async assessAdsForScale(ads: any[]) {
    const groups = await this.loadInventoryGroups();
    const colorCountByProduct = new Map<string, number>();
    for (const group of groups) {
      colorCountByProduct.set(group.productCode, (colorCountByProduct.get(group.productCode) || 0) + 1);
    }

    return (ads || []).map((ad: any) => {
      const matched = this.bestGroupForAd(ad, groups, colorCountByProduct, { activeOnly: false });

      if (!matched.group) {
        return {
          metaAdId: String(ad?.metaAdId || ad?.id || ''),
          safe: false,
          level: matched.ambiguous ? 'AMBIGUOUS' : 'UNMAPPED',
          sizes: [],
          reason: matched.ambiguous
            ? 'Tên ads match nhiều màu với cùng độ tin cậy, cần kiểm tra mapping'
            : 'Chưa match chắc chắn mã + màu với tồn kho',
          groups: [],
        };
      }

      const group = matched.group;
      const hasCritical = group.criticalSizes.length > 0;
      const hasLow = group.lowSizes.length > 0;
      const level = hasCritical ? 'CRITICAL' : hasLow ? 'LOW_STOCK' : 'NORMAL';

      return {
        metaAdId: String(ad?.metaAdId || ad?.id || ''),
        safe: !hasLow,
        level,
        matchScore: matched.score,
        colorKey: group.colorKey,
        productId: group.productId,
        productCode: group.productCode,
        productName: group.productName,
        color: group.color,
        sizes: group.sizes.map((row) => ({ size: row.size, qty: row.qty })),
        totalQty: group.totalQty,
        minQty: group.minQty,
        lowSizes: group.lowSizes,
        criticalSizes: group.criticalSizes,
        reason: !hasLow
          ? `Đã match ${group.productCode} / ${group.color}; tồn tất cả size >= ngưỡng an toàn`
          : `${group.colorKey}: size ${group.lowSizes.join(', ')} dưới ${this.warnThreshold}`,
        groups: [group],
      };
    });
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

      const bestGroupByAd = new Map<string, string>();
      for (const ad of ads || []) {
        const best = this.bestGroupForAd(ad, groups, colorCountByProduct, { activeOnly: true });
        if (best.group) bestGroupByAd.set(String(ad?.metaAdId || ad?.id || ''), best.group.key);
      }

      for (const group of critical) {
        const matched = (ads || []).filter((ad: any) =>
          isMetaAdActive(ad) && bestGroupByAd.get(String(ad?.metaAdId || ad?.id || '')) === group.key,
        );
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
