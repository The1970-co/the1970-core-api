import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { MetaAdsSyncService } from './meta-ads-sync.service';

type AutopilotLevel = 'NORMAL' | 'READY_TO_PAUSE' | 'AUTO_PAUSED' | 'CRITICAL_NO_AD_MATCH';
type AutomationLevel = 'manual' | 'semi' | 'auto';

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
  type: 'WARNING' | 'SUGGEST_PAUSE' | 'PAUSE' | 'DRY_RUN_PAUSE' | 'NO_MATCH' | 'ERROR';
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
  private runtimeLevel: AutomationLevel = this.envLevel(process.env.META_ADS_PERFORMANCE_AUTOMATION_LEVEL || 'manual');
  private runtimeWarnThreshold = Math.max(1, Number(process.env.META_ADS_INVENTORY_WARN_THRESHOLD || 10));
  private runtimePauseThreshold = Math.max(0, Number(process.env.META_ADS_INVENTORY_PAUSE_THRESHOLD || 5));
  private runtimeCriticalSizeCount = Math.max(1, Number(process.env.META_ADS_INVENTORY_CRITICAL_SIZE_COUNT || 2));
  private runtimePauseTotalQty = Math.max(0, Number(process.env.META_ADS_INVENTORY_PAUSE_TOTAL_QTY || 40));
  private runtimeRequireBoth = this.envBool('META_ADS_INVENTORY_REQUIRE_BOTH', true);
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

  private envLevel(value: any): AutomationLevel {
    const v = String(value || '').toLowerCase();
    return v === 'auto' || v === 'semi' ? v : 'manual';
  }

  private get warnThreshold() { return this.runtimeWarnThreshold; }
  private get pauseThreshold() { return this.runtimePauseThreshold; }
  private get criticalSizeCount() { return this.runtimeCriticalSizeCount; }
  private get pauseTotalQty() { return this.runtimePauseTotalQty; }
  private get requireBoth() { return this.runtimeRequireBoth; }

  private get intervalMs() {
    return Math.max(60_000, Number(process.env.META_ADS_INVENTORY_INTERVAL_MS || 1_800_000));
  }

  private runScheduledSafely(source: 'startup' | 'interval') {
    void this.runNow({ source }).catch((error: any) => {
      this.logger.error(`[$META_INVENTORY_AUTOPILOT] scheduled ${source} failed: ${error?.message || error}`);
    });
  }

  async onModuleInit() {
    await this.loadPersistedConfig();
    this.restartTimer();
    if (this.runtimeEnabled) {
      setTimeout(() => this.runScheduledSafely('startup'), 15_000);
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
      this.runScheduledSafely('interval');
    }, this.intervalMs);
  }

  async setRuntimeConfig(input: { enabled?: boolean; dryRun?: boolean; level?: AutomationLevel; warnThreshold?: number; pauseThreshold?: number; criticalSizeCount?: number; pauseTotalQty?: number; requireBoth?: boolean }) {
    if (typeof input?.enabled === 'boolean') this.runtimeEnabled = input.enabled;
    if (typeof input?.dryRun === 'boolean') this.runtimeDryRun = input.dryRun;
    if (input?.level) this.runtimeLevel = this.envLevel(input.level);
    if (Number.isFinite(Number(input?.warnThreshold))) this.runtimeWarnThreshold = Math.max(1, Number(input.warnThreshold));
    if (Number.isFinite(Number(input?.pauseThreshold))) this.runtimePauseThreshold = Math.max(0, Number(input.pauseThreshold));
    if (Number.isFinite(Number(input?.criticalSizeCount))) this.runtimeCriticalSizeCount = Math.max(1, Math.round(Number(input.criticalSizeCount)));
    if (Number.isFinite(Number(input?.pauseTotalQty))) this.runtimePauseTotalQty = Math.max(0, Math.round(Number(input.pauseTotalQty)));
    if (typeof input?.requireBoth === 'boolean') this.runtimeRequireBoth = input.requireBoth;
    this.restartTimer();
    await this.persistRuntimeConfig();
    return this.getStatus();
  }

  private async loadPersistedConfig() {
    try {
      const row = await (this.prisma as any).metaSyncLog.findFirst({ where: { syncType: 'META_ADS_AUTOPILOT_INVENTORY_CONFIG', status: 'SUCCESS' }, orderBy: { startedAt: 'desc' } });
      const config = (row?.errorJson as any)?.config || {};
      if (typeof config.enabled === 'boolean') this.runtimeEnabled = config.enabled;
      if (typeof config.dryRun === 'boolean') this.runtimeDryRun = config.dryRun;
      if (config.level) this.runtimeLevel = this.envLevel(config.level);
      if (Number.isFinite(Number(config.warnThreshold))) this.runtimeWarnThreshold = Math.max(1, Number(config.warnThreshold));
      if (Number.isFinite(Number(config.pauseThreshold))) this.runtimePauseThreshold = Math.max(0, Number(config.pauseThreshold));
      if (Number.isFinite(Number(config.criticalSizeCount))) this.runtimeCriticalSizeCount = Math.max(1, Math.round(Number(config.criticalSizeCount)));
      if (Number.isFinite(Number(config.pauseTotalQty))) this.runtimePauseTotalQty = Math.max(0, Math.round(Number(config.pauseTotalQty)));
      if (typeof config.requireBoth === 'boolean') this.runtimeRequireBoth = config.requireBoth;
    } catch (error: any) { this.logger.warn(`[INVENTORY_AUTOPILOT_CONFIG_LOAD] ${error?.message || error}`); }
  }

  private async persistRuntimeConfig() {
    try {
      const config = { enabled: this.runtimeEnabled, dryRun: this.runtimeDryRun, level: this.runtimeLevel, warnThreshold: this.warnThreshold, pauseThreshold: this.pauseThreshold, criticalSizeCount: this.criticalSizeCount, pauseTotalQty: this.pauseTotalQty, requireBoth: this.requireBoth };
      await (this.prisma as any).metaSyncLog.create({ data: { metaAccountId: null, syncType: 'META_ADS_AUTOPILOT_INVENTORY_CONFIG', status: 'SUCCESS', range: 'config', startedAt: new Date(), finishedAt: new Date(), durationMs: 0, scanned: 0, upserted: 1, failed: 0, message: 'Saved Inventory Autopilot config', errorJson: { config } } });
    } catch (error: any) { this.logger.warn(`[INVENTORY_AUTOPILOT_CONFIG_SAVE] ${error?.message || error}`); }
  }

  getStatus() {
    return {
      ok: true,
      enabled: this.runtimeEnabled,
      dryRun: this.runtimeDryRun,
      level: this.runtimeLevel,
      running: this.running,
      warnThreshold: this.warnThreshold,
      pauseThreshold: this.pauseThreshold,
      criticalSizeCount: this.criticalSizeCount,
      pauseTotalQty: this.pauseTotalQty,
      requireBoth: this.requireBoth,
      intervalMs: this.intervalMs,
      intervalMinutes: Math.round(this.intervalMs / 60_000),
      rule: `Cảnh báo khi bất kỳ size < ${this.warnThreshold}; critical khi ít nhất ${this.criticalSizeCount} size < ${this.pauseThreshold}${this.requireBoth ? ` và tổng tồn màu < ${this.pauseTotalQty}` : ` hoặc tổng tồn màu < ${this.pauseTotalQty}`}; chỉ auto pause ở Mức 3.`,
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
    // QUAN TRỌNG: dùng đúng nguồn tồn kho mà trang Kho hàng đang dùng:
    // InventoryItem -> variant -> product. Không đi vòng qua ProductVariant.inventoryItems,
    // vì source thật của inventory.service/getInventory là InventoryItem.
    const inventoryRows = await (this.prisma as any).inventoryItem.findMany({
      select: {
        id: true,
        branchId: true,
        availableQty: true,
        reservedQty: true,
        incomingQty: true,
        variantId: true,
        variant: {
          select: {
            id: true,
            sku: true,
            color: true,
            size: true,
            productId: true,
            product: {
              select: {
                id: true,
                name: true,
                slug: true,
                status: true,
              },
            },
          },
        },
      },
      take: 100000,
    });

    const groups = new Map<string, any>();

    for (const item of inventoryRows || []) {
      const variant = item?.variant || {};
      const product = variant?.product || {};
      if (!variant?.id) continue;
      if (String(product?.status || '').toUpperCase() === 'INACTIVE') continue;

      const productId = String(product?.id || variant?.productId || '').trim();
      const productName = String(product?.name || product?.slug || 'Sản phẩm').trim();
      const sku = String(variant?.sku || '').trim().toUpperCase();
      const productCode =
        this.extractProductCode(product?.slug, productName, sku) ||
        String(product?.slug || '').trim().replace(/^\/+|\/+$/g, '').toUpperCase();
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

      [productCode, product?.slug, this.extractProductCode(productName, sku)]
        .map((value) => String(value || '').trim())
        .filter(Boolean)
        .forEach((value) => group.productAliases.add(value));

      if (sku) {
        group.skuAliases.add(sku);
        const colorAlias = this.buildColorSkuAlias(sku, size);
        if (colorAlias) group.skuAliases.add(colorAlias);
      }

      // Giống InventoryPageClient: tổng tồn bán được = tổng availableQty của các chi nhánh.
      // Mỗi InventoryItem là 1 variant tại 1 branch, nên cộng dồn theo size.
      const qty = n(item?.availableQty);
      const sizeRow = group.sizes.get(size) || {
        size,
        qty: 0,
        variantIds: [],
        skus: [],
        branchQty: {},
      };
      sizeRow.qty += qty;
      const branchId = String(item?.branchId || '').trim();
      if (branchId) sizeRow.branchQty[branchId] = (sizeRow.branchQty[branchId] || 0) + qty;
      if (variant?.id && !sizeRow.variantIds.includes(String(variant.id))) sizeRow.variantIds.push(String(variant.id));
      if (sku && !sizeRow.skus.includes(sku)) sizeRow.skus.push(sku);
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
      const level: AutopilotLevel = lowSizes.length ? 'READY_TO_PAUSE' : 'NORMAL';

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


  private isCriticalGroup(group: ColorStockGroup) {
    const sizeCondition = group.criticalSizes.length >= this.criticalSizeCount;
    const totalCondition = group.totalQty < this.pauseTotalQty;
    return this.requireBoth ? sizeCondition && totalCondition : sizeCondition || totalCondition;
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
      const hasCritical = this.isCriticalGroup(group);
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
          : hasCritical
            ? `${group.colorKey}: ${group.criticalSizes.length} size dưới ${this.pauseThreshold} và tổng tồn ${group.totalQty} < ${this.pauseTotalQty}`
            : `${group.colorKey}: size ${group.lowSizes.join(', ')} dưới ${this.warnThreshold}; chưa đủ điều kiện auto pause`,
        groups: [group],
      };
    });
  }


  async getManualMappingOptions(limit = 1000) {
    const groups = await this.loadInventoryGroups();
    const byProduct = new Map<string, any>();

    for (const group of groups) {
      const code = String(group.productCode || '').trim().toUpperCase();
      if (!code) continue;
      const current = byProduct.get(code) || {
        productCode: code,
        productId: group.productId || null,
        productName: group.productName || code,
        colors: [],
      };
      current.colors.push({
        color: group.color,
        colorKey: group.colorKey,
        totalQty: group.totalQty,
        minQty: group.minQty,
        sizes: group.sizes.map((row) => ({ size: row.size, qty: row.qty })),
      });
      byProduct.set(code, current);
    }

    const items = Array.from(byProduct.values())
      .map((item: any) => ({
        ...item,
        colors: item.colors.sort((a: any, b: any) =>
          String(a.color).localeCompare(String(b.color), 'vi', { sensitivity: 'base' }),
        ),
      }))
      .sort((a: any, b: any) => String(a.productCode).localeCompare(String(b.productCode), 'vi', { numeric: true }))
      .slice(0, Math.min(Math.max(Number(limit || 1000), 1), 5000));

    return { ok: true, items, total: items.length };
  }

  async assessManualProductForLaunch(input: { productCode: string; color?: string }) {
    const code = String(input?.productCode || '').trim().toUpperCase();
    const color = String(input?.color || '').trim();

    if (!code) {
      return {
        safe: false,
        level: 'UNMAPPED',
        productCode: null,
        color: null,
        sizes: [],
        groups: [],
        reason: 'Chưa chọn mã sản phẩm',
      };
    }

    const groups = await this.loadInventoryGroups();
    const productGroups = groups.filter((group) => {
      if (String(group.productCode || '').trim().toUpperCase() === code) return true;
      return (group.productAliases || []).some((alias) => String(alias || '').trim().toUpperCase() === code);
    });

    if (!productGroups.length) {
      return {
        safe: false,
        level: 'UNMAPPED',
        productCode: code,
        color: color || null,
        sizes: [],
        groups: [],
        reason: `Không tìm thấy mã ${code} trong tồn kho`,
      };
    }

    let selected: ColorStockGroup | null = null;

    if (color) {
      const wanted = normalizeText(color);
      selected =
        productGroups.find((group) => normalizeText(group.color) === wanted) ||
        productGroups.find((group) => normalizeText(group.colorKey) === normalizeText(`${code}-${color}`)) ||
        null;

      if (!selected) {
        return {
          safe: false,
          level: 'AMBIGUOUS',
          productCode: code,
          color,
          sizes: [],
          groups: productGroups,
          availableColors: productGroups.map((group) => group.color),
          reason: `Mã ${code} có trong kho nhưng không có màu "${color}". Hãy chọn đúng màu trong danh sách.`,
        };
      }
    } else if (productGroups.length === 1) {
      selected = productGroups[0];
    } else {
      return {
        safe: false,
        level: 'AMBIGUOUS',
        productCode: code,
        color: null,
        sizes: [],
        groups: productGroups,
        availableColors: productGroups.map((group) => group.color),
        reason: `Mã ${code} có ${productGroups.length} màu. Cần chọn màu để map tồn kho chính xác.`,
      };
    }

    const hasCritical = this.isCriticalGroup(selected);
    const hasLow = selected.lowSizes.length > 0;
    const level = hasCritical ? 'CRITICAL' : hasLow ? 'LOW_STOCK' : 'NORMAL';

    return {
      safe: !hasLow,
      level,
      source: 'MANUAL_PRODUCT_OVERRIDE',
      matchScore: 100,
      productId: selected.productId,
      productCode: selected.productCode,
      productName: selected.productName,
      color: selected.color,
      colorKey: selected.colorKey,
      sizes: selected.sizes.map((row) => ({ size: row.size, qty: row.qty })),
      totalQty: selected.totalQty,
      minQty: selected.minQty,
      lowSizes: selected.lowSizes,
      criticalSizes: selected.criticalSizes,
      groups: [selected],
      availableColors: productGroups.map((group) => group.color),
      reason: `Đã xác nhận thủ công ${selected.productCode} / ${selected.color}; tồn kho lấy trực tiếp theo đúng mã + màu.`,
    };
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

      const warnings = groups.filter((group) => group.lowSizes.length > 0 && !this.isCriticalGroup(group));
      const critical = groups.filter((group) => this.isCriticalGroup(group));
      let matchedAds = 0;
      let pausedAds = 0;
      let failedAds = 0;
      let noMatchGroups = 0;
      const results: any[] = [];

      for (const group of warnings) {
        const reason = `Cảnh báo: size ${group.lowSizes.join(', ')} dưới ${this.warnThreshold}; chưa đủ điều kiện pause (${this.criticalSizeCount} size < ${this.pauseThreshold} ${this.requireBoth ? 'và' : 'hoặc'} tổng tồn < ${this.pauseTotalQty}).`;
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
          const reason = `Đủ điều kiện critical (${group.criticalSizes.length} size < ${this.pauseThreshold}, tổng tồn ${group.totalQty}) nhưng không match chắc chắn được ad ACTIVE đúng mã + màu.`;
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
          const reason = `Critical ${group.colorKey}: ${group.criticalSizes.length} size dưới ${this.pauseThreshold}, tổng tồn ${group.totalQty}. Chỉ pause ad con, không pause adset/campaign.`;
          try {
            const canAutoPause = this.runtimeEnabled && this.runtimeLevel === 'auto';
            if (canAutoPause && !dryRun) {
              await this.metaAdsSyncService.setAdStatus(ad.metaAdId || ad.id, 'PAUSED');
              pausedAds += 1;
            }
            this.pushAction({
              at: new Date().toISOString(),
              type: canAutoPause ? (dryRun ? 'DRY_RUN_PAUSE' : 'PAUSE') : 'SUGGEST_PAUSE',
              colorKey: group.colorKey,
              productName: group.productName,
              sizes: group.sizes.map((row) => ({ size: row.size, qty: row.qty })),
              metaAdId: ad.metaAdId || ad.id,
              adName: ad.name,
              campaignName: ad.campaignName,
              reason,
            });
            adResults.push({ metaAdId: ad.metaAdId || ad.id, name: ad.name, ok: true, dryRun, action: canAutoPause ? (dryRun ? 'DRY_RUN_PAUSE' : 'PAUSE') : 'SUGGEST_PAUSE' });
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
        results.push({ group, matchedAds: adResults, action: this.runtimeLevel === 'auto' ? (dryRun ? 'DRY_RUN_PAUSE' : 'PAUSE') : 'SUGGEST_PAUSE' });
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
        level: this.runtimeLevel,
        rule: { warnThreshold: this.warnThreshold, pauseThreshold: this.pauseThreshold, criticalSizeCount: this.criticalSizeCount, pauseTotalQty: this.pauseTotalQty, requireBoth: this.requireBoth },
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
