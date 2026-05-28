import { Injectable } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

type AnyRow = Record<string, any>;

function toNumber(value: any): number {
  const n = Number(value || 0);
  return Number.isFinite(n) ? n : 0;
}

function normalizeText(value: any): string {
  return String(value || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function unique<T>(items: T[]): T[] {
  return Array.from(new Set(items));
}

function skuFamily(value: any): string {
  const raw = String(value || '').trim().toUpperCase();
  if (!raw) return '';
  const first = raw.split(/[-_\s/]+/)[0] || raw;
  const matched = first.match(/[A-Z]{1,8}\d{2,6}/);
  return matched ? matched[0] : first;
}

function extractSkuFamiliesFromText(value: any): string[] {
  const text = String(value || '').toUpperCase();
  const matches = text.match(/[A-Z]{1,8}\d{2,6}/g) || [];
  return unique(matches.map(skuFamily).filter(Boolean));
}

function isFacebookSource(order: AnyRow) {
  const source = normalizeText(order.source || order.channel || order.salesChannel || order.orderSource || '');
  const group = normalizeText(order.sourceGroup || order.channelGroup || '');
  const joined = `${source} ${group}`;
  return joined.includes('facebook') || joined.includes('fb') || joined.includes('messenger') || joined.includes('manual');
}

function isPosSource(order: AnyRow) {
  const source = normalizeText(order.source || order.channel || order.salesChannel || order.orderSource || '');
  const group = normalizeText(order.sourceGroup || order.channelGroup || '');
  const joined = `${source} ${group}`;
  return joined.includes('pos') || joined.includes('ban tai quay') || joined.includes('offline');
}

function sourceAllowed(order: AnyRow, sourceMode: string) {
  const mode = String(sourceMode || 'facebook').toLowerCase();
  if (mode === 'all') return true;
  if (mode === 'pos') return isPosSource(order);
  if (mode === 'facebook') return isFacebookSource(order) && !isPosSource(order);
  return true;
}

function orderStatus(order: AnyRow): string {
  return String(order.status || '').toUpperCase();
}

function isCancelledOrReturned(order: AnyRow) {
  const status = orderStatus(order);
  return (
    status.includes('CANCEL') ||
    status.includes('CANCELLED') ||
    status.includes('CANCELED') ||
    status.includes('RETURN') ||
    status.includes('REFUND') ||
    status.includes('FAILED')
  );
}

function isCompleted(order: AnyRow) {
  const status = orderStatus(order);
  return status.includes('COMPLETED') || status.includes('DONE') || status.includes('SUCCESS');
}

function isShipped(order: AnyRow) {
  const status = orderStatus(order);
  return status.includes('SHIPPED') || status.includes('DELIVER') || status.includes('PACKING');
}

function pickLineRevenue(item: AnyRow, quantity: number): number {
  const direct =
    toNumber(item.totalPrice) ||
    toNumber(item.totalAmount) ||
    toNumber(item.finalAmount) ||
    toNumber(item.lineTotal) ||
    toNumber(item.subtotal) ||
    toNumber(item.amount) ||
    toNumber(item.revenue);

  if (direct > 0) return direct;

  const unit =
    toNumber(item.finalPrice) ||
    toNumber(item.salePrice) ||
    toNumber(item.sellingPrice) ||
    toNumber(item.price) ||
    toNumber(item.unitPrice);

  return unit > 0 ? unit * Math.max(1, quantity) : 0;
}

@Injectable()
export class MetaAdsOrderAttributionService {
  constructor(private readonly prisma: PrismaService) {}

  async getProductPerformance(params: {
    since: Date;
    until: Date;
    source?: string;
    sourceMode?: string;
    orderMode?: string;
    search?: string;
    limit?: number;
  }) {
    const limit = Math.min(Math.max(Number(params.limit || 100), 1), 500);
    const search = String(params.search || '').trim();
    const searchFamily = skuFamily(search);
    const orderMode = String(params.orderMode || 'valid').toLowerCase();

    const where: any = {};
    if (search) {
      where.OR = [
        { sku: { contains: search, mode: 'insensitive' } },
        { productName: { contains: search, mode: 'insensitive' } },
      ];
    }

    const rawItems = await (this.prisma as any).orderItem.findMany({
      where,
      include: { order: true },
      take: 30000,
    });

    const items = rawItems.filter((item: AnyRow) => {
      const order = item?.order || {};
      const orderAt = order?.createdAt ? new Date(order.createdAt) : null;
      if (!orderAt || Number.isNaN(orderAt.getTime())) return false;
      if (orderAt < params.since || orderAt > params.until) return false;
      if (!sourceAllowed(order, params.sourceMode || 'facebook')) return false;

      if (orderMode === 'valid' && isCancelledOrReturned(order)) return false;
      if (orderMode === 'cancelled' && !isCancelledOrReturned(order)) return false;

      if (searchFamily) {
        const fam = skuFamily(item.sku || item.variantSku || item.productSku);
        const product = normalizeText(item.productName || item.title || '');
        const sq = normalizeText(search);
        return fam === searchFamily || product.includes(sq);
      }

      return true;
    });

    const orderLineCount = new Map<string, number>();
    for (const item of items) {
      const orderId = String(item?.order?.id || item.orderId || '');
      if (orderId) orderLineCount.set(orderId, (orderLineCount.get(orderId) || 0) + 1);
    }

    const productMap = new Map<string, AnyRow>();
    const validOrderRevenueById = new Map<string, number>();
    const cancelledOrderRevenueById = new Map<string, number>();

    for (const item of items) {
      const order = item.order || {};
      const orderId = String(order.id || item.orderId || '');
      const sku = String(item.sku || item.variantSku || item.productSku || '').trim();
      const family = skuFamily(sku);
      const productName = String(item.productName || item.title || 'Sản phẩm chưa rõ').trim();
      const key = family || normalizeText(productName);
      const quantity = Math.max(1, toNumber(item.quantity || item.qty || 1));

      let lineRevenue = pickLineRevenue(item, quantity);

      if (lineRevenue <= 0) {
        const orderRevenue =
          toNumber(order.finalAmount) ||
          toNumber(order.totalAmount) ||
          toNumber(order.amount) ||
          toNumber(order.revenue) ||
          toNumber(order.grandTotal);
        const lineCount = Math.max(1, orderLineCount.get(orderId) || 1);
        lineRevenue = orderRevenue > 0 ? orderRevenue / lineCount : 0;
      }

      const existed = productMap.get(key) || {
        key,
        familySku: family,
        skuSamples: new Set<string>(),
        productName,
        orderIds: new Set<string>(),
        validOrderIds: new Set<string>(),
        cancelledOrderIds: new Set<string>(),
        completedOrderIds: new Set<string>(),
        shippedOrderIds: new Set<string>(),
        quantity: 0,
        revenue: 0,
        orderRevenue: 0,
        cancelledRevenue: 0,
        cancelledOrderRevenue: 0,
        facebookOrders: 0,
        posOrders: 0,
        sampleOrders: [],
        cancelledSampleOrders: [],
      };

      if (sku) existed.skuSamples.add(sku);

      const cancelled = isCancelledOrReturned(order);
      const orderRevenueForSample = toNumber(order.finalAmount || order.totalAmount || lineRevenue);

      // Tổng dashboard / Ads Center phải tính unique theo đơn, không cộng lại theo từng SKU family.
      // Một đơn có nhiều sản phẩm hoặc một SKU được nhiều ads kéo về chỉ được tính doanh thu đơn 1 lần.
      if (orderId) {
        if (cancelled) {
          if (!cancelledOrderRevenueById.has(orderId)) cancelledOrderRevenueById.set(orderId, orderRevenueForSample);
        } else if (!validOrderRevenueById.has(orderId)) {
          validOrderRevenueById.set(orderId, orderRevenueForSample);
        }
      }

      if (orderId && !existed.orderIds.has(orderId)) {
        existed.orderIds.add(orderId);

        if (cancelled) existed.cancelledOrderIds.add(orderId);
        else existed.validOrderIds.add(orderId);

        if (isCompleted(order)) existed.completedOrderIds.add(orderId);
        if (isShipped(order)) existed.shippedOrderIds.add(orderId);

        if (isFacebookSource(order) && !isPosSource(order)) existed.facebookOrders += 1;
        if (isPosSource(order)) existed.posOrders += 1;

        const sample = {
          orderId,
          orderCode: order.orderCode || order.code || '',
          customerName: order.customerName || order.customer?.name || null,
          source: order.source || order.channel || order.salesChannel || null,
          status: order.status || null,
          paymentStatus: order.paymentStatus || null,
          revenue: orderRevenueForSample,
          lineRevenue,
          quantity,
          sku,
          familySku: family,
          createdAt: order.createdAt,
        };

        if (cancelled) {
          if (existed.cancelledSampleOrders.length < 20) existed.cancelledSampleOrders.push(sample);
        } else if (existed.sampleOrders.length < 20) {
          existed.sampleOrders.push(sample);
        }
      }

      existed.quantity += quantity;

      if (cancelled) {
        existed.cancelledRevenue += lineRevenue;
        existed.cancelledOrderRevenue += orderRevenueForSample;
      } else {
        existed.revenue += lineRevenue;
        existed.orderRevenue += orderRevenueForSample;
      }

      productMap.set(key, existed);
    }

    const allRows = Array.from(productMap.values())
      .map((row) => ({
        key: row.key,
        sku: row.familySku || row.key,
        familySku: row.familySku || row.key,
        skuSamples: Array.from(row.skuSamples).slice(0, 20),
        productName: row.productName,
        orderCount: row.validOrderIds.size,
        grossOrderCount: row.orderIds.size,
        cancelledOrderCount: row.cancelledOrderIds.size,
        completedOrderCount: row.completedOrderIds.size,
        shippedOrderCount: row.shippedOrderIds.size,
        facebookOrders: row.facebookOrders,
        posOrders: row.posOrders,
        quantity: row.quantity,
        revenue: row.revenue,
        orderRevenue: row.orderRevenue,
        cancelledRevenue: row.cancelledRevenue,
        cancelledOrderRevenue: row.cancelledOrderRevenue,
        grossRevenue: row.revenue + row.cancelledRevenue,
        grossOrderRevenue: row.orderRevenue + row.cancelledOrderRevenue,
        averageOrderValue: row.validOrderIds.size ? row.revenue / row.validOrderIds.size : 0,
        sampleOrders: row.sampleOrders,
        cancelledSampleOrders: row.cancelledSampleOrders,
      }))
      .sort((a, b) => b.revenue - a.revenue);

    const rows = allRows.slice(0, limit);
    const uniqueOrderRevenue = Array.from(validOrderRevenueById.values()).reduce((sum, value) => sum + toNumber(value), 0);
    const uniqueCancelledOrderRevenue = Array.from(cancelledOrderRevenueById.values()).reduce((sum, value) => sum + toNumber(value), 0);

    return {
      ok: true,
      range: { since: params.since.toISOString(), until: params.until.toISOString() },
      sourceMode: params.sourceMode || 'facebook',
      orderMode,
      totalProducts: allRows.length,
      totalOrders: validOrderRevenueById.size,
      totalCancelledOrders: cancelledOrderRevenueById.size,
      totalQuantity: allRows.reduce((sum: number, row: any) => sum + toNumber(row.quantity), 0),
      // totalRevenue vẫn là doanh thu theo dòng sản phẩm; totalOrderRevenue mới là unique theo đơn.
      totalRevenue: allRows.reduce((sum: number, row: any) => sum + toNumber(row.revenue), 0),
      totalOrderRevenue: uniqueOrderRevenue,
      totalCancelledRevenue: allRows.reduce((sum: number, row: any) => sum + toNumber(row.cancelledRevenue), 0),
      totalCancelledOrderRevenue: uniqueCancelledOrderRevenue,
      rows,
      note: 'V17: SKU family + source filter + bỏ đơn huỷ + totalOrderRevenue unique theo order, không cộng trùng nhiều SKU/ads.',
    };
  }

  async attachProductOrdersToAds(rows: AnyRow[], params: { since: Date; until: Date; sourceMode?: string; orderMode?: string }) {
    if (!rows?.length) return rows || [];

    const productPerformance = await this.getProductPerformance({
      since: params.since,
      until: params.until,
      limit: 500,
      sourceMode: params.sourceMode || 'facebook',
      orderMode: params.orderMode || 'valid',
    });

    const productRows = productPerformance.rows || [];

    const matched = rows.map((adRow) => {
      const adFamilies = extractSkuFamiliesFromText(adRow.name);
      const scored = productRows
        .map((product: AnyRow) => ({
          product,
          score: this.scoreAdProduct(adRow.name, product, adFamilies),
        }))
        .filter((x: AnyRow) => x.score >= 35)
        .sort((a: AnyRow, b: AnyRow) => b.score - a.score);

      const best = scored[0]?.product || null;
      const confidence = scored[0]?.score || 0;

      return {
        adRow,
        best,
        confidence,
      };
    });

    // Nếu nhiều ads cùng match 1 SKU family, không được gán full doanh thu family cho từng ads.
    // Lúc đó doanh thu/ROAS ở row ads sẽ để dạng "family shared", chỉ dùng để tham khảo ở Product Center.
    const familyMatchCount = new Map<string, number>();
    for (const item of matched) {
      const family = skuFamily(item.best?.familySku || item.best?.sku || item.best?.key);
      if (!family) continue;
      familyMatchCount.set(family, (familyMatchCount.get(family) || 0) + 1);
    }

    return matched.map(({ adRow, best, confidence }) => {
      const spend = toNumber(adRow.metrics?.spend);
      const family = skuFamily(best?.familySku || best?.sku || best?.key);
      const sharedFamilyCount = family ? familyMatchCount.get(family) || 0 : 0;
      const isSharedFamily = Boolean(best && sharedFamilyCount > 1);
      const familyRevenue = toNumber(best?.revenue);

      const rowRevenue = isSharedFamily ? 0 : familyRevenue;
      const rowOrders = isSharedFamily ? 0 : toNumber(best?.orderCount);

      return {
        ...adRow,
        productAttribution: best
          ? {
              mode: 'sku_family_v4_no_duplicate_roas',
              allocationMode: isSharedFamily ? 'family_shared' : 'single_ad_family',
              label: isSharedFamily
                ? `Family ${family} có ${sharedFamilyCount} ads, không chia ROAS cho từng ads`
                : confidence >= 80
                  ? 'Match SKU family chắc'
                  : 'Match SKU family tham khảo',
              confidence,
              sku: best.familySku || best.sku,
              familySku: best.familySku || best.sku,
              skuSamples: best.skuSamples || [],
              productName: best.productName,
              orderCount: rowOrders,
              familyOrderCount: best.orderCount,
              grossOrderCount: best.grossOrderCount,
              cancelledOrderCount: best.cancelledOrderCount,
              completedOrderCount: best.completedOrderCount,
              shippedOrderCount: best.shippedOrderCount,
              facebookOrders: best.facebookOrders,
              posOrders: best.posOrders,
              quantity: isSharedFamily ? 0 : best.quantity,
              familyQuantity: best.quantity,
              revenue: rowRevenue,
              orderRevenue: isSharedFamily ? 0 : best.orderRevenue,
              familyRevenue,
              familyOrderRevenue: best.orderRevenue,
              cancelledRevenue: best.cancelledRevenue,
              cancelledOrderRevenue: best.cancelledOrderRevenue,
              grossRevenue: best.grossRevenue,
              grossOrderRevenue: best.grossOrderRevenue,
              averageOrderValue: best.averageOrderValue,
              realRoasEstimate: !isSharedFamily && spend > 0 ? rowRevenue / spend : 0,
              familyRoasEstimate: spend > 0 ? familyRevenue / spend : 0,
              sharedFamilyCount,
              sampleOrders: isSharedFamily ? [] : best.sampleOrders,
              familySampleOrders: best.sampleOrders,
              cancelledSampleOrders: best.cancelledSampleOrders,
              note: isSharedFamily
                ? 'Nhiều ads cùng SKU family nên không gán full doanh thu cho từng ads để tránh ROAS ảo.'
                : 'Gom SKU cha/family. Mặc định bỏ đơn huỷ và chỉ tính Facebook.',
            }
          : {
              mode: 'sku_family_v4_no_duplicate_roas',
              allocationMode: 'none',
              label: 'Chưa match SKU family',
              confidence: 0,
              sku: null,
              familySku: null,
              skuSamples: [],
              productName: null,
              orderCount: 0,
              familyOrderCount: 0,
              grossOrderCount: 0,
              cancelledOrderCount: 0,
              completedOrderCount: 0,
              shippedOrderCount: 0,
              facebookOrders: 0,
              posOrders: 0,
              quantity: 0,
              familyQuantity: 0,
              revenue: 0,
              orderRevenue: 0,
              familyRevenue: 0,
              familyOrderRevenue: 0,
              cancelledRevenue: 0,
              cancelledOrderRevenue: 0,
              grossRevenue: 0,
              grossOrderRevenue: 0,
              averageOrderValue: 0,
              realRoasEstimate: 0,
              familyRoasEstimate: 0,
              sharedFamilyCount: 0,
              sampleOrders: [],
              familySampleOrders: [],
              cancelledSampleOrders: [],
              note: 'Chưa match được SKU family từ tên ads.',
            },
      };
    });
  }

  private scoreAdProduct(adName: string, product: AnyRow, adFamilies: string[]): number {
    const productFamily = skuFamily(product.familySku || product.sku || product.key);
    if (productFamily && adFamilies.includes(productFamily)) return 100;

    const adText = normalizeText(adName);
    const productName = normalizeText(product.productName);
    const skuText = normalizeText(productFamily);
    let score = 0;

    if (skuText && adText.includes(skuText)) score += 85;

    const productTokens = productName.split(/\s+/).filter((x) => x.length >= 3);
    for (const token of productTokens) {
      if (adText.includes(token)) score += 8;
    }

    return Math.max(0, Math.min(100, score));
  }
}
