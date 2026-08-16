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

function isPosSource(order: AnyRow) {
  const raw = normalizeText(
    `${order?.salesChannel || ''} ${order?.channel || ''} ${order?.orderType || ''} ${order?.paymentMethod || ''} ${order?.source || ''} ${order?.orderSource || ''}`,
  );
  return (
    raw.includes('pos') ||
    raw.includes('ban le') ||
    raw.includes('retail') ||
    raw.includes('ban tai quay') ||
    raw.includes('quay') ||
    raw.includes('offline')
  );
}

function isFacebookSource(order: AnyRow) {
  if (isPosSource(order)) return false;

  const raw = normalizeText(
    `${order?.salesChannel || ''} ${order?.channel || ''} ${order?.orderType || ''} ${order?.paymentMethod || ''} ${order?.paymentType || ''} ${order?.source || ''} ${order?.orderSource || ''}`,
  );

  // Giữ đúng cách Dashboard đang gom nhóm Facebook/COD/online giao hàng.
  return (
    raw.includes('facebook') ||
    raw.includes('fb') ||
    raw.includes('meta') ||
    raw.includes('messenger') ||
    raw.includes('cod') ||
    raw.includes('giao hang') ||
    raw.includes('ship') ||
    raw.includes('delivery') ||
    raw.includes('manual')
  );
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

function pickProductIdFromItem(item: AnyRow): string {
  return String(
    item?.productId ||
      item?.product?.id ||
      item?.product?.productId ||
      item?.variant?.productId ||
      item?.variant?.product?.id ||
      item?.productVariant?.productId ||
      item?.productVariant?.product?.id ||
      '',
  ).trim();
}

function pickSkuFromItem(item: AnyRow): string {
  return String(item?.sku || item?.variantSku || item?.productSku || item?.barcode || '').trim();
}

@Injectable()
export class MetaAdsOrderAttributionService {
  constructor(private readonly prisma: PrismaService) {}

  private async buildProductIdLookupBySkuFamily(items: AnyRow[]) {
    const map = new Map<string, string>();

    for (const item of items) {
      const directProductId = pickProductIdFromItem(item);
      const sku = pickSkuFromItem(item);
      const family = skuFamily(sku);

      if (directProductId) {
        if (sku) map.set(String(sku).toUpperCase(), directProductId);
        if (family) map.set(family, directProductId);
      }
    }

    const wantedFamilies = unique(
      items
        .flatMap((item) => {
          const sku = pickSkuFromItem(item);
          return [skuFamily(sku), ...extractSkuFamiliesFromText(item?.productName || item?.title || '')];
        })
        .filter(Boolean),
    );

    if (!wantedFamilies.length) return map;

    try {
      const products = await (this.prisma as any).product.findMany({
        select: {
          id: true,
          slug: true,
          name: true,
          variants: {
            select: {
              sku: true,
            },
          },
        },
        take: 10000,
      });

      for (const product of products || []) {
        const productId = String(product?.id || '').trim();
        if (!productId) continue;

        const keys = [
          product?.slug,
          ...(Array.isArray(product?.variants)
            ? product.variants.map((variant: AnyRow) => variant?.sku)
            : []),
        ];

        for (const key of keys) {
          const normalizedKey = String(key || '').trim().toUpperCase();
          const family = skuFamily(normalizedKey);
          if (!family || !wantedFamilies.includes(family)) continue;

          map.set(family, productId);
          if (normalizedKey) map.set(normalizedKey, productId);
        }
      }
    } catch {
      // Nếu schema product/variants khác tên field thì bỏ qua lookup.
      // Endpoint vẫn trả dữ liệu cũ; frontend có fallback lookup riêng theo SKU.
    }

    return map;
  }


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
      const orderAtRaw = order?.soldAt || order?.createdAt;
      const orderAt = orderAtRaw ? new Date(orderAtRaw) : null;
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

    const productIdBySkuFamily = await this.buildProductIdLookupBySkuFamily(items);

    const productMap = new Map<string, AnyRow>();
    const validOrderRevenueById = new Map<string, number>();
    const cancelledOrderRevenueById = new Map<string, number>();

    for (const item of items) {
      const order = item.order || {};
      const orderId = String(order.id || item.orderId || '');
      const sku = pickSkuFromItem(item);
      const family = skuFamily(sku);
      const productId =
        pickProductIdFromItem(item) ||
        productIdBySkuFamily.get(String(sku || '').trim().toUpperCase()) ||
        productIdBySkuFamily.get(family) ||
        '';
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
        productId: productId || '',
        productIds: new Set<string>(),
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
        facebookRevenue: 0,
        posRevenue: 0,
        otherRevenue: 0,
        facebookQuantity: 0,
        posQuantity: 0,
        otherQuantity: 0,
        sampleOrders: [],
        cancelledSampleOrders: [],
      };

      if (sku) existed.skuSamples.add(sku);
      if (productId) {
        existed.productIds.add(productId);
        if (!existed.productId) existed.productId = productId;
      }

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
          soldAt: order.soldAt || null,
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

        // Dashboard phân nhóm POS / Facebook(COD-online) / Khác.
        // Doanh thu theo mã SP dùng revenue của dòng hàng, không gán full finalAmount của đơn
        // cho từng sản phẩm trong đơn để tránh nhân đôi doanh thu.
        if (isPosSource(order)) {
          existed.posRevenue += lineRevenue;
          existed.posQuantity += quantity;
        } else if (isFacebookSource(order)) {
          existed.facebookRevenue += lineRevenue;
          existed.facebookQuantity += quantity;
        } else {
          existed.otherRevenue += lineRevenue;
          existed.otherQuantity += quantity;
        }
      }

      productMap.set(key, existed);
    }

    const allRows = Array.from(productMap.values())
      .map((row) => ({
        key: row.key,
        productId: row.productId || Array.from(row.productIds || [])[0] || null,
        productIds: Array.from(row.productIds || []).slice(0, 20),
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
        facebookRevenue: row.facebookRevenue,
        posRevenue: row.posRevenue,
        otherRevenue: row.otherRevenue,
        channelRevenue: row.facebookRevenue + row.posRevenue,
        facebookQuantity: row.facebookQuantity,
        posQuantity: row.posQuantity,
        otherQuantity: row.otherQuantity,
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
      note: 'V19 Dashboard-channel: soldAt ưu tiên createdAt; phân POS / Facebook-COD / Khác giống Dashboard; doanh thu SP theo line revenue.',
    };
  }

  async attachProductOrdersToAds(
    rows: AnyRow[],
    params: { since: Date; until: Date; sourceMode?: string; orderMode?: string },
  ) {
    if (!rows?.length) return rows || [];

    // Luôn lấy ALL để có đủ POS + Facebook cho ROAS tổng.
    // Việc phân kênh nằm trong từng product family, đúng logic Dashboard.
    const productPerformance = await this.getProductPerformance({
      since: params.since,
      until: params.until,
      limit: 500,
      sourceMode: 'all',
      orderMode: params.orderMode || 'valid',
    });

    const productRows = productPerformance.rows || [];

    // Manual mapping được lưu ở MetaAd.rawJson để Ads cũ không cần đổi tên trên Meta.
    const adIds = Array.from(
      new Set(
        (rows || [])
          .map((row: AnyRow) => String(row?.metaAdId || row?.adId || row?.id || '').trim())
          .filter(Boolean),
      ),
    );

    const dbAds = adIds.length
      ? await (this.prisma as any).metaAd.findMany({
          where: { metaAdId: { in: adIds } },
          select: { metaAdId: true, rawJson: true },
        })
      : [];

    const manualByAd = new Map<string, AnyRow>();
    for (const dbAd of dbAds || []) {
      const raw = dbAd?.rawJson;
      const mapping = raw && typeof raw === 'object' ? raw?._autopilotMapping : null;
      if (mapping?.productCode) manualByAd.set(String(dbAd.metaAdId), mapping);
    }

    const matched: Array<{ adRow: AnyRow; best: AnyRow | null; confidence: number }> = rows.map((adRow: AnyRow) => {
      const metaAdId = String(adRow?.metaAdId || adRow?.adId || adRow?.id || '').trim();
      const manualMapping = manualByAd.get(metaAdId) || adRow?.manualMapping || null;
      const manualProductCode = String(
        manualMapping?.productCode || adRow?.manualProductCode || '',
      ).trim().toUpperCase();

      if (manualProductCode) {
        const exact = productRows.find(
          (product: AnyRow) =>
            String(product?.familySku || product?.sku || '').trim().toUpperCase() === manualProductCode,
        );

        if (exact) {
          return {
            adRow: {
              ...adRow,
              manualProductCode,
              manualColor: manualMapping?.color || adRow?.manualColor || null,
              manualMapping,
            },
            best: exact,
            confidence: 100,
          };
        }
      }

      const adName = String(adRow?.name || adRow?.adName || '');
      const adFamilies = extractSkuFamiliesFromText(adName);
      const scored = productRows
        .map((product: AnyRow) => ({
          product,
          score: this.scoreAdProduct(adName, product, adFamilies),
        }))
        .filter((x: AnyRow) => x.score >= 35)
        .sort((a: AnyRow, b: AnyRow) => b.score - a.score);

      return {
        adRow,
        best: scored[0]?.product || null,
        confidence: scored[0]?.score || 0,
      };
    });

    // Một SKU family có thể chạy nhiều Ads. ROAS phải tính ở cấp mã SP/family:
    // doanh thu family / TỔNG spend của toàn bộ Ads match family.
    // Không chia full doanh thu cho từng Ads và cũng không làm ROAS = 0 khi family có nhiều Ads.
    const familySpend = new Map<string, number>();
    const familyAdCount = new Map<string, number>();

    for (const item of matched) {
      const family = skuFamily(item.best?.familySku || item.best?.sku || item.best?.key);
      if (!family) continue;
      const spend = toNumber(item.adRow?.metrics?.spend);
      familySpend.set(family, (familySpend.get(family) || 0) + spend);
      familyAdCount.set(family, (familyAdCount.get(family) || 0) + 1);
    }

    return matched.map(({ adRow, best, confidence }) => {
      if (!best) {
        return {
          ...adRow,
          productAttribution: {
            mode: 'dashboard_channel_roas_v1',
            allocationMode: 'none',
            label: 'Chưa match SKU family',
            confidence: 0,
            productId: null,
            productIds: [],
            sku: null,
            familySku: null,
            skuSamples: [],
            productName: null,
            orderCount: 0,
            facebookOrders: 0,
            posOrders: 0,
            facebookRevenue: 0,
            posRevenue: 0,
            otherRevenue: 0,
            totalRevenue: 0,
            facebookRoas: 0,
            posRoas: 0,
            totalRoas: 0,
            realRoasEstimate: 0,
            familySpend: 0,
            sharedFamilyCount: 0,
            note: 'Chưa match được SKU family từ tên Ads.',
          },
        };
      }

      const family = skuFamily(best.familySku || best.sku || best.key);
      const spend = toNumber(familySpend.get(family));
      const facebookRevenue = toNumber(best.facebookRevenue);
      const posRevenue = toNumber(best.posRevenue);
      const otherRevenue = toNumber(best.otherRevenue);

      // Theo yêu cầu vận hành: ROAS tổng = (POS + Facebook) / Ads spend.
      // "Khác" vẫn trả riêng để quan sát nhưng không cộng vào ROAS tổng.
      const totalRevenue = facebookRevenue + posRevenue;
      const facebookRoas = spend > 0 ? facebookRevenue / spend : 0;
      const posRoas = spend > 0 ? posRevenue / spend : 0;
      const totalRoas = spend > 0 ? totalRevenue / spend : 0;
      const sharedFamilyCount = familyAdCount.get(family) || 1;

      return {
        ...adRow,
        productAttribution: {
          mode: 'dashboard_channel_roas_v1',
          allocationMode: 'product_family_dashboard_channels',
          label:
            confidence >= 80
              ? 'Match SKU family chắc · doanh thu theo chuẩn Dashboard'
              : 'Match SKU family tham khảo · doanh thu theo chuẩn Dashboard',
          confidence,
          productId: best.productId || null,
          productIds: best.productIds || [],
          sku: best.familySku || best.sku,
          familySku: best.familySku || best.sku,
          skuSamples: best.skuSamples || [],
          productName: best.productName,

          orderCount: toNumber(best.orderCount),
          familyOrderCount: toNumber(best.orderCount),
          facebookOrders: toNumber(best.facebookOrders),
          posOrders: toNumber(best.posOrders),

          facebookQuantity: toNumber(best.facebookQuantity),
          posQuantity: toNumber(best.posQuantity),
          otherQuantity: toNumber(best.otherQuantity),
          quantity: toNumber(best.quantity),

          facebookRevenue,
          posRevenue,
          otherRevenue,
          totalRevenue,

          // Giữ các field cũ để UI cũ vẫn chạy.
          revenue: totalRevenue,
          orderRevenue: totalRevenue,
          familyRevenue: totalRevenue,
          familyOrderRevenue: totalRevenue,

          facebookRoas,
          posRoas,
          totalRoas,

          // Auto Scale và UI cũ đang đọc realRoasEstimate => chuyển sang ROAS tổng.
          realRoasEstimate: totalRoas,
          familyRoasEstimate: totalRoas,

          familySpend: spend,
          sharedFamilyCount,

          grossOrderCount: toNumber(best.grossOrderCount),
          cancelledOrderCount: toNumber(best.cancelledOrderCount),
          completedOrderCount: toNumber(best.completedOrderCount),
          shippedOrderCount: toNumber(best.shippedOrderCount),
          cancelledRevenue: toNumber(best.cancelledRevenue),
          cancelledOrderRevenue: toNumber(best.cancelledOrderRevenue),
          grossRevenue: toNumber(best.grossRevenue),
          grossOrderRevenue: toNumber(best.grossOrderRevenue),
          averageOrderValue: toNumber(best.averageOrderValue),
          sampleOrders: best.sampleOrders || [],
          familySampleOrders: best.sampleOrders || [],
          cancelledSampleOrders: best.cancelledSampleOrders || [],

          note:
            'ROAS theo mã SP: POS và Facebook/COD phân loại giống Dashboard; thời gian ưu tiên soldAt; ROAS tổng = (doanh thu POS + Facebook) / tổng Meta spend của các Ads cùng SKU family.',
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
