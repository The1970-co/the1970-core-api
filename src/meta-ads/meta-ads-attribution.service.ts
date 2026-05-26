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

function guessTokensFromAdName(name: string): string[] {
  const normalized = normalizeText(name);
  const rawTokens = normalized.split(/\s+/).filter(Boolean);

  const skuLike = rawTokens.filter((token) =>
    /^(ap|qs|qkk|sm|qj|ak|pk|ao|polo|short|quan|so|mi)[a-z0-9]*\d{2,}$/i.test(token) ||
    /^[a-z]{1,5}\d{2,}$/i.test(token),
  );

  const usefulWords = rawTokens.filter((token) =>
    token.length >= 3 &&
    ![
      'ngay',
      'thang',
      'nam',
      'chay',
      'test',
      'copy',
      'new',
      'meta',
      'ads',
      'quang',
      'cao',
      'chien',
      'dich',
      'nhom',
      'hom',
      'qua',
    ].includes(token),
  );

  return unique([...skuLike, ...usefulWords]).slice(0, 8);
}

function calcConfidence(adName: string, item: AnyRow): number {
  const tokens = guessTokensFromAdName(adName);
  const sku = normalizeText(item.sku || '');
  const productName = normalizeText(item.productName || item.name || '');
  const joined = `${sku} ${productName}`;

  let score = 0;
  for (const token of tokens) {
    if (!token) continue;
    if (sku && sku.includes(token)) score += 22;
    else if (joined.includes(token)) score += 10;
  }

  const adNorm = normalizeText(adName);
  if (productName && adNorm.includes(productName.slice(0, Math.min(productName.length, 18)))) score += 20;

  return Math.max(0, Math.min(100, score));
}

@Injectable()
export class MetaAdsAttributionService {
  constructor(private readonly prisma: PrismaService) {}

  /**
   * Sơ bộ attribution theo tên ads/SKU/tên sản phẩm.
   * Đây KHÔNG phải attribution chuẩn pixel/fbclid.
   * Mục tiêu: giúp bảng Ads Manager nội bộ nhìn được SP A hôm nay tạo bao nhiêu đơn trong hệ thống.
   */
  async attachOrderAttribution(rows: AnyRow[], params: { since: Date; until: Date }) {
    if (!rows?.length) return rows || [];

    const adNames = rows.map((row) => String(row.name || row.adName || '')).filter(Boolean);
    const tokens = unique(adNames.flatMap(guessTokensFromAdName)).filter((x) => x.length >= 3).slice(0, 80);

    if (!tokens.length) {
      return rows.map((row) => ({
        ...row,
        orderAttribution: this.emptyAttribution(row),
      }));
    }

    const orConditions: any[] = [];
    for (const token of tokens) {
      orConditions.push({ sku: { contains: token, mode: 'insensitive' } });
      orConditions.push({ productName: { contains: token, mode: 'insensitive' } });
    }

    const orderItems = await (this.prisma as any).orderItem.findMany({
      where: {
        OR: orConditions,
        order: {
          createdAt: {
            gte: params.since,
            lte: params.until,
          },
        },
      },
      include: {
        order: true,
      },
      take: 5000,
    });

    return rows.map((row) => {
      const scored = orderItems
        .map((item: AnyRow) => ({ item, confidence: calcConfidence(String(row.name || ''), item) }))
        .filter((x: AnyRow) => x.confidence >= 18);

      const orderMap = new Map<string, AnyRow>();
      for (const hit of scored) {
        const order = hit.item.order;
        const id = String(order?.id || hit.item.orderId || '');
        if (!id) continue;

        const existed = orderMap.get(id);
        const itemRevenue = toNumber(hit.item.totalPrice || hit.item.finalPrice || hit.item.price) * Math.max(1, toNumber(hit.item.quantity || 1));
        if (!existed) {
          orderMap.set(id, {
            orderId: id,
            orderCode: order?.orderCode || order?.code || '',
            revenue: toNumber(order?.finalAmount || order?.totalAmount || order?.amount || itemRevenue),
            itemRevenue,
            quantity: toNumber(hit.item.quantity || 1),
            confidence: hit.confidence,
            status: order?.status || null,
            paymentStatus: order?.paymentStatus || null,
            productNames: [hit.item.productName].filter(Boolean),
            skus: [hit.item.sku].filter(Boolean),
          });
        } else {
          existed.itemRevenue += itemRevenue;
          existed.quantity += toNumber(hit.item.quantity || 1);
          existed.confidence = Math.max(existed.confidence, hit.confidence);
          existed.productNames = unique([...existed.productNames, hit.item.productName].filter(Boolean));
          existed.skus = unique([...existed.skus, hit.item.sku].filter(Boolean));
        }
      }

      const orders = Array.from(orderMap.values());
      const revenue = orders.reduce((sum, order) => sum + toNumber(order.revenue), 0);
      const itemRevenue = orders.reduce((sum, order) => sum + toNumber(order.itemRevenue), 0);
      const quantity = orders.reduce((sum, order) => sum + toNumber(order.quantity), 0);
      const spend = toNumber(row.metrics?.spend);
      const confidence = orders.length
        ? Math.round(orders.reduce((sum, order) => sum + toNumber(order.confidence), 0) / orders.length)
        : 0;

      return {
        ...row,
        orderAttribution: {
          mode: 'name_sku_heuristic',
          label: confidence >= 55 ? 'Match khá chắc' : confidence >= 25 ? 'Match tham khảo' : 'Chưa match',
          confidence,
          orderCount: orders.length,
          revenue,
          itemRevenue,
          quantity,
          realRoasEstimate: spend > 0 ? revenue / spend : 0,
          matchedProducts: unique(orders.flatMap((order) => order.productNames || [])).slice(0, 5),
          matchedSkus: unique(orders.flatMap((order) => order.skus || [])).slice(0, 8),
          sampleOrders: orders.slice(0, 8),
          note: 'Sơ bộ theo tên ads/SKU/tên sản phẩm. Chưa phải attribution chuẩn fbclid/pixel.',
        },
      };
    });
  }

  private emptyAttribution(row: AnyRow) {
    return {
      mode: 'name_sku_heuristic',
      label: 'Chưa match',
      confidence: 0,
      orderCount: 0,
      revenue: 0,
      itemRevenue: 0,
      quantity: 0,
      realRoasEstimate: 0,
      matchedProducts: [],
      matchedSkus: [],
      sampleOrders: [],
      note: 'Chưa đủ token để match sản phẩm.',
    };
  }
}
