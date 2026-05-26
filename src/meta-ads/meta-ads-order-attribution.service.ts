import { Injectable } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

type AnyRow = Record<string, any>;

function toNumber(value: any): number {
  const parsed = Number(value || 0);
  return Number.isFinite(parsed) ? parsed : 0;
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

function guessProductTokens(name: string): string[] {
  const normalized = normalizeText(name);
  const tokens = normalized.split(/\s+/).filter(Boolean);

  const skuTokens = tokens.filter((token) => /^[a-z]{1,6}\d{2,}[a-z0-9-]*$/i.test(token));
  const strongWords = tokens.filter(
    (token) =>
      token.length >= 3 &&
      ![
        'ngay',
        'thang',
        'nam',
        'hom',
        'qua',
        'quang',
        'cao',
        'chien',
        'dich',
        'nhom',
        'copy',
        'test',
        'meta',
        'ads',
        'moi',
        'cu',
        'dang',
        'chay',
      ].includes(token),
  );

  return unique([...skuTokens, ...strongWords]).slice(0, 10);
}

function calcRowMatchScore(adName: string, orderItem: AnyRow): number {
  const tokens = guessProductTokens(adName);
  const sku = normalizeText(orderItem.sku || '');
  const productName = normalizeText(orderItem.productName || orderItem.name || '');
  const haystack = `${sku} ${productName}`;

  let score = 0;
  for (const token of tokens) {
    if (!token) continue;
    if (sku && sku.includes(token)) score += 30;
    else if (haystack.includes(token)) score += 10;
  }

  return Math.max(0, Math.min(100, score));
}

@Injectable()
export class MetaAdsOrderAttributionService {
  constructor(private readonly prisma: PrismaService) {}

  async getProductPerformance(params: {
    since: Date;
    until: Date;
    source?: string;
    search?: string;
    limit?: number;
  }) {
    const limit = Math.min(Math.max(Number(params.limit || 100), 1), 500);
    const search = String(params.search || '').trim();

    const where: any = {
      order: {
        createdAt: {
          gte: params.since,
          lte: params.until,
        },
      },
    };

    if (search) {
      where.OR = [
        { sku: { contains: search, mode: 'insensitive' } },
        { productName: { contains: search, mode: 'insensitive' } },
        { name: { contains: search, mode: 'insensitive' } },
      ];
    }

    const items = await (this.prisma as any).orderItem.findMany({
      where,
      include: {
        order: true,
      },
      take: 20000,
    });

    const productMap = new Map<string, AnyRow>();

    for (const item of items) {
      const order = item.order || {};
      const sku = String(item.sku || '').trim();
      const productName = String(item.productName || item.name || 'Sản phẩm chưa rõ').trim();
      const key = sku || normalizeText(productName);
      const quantity = Math.max(1, toNumber(item.quantity || 1));
      const lineRevenue =
        toNumber(item.totalPrice) ||
        toNumber(item.finalPrice) * quantity ||
        toNumber(item.price) * quantity ||
        0;

      const orderId = String(order.id || item.orderId || '');
      const existed = productMap.get(key) || {
        key,
        sku,
        productName,
        orderIds: new Set<string>(),
        orderCodes: new Set<string>(),
        quantity: 0,
        revenue: 0,
        statuses: new Map<string, number>(),
        sources: new Map<string, number>(),
        sampleOrders: [],
      };

      if (orderId && !existed.orderIds.has(orderId)) {
        existed.orderIds.add(orderId);
        if (order.orderCode || order.code) existed.orderCodes.add(order.orderCode || order.code);
        if (existed.sampleOrders.length < 8) {
          existed.sampleOrders.push({
            orderId,
            orderCode: order.orderCode || order.code || '',
            status: order.status || null,
            paymentStatus: order.paymentStatus || null,
            revenue: toNumber(order.finalAmount || order.totalAmount || lineRevenue),
            createdAt: order.createdAt,
          });
        }
      }

      existed.quantity += quantity;
      existed.revenue += lineRevenue;

      const status = String(order.status || 'UNKNOWN');
      existed.statuses.set(status, (existed.statuses.get(status) || 0) + 1);

      const source = String(order.source || order.channel || order.salesChannel || 'UNKNOWN');
      existed.sources.set(source, (existed.sources.get(source) || 0) + 1);

      productMap.set(key, existed);
    }

    const rows = Array.from(productMap.values())
      .map((row) => ({
        key: row.key,
        sku: row.sku,
        productName: row.productName,
        orderCount: row.orderIds.size,
        quantity: row.quantity,
        revenue: row.revenue,
        averageOrderValue: row.orderIds.size ? row.revenue / row.orderIds.size : 0,
        statuses: Object.fromEntries(row.statuses.entries()),
        sources: Object.fromEntries(row.sources.entries()),
        sampleOrders: row.sampleOrders,
      }))
      .sort((a, b) => b.revenue - a.revenue)
      .slice(0, limit);

    return {
      ok: true,
      range: {
        since: params.since.toISOString().slice(0, 10),
        until: params.until.toISOString().slice(0, 10),
      },
      totalProducts: rows.length,
      totalOrders: unique(rows.flatMap((row: any) => row.sampleOrders.map((order: any) => order.orderId))).length,
      totalQuantity: rows.reduce((sum: number, row: any) => sum + toNumber(row.quantity), 0),
      totalRevenue: rows.reduce((sum: number, row: any) => sum + toNumber(row.revenue), 0),
      rows,
      note: 'Báo cáo sản phẩm theo đơn thật trong hệ thống. Không phải heuristic theo ads.',
    };
  }

  async attachProductOrdersToAds(rows: AnyRow[], params: { since: Date; until: Date }) {
    if (!rows?.length) return rows || [];

    const productPerformance = await this.getProductPerformance({
      since: params.since,
      until: params.until,
      limit: 500,
    });

    const productRows = productPerformance.rows || [];

    return rows.map((adRow) => {
      const scored = productRows
        .map((product: AnyRow) => ({
          product,
          score: this.scoreAdProduct(adRow.name, product),
        }))
        .filter((x: AnyRow) => x.score >= 35)
        .sort((a: AnyRow, b: AnyRow) => b.score - a.score);

      const best = scored[0]?.product || null;
      const spend = toNumber(adRow.metrics?.spend);
      const revenue = toNumber(best?.revenue);

      return {
        ...adRow,
        productAttribution: best
          ? {
              mode: 'product_order_exact_then_ad_name_match',
              label: scored[0].score >= 70 ? 'Match sản phẩm khá chắc' : 'Match sản phẩm tham khảo',
              confidence: scored[0].score,
              sku: best.sku,
              productName: best.productName,
              orderCount: best.orderCount,
              quantity: best.quantity,
              revenue: best.revenue,
              averageOrderValue: best.averageOrderValue,
              realRoasEstimate: spend > 0 ? revenue / spend : 0,
              sampleOrders: best.sampleOrders,
              note: 'Đơn là đơn thật theo sản phẩm trong hệ thống. Việc gắn vào ads vẫn dựa tên/SKU, chưa phải fbclid.',
            }
          : {
              mode: 'product_order_exact_then_ad_name_match',
              label: 'Chưa match sản phẩm',
              confidence: 0,
              sku: null,
              productName: null,
              orderCount: 0,
              quantity: 0,
              revenue: 0,
              averageOrderValue: 0,
              realRoasEstimate: 0,
              sampleOrders: [],
              note: 'Chưa match được sản phẩm từ tên ads.',
            },
      };
    });
  }

  private scoreAdProduct(adName: string, product: AnyRow): number {
    const adTokens = guessProductTokens(adName);
    const sku = normalizeText(product.sku);
    const productName = normalizeText(product.productName);
    const haystack = `${sku} ${productName}`;

    let score = 0;
    for (const token of adTokens) {
      if (!token) continue;
      if (sku && sku.includes(token)) score += 45;
      else if (haystack.includes(token)) score += 12;
    }

    return Math.max(0, Math.min(100, score));
  }
}
