import { Injectable } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

type Tone = 'safe' | 'warning' | 'critical';

type CriticalSku = {
  variantId: string;
  productId: string;
  sku: string;
  productName: string;
  color?: string | null;
  size?: string | null;
  category?: string | null;
  stock: number;
  reserved: number;
  incoming: number;
  sold7: number;
  sold14: number;
  sold30: number;
  revenue30: number;
  velocity7: number;
  velocity14: number;
  daysToOut: number | null;
  score: number;
  branchStocks: Array<{ branchId: string; branchName: string; availableQty: number; reservedQty: number; incomingQty: number }>;
  actionUrl: string;
};

@Injectable()
export class DashboardService {
  constructor(private readonly prisma: PrismaService) {}

  private n(value: any) {
    const parsed = Number(value || 0);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  private money(value: any) {
    const amount = this.n(value);
    if (amount >= 1_000_000_000) return `${(amount / 1_000_000_000).toFixed(1)}B`;
    if (amount >= 1_000_000) return `${(amount / 1_000_000).toFixed(1)}M`;
    if (amount >= 1_000) return `${Math.round(amount / 1_000)}K`;
    return `${Math.round(amount)}`;
  }

  private qty(value: any) {
    return new Intl.NumberFormat('vi-VN').format(this.n(value));
  }

  private pct(value: number) {
    if (!Number.isFinite(value)) return '0%';
    const sign = value > 0 ? '+' : '';
    return `${sign}${value.toFixed(1)}%`;
  }

  private dayKey(date: Date) {
    const y = date.getFullYear();
    const m = String(date.getMonth() + 1).padStart(2, '0');
    const d = String(date.getDate()).padStart(2, '0');
    return `${y}-${m}-${d}`;
  }

  private startOfDay(date: Date) {
    return new Date(date.getFullYear(), date.getMonth(), date.getDate(), 0, 0, 0, 0);
  }

  private addDays(date: Date, days: number) {
    const next = new Date(date);
    next.setDate(next.getDate() + days);
    return next;
  }

  private channelLabel(channel?: string | null) {
    const key = String(channel || 'OTHER');
    const map: Record<string, string> = {
      VN_WEB: 'Website VN',
      INTL_WEB: 'Website Quốc tế',
      FACEBOOK_MANUAL: 'Facebook',
      SHOWROOM: 'Showroom',
      POS: 'POS',
      OTHER: 'Khác',
    };
    return map[key] || key;
  }

  private productLabel(item: { productName: string; sku: string; color?: string | null; size?: string | null }) {
    const attrs = [item.color, item.size].filter(Boolean).join(' / ');
    return attrs ? `${item.productName} - ${attrs}` : item.productName || item.sku;
  }

  async getOverview(branchId?: string) {
    const selectedBranchId = branchId && branchId !== 'ALL' ? branchId : undefined;
    const now = new Date();
    const todayStart = this.startOfDay(now);
    const tomorrowStart = this.addDays(todayStart, 1);
    const yesterdayStart = this.addDays(todayStart, -1);
    const monthStart = new Date(now.getFullYear(), now.getMonth(), 1, 0, 0, 0, 0);
    const last7Start = this.addDays(todayStart, -7);
    const last14Start = this.addDays(todayStart, -14);
    const last30Start = this.addDays(todayStart, -30);

    const activeBranches = await this.prisma.branch.findMany({
      where: { isActive: true },
      select: { id: true, name: true, isActive: true },
      orderBy: { name: 'asc' },
    });

    const activeBranchIds = activeBranches.map((b) => b.id);
    const branchMap = new Map(activeBranches.map((b) => [b.id, b.name]));
    const branchFilterIds = selectedBranchId ? [selectedBranchId] : activeBranchIds;

    const orderWhere: any = selectedBranchId ? { branchId: selectedBranchId } : {};
    const validOrderWhere: any = { ...orderWhere, status: { not: 'CANCELLED' } };
    const completedOrderWhere: any = { ...orderWhere, status: 'COMPLETED' };

    const inventoryWhere: any = {
      ...(selectedBranchId ? { branchId: selectedBranchId } : branchFilterIds.length ? { branchId: { in: branchFilterIds } } : {}),
      variant: {
        status: 'ACTIVE',
        product: { status: 'ACTIVE' },
      },
    };

    const salesWhereBase: any = {
      order: {
        ...validOrderWhere,
        soldAt: { gte: last30Start, lt: tomorrowStart },
      },
      variantId: { not: null },
    };

    const [
      totalOrders,
      newOrders,
      completedOrders,
      cancelledOrders,
      revenueAgg,
      todayRevenueAgg,
      yesterdayRevenueAgg,
      inventoryAgg,
      productCount,
      variantCount,
      pendingTransfers,
      variantStockGroups,
      warehouseGroups,
      channelRevenueRaw,
      topProductsRaw,
      sales7Raw,
      sales14Raw,
      sales30Raw,
      monthOrders,
      recentOrdersRaw,
    ] = await Promise.all([
      this.prisma.order.count({ where: orderWhere }),
      this.prisma.order.count({ where: { ...orderWhere, status: 'NEW' } }),
      this.prisma.order.count({ where: completedOrderWhere }),
      this.prisma.order.count({ where: { ...orderWhere, status: 'CANCELLED' } }),
      this.prisma.order.aggregate({ where: validOrderWhere, _sum: { finalAmount: true } }),
      this.prisma.order.aggregate({
        where: { ...validOrderWhere, soldAt: { gte: todayStart, lt: tomorrowStart } },
        _sum: { finalAmount: true },
      }),
      this.prisma.order.aggregate({
        where: { ...validOrderWhere, soldAt: { gte: yesterdayStart, lt: todayStart } },
        _sum: { finalAmount: true },
      }),
      this.prisma.inventoryItem.aggregate({
        where: inventoryWhere,
        _sum: { availableQty: true, reservedQty: true, incomingQty: true },
      }),
      this.prisma.product.count({ where: { status: 'ACTIVE' } }),
      this.prisma.productVariant.count({ where: { status: 'ACTIVE', product: { status: 'ACTIVE' } } }),
      this.prisma.stockTransfer
        .count({
          where: {
            ...(selectedBranchId ? { OR: [{ fromBranchId: selectedBranchId }, { toBranchId: selectedBranchId }] } : {}),
            status: { in: ['DRAFT', 'PENDING', 'CONFIRMED', 'IN_TRANSIT'] as any },
          },
        })
        .catch(() => 0),
      this.prisma.inventoryItem.groupBy({
        by: ['variantId'],
        where: inventoryWhere,
        _sum: { availableQty: true, reservedQty: true, incomingQty: true },
      }),
      this.prisma.inventoryItem.groupBy({
        by: ['branchId'],
        where: inventoryWhere,
        _sum: { availableQty: true, reservedQty: true, incomingQty: true },
      }),
      this.prisma.order.groupBy({
        by: ['salesChannel'],
        where: validOrderWhere,
        _sum: { finalAmount: true },
        _count: { _all: true },
      }),
      this.prisma.orderItem.groupBy({
        by: ['variantId', 'sku', 'productName'],
        where: salesWhereBase,
        _sum: { qty: true, lineTotal: true },
        _count: { _all: true },
        orderBy: { _sum: { lineTotal: 'desc' } },
        take: 10,
      }),
      this.prisma.orderItem.groupBy({
        by: ['variantId'],
        where: { ...salesWhereBase, order: { ...validOrderWhere, soldAt: { gte: last7Start, lt: tomorrowStart } } },
        _sum: { qty: true, lineTotal: true },
      }),
      this.prisma.orderItem.groupBy({
        by: ['variantId'],
        where: { ...salesWhereBase, order: { ...validOrderWhere, soldAt: { gte: last14Start, lt: tomorrowStart } } },
        _sum: { qty: true, lineTotal: true },
      }),
      this.prisma.orderItem.groupBy({
        by: ['variantId'],
        where: salesWhereBase,
        _sum: { qty: true, lineTotal: true },
      }),
      this.prisma.order.findMany({
        where: { ...validOrderWhere, soldAt: { gte: monthStart, lt: tomorrowStart } },
        orderBy: { soldAt: 'desc' },
        select: {
          id: true,
          orderCode: true,
          status: true,
          paymentStatus: true,
          fulfillmentStatus: true,
          finalAmount: true,
          salesChannel: true,
          branchId: true,
          soldAt: true,
          createdAt: true,
          customerName: true,
          customerPhone: true,
          items: {
            select: {
              id: true,
              qty: true,
              lineTotal: true,
              unitPrice: true,
              sku: true,
              productName: true,
              variant: { select: { id: true, sku: true, costPrice: true } },
            },
          },
        },
      }),
      this.prisma.order.findMany({
        where: orderWhere,
        orderBy: { createdAt: 'desc' },
        take: 8,
        select: {
          id: true,
          orderCode: true,
          customerName: true,
          customerPhone: true,
          status: true,
          paymentStatus: true,
          fulfillmentStatus: true,
          finalAmount: true,
          salesChannel: true,
          branchId: true,
          createdAt: true,
          soldAt: true,
        },
      }),
    ]);

    const revenue = this.n(revenueAgg._sum.finalAmount);
    const todayRevenue = this.n(todayRevenueAgg._sum.finalAmount);
    const yesterdayRevenue = this.n(yesterdayRevenueAgg._sum.finalAmount);
    const availableQty = this.n(inventoryAgg._sum.availableQty);
    const reservedQty = this.n(inventoryAgg._sum.reservedQty);
    const incomingQty = this.n(inventoryAgg._sum.incomingQty);

    const stockByVariant = new Map(
      variantStockGroups.map((row) => [
        row.variantId,
        {
          stock: this.n(row._sum.availableQty),
          reserved: this.n(row._sum.reservedQty),
          incoming: this.n(row._sum.incomingQty),
        },
      ]),
    );

    const sold7Map = new Map(sales7Raw.map((row) => [row.variantId, { qty: this.n(row._sum.qty), revenue: this.n(row._sum.lineTotal) }]));
    const sold14Map = new Map(sales14Raw.map((row) => [row.variantId, { qty: this.n(row._sum.qty), revenue: this.n(row._sum.lineTotal) }]));
    const sold30Map = new Map(sales30Raw.map((row) => [row.variantId, { qty: this.n(row._sum.qty), revenue: this.n(row._sum.lineTotal) }]));

    const candidateVariantIds = Array.from(
      new Set([
        ...variantStockGroups.filter((row) => this.n(row._sum.availableQty) <= 3).map((row) => row.variantId),
        ...sales30Raw.map((row) => row.variantId).filter(Boolean),
        ...topProductsRaw.map((row) => row.variantId).filter(Boolean),
      ]),
    ) as string[];

    const candidateVariants = candidateVariantIds.length
      ? await this.prisma.productVariant.findMany({
          where: { id: { in: candidateVariantIds }, status: 'ACTIVE', product: { status: 'ACTIVE' } },
          select: {
            id: true,
            productId: true,
            sku: true,
            color: true,
            size: true,
            price: true,
            costPrice: true,
            product: { select: { name: true, category: true } },
            inventoryItems: {
              where: selectedBranchId ? { branchId: selectedBranchId } : branchFilterIds.length ? { branchId: { in: branchFilterIds } } : {},
              select: { branchId: true, availableQty: true, reservedQty: true, incomingQty: true },
            },
          },
        })
      : [];

    const variantMap = new Map(candidateVariants.map((v) => [v.id, v]));

    const criticalCandidates: CriticalSku[] = candidateVariants
      .map((variant) => {
        const stockInfo = stockByVariant.get(variant.id) || { stock: 0, reserved: 0, incoming: 0 };
        const sold7 = sold7Map.get(variant.id)?.qty || 0;
        const sold14 = sold14Map.get(variant.id)?.qty || 0;
        const sold30 = sold30Map.get(variant.id)?.qty || 0;
        const revenue30 = sold30Map.get(variant.id)?.revenue || 0;
        const velocity7 = sold7 / 7;
        const velocity14 = sold14 / 14;
        const velocity = Math.max(velocity7, velocity14, sold30 / 30);
        const daysToOut = velocity > 0 ? stockInfo.stock / velocity : stockInfo.stock <= 2 ? 999 : null;
        const branchStocks = variant.inventoryItems.map((item) => ({
          branchId: item.branchId,
          branchName: branchMap.get(item.branchId) || item.branchId,
          availableQty: this.n(item.availableQty),
          reservedQty: this.n(item.reservedQty),
          incomingQty: this.n(item.incomingQty),
        }));
        const productName = variant.product?.name || variant.sku;
        const score = Math.min(
          99,
          Math.round(
            (stockInfo.stock <= 0 ? 45 : stockInfo.stock <= 1 ? 35 : stockInfo.stock <= 2 ? 25 : 10) +
              Math.min(25, velocity * 7) +
              Math.min(20, revenue30 / 1_000_000) +
              (daysToOut !== null && daysToOut <= 7 ? 15 : 0),
          ),
        );

        return {
          variantId: variant.id,
          productId: variant.productId,
          sku: variant.sku,
          productName,
          color: variant.color,
          size: variant.size,
          category: variant.product?.category,
          stock: stockInfo.stock,
          reserved: stockInfo.reserved,
          incoming: stockInfo.incoming,
          sold7,
          sold14,
          sold30,
          revenue30,
          velocity7,
          velocity14,
          daysToOut,
          score,
          branchStocks,
          actionUrl: `/products/${variant.productId}`,
        };
      })
      .filter((item) => {
        const hasRecentSales = item.sold30 > 0;
        const isLow = item.stock > 0 && item.stock <= 3;
        const isOutButSelling = item.stock <= 0 && hasRecentSales;
        const willRunOutSoon = item.daysToOut !== null && item.daysToOut <= 14;
        return hasRecentSales && (isLow || isOutButSelling || willRunOutSoon);
      })
      .sort((a, b) => b.score - a.score || a.stock - b.stock)
      .slice(0, 10);

    const lowStockItems = criticalCandidates.filter((item) => item.stock > 0 && item.stock <= 3).length;
    const outOfStockItems = criticalCandidates.filter((item) => item.stock <= 0).length;
    const rawLowStockPool = variantStockGroups.filter((row) => {
      const qty = this.n(row._sum.availableQty);
      const sold30 = sold30Map.get(row.variantId)?.qty || 0;
      return sold30 > 0 && qty > 0 && qty <= 3;
    }).length;
    const rawOutOfStockPool = variantStockGroups.filter((row) => {
      const qty = this.n(row._sum.availableQty);
      const sold30 = sold30Map.get(row.variantId)?.qty || 0;
      return sold30 > 0 && qty <= 0;
    }).length;

    const totalWarehouseQty = warehouseGroups.reduce((sum, row) => sum + this.n(row._sum.availableQty), 0);
    const warehouseMix = warehouseGroups
      .map((row) => {
        const qty = this.n(row._sum.availableQty);
        const reserved = this.n(row._sum.reservedQty);
        const incoming = this.n(row._sum.incomingQty);
        const percent = totalWarehouseQty > 0 ? Math.round((qty / totalWarehouseQty) * 100) : 0;
        const level = percent >= 45 ? 'Kho đang giữ tỷ trọng lớn' : percent <= 10 ? 'Kho tỷ trọng thấp' : 'Tỷ trọng ổn';
        return {
          name: branchMap.get(row.branchId) || row.branchId,
          value: `${percent}%`,
          note: `${this.qty(qty)} khả dụng · ${this.qty(reserved)} giữ · ${this.qty(incoming)} sắp về · ${level}`,
          branchId: row.branchId,
          qty,
          reserved,
          incoming,
          percent,
        };
      })
      .sort((a, b) => b.qty - a.qty)
      .map(({ qty, reserved, incoming, percent, ...rest }) => rest);

    const topProducts = topProductsRaw.map((row, index) => {
      const variant = row.variantId ? variantMap.get(row.variantId) : null;
      const nameParts = [variant?.product?.name || row.productName || row.sku || 'Sản phẩm chưa đặt tên'];
      const attrs = [variant?.color, variant?.size].filter(Boolean).join(' / ');
      if (attrs) nameParts.push(attrs);
      const sold14 = row.variantId ? sold14Map.get(row.variantId)?.qty || 0 : 0;
      const stock = row.variantId ? stockByVariant.get(row.variantId)?.stock || 0 : 0;
      const velocity = sold14 / 14;
      const daysToOut = velocity > 0 ? stock / velocity : null;
      return {
        rank: index + 1,
        name: nameParts.join(' - '),
        meta: `${variant?.sku || row.sku || 'NO-SKU'} · ${variant?.product?.category || 'Sản phẩm'}${daysToOut !== null ? ` · còn ~${Math.ceil(daysToOut)} ngày` : ''}`,
        qty: `${this.qty(row._sum.qty)} sp`,
        revenue: this.money(row._sum.lineTotal),
        variantId: row.variantId,
        productId: variant?.productId,
        actionUrl: variant?.productId ? `/products/${variant.productId}` : undefined,
      };
    });

    const maxChannelRevenue = Math.max(...channelRevenueRaw.map((row) => this.n(row._sum.finalAmount)), 1);
    const channelRevenue = channelRevenueRaw
      .map((row) => {
        const value = this.n(row._sum.finalAmount);
        return {
          name: this.channelLabel(row.salesChannel),
          value: this.money(value),
          width: `${Math.max(4, Math.round((value / maxChannelRevenue) * 100))}%`,
          orders: this.n(row._count._all),
        };
      })
      .sort((a, b) => this.n((b as any).orders) - this.n((a as any).orders));

    const adsCostByDay = new Map<string, number>();
    // Chưa có bảng ads spend live. Giữ 0 để công thức profit chuẩn: doanh thu - giá vốn - ads.

    const dailyMap = new Map<string, { revenue: number; cost: number; adsCost: number; profit: number; orders: number }>();
    for (let d = new Date(monthStart); d <= todayStart; d = this.addDays(d, 1)) {
      const key = this.dayKey(d);
      dailyMap.set(key, { revenue: 0, cost: 0, adsCost: adsCostByDay.get(key) || 0, profit: 0, orders: 0 });
    }

    for (const order of monthOrders) {
      const key = this.dayKey(order.soldAt || order.createdAt);
      const row = dailyMap.get(key) || { revenue: 0, cost: 0, adsCost: adsCostByDay.get(key) || 0, profit: 0, orders: 0 };
      const orderRevenue = this.n(order.finalAmount);
      const orderCost = order.items.reduce((sum, item) => {
        const costPrice = this.n(item.variant?.costPrice);
        return sum + costPrice * this.n(item.qty);
      }, 0);
      row.revenue += orderRevenue;
      row.cost += orderCost;
      row.orders += 1;
      dailyMap.set(key, row);
    }

    for (const row of dailyMap.values()) {
      row.profit = row.revenue - row.cost - row.adsCost;
    }

    const days = Array.from(dailyMap.entries()).sort((a, b) => (a[0] < b[0] ? 1 : -1));
    const dailyRows = days.map(([key, row], index) => {
      const date = new Date(`${key}T00:00:00`);
      const prev = dailyMap.get(this.dayKey(this.addDays(date, -1)));
      const prevRevenue = prev?.revenue || 0;
      const compareValue = prevRevenue > 0 ? ((row.revenue - prevRevenue) / prevRevenue) * 100 : row.revenue > 0 ? 100 : 0;
      const day = String(date.getDate()).padStart(2, '0');
      const isToday = key === this.dayKey(todayStart);
      const isYesterday = key === this.dayKey(yesterdayStart);
      const roas = row.adsCost > 0 ? `${(row.revenue / row.adsCost).toFixed(2)}x` : 'Chưa nối ads';
      return {
        day,
        note: isToday ? 'Hôm nay' : isYesterday ? 'Hôm qua' : 'Trong tháng',
        revenue: this.money(row.revenue),
        cost: row.cost > 0 ? this.money(row.cost) : 'Chưa có giá vốn',
        adsCost: row.adsCost > 0 ? this.money(row.adsCost) : '0',
        profit: row.cost > 0 ? this.money(row.profit) : 'Chưa có giá vốn',
        orders: this.qty(row.orders),
        roas,
        compare: index === days.length - 1 ? '—' : this.pct(compareValue),
        positive: compareValue >= 0,
        isToday,
        raw: { revenue: row.revenue, cost: row.cost, adsCost: row.adsCost, profit: row.profit, orders: row.orders },
      };
    });

    const totalCost = monthOrders.reduce((sum, order) => {
      return (
        sum +
        order.items.reduce((itemSum, item) => {
          return itemSum + this.n(item.variant?.costPrice) * this.n(item.qty);
        }, 0)
      );
    }, 0);
    const monthRevenue = monthOrders.reduce((sum, order) => sum + this.n(order.finalAmount), 0);
    const totalAdsCost = Array.from(adsCostByDay.values()).reduce((sum, value) => sum + value, 0);
    const profit = monthRevenue - totalCost - totalAdsCost;
    const hasCostData = totalCost > 0;

    const todayCompletedOrders = monthOrders.filter((order) => {
      const key = this.dayKey(order.soldAt || order.createdAt);
      return key === this.dayKey(todayStart) && order.status === 'COMPLETED';
    }).length;
    const todayOrders = monthOrders.filter((order) => this.dayKey(order.soldAt || order.createdAt) === this.dayKey(todayStart)).length;
    const checkoutPurchase = todayOrders > 0 ? `${todayCompletedOrders}/${todayOrders}` : `${completedOrders}/${totalOrders}`;

    const decisionCards: Array<{
      id: string;
      eyebrow: string;
      title: string;
      desc: string;
      source: string;
      score: string;
      tag: string;
      tone: Tone;
      actionUrl?: string;
      actionType?: string;
      variantId?: string;
      productId?: string;
    }> = [];

    for (const item of criticalCandidates.slice(0, 4)) {
      const label = this.productLabel({ productName: item.productName, sku: item.sku, color: item.color, size: item.size });
      const daysText = item.daysToOut !== null && item.daysToOut < 999 ? ` · dự kiến hết sau ${Math.max(1, Math.ceil(item.daysToOut))} ngày` : '';
      decisionCards.push({
        id: `critical-stock-${item.variantId}`,
        eyebrow: item.score >= 95 ? 'Bảo vệ tồn' : 'Sales velocity',
        title: `Sắp hết hàng ${label}`,
        desc: `Tồn ${this.qty(item.stock)} · bán 14 ngày ${this.qty(item.sold14)} sp${daysText}. Nên kiểm tra nhập hàng, điều chuyển kho hoặc giảm đẩy bán nếu đang chạy ads.`,
        source: 'Inventory',
        score: `${item.score}%`,
        tag: item.score >= 95 ? 'Khẩn cấp' : 'Cảnh báo',
        tone: item.score >= 95 ? 'critical' : 'warning',
        actionUrl: item.actionUrl,
        actionType: 'open-product',
        variantId: item.variantId,
        productId: item.productId,
      });
    }

    const reorderCandidates = criticalCandidates
      .filter((item) => item.velocity14 > 0 && item.daysToOut !== null && item.daysToOut <= 14)
      .slice(0, 2);
    for (const item of reorderCandidates) {
      const targetCoverDays = 21;
      const suggestedQty = Math.max(3, Math.ceil(item.velocity14 * targetCoverDays - item.stock - item.incoming));
      if (suggestedQty <= 0) continue;
      decisionCards.push({
        id: `reorder-${item.variantId}`,
        eyebrow: 'Autopilot nhập',
        title: `Gợi ý nhập thêm ${this.qty(suggestedQty)} sp ${this.productLabel(item)}`,
        desc: `Velocity 14 ngày: ${item.velocity14.toFixed(2)} sp/ngày · tồn ${this.qty(item.stock)} · mục tiêu đủ bán ${targetCoverDays} ngày.`,
        source: 'Autopilot Reorder',
        score: `${Math.min(99, item.score + 1)}%`,
        tag: 'Gợi ý nhập',
        tone: 'warning',
        actionUrl: item.actionUrl,
        actionType: 'reorder-suggestion',
        variantId: item.variantId,
        productId: item.productId,
      });
    }

    const transferSuggestion = criticalCandidates.find((item) => {
      const lowBranch = item.branchStocks.some((b) => b.availableQty <= 1);
      const highBranch = item.branchStocks.some((b) => b.availableQty >= 5);
      return lowBranch && highBranch;
    });
    if (transferSuggestion) {
      const source = [...transferSuggestion.branchStocks].sort((a, b) => b.availableQty - a.availableQty)[0];
      const target = [...transferSuggestion.branchStocks].sort((a, b) => a.availableQty - b.availableQty)[0];
      const qty = Math.max(1, Math.min(3, source.availableQty - 2));
      decisionCards.push({
        id: `transfer-${transferSuggestion.variantId}`,
        eyebrow: 'Autopilot chuyển kho',
        title: `Gợi ý chuyển ${qty} sp từ ${source.branchName} sang ${target.branchName}`,
        desc: `${this.productLabel(transferSuggestion)} · ${source.branchName} còn ${source.availableQty}, ${target.branchName} còn ${target.availableQty}.`,
        source: 'Stock Transfer',
        score: '92%',
        tag: 'Điều chuyển',
        tone: 'warning',
        actionUrl: transferSuggestion.actionUrl,
        actionType: 'transfer-suggestion',
        variantId: transferSuggestion.variantId,
        productId: transferSuggestion.productId,
      });
    }

    if (newOrders > 0) {
      decisionCards.push({
        id: 'new-orders',
        eyebrow: 'Xử lý đơn',
        title: `${newOrders} đơn mới cần xử lý`,
        desc: `Ưu tiên duyệt đơn, đóng gói và xuất kho để tránh lệch tồn khả dụng. Click Open để sang danh sách đơn chưa xuất kho.`,
        source: 'Orders',
        score: newOrders >= 10 ? '91%' : '82%',
        tag: 'Theo dõi',
        tone: newOrders >= 10 ? 'warning' : 'safe',
        actionUrl: '/orders?status=NEW&fulfillmentStatus=UNFULFILLED',
        actionType: 'open-orders',
      });
    }

    if (pendingTransfers > 0) {
      decisionCards.push({
        id: 'pending-transfers',
        eyebrow: 'Điều chuyển',
        title: `${pendingTransfers} phiếu chuyển kho đang chờ`,
        desc: `Cần xác nhận hoặc hoàn tất phiếu chuyển để tồn kho từng chi nhánh hiển thị đúng.`,
        source: 'Stock Transfer',
        score: '88%',
        tag: 'Cần xử lý',
        tone: 'warning',
        actionUrl: '/stock-transfers?status=PENDING',
        actionType: 'open-transfers',
      });
    }

    const revenueDropPct = yesterdayRevenue > 0 ? ((todayRevenue - yesterdayRevenue) / yesterdayRevenue) * 100 : 0;
    if (yesterdayRevenue > 0 && revenueDropPct <= -10) {
      decisionCards.push({
        id: 'revenue-drop',
        eyebrow: 'Doanh thu',
        title: `Doanh thu hôm nay giảm ${Math.abs(revenueDropPct).toFixed(1)}%`,
        desc: `Hôm nay ${this.money(todayRevenue)} so với hôm qua ${this.money(yesterdayRevenue)}. Nên kiểm tra nguồn đơn, POS và trạng thái thanh toán.`,
        source: 'Orders',
        score: '90%',
        tag: 'Cảnh báo',
        tone: 'warning',
        actionUrl: '/orders',
        actionType: 'open-orders',
      });
    }

    const sortedDecisionCards = decisionCards.sort((a, b) => this.n(b.score) - this.n(a.score)).slice(0, 10);
    if (!sortedDecisionCards.length) {
      sortedDecisionCards.push({
        id: 'system-ok',
        eyebrow: 'Ổn định',
        title: 'Chưa có cảnh báo lớn',
        desc: `Hệ thống ghi nhận ${this.qty(totalOrders)} đơn và ${this.qty(availableQty)} tồn khả dụng.`,
        source: 'Dashboard',
        score: '80%',
        tag: 'Live data',
        tone: 'safe',
        actionUrl: '/orders',
        actionType: 'open-orders',
      });
    }

    const systemTone: Tone = sortedDecisionCards.some((c) => c.tone === 'critical') ? 'critical' : sortedDecisionCards.some((c) => c.tone === 'warning') ? 'warning' : 'safe';
    const statusTitle = systemTone === 'safe' ? 'SYSTEM STATUS: SAFE' : systemTone === 'critical' ? 'SYSTEM STATUS: CRITICAL' : 'SYSTEM STATUS: WARNING';

    const recentOrders = recentOrdersRaw.map((order) => ({
      id: order.id,
      code: order.orderCode,
      customerName: order.customerName || 'Khách lẻ',
      phone: order.customerPhone || '',
      status: order.status,
      paymentStatus: order.paymentStatus,
      fulfillmentStatus: order.fulfillmentStatus,
      finalAmount: this.n(order.finalAmount),
      salesChannel: order.salesChannel,
      branchId: order.branchId,
      createdAt: order.createdAt,
      soldAt: order.soldAt,
      actionUrl: `/orders/${order.id}`,
    }));

    const highestVelocity = criticalCandidates[0];
    const topWarehouse = warehouseMix[0];
    const bestChannel = channelRevenue[0];

    return {
      success: true,
      branchId: selectedBranchId || 'ALL',
      generatedAt: now.toISOString(),
      hero: {
        status: systemTone,
        title: statusTitle,
        subtitle: `Doanh thu ${this.money(revenue)} · ${this.qty(totalOrders)} đơn · ${this.qty(criticalCandidates.length)} SKU critical`,
        chips: [`${this.qty(totalOrders)} đơn`, `${this.money(revenue)} doanh thu`, `${this.qty(criticalCandidates.length)} SKU critical`],
        autoMode: 'SEMI',
        metaMode: 'DISCONNECTED',
        metaAccount: 'Meta Ads chưa nối live data',
        scheduler: { label: 'Chưa có lịch live', times: [] },
      },
      warningSummary: {
        level: systemTone,
        title: systemTone === 'safe' ? 'Hệ thống đang ổn định' : 'Có tín hiệu rủi ro cần theo dõi sát',
        subtitle: `Smart alert: chỉ hiện top ${this.qty(criticalCandidates.length)} SKU critical từ ${this.qty(rawLowStockPool)} SKU tồn thấp có bán gần đây.`,
        revenue: revenue > 0 ? this.money(revenue) : 'Chưa ghi nhận',
        roas: totalAdsCost > 0 ? `${(revenue / totalAdsCost).toFixed(2)}x` : 'Chưa nối Meta',
        inventory: criticalCandidates.length > 0 ? `${this.qty(criticalCandidates.length)} SKU critical` : 'Ổn định',
      },
      cards: {
        revenue,
        todayRevenue,
        yesterdayRevenue,
        profit: hasCostData ? profit : 0,
        profitLabel: hasCostData ? this.money(profit) : 'Chưa có giá vốn',
        totalCost,
        totalAdsCost,
        totalOrders,
        newOrders,
        completedOrders,
        cancelledOrders,
        productCount,
        variantCount,
        availableQty,
        reservedQty,
        incomingQty,
        lowStockItems: criticalCandidates.length,
        outOfStockItems,
        rawLowStockPool,
        rawOutOfStockPool,
        pendingTransfers,
      },
      decisionCards: sortedDecisionCards,
      smartAlerts: criticalCandidates,
      insightRow: [
        {
          id: 'orders-live',
          title: 'Tổng quan đơn hàng live',
          desc: `${this.qty(totalOrders)} đơn · ${this.qty(newOrders)} đơn mới · ${this.qty(completedOrders)} hoàn thành · ${this.qty(cancelledOrders)} huỷ.`,
          tone: cancelledOrders > 0 ? 'warning' : 'safe',
          badge: 'Orders',
        },
        {
          id: 'sales-velocity',
          title: 'Sales velocity',
          desc: highestVelocity
            ? `${this.productLabel(highestVelocity)} đang bán nhanh nhất trong nhóm rủi ro: ${highestVelocity.velocity14.toFixed(2)} sp/ngày.`
            : 'Chưa có SKU bán nhanh chạm ngưỡng tồn thấp.',
          tone: highestVelocity ? 'warning' : 'safe',
          badge: 'Velocity',
        },
        {
          id: 'profit-live',
          title: 'Profit sau giá vốn & ads',
          desc: hasCostData
            ? `Lợi nhuận tháng này ${this.money(profit)} = doanh thu ${this.money(monthRevenue)} - giá vốn ${this.money(totalCost)} - ads ${this.money(totalAdsCost)}.`
            : 'Chưa đủ giá vốn để tính lợi nhuận chuẩn.',
          tone: hasCostData && profit > 0 ? 'safe' : 'warning',
          badge: 'Profit',
        },
      ],
      realtime: {
        delta: this.money(todayRevenue),
        deltaPct: yesterdayRevenue > 0 ? this.pct(revenueDropPct) : 'Live',
        checkoutPurchase,
        chokeLabel: 'Đơn hoàn thành / tổng đơn hôm nay',
        lowStock: criticalCandidates.slice(0, 5).map((item) => {
          const days = item.daysToOut !== null && item.daysToOut < 999 ? ` · ~${Math.ceil(item.daysToOut)} ngày` : '';
          return `${this.productLabel(item)} • còn ${this.qty(item.stock)}${days}`;
        }),
      },
      kpis: [
        { id: 'k1', label: 'Doanh thu', value: this.money(revenue), delta: 'Live' },
        { id: 'k2', label: 'Đơn hàng', value: this.qty(totalOrders), delta: `${this.qty(newOrders)} mới` },
        { id: 'k3', label: 'Tồn khả dụng', value: this.qty(availableQty), delta: `${this.qty(reservedQty)} giữ` },
        { id: 'k4', label: 'Top critical SKU', value: this.qty(criticalCandidates.length), delta: `${this.qty(rawLowStockPool)} pool` },
        { id: 'k5', label: 'Lợi nhuận', value: hasCostData ? this.money(profit) : '—', delta: hasCostData ? 'Sau giá vốn + ads' : 'Thiếu giá vốn' },
      ],
      dailyRows,
      drilldown: [
        { label: 'Doanh thu', value: this.money(todayRevenue || revenue) },
        { label: 'Giá vốn', value: hasCostData ? this.money(totalCost) : '—' },
        { label: 'Chi phí ads', value: this.money(totalAdsCost) },
        { label: 'Hoàn thành', value: this.qty(todayCompletedOrders || completedOrders), tone: 'dark' },
        { label: 'Lợi nhuận', value: hasCostData ? this.money(profit) : '—', tone: 'mint' },
      ],
      topProducts,
      channelRevenue,
      warehouseMix,
      quickInsights: [
        `Smart alert đang lọc top ${this.qty(criticalCandidates.length)} SKU critical thay vì spam ${this.qty(rawLowStockPool)} SKU tồn thấp.`,
        highestVelocity
          ? `${this.productLabel(highestVelocity)} có velocity ${highestVelocity.velocity14.toFixed(2)} sp/ngày, còn ${this.qty(highestVelocity.stock)} sp.`
          : 'Chưa có SKU bán nhanh chạm ngưỡng tồn thấp.',
        topWarehouse ? `Kho có tỷ trọng lớn nhất: ${topWarehouse.name} (${topWarehouse.value}).` : 'Chưa có dữ liệu phân bổ kho.',
        bestChannel ? `Kênh doanh thu nổi bật: ${bestChannel.name} (${bestChannel.value}).` : 'Chưa có dữ liệu doanh thu theo kênh.',
      ],
      moneyFlow: channelRevenue.length
        ? channelRevenue.map((row) => ({
            channel: row.name,
            text: `${row.name} ghi nhận ${row.value} từ ${this.qty((row as any).orders)} đơn.`,
            badge: 'Live',
            tone: 'green',
          }))
        : [
            {
              channel: 'Chưa nối dữ liệu kênh',
              text: 'Chưa có dữ liệu live từ ads/payment channels.',
              badge: 'Pending',
              tone: 'amber',
            },
          ],
      funnel: [
        { label: 'Orders', value: this.qty(totalOrders), width: '100%' },
        { label: 'New', value: this.qty(newOrders), width: totalOrders > 0 ? `${Math.max(4, Math.round((newOrders / totalOrders) * 100))}%` : '4%' },
        { label: 'Completed', value: this.qty(completedOrders), width: totalOrders > 0 ? `${Math.max(4, Math.round((completedOrders / totalOrders) * 100))}%` : '4%' },
        { label: 'Cancelled', value: this.qty(cancelledOrders), width: totalOrders > 0 ? `${Math.max(4, Math.round((cancelledOrders / totalOrders) * 100))}%` : '4%' },
      ],
      floatingApproval: {
        count: `${this.qty(pendingTransfers + criticalCandidates.length)} pending`,
        title: criticalCandidates.length > 0 ? 'Xử lý top SKU critical' : pendingTransfers > 0 ? 'Xử lý chuyển kho' : 'Không có cảnh báo lớn',
        subtitle: `Orders ${this.qty(totalOrders)} · Inventory ${this.qty(availableQty)}`,
      },
      recentOrders,
    };
  }
}
