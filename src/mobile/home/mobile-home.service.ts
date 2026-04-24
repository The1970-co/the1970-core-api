import { Injectable } from "@nestjs/common";
import { PrismaService } from "../../prisma/prisma.service";

@Injectable()
export class MobileHomeService {
  constructor(private prisma: PrismaService) {}

  async getHome(branchId?: string) {
    const commonWhere: any = {};

    if (branchId && branchId !== "all") {
      commonWhere.branchId = branchId;
    }

    const todayStart = new Date();
    todayStart.setHours(0, 0, 0, 0);

    const [completedTodayOrders, groupedOrders, inventoryItems, orderItems] =
      await Promise.all([
        this.prisma.order.findMany({
          where: {
            ...commonWhere,
            createdAt: {
              gte: todayStart,
            },
            status: "COMPLETED",
          },
          select: {
            finalAmount: true,
          },
        }),

        this.prisma.order.groupBy({
          by: ["status"],
          where: commonWhere,
          _count: {
            status: true,
          },
        }),

        this.prisma.inventoryItem.findMany({
          where: commonWhere,
          select: {
            availableQty: true,
          },
        }),

        this.prisma.orderItem.findMany({
          where: {
            order: {
              ...commonWhere,
              status: {
                not: "CANCELLED",
              },
            },
          },
          select: {
            productName: true,
            qty: true,
            lineTotal: true,
          },
        }),
      ]);

    const revenueToday = completedTodayOrders.reduce(
      (sum, order) => sum + Number(order.finalAmount || 0),
      0
    );

    const orderSummary = {
      NEW: 0,
      APPROVED: 0,
      PACKING: 0,
      SHIPPED: 0,
      COMPLETED: 0,
      CANCELLED: 0,
    };

    for (const row of groupedOrders) {
      orderSummary[row.status] = row._count.status;
    }

    let lowStockCount = 0;
    let outOfStockCount = 0;

    for (const item of inventoryItems) {
      const qty = item.availableQty || 0;
      if (qty === 0) outOfStockCount++;
      else if (qty <= 5) lowStockCount++;
    }

    const topProductsMap = new Map<
      string,
      { productName: string; qty: number; revenue: number }
    >();

    for (const item of orderItems) {
      const key = item.productName;

      if (!topProductsMap.has(key)) {
        topProductsMap.set(key, {
          productName: key,
          qty: 0,
          revenue: 0,
        });
      }

      const current = topProductsMap.get(key)!;
      current.qty += item.qty;
      current.revenue += Number(item.lineTotal || 0);
    }

    const topProducts = Array.from(topProductsMap.values())
      .sort((a, b) => b.revenue - a.revenue)
      .slice(0, 5);

    const alerts: string[] = [];

    if (orderSummary.NEW > 0) {
      alerts.push(`${orderSummary.NEW} đơn mới chưa xử lý`);
    }

    if (lowStockCount > 0) {
      alerts.push(`${lowStockCount} SKU sắp hết hàng`);
    }

    if (outOfStockCount > 0) {
      alerts.push(`${outOfStockCount} SKU đã hết hàng`);
    }

    const [codPendingAgg, codReceivedTodayAgg] = await Promise.all([
      this.prisma.order.aggregate({
        _sum: {
          finalAmount: true,
        },
        where: {
          ...commonWhere,
          paymentStatus: "PENDING_COD",
        },
      }),

      this.prisma.order.aggregate({
        _sum: {
          finalAmount: true,
        },
        where: {
          ...commonWhere,
          paymentStatus: "PAID",
          createdAt: {
            gte: todayStart,
          },
        },
      }),
    ]);

    const finance = {
      codPending: Number(codPendingAgg._sum.finalAmount || 0),
      codReceivedToday: Number(codReceivedTodayAgg._sum.finalAmount || 0),
    };

    return {
      summary: {
        revenueToday,
        ordersToday: completedTodayOrders.length,
      },
      orders: orderSummary,
      inventory: {
        lowStockCount,
        outOfStockCount,
      },
      topProducts,
      alerts,
      finance,
    };
  }
}