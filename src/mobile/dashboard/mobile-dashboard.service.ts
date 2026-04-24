import { Injectable } from "@nestjs/common";
import { PrismaService } from "../../prisma/prisma.service";

type MobileDashboardSummary = {
  revenueToday: number;
  revenueVsYesterdayPct: number;
  ordersToday: number;
  ordersVsYesterdayPct: number;
  aovToday: number;
  aovVsYesterdayPct: number;
};

type MobileBranchBreakdownRow = {
  branchId: string;
  branchName: string;
  revenue: number;
  orders: number;
  aov: number;
};

type MobileDashboardAlert = {
  id: string;
  level: "critical" | "warning";
  title: string;
  message: string;
};

@Injectable()
export class MobileDashboardService {
  constructor(private prisma: PrismaService) {}

  private getTodayRange() {
    const start = new Date();
    start.setHours(0, 0, 0, 0);

    const end = new Date();
    end.setHours(23, 59, 59, 999);

    return { start, end };
  }

  private getYesterdayRange() {
    const start = new Date();
    start.setDate(start.getDate() - 1);
    start.setHours(0, 0, 0, 0);

    const end = new Date();
    end.setDate(end.getDate() - 1);
    end.setHours(23, 59, 59, 999);

    return { start, end };
  }

  private calcPct(current: number, previous: number) {
    if (previous <= 0) {
      if (current > 0) return 100;
      return 0;
    }
    return Math.round(((current - previous) / previous) * 100);
  }

  async getSummary(branchId?: string): Promise<MobileDashboardSummary> {
    const today = this.getTodayRange();
    const yesterday = this.getYesterdayRange();

    const todayWhere: any = {
      createdAt: {
        gte: today.start,
        lte: today.end,
      },
      status: "COMPLETED",
    };

    const yesterdayWhere: any = {
      createdAt: {
        gte: yesterday.start,
        lte: yesterday.end,
      },
      status: "COMPLETED",
    };

    if (branchId && branchId !== "all") {
      todayWhere.branchId = branchId;
      yesterdayWhere.branchId = branchId;
    }

    const [todayOrders, yesterdayOrders] = await Promise.all([
      this.prisma.order.findMany({
        where: todayWhere,
        select: {
          finalAmount: true,
        },
      }),
      this.prisma.order.findMany({
        where: yesterdayWhere,
        select: {
          finalAmount: true,
        },
      }),
    ]);

    const revenueToday = todayOrders.reduce(
      (sum, o) => sum + Number(o.finalAmount || 0),
      0
    );
    const ordersToday = todayOrders.length;
    const aovToday =
      ordersToday > 0 ? Math.round(revenueToday / ordersToday) : 0;

    const revenueYesterday = yesterdayOrders.reduce(
      (sum, o) => sum + Number(o.finalAmount || 0),
      0
    );
    const ordersYesterday = yesterdayOrders.length;
    const aovYesterday =
      ordersYesterday > 0 ? Math.round(revenueYesterday / ordersYesterday) : 0;

    return {
      revenueToday,
      revenueVsYesterdayPct: this.calcPct(revenueToday, revenueYesterday),
      ordersToday,
      ordersVsYesterdayPct: this.calcPct(ordersToday, ordersYesterday),
      aovToday,
      aovVsYesterdayPct: this.calcPct(aovToday, aovYesterday),
    };
  }

  async getBranchBreakdown(branchId?: string): Promise<MobileBranchBreakdownRow[]> {
    const today = this.getTodayRange();

    const completedOrdersToday = await this.prisma.order.findMany({
      where: {
        createdAt: {
          gte: today.start,
          lte: today.end,
        },
        status: "COMPLETED",
        ...(branchId && branchId !== "all" ? { branchId } : {}),
      },
      select: {
        branchId: true,
        finalAmount: true,
      },
    });

    if (branchId && branchId !== "all") {
      const branch = await this.prisma.branch.findUnique({
        where: { id: branchId },
        select: { id: true, name: true },
      });

      const revenue = completedOrdersToday.reduce(
        (sum, o) => sum + Number(o.finalAmount || 0),
        0
      );
      const orders = completedOrdersToday.length;
      const aov = orders > 0 ? Math.round(revenue / orders) : 0;

      return [
        {
          branchId,
          branchName: branch?.name ?? branchId,
          revenue,
          orders,
          aov,
        },
      ];
    }

    const branches = await this.prisma.branch.findMany({
      where: { isActive: true },
      orderBy: { name: "asc" },
      select: {
        id: true,
        name: true,
      },
    });

    const grouped = new Map<
      string,
      { revenue: number; orders: number }
    >();

    for (const order of completedOrdersToday) {
      const key = order.branchId || "unknown";
      const current = grouped.get(key) || { revenue: 0, orders: 0 };
      current.revenue += Number(order.finalAmount || 0);
      current.orders += 1;
      grouped.set(key, current);
    }

    return branches.map((branch) => {
      const data = grouped.get(branch.id) || { revenue: 0, orders: 0 };
      return {
        branchId: branch.id,
        branchName: branch.name,
        revenue: data.revenue,
        orders: data.orders,
        aov: data.orders > 0 ? Math.round(data.revenue / data.orders) : 0,
      };
    });
  }

  async getAlerts(branchId?: string): Promise<MobileDashboardAlert[]> {
    const today = this.getTodayRange();

    const where: any = {
      createdAt: {
        gte: today.start,
        lte: today.end,
      },
      status: "SHIPPED",
    };

    if (branchId && branchId !== "all") {
      where.branchId = branchId;
    }

    const shippedCount = await this.prisma.order.count({ where });

    const alerts: MobileDashboardAlert[] = [];

    if (shippedCount >= 50) {
      alerts.push({
        id: "high-shipped-orders",
        level: "warning",
        title: "Đơn đang giao cao",
        message: `${shippedCount} đơn đang ở trạng thái SHIPPED`,
      });
    }

    return alerts;
  }
}