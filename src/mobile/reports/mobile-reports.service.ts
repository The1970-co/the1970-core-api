import { Injectable } from "@nestjs/common";
import { PrismaService } from "../../prisma/prisma.service";

@Injectable()
export class MobileReportsService {
  constructor(private prisma: PrismaService) {}

  async getSales(days = 7, branchId?: string) {
    const now = new Date();
    const start = new Date();
    start.setDate(now.getDate() - (days - 1));
    start.setHours(0, 0, 0, 0);

    const where: any = {
      createdAt: {
        gte: start,
        lte: now,
      },
      status: "COMPLETED",
    };

    if (branchId && branchId !== "all") {
      where.branchId = branchId;
    }

    const orders = await this.prisma.order.findMany({
      where,
      select: {
        finalAmount: true,
        createdAt: true,
      },
    });

    const revenueMap = new Map<string, number>();

    for (let i = 0; i < days; i++) {
      const d = new Date();
      d.setDate(now.getDate() - i);
      const key = d.toISOString().slice(0, 10);
      revenueMap.set(key, 0);
    }

    for (const order of orders) {
      const key = order.createdAt.toISOString().slice(0, 10);
      revenueMap.set(key, (revenueMap.get(key) || 0) + Number(order.finalAmount || 0));
    }

    return Array.from(revenueMap.entries())
      .map(([date, revenue]) => ({
        date,
        revenue,
      }))
      .sort((a, b) => a.date.localeCompare(b.date));
  }

  async getInventory(branchId?: string) {
    const where: any = {};

    if (branchId && branchId !== "all") {
      where.branchId = branchId;
    }

    const inventory = await this.prisma.inventoryItem.findMany({
      where,
      select: {
        id: true,
        branchId: true,
        availableQty: true,
        reservedQty: true,
        incomingQty: true,
        variantId: true,
        variant: {
          select: {
            sku: true,
            color: true,
            size: true,
            product: {
              select: {
                id: true,
                name: true,
              },
            },
          },
        },
      },
      orderBy: {
        updatedAt: "desc",
      },
    });

    let totalQuantity = 0;

    const lowStock: Array<{
      inventoryItemId: string;
      productId: string | null;
      productName: string;
      sku: string;
      color: string | null;
      size: string | null;
      branchId: string;
      quantity: number;
    }> = [];

    const outOfStock: Array<{
      inventoryItemId: string;
      productId: string | null;
      productName: string;
      sku: string;
      color: string | null;
      size: string | null;
      branchId: string;
      quantity: number;
    }> = [];

    for (const item of inventory) {
      const qty = item.availableQty || 0;
      totalQuantity += qty;

      const row = {
        inventoryItemId: item.id,
        productId: item.variant?.product?.id ?? null,
        productName: item.variant?.product?.name ?? "Unknown product",
        sku: item.variant?.sku ?? "",
        color: item.variant?.color ?? null,
        size: item.variant?.size ?? null,
        branchId: item.branchId,
        quantity: qty,
      };

      if (qty === 0) {
        outOfStock.push(row);
      } else if (qty <= 5) {
        lowStock.push(row);
      }
    }

    return {
      totalSku: inventory.length,
      totalQuantity,
      lowStock: lowStock.slice(0, 20),
      outOfStock: outOfStock.slice(0, 20),
    };
  }

  async getTopProducts(branchId?: string) {
    const orderWhere: any = {
      status: {
        not: "CANCELLED",
      },
    };

    if (branchId && branchId !== "all") {
      orderWhere.branchId = branchId;
    }

    const items = await this.prisma.orderItem.findMany({
      where: {
        order: orderWhere,
      },
      select: {
        productName: true,
        qty: true,
        lineTotal: true,
      },
    });

    const map = new Map<
      string,
      { productName: string; qty: number; revenue: number }
    >();

    for (const item of items) {
      const key = item.productName;

      if (!map.has(key)) {
        map.set(key, {
          productName: key,
          qty: 0,
          revenue: 0,
        });
      }

      const current = map.get(key)!;
      current.qty += item.qty;
      current.revenue += Number(item.lineTotal || 0);
    }

    return Array.from(map.values())
      .sort((a, b) => b.revenue - a.revenue)
      .slice(0, 5);
  }
  async getLowStockDetail(branchId?: string) {
  const where: any = {
    availableQty: {
      gt: 0,
      lte: 5,
    },
  };

  if (branchId && branchId !== "all") {
    where.branchId = branchId;
  }

  const rows = await this.prisma.inventoryItem.findMany({
    where,
    select: {
      id: true,
      branchId: true,
      availableQty: true,
      variant: {
        select: {
          sku: true,
          color: true,
          size: true,
          product: {
            select: {
              id: true,
              name: true,
            },
          },
        },
      },
    },
    orderBy: {
      availableQty: "asc",
    },
    take: 50,
  });

  return rows.map((row) => ({
    inventoryItemId: row.id,
    branchId: row.branchId,
    quantity: row.availableQty,
    productId: row.variant?.product?.id ?? null,
    productName: row.variant?.product?.name ?? "Unknown product",
    sku: row.variant?.sku ?? "",
    color: row.variant?.color ?? null,
    size: row.variant?.size ?? null,
  }));
}

async getOutOfStockDetail(branchId?: string) {
  const where: any = {
    availableQty: 0,
  };

  if (branchId && branchId !== "all") {
    where.branchId = branchId;
  }

  const rows = await this.prisma.inventoryItem.findMany({
    where,
    select: {
      id: true,
      branchId: true,
      availableQty: true,
      variant: {
        select: {
          sku: true,
          color: true,
          size: true,
          product: {
            select: {
              id: true,
              name: true,
            },
          },
        },
      },
    },
    orderBy: {
      updatedAt: "desc",
    },
    take: 100,
  });

  return rows.map((row) => ({
    inventoryItemId: row.id,
    branchId: row.branchId,
    quantity: row.availableQty,
    productId: row.variant?.product?.id ?? null,
    productName: row.variant?.product?.name ?? "Unknown product",
    sku: row.variant?.sku ?? "",
    color: row.variant?.color ?? null,
    size: row.variant?.size ?? null,
  }));
}
}