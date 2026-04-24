import { Injectable } from "@nestjs/common";
import { PrismaService } from "../../prisma/prisma.service";

@Injectable()
export class MobileOperationsService {
  constructor(private prisma: PrismaService) {}

  async getOrderSummary(branchId?: string) {
    const where: any = {};

    if (branchId && branchId !== "all") {
      where.branchId = branchId;
    }

    const grouped = await this.prisma.order.groupBy({
      by: ["status"],
      where,
      _count: {
        status: true,
      },
    });

    const result = {
      NEW: 0,
      APPROVED: 0,
      PACKING: 0,
      SHIPPED: 0,
      COMPLETED: 0,
      CANCELLED: 0,
    };

    for (const row of grouped) {
      result[row.status] = row._count.status;
    }

    return result;
  }

  async searchInventory(q?: string, branchId?: string) {
    if (!q || !q.trim()) {
      return [];
    }

    const text = q.trim();

    const where: any = {
      OR: [
        {
          variant: {
            sku: {
              contains: text,
              mode: "insensitive",
            },
          },
        },
        {
          variant: {
            product: {
              name: {
                contains: text,
                mode: "insensitive",
              },
            },
          },
        },
      ],
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
        reservedQty: true,
        incomingQty: true,
        variant: {
          select: {
            id: true,
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
      take: 30,
    });

    return rows.map((row) => ({
      inventoryItemId: row.id,
      branchId: row.branchId,
      availableQty: row.availableQty,
      reservedQty: row.reservedQty,
      incomingQty: row.incomingQty,
      variantId: row.variant?.id ?? null,
      sku: row.variant?.sku ?? "",
      color: row.variant?.color ?? null,
      size: row.variant?.size ?? null,
      productId: row.variant?.product?.id ?? null,
      productName: row.variant?.product?.name ?? "Unknown product",
    }));
  }

  async getGroupedInventory(q?: string, branchId?: string) {
    if (!q || !q.trim()) return [];

    const text = q.trim();

    const where: any = {
      OR: [
        {
          variant: {
            sku: { contains: text, mode: "insensitive" },
          },
        },
        {
          variant: {
            product: {
              name: { contains: text, mode: "insensitive" },
            },
          },
        },
      ],
    };

    if (branchId && branchId !== "all") {
      where.branchId = branchId;
    }

    const rows = await this.prisma.inventoryItem.findMany({
      where,
      select: {
        branchId: true,
        availableQty: true,
        variant: {
          select: {
            sku: true,
            size: true,
            product: {
              select: {
                name: true,
              },
            },
          },
        },
      },
    });

    const map = new Map<
      string,
      {
        productName: string;
        variants: Record<string, { branchId: string; qty: number }[]>;
      }
    >();

    for (const row of rows) {
      const productName = row.variant?.product?.name || "Unknown";
      const size = row.variant?.size || "N/A";
      const key = productName;

      if (!map.has(key)) {
        map.set(key, {
          productName,
          variants: {},
        });
      }

      const product = map.get(key)!;

      if (!product.variants[size]) {
        product.variants[size] = [];
      }

      product.variants[size].push({
        branchId: row.branchId,
        qty: row.availableQty || 0,
      });
    }

    return Array.from(map.values()).map((p) => ({
      productName: p.productName,
      variants: Object.entries(p.variants).map(([size, branches]) => ({
        size,
        branches,
      })),
    }));
  }

  async getTransferSuggestions() {
    const inventory = await this.prisma.inventoryItem.findMany({
      select: {
        branchId: true,
        availableQty: true,
        variant: {
          select: {
            size: true,
            sku: true,
            product: {
              select: {
                name: true,
              },
            },
          },
        },
      },
    });

    type InventoryGroupedRow = {
      branchId: string;
      qty: number;
      productName: string;
      size: string;
    };

    type TransferSuggestion = {
      productName: string;
      size: string;
      from: string;
      to: string;
      qty: number;
    };

    const grouped = new Map<string, InventoryGroupedRow[]>();

    for (const item of inventory) {
      const productName = item.variant?.product?.name ?? "Unknown product";
      const size = item.variant?.size ?? "N/A";
      const key = `${productName}_${size}`;

      if (!grouped.has(key)) {
        grouped.set(key, []);
      }

      grouped.get(key)!.push({
        branchId: item.branchId,
        qty: item.availableQty || 0,
        productName,
        size,
      });
    }

    const suggestions: TransferSuggestion[] = [];

    for (const [, items] of grouped) {
      const shortage = items.filter((i) => i.qty <= 2);
      const surplus = items.filter((i) => i.qty >= 5);

      for (const need of shortage) {
        for (const have of surplus) {
          if (need.branchId === have.branchId) continue;

          const qty = Math.min(3, have.qty - 4);

          if (qty > 0) {
            suggestions.push({
              productName: need.productName,
              size: need.size,
              from: have.branchId,
              to: need.branchId,
              qty,
            });
          }
        }
      }
    }

    return suggestions;
  }
  async getOrders(status?: string, branchId?: string) {
  const where: any = {};

  if (status && status !== "all") {
    where.status = status;
  }

  if (branchId && branchId !== "all") {
    where.branchId = branchId;
  }

  const orders = await this.prisma.order.findMany({
    where,
    select: {
      id: true,
      orderCode: true,
      customerName: true,
      customerPhone: true,
      branchId: true,
      status: true,
      paymentStatus: true,
      fulfillmentStatus: true,
      finalAmount: true,
      createdAt: true,
    },
    orderBy: {
      createdAt: "desc",
    },
    take: 100,
  });

  return orders.map((order) => ({
    id: order.id,
    orderCode: order.orderCode,
    customerName: order.customerName || "Khách lẻ",
    customerPhone: order.customerPhone || "",
    branchId: order.branchId,
    status: order.status,
    paymentStatus: order.paymentStatus,
    fulfillmentStatus: order.fulfillmentStatus,
    finalAmount: Number(order.finalAmount || 0),
    createdAt: order.createdAt,
  }));
}
async getOrderDetail(id: string) {
  const order = await this.prisma.order.findUnique({
    where: { id },
    select: {
      id: true,
      orderCode: true,
      customerName: true,
      customerPhone: true,
      branchId: true,
      status: true,
      paymentStatus: true,
      fulfillmentStatus: true,
      totalAmount: true,
      discountAmount: true,
      shippingFee: true,
      finalAmount: true,
      note: true,
      createdAt: true,

      shippingRecipientName: true,
      shippingPhone: true,
      shippingAddressLine1: true,
      shippingWard: true,
      shippingDistrict: true,
      shippingProvince: true,

      items: {
        select: {
          id: true,
          productName: true,
          sku: true,
          color: true,
          size: true,
          qty: true,
          unitPrice: true,
          lineTotal: true,
        },
      },

      shipment: {
        select: {
          carrier: true,
          trackingCode: true,
          shippingStatus: true,
          partnerStatus: true,
          codAmount: true,
          shippingFee: true,
        },
      },
    },
  });

  if (!order) {
    return null;
  }

  return {
    ...order,
    totalAmount: Number(order.totalAmount || 0),
    discountAmount: Number(order.discountAmount || 0),
    shippingFee: Number(order.shippingFee || 0),
    finalAmount: Number(order.finalAmount || 0),
    items: order.items.map((item) => ({
      ...item,
      unitPrice: Number(item.unitPrice || 0),
      lineTotal: Number(item.lineTotal || 0),
    })),
    shipment: order.shipment
      ? {
          ...order.shipment,
          codAmount: Number(order.shipment.codAmount || 0),
          shippingFee: Number(order.shipment.shippingFee || 0),
        }
      : null,
  };
}
}