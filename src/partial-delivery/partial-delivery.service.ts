import { Injectable, BadRequestException } from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";
import { Prisma } from "@prisma/client";

@Injectable()
export class PartialDeliveryService {
  constructor(private readonly prisma: PrismaService) {}

  async createPartialDelivery(data: any, user?: any) {
    if (!data?.orderId) {
      throw new BadRequestException("Thiếu orderId");
    }

    const order = await this.prisma.order.findUnique({
      where: { id: String(data.orderId) },
      include: {
        shipment: true,
        items: true,
      },
    });

    if (!order) {
      throw new BadRequestException("Không tìm thấy đơn hàng");
    }

    const originalCod = Number(data.originalCod || 0);
    const adjustedCod = Number(data.adjustedCod || 0);
    const items = Array.isArray(data.items) ? data.items : [];

    const record = await this.prisma.partialDeliveryRecord.create({
      data: {
        orderId: order.id,
        orderCode: order.orderCode,
        ghnTrackingCode: data.ghnTrackingCode || order.shipment?.trackingCode || null,
        originalCod: new Prisma.Decimal(originalCod),
        adjustedCod: new Prisma.Decimal(adjustedCod),
        reason: data.reason || null,
        approvedBy: data.approvedBy || user?.name || user?.fullName || user?.email || "Admin",
        approvedById: user?.id || null,
        note: data.note || null,
        items: {
          create: items.map((item: any) => ({
            orderItemId: item.orderItemId || null,
            productName: String(item.productName || ""),
            sku: String(item.sku || ""),
            orderedQty: Number(item.orderedQty || 0),
            deliveredQty: Number(item.deliveredQty || 0),
            unitPrice: new Prisma.Decimal(Number(item.unitPrice || 0)),
            lineTotal: new Prisma.Decimal(
              Number(item.deliveredQty || 0) * Number(item.unitPrice || 0)
            ),
          })),
        },
      },
      include: {
        items: true,
      },
    });

    await this.prisma.order.update({
      where: { id: order.id },
      data: {
        isPartialDelivery: true,
        partialReason: data.reason || "Giao hàng 1 phần",
      },
    });

    return record;
  }
}