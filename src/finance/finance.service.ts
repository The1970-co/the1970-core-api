import { BadRequestException, Injectable, NotFoundException } from "@nestjs/common";
import { FulfillmentStatus, OrderStatus, PaymentStatus, Prisma } from "@prisma/client";
import { PrismaService } from "../prisma/prisma.service";

@Injectable()
export class FinanceService {
  constructor(private readonly prisma: PrismaService) {}

  private toNumber(value: unknown) {
    return Number(value || 0);
  }

  private makeDateRange(dateFrom?: string, dateTo?: string) {
    const today = new Date().toISOString().slice(0, 10);
    const fromText = dateFrom || today;
    const toText = dateTo || fromText;

    return {
      from: new Date(`${fromText}T00:00:00.000+07:00`),
      to: new Date(`${toText}T23:59:59.999+07:00`),
      dateFrom: fromText,
      dateTo: toText,
    };
  }

  private normalizeText(value: unknown) {
    return String(value || "").trim().toLowerCase();
  }

  private normalizeCarrier(value: unknown) {
    const text = String(value || "").trim();
    if (!text) return "—";
    const upper = text.toUpperCase();
    if (upper.includes("AHAMOVE")) return "Ahamove";
    if (upper.includes("GHN") || upper.includes("GIAOHANGNHANH")) return "GHN";
    if (upper.includes("GRAB")) return "Grab";
    if (upper.includes("SHIPPER") || upper.includes("INTERNAL") || upper.includes("NOI_THANH")) {
      return "Shipper nội thành";
    }
    return text;
  }

  private isGhnCarrier(value: unknown) {
    const carrier = String(value || "").toUpperCase();
    return carrier.includes("GHN") || carrier.includes("GIAOHANGNHANH");
  }

  private normalizeLocalDeliveryStatus(order: any, shipment: any) {
    const values = [
      shipment?.shippingStatus,
      shipment?.partnerStatus,
      shipment?.ahamoveStatus,
      shipment?.ahamoveSubStatus,
      order?.status,
      order?.fulfillmentStatus,
    ]
      .map((value) => String(value || "").toUpperCase())
      .filter(Boolean);

    if (
      values.some((value) =>
        ["DELIVERED", "COMPLETED", "SUCCESS", "FULFILLED", "HOAN_THANH", "ĐÃ GIAO"].includes(value)
      ) || order?.status === OrderStatus.COMPLETED
    ) {
      return "DELIVERED";
    }

    if (
      values.some((value) =>
        ["FAILED", "CANCELLED", "CANCELED", "RETURNED", "CANCEL", "FAIL"].includes(value)
      )
    ) {
      return "FAILED";
    }

    if (
      values.some((value) =>
        ["SHIPPING", "PICKING", "PROCESSING", "IN_TRANSIT", "ON_DELIVERY", "ASSIGNED", "ACCEPTED"].includes(value)
      )
    ) {
      return "DELIVERING";
    }

    return "PENDING";
  }

  private localDeliveryStatusLabel(status: string) {
    switch (status) {
      case "DELIVERED":
        return "Đã giao thành công";
      case "DELIVERING":
        return "Đang giao";
      case "FAILED":
        return "Giao thất bại";
      default:
        return "Chờ đối soát";
    }
  }

  async getDailyReconciliation(params: {
    dateFrom?: string;
    dateTo?: string;
    branchId?: string;
    paymentSourceId?: string;
    status?: string;
    q?: string;
  }) {
    const { from, to, dateFrom, dateTo } = this.makeDateRange(
      params.dateFrom,
      params.dateTo
    );

    const where: Prisma.PaymentWhereInput = {
      createdAt: {
        gte: from,
        lte: to,
      },
    };

    if (params.paymentSourceId && params.paymentSourceId !== "ALL") {
      where.paymentSourceId = params.paymentSourceId;
    }

    if (params.status && params.status !== "ALL") {
      where.status = params.status as PaymentStatus;
    }

    if (params.branchId && params.branchId !== "ALL") {
      where.order = {
        branchId: params.branchId,
      };
    }

    if (params.q?.trim()) {
      const keyword = params.q.trim();
      where.OR = [
        { method: { contains: keyword, mode: "insensitive" } },
        { note: { contains: keyword, mode: "insensitive" } },
        {
          order: {
            OR: [
              { orderCode: { contains: keyword, mode: "insensitive" } },
              { customerName: { contains: keyword, mode: "insensitive" } },
              { customerPhone: { contains: keyword, mode: "insensitive" } },
            ],
          },
        },
      ];
    }

    const payments = await this.prisma.payment.findMany({
      where,
      include: {
        paymentSource: true,
        order: {
          select: {
            id: true,
            orderCode: true,
            branchId: true,
            customerName: true,
            customerPhone: true,
            finalAmount: true,
            paymentStatus: true,
            status: true,
            createdAt: true,
          },
        },
      },
      orderBy: { createdAt: "desc" },
    });

    const bySource = new Map<string, any>();

    let totalPaid = 0;
    let totalCodPending = 0;
    let totalPartial = 0;
    let totalRefunded = 0;
    let totalFailed = 0;
    let totalAll = 0;
    let totalCollected = 0;

    for (const p of payments) {
      const amount = this.toNumber(p.amount);
      const key = p.paymentSourceId || "NO_SOURCE";
      const sourceName = p.paymentSource?.name || p.method || "Chưa rõ";
      const sourceCode = p.paymentSource?.code || "NO_SOURCE";
      const sourceType = p.paymentSource?.type || "OTHER";

      if (!bySource.has(key)) {
        bySource.set(key, {
          paymentSourceId: p.paymentSourceId,
          sourceName,
          sourceCode,
          sourceType,
          paidAmount: 0,
          codPendingAmount: 0,
          partialAmount: 0,
          collectedAmount: 0,
          refundedAmount: 0,
          failedAmount: 0,
          totalAmount: 0,
          count: 0,
        });
      }

      const row = bySource.get(key);
      row.count += 1;
      row.totalAmount += amount;

      if (p.status === PaymentStatus.PAID) {
        totalPaid += amount;
        totalCollected += amount;
        row.paidAmount += amount;
        row.collectedAmount += amount;
      }

      if (p.status === PaymentStatus.PENDING_COD) {
        totalCodPending += amount;
        row.codPendingAmount += amount;
      }

      if (p.status === PaymentStatus.PARTIAL) {
        totalPartial += amount;
        totalCollected += amount;
        row.partialAmount += amount;
        row.collectedAmount += amount;
      }

      if (p.status === PaymentStatus.REFUNDED) {
        totalRefunded += amount;
        row.refundedAmount += amount;
      }

      if (p.status === PaymentStatus.FAILED) {
        totalFailed += amount;
        row.failedAmount += amount;
      }

      totalAll += amount;
    }

    const bySourceRows = Array.from(bySource.values()).sort(
      (a, b) => b.totalAmount - a.totalAmount
    );

    return {
      dateFrom,
      dateTo,
      summary: {
        // totalPaid giữ tên cũ cho frontend cũ, nhưng hiểu là tiền đã thực thu.
        // Tiền CK của đơn COD đã thanh toán trước vẫn vào đây vì lấy từ bảng Payment, không lọc Order COMPLETED.
        totalPaid: totalCollected,
        totalCollected,
        totalPaymentPaid: totalPaid,
        totalCodPending,
        totalPartial,
        totalRefunded,
        totalFailed,
        totalAll,
        totalPayments: payments.length,
        averagePayment:
          payments.length > 0 ? Math.round(totalAll / payments.length) : 0,
      },
      bySource: bySourceRows,
      payments: payments.map((p) => ({
        id: p.id,
        orderId: p.orderId,
        orderCode: p.order?.orderCode || "—",
        branchId: p.order?.branchId || "—",
        customerName: p.order?.customerName || "Khách lẻ",
        customerPhone: p.order?.customerPhone || "—",
        orderStatus: p.order?.status || null,
        orderPaymentStatus: p.order?.paymentStatus || null,
        amount: this.toNumber(p.amount),
        status: p.status,
        method: p.method,
        sourceName: p.paymentSource?.name || p.method || "Chưa rõ",
        sourceCode: p.paymentSource?.code || null,
        sourceType: p.paymentSource?.type || null,
        note: p.note || "",
        createdAt: p.createdAt,
        paidAt: p.paidAt,
      })),
    };
  }

  async getLocalDeliveryReconciliation(params: {
    dateFrom?: string;
    dateTo?: string;
    branchId?: string;
    carrier?: string;
    status?: string;
    q?: string;
  }) {
    const { from, to, dateFrom, dateTo } = this.makeDateRange(
      params.dateFrom,
      params.dateTo
    );

    const where: Prisma.ShipmentWhereInput = {
      createdAt: {
        gte: from,
        lte: to,
      },
      NOT: [
        { carrier: { equals: "GHN", mode: "insensitive" } },
        { carrier: { equals: "", mode: "insensitive" } },
      ],
      order: {
        status: { not: OrderStatus.CANCELLED },
      },
    };

    if (params.carrier && params.carrier !== "ALL") {
      where.carrier = { contains: params.carrier, mode: "insensitive" };
    }

    if (params.branchId && params.branchId !== "ALL") {
      where.order = {
        ...(where.order as Prisma.OrderWhereInput),
        branchId: params.branchId,
      };
    }

    if (params.q?.trim()) {
      const keyword = params.q.trim();
      where.OR = [
        { trackingCode: { contains: keyword, mode: "insensitive" } },
        { carrier: { contains: keyword, mode: "insensitive" } },
        {
          order: {
            OR: [
              { orderCode: { contains: keyword, mode: "insensitive" } },
              { customerName: { contains: keyword, mode: "insensitive" } },
              { customerPhone: { contains: keyword, mode: "insensitive" } },
              { shippingRecipientName: { contains: keyword, mode: "insensitive" } },
              { shippingPhone: { contains: keyword, mode: "insensitive" } },
            ],
          },
        },
      ];
    }

    const shipments = await this.prisma.shipment.findMany({
      where,
      include: {
        order: {
          include: {
            payments: {
              include: { paymentSource: true },
              orderBy: { createdAt: "asc" },
            },
          },
        },
      },
      orderBy: { createdAt: "desc" },
      take: 500,
    });

    const rows = shipments
      .filter((shipment) => !this.isGhnCarrier(shipment.carrier))
      .map((shipment) => {
        const order = shipment.order;
        const status = this.normalizeLocalDeliveryStatus(order, shipment);
        const payments = Array.isArray(order?.payments) ? order.payments : [];
        const paidAmount = payments
          .filter((payment) => payment.status === PaymentStatus.PAID || payment.status === PaymentStatus.PARTIAL)
          .reduce((sum, payment) => sum + this.toNumber(payment.amount), 0);
        const pendingCodAmount = payments
          .filter((payment) => payment.status === PaymentStatus.PENDING_COD)
          .reduce((sum, payment) => sum + this.toNumber(payment.amount), 0);
        const codAmount = this.toNumber(shipment.codAmount || order?.finalAmount || 0);
        const shippingFee = this.toNumber(shipment.shippingFee || order?.shippingFee || 0);

        return {
          orderId: order.id,
          shipmentId: shipment.id,
          orderCode: order.orderCode,
          customerName: order.shippingRecipientName || order.customerName || "Khách lẻ",
          customerPhone: order.shippingPhone || order.customerPhone || "—",
          branchId: order.branchId || "—",
          carrier: shipment.carrier,
          carrierName: this.normalizeCarrier(shipment.carrier),
          trackingCode: shipment.trackingCode || shipment.ahamoveOrderId || "—",
          shippingStatus: shipment.shippingStatus,
          partnerStatus: shipment.partnerStatus,
          ahamoveStatus: shipment.ahamoveStatus,
          localStatus: status,
          localStatusLabel: this.localDeliveryStatusLabel(status),
          orderStatus: order.status,
          paymentStatus: order.paymentStatus,
          codAmount,
          shippingFee,
          finalAmount: this.toNumber(order.finalAmount),
          paidAmount,
          pendingCodAmount,
          needCollectAmount: Math.max(0, this.toNumber(order.finalAmount) - paidAmount),
          address:
            order.shippingAddressLine1 ||
            [order.shippingWard, order.shippingDistrict, order.shippingProvince]
              .filter(Boolean)
              .join(", ") ||
            "—",
          note: shipment.note || order.note || "",
          createdAt: shipment.createdAt,
          updatedAt: shipment.updatedAt,
        };
      })
      .filter((row) => {
        if (!params.status || params.status === "ALL") return true;
        return row.localStatus === params.status;
      });

    const summary = rows.reduce(
      (acc, row) => {
        acc.totalRows += 1;
        acc.totalCod += row.codAmount;
        acc.totalFee += row.shippingFee;
        acc.totalNeedCollect += row.needCollectAmount;
        if (row.localStatus === "DELIVERED") acc.delivered += 1;
        else if (row.localStatus === "FAILED") acc.failed += 1;
        else if (row.localStatus === "DELIVERING") acc.delivering += 1;
        else acc.pending += 1;
        return acc;
      },
      {
        totalRows: 0,
        delivered: 0,
        delivering: 0,
        pending: 0,
        failed: 0,
        totalCod: 0,
        totalFee: 0,
        totalNeedCollect: 0,
      }
    );

    return {
      dateFrom,
      dateTo,
      summary,
      rows,
    };
  }

  async markLocalDeliveryDelivered(
    orderId: string,
    body: {
      collectCod?: boolean;
      paymentSourceId?: string;
      amount?: number;
      note?: string;
    }
  ) {
    const order = await this.prisma.order.findUnique({
      where: { id: orderId },
      include: {
        shipment: true,
        payments: {
          include: { paymentSource: true },
        },
      },
    });

    if (!order || !order.shipment) {
      throw new NotFoundException("Không tìm thấy đơn nội thành cần đối soát.");
    }

    if (this.isGhnCarrier(order.shipment.carrier)) {
      throw new BadRequestException("Đơn GHN dùng màn đối soát GHN, không xử lý ở đối soát nội thành.");
    }

    const shouldCollectCod = Boolean(body.collectCod);
    const now = new Date();

    return this.prisma.$transaction(async (tx) => {
      await tx.shipment.update({
        where: { id: order.shipment!.id },
        data: {
          shippingStatus: "DELIVERED",
          partnerStatus: "DELIVERED",
          ahamoveStatus: order.shipment!.ahamoveStatus ? "COMPLETED" : order.shipment!.ahamoveStatus,
          codReconciliationStatus: shouldCollectCod ? "RECONCILED" : order.shipment!.codReconciliationStatus,
          codReconciledAt: shouldCollectCod ? now : order.shipment!.codReconciledAt,
          note: body.note || order.shipment!.note || null,
          lastSyncedAt: now,
        },
      });

      let nextPaymentStatus = order.paymentStatus;

      if (shouldCollectCod) {
        if (!body.paymentSourceId) {
          throw new BadRequestException("Chọn nguồn tiền nhận COD trước khi ghi nhận tiền.");
        }

        const source = await tx.paymentSource.findUnique({
          where: { id: body.paymentSourceId },
        });

        if (!source) {
          throw new BadRequestException("Nguồn tiền không tồn tại.");
        }

        const paidAlready = order.payments
          .filter((payment) => payment.status === PaymentStatus.PAID || payment.status === PaymentStatus.PARTIAL)
          .reduce((sum, payment) => sum + this.toNumber(payment.amount), 0);

        const amount = Math.max(
          0,
          this.toNumber(body.amount || order.shipment!.codAmount || order.finalAmount) ||
            this.toNumber(order.finalAmount) - paidAlready
        );

        const pendingCodPayment = order.payments.find(
          (payment) => payment.status === PaymentStatus.PENDING_COD
        );

        if (pendingCodPayment) {
          await tx.payment.update({
            where: { id: pendingCodPayment.id },
            data: {
              status: PaymentStatus.PAID,
              paidAt: now,
              amount: new Prisma.Decimal(amount),
              method: source.name,
              paymentSourceId: source.id,
              note: body.note || pendingCodPayment.note || "Đối soát nội thành: đã nhận COD",
            },
          });
        } else if (amount > 0) {
          await tx.payment.create({
            data: {
              orderId: order.id,
              amount: new Prisma.Decimal(amount),
              status: PaymentStatus.PAID,
              paidAt: now,
              method: source.name,
              paymentSourceId: source.id,
              note: body.note || "Đối soát nội thành: đã nhận COD",
            },
          });
        }

        const finalAmount = this.toNumber(order.finalAmount);
        const nextPaid = paidAlready + amount;
        nextPaymentStatus = nextPaid >= finalAmount ? PaymentStatus.PAID : PaymentStatus.PARTIAL;
      }

      const updatedOrder = await tx.order.update({
        where: { id: order.id },
        data: {
          status: OrderStatus.COMPLETED,
          fulfillmentStatus: FulfillmentStatus.FULFILLED,
          paymentStatus: nextPaymentStatus,
        },
        include: {
          shipment: true,
          payments: {
            include: { paymentSource: true },
          },
        },
      });

      return {
        ok: true,
        orderId: updatedOrder.id,
        orderCode: updatedOrder.orderCode,
        status: updatedOrder.status,
        paymentStatus: updatedOrder.paymentStatus,
        shipmentStatus: updatedOrder.shipment?.shippingStatus,
      };
    });
  }

  async markLocalDeliveryCodReceived(
    orderId: string,
    body: {
      paymentSourceId?: string;
      amount?: number;
      note?: string;
    } = {}
  ) {
    return this.markLocalDeliveryDelivered(orderId, {
      ...body,
      collectCod: true,
    });
  }

}
