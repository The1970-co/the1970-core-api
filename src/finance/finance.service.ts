import { BadRequestException, ForbiddenException, Injectable, NotFoundException } from "@nestjs/common";
import { FulfillmentStatus, OrderStatus, PaymentStatus, Prisma } from "@prisma/client";
import { PrismaService } from "../prisma/prisma.service";

@Injectable()
export class FinanceService {
  constructor(private readonly prisma: PrismaService) {}

  private isOwner(user?: any) {
    const roles = [
      ...(Array.isArray(user?.roles) ? user.roles : []),
      user?.role,
    ]
      .map((role) => String(role || "").toLowerCase())
      .filter(Boolean);

    return roles.includes("owner") || roles.includes("admin") ||
      (Array.isArray(user?.permissions) && user.permissions.includes("*"));
  }

  private userBranch(user?: any) {
    return user?.branchId || null;
  }

  private ensureBranchScope(user?: any, branchId?: string | null) {
    if (this.isOwner(user)) return;

    const currentBranchId = this.userBranch(user);
    if (!currentBranchId) {
      throw new ForbiddenException("Tài khoản chưa được gán chi nhánh.");
    }

    if (branchId && String(branchId) !== String(currentBranchId)) {
      throw new ForbiddenException("Không có quyền xem hoặc thao tác dữ liệu chi nhánh khác.");
    }
  }

  private scopedBranchId(user?: any, requestedBranchId?: string | null) {
    if (this.isOwner(user)) {
      return requestedBranchId && requestedBranchId !== "ALL" ? requestedBranchId : null;
    }

    const currentBranchId = this.userBranch(user);
    if (!currentBranchId) {
      throw new ForbiddenException("Tài khoản chưa được gán chi nhánh.");
    }

    if (requestedBranchId && requestedBranchId !== "ALL" && String(requestedBranchId) !== String(currentBranchId)) {
      throw new ForbiddenException("Không có quyền xem dữ liệu chi nhánh khác.");
    }

    return currentBranchId;
  }

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


  private async ensureCashVoucherTable() {
    await this.prisma.$executeRawUnsafe(`
      CREATE TABLE IF NOT EXISTS "CashVoucher" (
        "id" TEXT PRIMARY KEY,
        "code" TEXT UNIQUE,
        "voucherCode" TEXT UNIQUE,
        "direction" TEXT,
        "voucherType" TEXT,
        "type" TEXT,
        "status" TEXT DEFAULT 'DRAFT',
        "amount" NUMERIC(18,2) NOT NULL DEFAULT 0,
        "paymentSourceId" TEXT,
        "branchId" TEXT,
        "staffId" TEXT,
        "staffName" TEXT,
        "customerName" TEXT,
        "customerPhone" TEXT,
        "category" TEXT,
        "title" TEXT,
        "partnerName" TEXT,
        "partnerPhone" TEXT,
        "refType" TEXT,
        "refId" TEXT,
        "note" TEXT,
        "createdById" TEXT,
        "createdByName" TEXT,
        "confirmedById" TEXT,
        "confirmedByName" TEXT,
        "cancelledById" TEXT,
        "cancelledByName" TEXT,
        "confirmedAt" TIMESTAMP,
        "cancelledAt" TIMESTAMP,
        "createdAt" TIMESTAMP NOT NULL DEFAULT NOW(),
        "updatedAt" TIMESTAMP NOT NULL DEFAULT NOW()
      );
    `);

    await this.prisma.$executeRawUnsafe(`
      ALTER TABLE "CashVoucher"
      ADD COLUMN IF NOT EXISTS "code" TEXT,
      ADD COLUMN IF NOT EXISTS "voucherCode" TEXT,
      ADD COLUMN IF NOT EXISTS "direction" TEXT,
      ADD COLUMN IF NOT EXISTS "voucherType" TEXT,
      ADD COLUMN IF NOT EXISTS "type" TEXT,
      ADD COLUMN IF NOT EXISTS "status" TEXT DEFAULT 'DRAFT',
      ADD COLUMN IF NOT EXISTS "category" TEXT,
      ADD COLUMN IF NOT EXISTS "title" TEXT,
      ADD COLUMN IF NOT EXISTS "partnerName" TEXT,
      ADD COLUMN IF NOT EXISTS "partnerPhone" TEXT,
      ADD COLUMN IF NOT EXISTS "createdById" TEXT,
      ADD COLUMN IF NOT EXISTS "createdByName" TEXT,
      ADD COLUMN IF NOT EXISTS "confirmedById" TEXT,
      ADD COLUMN IF NOT EXISTS "confirmedByName" TEXT,
      ADD COLUMN IF NOT EXISTS "cancelledById" TEXT,
      ADD COLUMN IF NOT EXISTS "cancelledByName" TEXT,
      ADD COLUMN IF NOT EXISTS "confirmedAt" TIMESTAMP,
      ADD COLUMN IF NOT EXISTS "cancelledAt" TIMESTAMP;
    `);

    await this.prisma.$executeRawUnsafe(`
      UPDATE "CashVoucher"
      SET
        "voucherCode" = COALESCE("voucherCode", "code"),
        "code" = COALESCE("code", "voucherCode"),
        "type" = COALESCE(
          "type",
          CASE
            WHEN UPPER(COALESCE("direction", '')) IN ('OUT', 'PAYMENT', 'CHI', 'CASH_OUT') THEN 'PAYMENT'
            ELSE 'RECEIPT'
          END
        ),
        "direction" = COALESCE(
          "direction",
          CASE
            WHEN COALESCE("type", '') = 'PAYMENT' THEN 'OUT'
            ELSE 'IN'
          END
        ),
        "status" = COALESCE("status", 'DRAFT'),
        "category" = COALESCE("category", "voucherType"),
        "voucherType" = COALESCE("voucherType", "category"),
        "title" = COALESCE("title", "note", "voucherType", 'Phiếu thu/chi'),
        "partnerName" = COALESCE("partnerName", "customerName"),
        "partnerPhone" = COALESCE("partnerPhone", "customerPhone"),
        "customerName" = COALESCE("customerName", "partnerName"),
        "customerPhone" = COALESCE("customerPhone", "partnerPhone"),
        "createdById" = COALESCE("createdById", "staffId"),
        "createdByName" = COALESCE("createdByName", "staffName")
      WHERE
        "voucherCode" IS NULL
        OR "code" IS NULL
        OR "type" IS NULL
        OR "direction" IS NULL
        OR "status" IS NULL
        OR "title" IS NULL;
    `);

    await this.prisma.$executeRawUnsafe(`
      CREATE UNIQUE INDEX IF NOT EXISTS "CashVoucher_voucherCode_key"
      ON "CashVoucher" ("voucherCode")
      WHERE "voucherCode" IS NOT NULL;
    `);

    await this.prisma.$executeRawUnsafe(`
      CREATE INDEX IF NOT EXISTS "CashVoucher_type_status_createdAt_idx"
      ON "CashVoucher" ("type", "status", "createdAt");
    `);

    await this.prisma.$executeRawUnsafe(`
      CREATE INDEX IF NOT EXISTS "CashVoucher_branchId_createdAt_idx"
      ON "CashVoucher" ("branchId", "createdAt");
    `);

    await this.prisma.$executeRawUnsafe(`
      CREATE INDEX IF NOT EXISTS "CashVoucher_paymentSourceId_createdAt_idx"
      ON "CashVoucher" ("paymentSourceId", "createdAt");
    `);
  }

  private async generateCashVoucherCode(type: "RECEIPT" | "PAYMENT") {
    await this.ensureCashVoucherTable();

    const prefix = type === "RECEIPT" ? "PT" : "PC";
    const today = new Date();
    const ymd = [
      today.getFullYear(),
      String(today.getMonth() + 1).padStart(2, "0"),
      String(today.getDate()).padStart(2, "0"),
    ].join("");

    const rows = await this.prisma.$queryRawUnsafe<Array<{ count: bigint | number | string }>>(
      `SELECT COUNT(*)::int AS count FROM "CashVoucher" WHERE COALESCE("voucherCode", "code", '') LIKE $1`,
      `${prefix}${ymd}%`,
    );

    const count = Number(rows?.[0]?.count || 0) + 1;
    return `${prefix}${ymd}-${String(count).padStart(4, "0")}`;
  }

  private cashVoucherPermissionForCreate(type?: string) {
    return type === "PAYMENT"
      ? "cash_voucher.create_payment"
      : "cash_voucher.create_receipt";
  }

  private normalizeCashVoucherType(value: unknown): "RECEIPT" | "PAYMENT" {
    const type = String(value || "").trim().toUpperCase();
    return type === "PAYMENT" ? "PAYMENT" : "RECEIPT";
  }

  private cashVoucherDirection(type: "RECEIPT" | "PAYMENT") {
    return type === "PAYMENT" ? "OUT" : "IN";
  }

  private cashVoucherTypeSql() {
    return `COALESCE(v."type", CASE WHEN UPPER(COALESCE(v."direction", '')) IN ('OUT', 'PAYMENT', 'CHI', 'CASH_OUT') THEN 'PAYMENT' ELSE 'RECEIPT' END)`;
  }

  async getCashVouchers(params: {
    type?: "RECEIPT" | "PAYMENT" | "ALL";
    dateFrom?: string;
    dateTo?: string;
    branchId?: string;
    paymentSourceId?: string;
    status?: string;
    q?: string;
  }, user?: any) {
    await this.ensureCashVoucherTable();

    const values: any[] = [];
    const where: string[] = [];

    // Mặc định lấy theo khoảng ngày nếu FE truyền, nhưng nới thêm 1 ngày hai đầu để tránh lệch timezone/server.
    if (params.dateFrom || params.dateTo) {
      const { from, to } = this.makeDateRange(params.dateFrom, params.dateTo);
      const fromBuffer = new Date(from);
      fromBuffer.setDate(fromBuffer.getDate() - 1);
      const toBuffer = new Date(to);
      toBuffer.setDate(toBuffer.getDate() + 1);

      values.push(fromBuffer, toBuffer);
      where.push(`v."createdAt" BETWEEN $1 AND $2`);
    }

    if (params.type && params.type !== "ALL") {
      values.push(params.type);
      where.push(`${this.cashVoucherTypeSql()} = $${values.length}`);
    }

    const scopedBranchId = this.scopedBranchId(user, params.branchId);
    if (scopedBranchId) {
      values.push(scopedBranchId);
      where.push(`v."branchId" = $${values.length}`);
    }

    if (params.paymentSourceId && params.paymentSourceId !== "ALL") {
      values.push(params.paymentSourceId);
      where.push(`v."paymentSourceId" = $${values.length}`);
    }

    if (params.status && params.status !== "ALL") {
      values.push(params.status);
      where.push(`COALESCE(v."status", 'DRAFT') = $${values.length}`);
    }

    if (params.q?.trim()) {
      values.push(`%${params.q.trim()}%`);
      where.push(`(
        COALESCE(v."voucherCode", v."code", '') ILIKE $${values.length}
        OR COALESCE(v."title", '') ILIKE $${values.length}
        OR COALESCE(v."partnerName", v."customerName", '') ILIKE $${values.length}
        OR COALESCE(v."partnerPhone", v."customerPhone", '') ILIKE $${values.length}
        OR COALESCE(v."note", '') ILIKE $${values.length}
      )`);
    }

    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

    const rows = await this.prisma.$queryRawUnsafe<any[]>(
      `
        SELECT
          v.*,
          COALESCE(v."voucherCode", v."code") AS "voucherCode",
          ${this.cashVoucherTypeSql()} AS "type",
          COALESCE(v."status", 'DRAFT') AS "status",
          COALESCE(v."title", v."note", v."voucherType", '') AS "title",
          COALESCE(v."category", v."voucherType", '') AS "category",
          COALESCE(v."partnerName", v."customerName", '') AS "partnerName",
          COALESCE(v."partnerPhone", v."customerPhone", '') AS "partnerPhone",
          b."name" AS "branchName",
          ps."name" AS "paymentSourceName",
          ps."code" AS "paymentSourceCode",
          ps."type" AS "paymentSourceType"
        FROM "CashVoucher" v
        LEFT JOIN "Branch" b ON b."id" = v."branchId"
        LEFT JOIN "PaymentSource" ps ON ps."id" = v."paymentSourceId"
        ${whereSql}
        ORDER BY v."createdAt" DESC
      `,
      ...values,
    );

    let totalReceipt = 0;
    let totalPayment = 0;
    let confirmedReceipt = 0;
    let confirmedPayment = 0;
    let pendingAmount = 0;
    let cancelledAmount = 0;

    for (const row of rows) {
      const amount = this.toNumber(row.amount);

      if (row.type === "RECEIPT") totalReceipt += amount;
      if (row.type === "PAYMENT") totalPayment += amount;

      if (row.status === "CONFIRMED" && row.type === "RECEIPT") confirmedReceipt += amount;
      if (row.status === "CONFIRMED" && row.type === "PAYMENT") confirmedPayment += amount;
      if (row.status === "DRAFT") pendingAmount += amount;
      if (row.status === "CANCELLED") cancelledAmount += amount;
    }

    return {
      dateFrom: params.dateFrom,
      dateTo: params.dateTo,
      summary: {
        totalReceipt,
        totalPayment,
        confirmedReceipt,
        confirmedPayment,
        netCashFlow: confirmedReceipt - confirmedPayment,
        pendingAmount,
        cancelledAmount,
        totalRows: rows.length,
      },
      rows: rows.map((row) => ({
        ...row,
        amount: this.toNumber(row.amount),
        createdAt: row.createdAt,
        updatedAt: row.updatedAt,
        confirmedAt: row.confirmedAt,
        cancelledAt: row.cancelledAt,
      })),
    };
  }

  async createCashVoucher(body: {
    type: "RECEIPT" | "PAYMENT";
    branchId?: string;
    paymentSourceId?: string;
    amount: number;
    category?: string;
    title: string;
    partnerName?: string;
    partnerPhone?: string;
    note?: string;
    createdById?: string;
    createdByName?: string;
  }, user?: any) {
    await this.ensureCashVoucherTable();

    const type = this.normalizeCashVoucherType(body.type);
    const amount = this.toNumber(body.amount);

    if (!body.title?.trim()) {
      throw new BadRequestException("Thiếu nội dung phiếu.");
    }

    if (!Number.isFinite(amount) || amount <= 0) {
      throw new BadRequestException("Số tiền phiếu phải lớn hơn 0.");
    }

    const branchId = this.scopedBranchId(user, body.branchId || null);
    const voucherCode = await this.generateCashVoucherCode(type);
    const id = `cv_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
    const direction = this.cashVoucherDirection(type);
    const category = body.category?.trim() || (type === "RECEIPT" ? "Thu khác" : "Chi khác");
    const title = body.title.trim();

    const rows = await this.prisma.$queryRawUnsafe<any[]>(
      `
        INSERT INTO "CashVoucher" (
          "id", "code", "voucherCode", "direction", "voucherType", "type", "status",
          "branchId", "paymentSourceId", "amount", "category", "title",
          "partnerName", "partnerPhone", "customerName", "customerPhone", "note",
          "createdById", "createdByName", "staffId", "staffName", "createdAt", "updatedAt"
        )
        VALUES (
          $1, $2, $2, $3, $4, $5, 'DRAFT',
          $6, $7, $8, $4, $9,
          $10, $11, $10, $11, $12,
          $13, $14, $13, $14, NOW(), NOW()
        )
        RETURNING *,
          COALESCE("voucherCode", "code") AS "voucherCode",
          COALESCE("type", $5) AS "type",
          COALESCE("status", 'DRAFT') AS "status",
          COALESCE("title", "note", "voucherType", '') AS "title",
          COALESCE("category", "voucherType", '') AS "category",
          COALESCE("partnerName", "customerName", '') AS "partnerName",
          COALESCE("partnerPhone", "customerPhone", '') AS "partnerPhone"
      `,
      id,
      voucherCode,
      direction,
      category,
      type,
      branchId || null,
      body.paymentSourceId || null,
      amount,
      title,
      body.partnerName?.trim() || null,
      body.partnerPhone?.trim() || null,
      body.note?.trim() || null,
      body.createdById || user?.id || null,
      body.createdByName || user?.name || user?.username || user?.email || null,
    );

    return rows[0];
  }

  async updateCashVoucher(
    id: string,
    body: {
      branchId?: string;
      paymentSourceId?: string;
      amount?: number;
      category?: string;
      title?: string;
      partnerName?: string;
      partnerPhone?: string;
      note?: string;
    },
    user?: any,
  ) {
    await this.ensureCashVoucherTable();

    const current = await this.prisma.$queryRawUnsafe<any[]>(
      `SELECT *, COALESCE("status", 'DRAFT') AS "status" FROM "CashVoucher" WHERE "id" = $1 LIMIT 1`,
      id,
    );

    if (!current.length) throw new NotFoundException("Không tìm thấy phiếu thu/chi.");

    this.ensureBranchScope(user, current[0].branchId);

    if (body.branchId !== undefined) this.ensureBranchScope(user, body.branchId || null);

    if (current[0].status !== "DRAFT") {
      throw new BadRequestException("Chỉ sửa được phiếu đang nháp.");
    }

    const amount =
      body.amount === undefined || body.amount === null
        ? this.toNumber(current[0].amount)
        : this.toNumber(body.amount);

    if (!Number.isFinite(amount) || amount <= 0) {
      throw new BadRequestException("Số tiền phiếu phải lớn hơn 0.");
    }

    const nextCategory = body.category !== undefined ? body.category?.trim() || null : current[0].category || current[0].voucherType || null;
    const nextTitle = body.title !== undefined ? body.title?.trim() || current[0].title || current[0].note || "Phiếu thu/chi" : current[0].title || current[0].note || "Phiếu thu/chi";
    const nextPartnerName = body.partnerName !== undefined ? body.partnerName?.trim() || null : current[0].partnerName || current[0].customerName || null;
    const nextPartnerPhone = body.partnerPhone !== undefined ? body.partnerPhone?.trim() || null : current[0].partnerPhone || current[0].customerPhone || null;

    const rows = await this.prisma.$queryRawUnsafe<any[]>(
      `
        UPDATE "CashVoucher"
        SET
          "branchId" = $2,
          "paymentSourceId" = $3,
          "amount" = $4,
          "category" = $5,
          "voucherType" = $5,
          "title" = $6,
          "partnerName" = $7,
          "partnerPhone" = $8,
          "customerName" = $7,
          "customerPhone" = $8,
          "note" = $9,
          "updatedAt" = NOW()
        WHERE "id" = $1
        RETURNING *,
          COALESCE("voucherCode", "code") AS "voucherCode",
          ${this.cashVoucherTypeSql()} AS "type",
          COALESCE("status", 'DRAFT') AS "status",
          COALESCE("title", "note", "voucherType", '') AS "title",
          COALESCE("category", "voucherType", '') AS "category",
          COALESCE("partnerName", "customerName", '') AS "partnerName",
          COALESCE("partnerPhone", "customerPhone", '') AS "partnerPhone"
      `,
      id,
      body.branchId !== undefined ? body.branchId || null : current[0].branchId,
      body.paymentSourceId !== undefined ? body.paymentSourceId || null : current[0].paymentSourceId,
      amount,
      nextCategory,
      nextTitle,
      nextPartnerName,
      nextPartnerPhone,
      body.note !== undefined ? body.note?.trim() || null : current[0].note,
    );

    return rows[0];
  }

  async confirmCashVoucher(
    id: string,
    body: {
      confirmedById?: string;
      confirmedByName?: string;
      note?: string;
    } = {},
    user?: any,
  ) {
    await this.ensureCashVoucherTable();

    const current = await this.prisma.$queryRawUnsafe<any[]>(
      `SELECT *, COALESCE("status", 'DRAFT') AS "status" FROM "CashVoucher" WHERE "id" = $1 LIMIT 1`,
      id,
    );

    if (!current.length) throw new NotFoundException("Không tìm thấy phiếu thu/chi.");

    this.ensureBranchScope(user, current[0].branchId);

    const rows = await this.prisma.$queryRawUnsafe<any[]>(
      `
        UPDATE "CashVoucher"
        SET
          "status" = 'CONFIRMED',
          "confirmedAt" = NOW(),
          "confirmedById" = $2,
          "confirmedByName" = $3,
          "note" = COALESCE($4, "note"),
          "updatedAt" = NOW()
        WHERE "id" = $1 AND COALESCE("status", 'DRAFT') = 'DRAFT'
        RETURNING *,
          COALESCE("voucherCode", "code") AS "voucherCode",
          ${this.cashVoucherTypeSql()} AS "type",
          COALESCE("status", 'DRAFT') AS "status",
          COALESCE("title", "note", "voucherType", '') AS "title",
          COALESCE("category", "voucherType", '') AS "category",
          COALESCE("partnerName", "customerName", '') AS "partnerName",
          COALESCE("partnerPhone", "customerPhone", '') AS "partnerPhone"
      `,
      id,
      body.confirmedById || user?.id || null,
      body.confirmedByName || user?.name || user?.username || user?.email || null,
      body.note?.trim() || null,
    );

    if (!rows.length) {
      throw new BadRequestException("Không thể xác nhận phiếu. Phiếu không tồn tại hoặc không còn ở trạng thái nháp.");
    }

    return rows[0];
  }

  async cancelCashVoucher(
    id: string,
    body: {
      cancelledById?: string;
      cancelledByName?: string;
      note?: string;
    } = {},
    user?: any,
  ) {
    await this.ensureCashVoucherTable();

    const current = await this.prisma.$queryRawUnsafe<any[]>(
      `SELECT *, COALESCE("status", 'DRAFT') AS "status" FROM "CashVoucher" WHERE "id" = $1 LIMIT 1`,
      id,
    );

    if (!current.length) throw new NotFoundException("Không tìm thấy phiếu thu/chi.");

    this.ensureBranchScope(user, current[0].branchId);

    const rows = await this.prisma.$queryRawUnsafe<any[]>(
      `
        UPDATE "CashVoucher"
        SET
          "status" = 'CANCELLED',
          "cancelledAt" = NOW(),
          "cancelledById" = $2,
          "cancelledByName" = $3,
          "note" = COALESCE($4, "note"),
          "updatedAt" = NOW()
        WHERE "id" = $1 AND COALESCE("status", 'DRAFT') != 'CANCELLED'
        RETURNING *,
          COALESCE("voucherCode", "code") AS "voucherCode",
          ${this.cashVoucherTypeSql()} AS "type",
          COALESCE("status", 'DRAFT') AS "status",
          COALESCE("title", "note", "voucherType", '') AS "title",
          COALESCE("category", "voucherType", '') AS "category",
          COALESCE("partnerName", "customerName", '') AS "partnerName",
          COALESCE("partnerPhone", "customerPhone", '') AS "partnerPhone"
      `,
      id,
      body.cancelledById || user?.id || null,
      body.cancelledByName || user?.name || user?.username || user?.email || null,
      body.note?.trim() || null,
    );

    if (!rows.length) {
      throw new BadRequestException("Không thể huỷ phiếu. Phiếu không tồn tại hoặc đã huỷ.");
    }

    return rows[0];
  }
}
