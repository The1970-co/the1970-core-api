import { BadRequestException, ForbiddenException, Injectable, NotFoundException } from "@nestjs/common";
import { randomUUID } from "crypto";
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
  }, user?: any) {
    const { from, to, dateFrom, dateTo } = this.makeDateRange(
      params.dateFrom,
      params.dateTo
    );

    const scopedBranchId = user
      ? this.scopedBranchId(user, params.branchId)
      : (params.branchId && params.branchId !== "ALL" ? params.branchId : null);

    const where: Prisma.PaymentWhereInput = {
      AND: [
        {
          OR: [
            { paidAt: { gte: from, lte: to } },
            { paidAt: null, createdAt: { gte: from, lte: to } },
          ],
        },
      ],
    };

    if (params.paymentSourceId && params.paymentSourceId !== "ALL") {
      where.paymentSourceId = params.paymentSourceId;
    }

    if (params.status && params.status !== "ALL") {
      const requestedStatus = String(params.status).toUpperCase();
      if (!["RECEIPT", "PAYMENT"].includes(requestedStatus)) {
        where.status = params.status as PaymentStatus;
      }
    }

    if (scopedBranchId) {
      where.order = {
        branchId: scopedBranchId,
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
            salesChannel: true,
            createdByStaffId: true,
            createdByStaffName: true,
            createdAt: true,
          },
        },
      },
      orderBy: { createdAt: "desc" },
    });

    await this.ensureCashVoucherTable();

    const voucherValues: any[] = [from, to];
    const voucherWhere: string[] = [
      `COALESCE(v."confirmedAt", v."createdAt") BETWEEN $1 AND $2`,
      `COALESCE(v."status", 'DRAFT') = 'CONFIRMED'`,
    ];

    if (params.status && params.status !== "ALL") {
      const requestedVoucherType = String(params.status).toUpperCase();
      if (requestedVoucherType === "RECEIPT" || requestedVoucherType === "PAYMENT") {
        voucherValues.push(requestedVoucherType);
        voucherWhere.push(`${this.cashVoucherTypeSql()} = $${voucherValues.length}`);
      }
    }

    if (params.paymentSourceId && params.paymentSourceId !== "ALL") {
      voucherValues.push(params.paymentSourceId);
      voucherWhere.push(`v."paymentSourceId" = $${voucherValues.length}`);
    }

    if (scopedBranchId) {
      voucherValues.push(scopedBranchId);
      voucherWhere.push(`v."branchId" = $${voucherValues.length}`);
    }

    if (params.q?.trim()) {
      voucherValues.push(`%${params.q.trim()}%`);
      voucherWhere.push(`(
        COALESCE(v."voucherCode", v."code", '') ILIKE $${voucherValues.length}
        OR COALESCE(v."title", '') ILIKE $${voucherValues.length}
        OR COALESCE(v."partnerName", v."customerName", '') ILIKE $${voucherValues.length}
        OR COALESCE(v."partnerPhone", v."customerPhone", '') ILIKE $${voucherValues.length}
        OR COALESCE(v."note", '') ILIKE $${voucherValues.length}
      )`);
    }

    const cashVouchers = await this.prisma.$queryRawUnsafe<any[]>(
      `
        SELECT
          v.*,
          COALESCE(v."voucherCode", v."code") AS "voucherCode",
          ${this.cashVoucherTypeSql("v")} AS "voucherFlowType",
          COALESCE(v."title", v."note", v."voucherType", '') AS "title",
          COALESCE(v."partnerName", v."customerName", '') AS "partnerName",
          COALESCE(v."partnerPhone", v."customerPhone", '') AS "partnerPhone",
          ps."name" AS "paymentSourceName",
          ps."code" AS "paymentSourceCode",
          ps."type" AS "paymentSourceType",
          b."name" AS "branchName"
        FROM "CashVoucher" v
        LEFT JOIN "PaymentSource" ps ON ps."id" = v."paymentSourceId"
        LEFT JOIN "Branch" b ON b."id" = v."branchId"
        WHERE ${voucherWhere.join(" AND ")}
        ORDER BY v."createdAt" DESC
      `,
      ...voucherValues,
    );

    const moneyPayments = payments.filter((payment) => {
      const paymentStatus = String(payment.status || "").toUpperCase();
      const sourceType = String(payment.paymentSource?.type || "").toUpperCase();
      const salesChannel = String((payment.order as any)?.salesChannel || "").toUpperCase();
      const orderStatus = String(payment.order?.status || "").toUpperCase();

      if (!payment.paymentSourceId) return false;
      if (sourceType === "COD") return false;
      if (paymentStatus !== "PAID" && paymentStatus !== "PARTIAL") return false;

      if (salesChannel === "POS") {
        return orderStatus === "COMPLETED";
      }

      return true;
    });

    const paymentDedupKeys = new Set(
      moneyPayments.map((payment) =>
        [
          payment.orderId || "",
          payment.paymentSourceId || "",
          this.toNumber(payment.amount),
        ].join("|"),
      ),
    );

    const bySource = new Map<string, any>();

    let totalPaid = 0;
    let totalCodPending = 0;
    let totalPartial = 0;
    let totalRefunded = 0;
    let totalFailed = 0;
    let totalAll = 0;
    let totalCollected = 0;

    for (const p of moneyPayments) {
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

    const mappedCashVouchers: any[] = [];

    for (const voucher of cashVouchers) {
      const amount = this.toNumber(voucher.amount);
      if (amount <= 0) continue;

      const voucherType = String(voucher.voucherFlowType || voucher.type || "").toUpperCase() === "PAYMENT"
        ? "PAYMENT"
        : "RECEIPT";

      const dedupKey = [
        String(voucher.refId || ""),
        String(voucher.paymentSourceId || ""),
        amount,
      ].join("|");

      // POS/ORDER tạo cả Payment và CashVoucher để lưu phiếu thu.
      // Tổng quan dòng tiền chỉ tính 1 lần: nếu đã có Payment cùng đơn/nguồn/số tiền thì bỏ voucher thu khỏi tổng.
      // Phiếu chi không dedup với Payment vì Payment là trạng thái đơn hàng, CashVoucher mới là dòng tiền ra.
      if (
        voucherType === "RECEIPT" &&
        String(voucher.refType || "").toUpperCase() === "ORDER" &&
        paymentDedupKeys.has(dedupKey)
      ) {
        continue;
      }

      const key = voucher.paymentSourceId || "NO_SOURCE";
      const sourceName = voucher.paymentSourceName || voucher.paymentSourceCode || "Tiền mặt";
      const sourceCode = voucher.paymentSourceCode || "NO_SOURCE";
      const sourceType = voucher.paymentSourceType || "CASH";

      if (!bySource.has(key)) {
        bySource.set(key, {
          paymentSourceId: voucher.paymentSourceId,
          sourceName,
          sourceCode,
          sourceType,
          paidAmount: 0,
          codPendingAmount: 0,
          partialAmount: 0,
          collectedAmount: 0,
          paymentAmount: 0,
          spentAmount: 0,
          refundedAmount: 0,
          failedAmount: 0,
          totalAmount: 0,
          count: 0,
        });
      }

      const row = bySource.get(key);
      row.count += 1;

      if (voucherType === "PAYMENT") {
        row.paymentAmount = (row.paymentAmount || 0) + amount;
        row.spentAmount = (row.spentAmount || 0) + amount;
        row.refundedAmount += amount;
        row.totalAmount -= amount;

        totalRefunded += amount;
        totalAll += amount;
      } else {
        row.totalAmount += amount;
        row.paidAmount += amount;
        row.collectedAmount += amount;

        totalPaid += amount;
        totalCollected += amount;
        totalAll += amount;
      }

      mappedCashVouchers.push({
        id: voucher.id,
        orderId: voucher.refId || null,
        orderCode: voucher.refType === "ORDER" ? voucher.refId : voucher.voucherCode,
        voucherCode: voucher.voucherCode,
        branchId: voucher.branchId || "—",
        branchName: voucher.branchName || voucher.branchId || "—",
        customerName: voucher.partnerName || "Khách lẻ",
        customerPhone: voucher.partnerPhone || "—",
        createdById: voucher.createdById || voucher.staffId || null,
        createdByName: voucher.createdByName || voucher.staffName || null,
        staffId: voucher.staffId || voucher.createdById || null,
        staffName: voucher.staffName || voucher.createdByName || null,
        orderStatus: null,
        orderPaymentStatus: null,
        amount,
        flowType: voucherType,
        type: voucherType,
        status: "CONFIRMED",
        method: sourceName,
        sourceName,
        sourceCode,
        sourceType,
        title: voucher.title || "",
        category: voucher.category || "",
        note: voucher.note || voucher.title || (voucherType === "PAYMENT" ? "Phiếu chi" : "Phiếu thu"),
        createdAt: voucher.createdAt,
        paidAt: voucher.confirmedAt || voucher.createdAt,
        recordType: "CASH_VOUCHER",
      });
    }

    const bySourceRows = Array.from(bySource.values()).sort(
      (a, b) => Math.abs(b.totalAmount || 0) - Math.abs(a.totalAmount || 0)
    );

    // Một nguồn sự thật duy nhất cho bảng chốt tiền: dùng chính core daily-ledger.
    // Frontend chỉ render ledgerRows/dailyRows, không tự merge live/snapshot nữa.
    const ledgerBook = await this.getDailyLedger({
      dateFrom,
      dateTo,
      branchId: scopedBranchId || params.branchId,
      paymentSourceId: params.paymentSourceId,
    }, user);

    const ledgerRows = Array.isArray((ledgerBook as any)?.rows) ? (ledgerBook as any).rows : [];
    const ledgerSummary = (ledgerBook as any)?.summary || {};
    const cashClosingBalance = ledgerRows
      .filter((row: any) => String(row.sourceType || row.paymentSourceType || "").toUpperCase() === "CASH" || /tien mat|tiền mặt|cash/i.test(String([row.paymentSourceName, row.paymentSourceCode].filter(Boolean).join(" "))))
      .filter((row: any) => String(row.date || "").slice(0, 10) === dateTo)
      .reduce((sum: number, row: any) => sum + this.toNumber(row.closingBalance), 0);

    return {
      dateFrom,
      dateTo,
      ledger: ledgerBook,
      ledgerRows,
      dailyRows: ledgerRows,
      summary: {
        // totalPaid giữ tên cũ cho frontend cũ, nhưng hiểu là tiền đã thực thu.
        // Tiền CK của đơn COD đã thanh toán trước vẫn vào đây vì lấy từ bảng Payment, không lọc Order COMPLETED.
        totalPaid: totalCollected,
        totalCollected,
        totalPaymentPaid: totalPaid,
        totalReceipt: totalCollected,
        totalPayment: totalRefunded,
        totalSpent: totalRefunded,
        netCashFlow: totalCollected - totalRefunded,
        totalCodPending,
        totalPartial,
        totalRefunded,
        totalFailed,
        totalAll,
        totalPayments: moneyPayments.length + mappedCashVouchers.length,
        averagePayment:
          moneyPayments.length + mappedCashVouchers.length > 0
            ? Math.round(totalAll / (moneyPayments.length + mappedCashVouchers.length))
            : 0,
        ledgerOpeningBalance: this.toNumber(ledgerSummary.openingBalance),
        ledgerClosingBalance: this.toNumber(ledgerSummary.closingBalance),
        ledgerTotalReceipt: this.toNumber(ledgerSummary.totalReceipt),
        ledgerTotalPayment: this.toNumber(ledgerSummary.totalPayment),
        ledgerNetAmount: this.toNumber(ledgerSummary.netAmount),
        cashClosingBalance,
      },
      bySource: bySourceRows,
      payments: [
        ...moneyPayments.map((p) => ({
        id: p.id,
        orderId: p.orderId,
        orderCode: p.order?.orderCode || "—",
        branchId: p.order?.branchId || "—",
        customerName: p.order?.customerName || "Khách lẻ",
        customerPhone: p.order?.customerPhone || "—",
        createdById: (p.order as any)?.createdByStaffId || null,
        createdByName: (p.order as any)?.createdByStaffName || null,
        staffId: (p.order as any)?.createdByStaffId || null,
        staffName: (p.order as any)?.createdByStaffName || null,
        orderStatus: p.order?.status || null,
        orderPaymentStatus: p.order?.paymentStatus || null,
        amount: this.toNumber(p.amount),
        flowType: "RECEIPT",
        type: "RECEIPT",
        status: p.status,
        method: p.method,
        sourceName: p.paymentSource?.name || p.method || "Chưa rõ",
        sourceCode: p.paymentSource?.code || null,
        sourceType: p.paymentSource?.type || null,
        note: p.note || "",
        createdAt: p.createdAt,
        paidAt: p.paidAt,
        recordType: "PAYMENT",
      })),
        ...mappedCashVouchers,
      ].sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime()),
    };
  }


  private async ensureLocalDeliveryReconciliationTable() {
    await this.prisma.$executeRawUnsafe(`
      CREATE TABLE IF NOT EXISTS "LocalDeliveryReconciliation" (
        "id" TEXT PRIMARY KEY,
        "code" TEXT UNIQUE,
        "orderId" TEXT NOT NULL,
        "shipmentId" TEXT,
        "branchId" TEXT,
        "carrier" TEXT,
        "trackingCode" TEXT,
        "status" TEXT NOT NULL DEFAULT 'DRAFT',
        "codAmount" NUMERIC(18,2) NOT NULL DEFAULT 0,
        "shippingFee" NUMERIC(18,2) NOT NULL DEFAULT 0,
        "paidAmount" NUMERIC(18,2) NOT NULL DEFAULT 0,
        "paymentRows" JSONB,
        "note" TEXT,
        "createdById" TEXT,
        "createdByName" TEXT,
        "confirmedById" TEXT,
        "confirmedByName" TEXT,
        "paidById" TEXT,
        "paidByName" TEXT,
        "cancelledById" TEXT,
        "cancelledByName" TEXT,
        "createdAt" TIMESTAMP NOT NULL DEFAULT NOW(),
        "updatedAt" TIMESTAMP NOT NULL DEFAULT NOW(),
        "confirmedAt" TIMESTAMP,
        "paidAt" TIMESTAMP,
        "cancelledAt" TIMESTAMP
      );
    `);

    await this.prisma.$executeRawUnsafe(`
      ALTER TABLE "LocalDeliveryReconciliation"
      ADD COLUMN IF NOT EXISTS "code" TEXT,
      ADD COLUMN IF NOT EXISTS "shipmentId" TEXT,
      ADD COLUMN IF NOT EXISTS "branchId" TEXT,
      ADD COLUMN IF NOT EXISTS "carrier" TEXT,
      ADD COLUMN IF NOT EXISTS "trackingCode" TEXT,
      ADD COLUMN IF NOT EXISTS "status" TEXT DEFAULT 'DRAFT',
      ADD COLUMN IF NOT EXISTS "codAmount" NUMERIC(18,2) DEFAULT 0,
      ADD COLUMN IF NOT EXISTS "shippingFee" NUMERIC(18,2) DEFAULT 0,
      ADD COLUMN IF NOT EXISTS "paidAmount" NUMERIC(18,2) DEFAULT 0,
      ADD COLUMN IF NOT EXISTS "paymentRows" JSONB,
      ADD COLUMN IF NOT EXISTS "note" TEXT,
      ADD COLUMN IF NOT EXISTS "createdById" TEXT,
      ADD COLUMN IF NOT EXISTS "createdByName" TEXT,
      ADD COLUMN IF NOT EXISTS "confirmedById" TEXT,
      ADD COLUMN IF NOT EXISTS "confirmedByName" TEXT,
      ADD COLUMN IF NOT EXISTS "paidById" TEXT,
      ADD COLUMN IF NOT EXISTS "paidByName" TEXT,
      ADD COLUMN IF NOT EXISTS "cancelledById" TEXT,
      ADD COLUMN IF NOT EXISTS "cancelledByName" TEXT,
      ADD COLUMN IF NOT EXISTS "confirmedAt" TIMESTAMP,
      ADD COLUMN IF NOT EXISTS "paidAt" TIMESTAMP,
      ADD COLUMN IF NOT EXISTS "cancelledAt" TIMESTAMP;
    `);

    await this.prisma.$executeRawUnsafe(`
      CREATE UNIQUE INDEX IF NOT EXISTS "LocalDeliveryReconciliation_code_key"
      ON "LocalDeliveryReconciliation" ("code")
      WHERE "code" IS NOT NULL;
    `);

    await this.prisma.$executeRawUnsafe(`
      CREATE INDEX IF NOT EXISTS "LocalDeliveryReconciliation_order_status_idx"
      ON "LocalDeliveryReconciliation" ("orderId", "status", "createdAt");
    `);

    await this.prisma.$executeRawUnsafe(`
      CREATE INDEX IF NOT EXISTS "LocalDeliveryReconciliation_shipment_status_idx"
      ON "LocalDeliveryReconciliation" ("shipmentId", "status", "createdAt");
    `);

    await this.prisma.$executeRawUnsafe(`
      CREATE INDEX IF NOT EXISTS "LocalDeliveryReconciliation_branch_createdAt_idx"
      ON "LocalDeliveryReconciliation" ("branchId", "createdAt");
    `);
  }

  private async generateLocalDeliveryReconciliationCode() {
    await this.ensureLocalDeliveryReconciliationTable();

    const today = new Date();
    const ymd = [
      today.getFullYear(),
      String(today.getMonth() + 1).padStart(2, "0"),
      String(today.getDate()).padStart(2, "0"),
    ].join("");

    const rows = await this.prisma.$queryRawUnsafe<Array<{ count: bigint | number | string }>>(
      `SELECT COUNT(*)::int AS count FROM "LocalDeliveryReconciliation" WHERE COALESCE("code", '') LIKE $1`,
      `DSNT${ymd}%`,
    );

    const count = Number(rows?.[0]?.count || 0) + 1;
    return `DSNT${ymd}-${String(count).padStart(4, "0")}`;
  }

  private localReconciliationStatusLabel(status: string) {
    switch (String(status || "").toUpperCase()) {
      case "CONFIRMED":
        return "Đã xác nhận";
      case "PAID":
        return "Đã thanh toán";
      case "CANCELLED":
        return "Đã huỷ";
      default:
        return "Nháp";
    }
  }

  private async findActiveLocalReconciliationByOrderIds(orderIds: string[]) {
    await this.ensureLocalDeliveryReconciliationTable();

    if (!orderIds.length) return new Map<string, any>();

    const placeholders = orderIds.map((_, index) => `$${index + 1}`).join(",");
    const rows = await this.prisma.$queryRawUnsafe<any[]>(
      `
        SELECT *
        FROM "LocalDeliveryReconciliation"
        WHERE "orderId" IN (${placeholders})
          AND COALESCE("status", 'DRAFT') != 'CANCELLED'
        ORDER BY "createdAt" DESC
      `,
      ...orderIds,
    );

    const map = new Map<string, any>();
    rows.forEach((row) => {
      if (!map.has(String(row.orderId))) {
        map.set(String(row.orderId), row);
      }
    });

    return map;
  }

  async createLocalDeliveryReconciliation(
    body: {
      orderIds?: string[];
      shipmentIds?: string[];
      note?: string;
      createdById?: string;
      createdByName?: string;
    },
    user?: any
  ) {
    await this.ensureLocalDeliveryReconciliationTable();

    const orderIds = Array.from(new Set((body.orderIds || []).map((id) => String(id || "").trim()).filter(Boolean)));
    const shipmentIds = Array.from(new Set((body.shipmentIds || []).map((id) => String(id || "").trim()).filter(Boolean)));

    if (!orderIds.length && !shipmentIds.length) {
      throw new BadRequestException("Chọn ít nhất 1 đơn nội thành để tạo đối soát.");
    }

    const where: Prisma.ShipmentWhereInput = {
      NOT: [{ carrier: { equals: "GHN", mode: "insensitive" } }],
      order: {
        status: { not: OrderStatus.CANCELLED },
      },
    };

    if (orderIds.length) {
      where.orderId = { in: orderIds };
    }

    if (shipmentIds.length) {
      where.id = { in: shipmentIds };
    }

    const shipments = await this.prisma.shipment.findMany({
      where,
      include: {
        order: {
          include: {
            payments: true,
          },
        },
      },
      take: 200,
    });

    if (!shipments.length) {
      throw new NotFoundException("Không tìm thấy đơn nội thành hợp lệ để tạo đối soát.");
    }

    const scopedOrderIds = shipments.map((shipment) => shipment.orderId).filter(Boolean) as string[];
    const existingMap = await this.findActiveLocalReconciliationByOrderIds(scopedOrderIds);
    const created: any[] = [];
    const reused: any[] = [];

    for (const shipment of shipments) {
      const order = shipment.order;
      this.ensureBranchScope(user, order.branchId);

      if (this.isGhnCarrier(shipment.carrier)) {
        continue;
      }

      const existing = existingMap.get(String(order.id));
      if (existing) {
        reused.push(existing);
        continue;
      }

      const paidAmount = (order.payments || [])
        .filter((payment) => payment.status === PaymentStatus.PAID || payment.status === PaymentStatus.PARTIAL)
        .reduce((sum, payment) => sum + this.toNumber(payment.amount), 0);
      const codAmount = Math.max(0, this.toNumber(order.finalAmount) - paidAmount);
      const shippingFee = this.toNumber(shipment.shippingFee || order.shippingFee || 0);
      const code = await this.generateLocalDeliveryReconciliationCode();

      const rows = await this.prisma.$queryRawUnsafe<any[]>(
        `
          INSERT INTO "LocalDeliveryReconciliation" (
            "id", "code", "orderId", "shipmentId", "branchId", "carrier", "trackingCode",
            "status", "codAmount", "shippingFee", "paidAmount", "note",
            "createdById", "createdByName", "createdAt", "updatedAt"
          )
          VALUES ($1,$2,$3,$4,$5,$6,$7,'DRAFT',$8,$9,0,$10,$11,$12,NOW(),NOW())
          RETURNING *
        `,
        randomUUID(),
        code,
        order.id,
        shipment.id,
        order.branchId || null,
        shipment.carrier || null,
        shipment.trackingCode || (shipment as any).ahamoveOrderId || null,
        codAmount,
        shippingFee,
        body.note?.trim() || null,
        body.createdById || user?.id || null,
        body.createdByName || user?.name || user?.fullName || null,
      );

      created.push(rows[0]);
    }

    return {
      ok: true,
      createdCount: created.length,
      reusedCount: reused.length,
      created,
      reused,
      rows: [...created, ...reused],
    };
  }

  async updateLocalDeliveryReconciliation(
    id: string,
    body: {
      codAmount?: number;
      shippingFee?: number;
      note?: string;
    },
    user?: any
  ) {
    await this.ensureLocalDeliveryReconciliationTable();

    const currentRows = await this.prisma.$queryRawUnsafe<any[]>(
      `SELECT * FROM "LocalDeliveryReconciliation" WHERE "id" = $1 LIMIT 1`,
      id,
    );
    const current = currentRows[0];

    if (!current) {
      throw new NotFoundException("Không tìm thấy phiếu đối soát nội thành.");
    }

    this.ensureBranchScope(user, current.branchId);

    if (["PAID", "CANCELLED"].includes(String(current.status || "").toUpperCase())) {
      throw new BadRequestException("Phiếu đã thanh toán hoặc đã huỷ thì không sửa được.");
    }

    const rows = await this.prisma.$queryRawUnsafe<any[]>(
      `
        UPDATE "LocalDeliveryReconciliation"
        SET
          "codAmount" = COALESCE($2, "codAmount"),
          "shippingFee" = COALESCE($3, "shippingFee"),
          "note" = COALESCE($4, "note"),
          "updatedAt" = NOW()
        WHERE "id" = $1
        RETURNING *
      `,
      id,
      body.codAmount === undefined ? null : Number(body.codAmount || 0),
      body.shippingFee === undefined ? null : Number(body.shippingFee || 0),
      body.note === undefined ? null : body.note?.trim() || null,
    );

    return rows[0];
  }

  async confirmLocalDeliveryReconciliation(
    id: string,
    body: {
      confirmedById?: string;
      confirmedByName?: string;
      note?: string;
    },
    user?: any
  ) {
    await this.ensureLocalDeliveryReconciliationTable();

    const currentRows = await this.prisma.$queryRawUnsafe<any[]>(
      `SELECT * FROM "LocalDeliveryReconciliation" WHERE "id" = $1 LIMIT 1`,
      id,
    );
    const current = currentRows[0];

    if (!current) {
      throw new NotFoundException("Không tìm thấy phiếu đối soát nội thành.");
    }

    this.ensureBranchScope(user, current.branchId);

    if (String(current.status || "").toUpperCase() === "PAID") {
      throw new BadRequestException("Phiếu đã thanh toán.");
    }

    if (String(current.status || "").toUpperCase() === "CANCELLED") {
      throw new BadRequestException("Phiếu đã huỷ.");
    }

    const rows = await this.prisma.$queryRawUnsafe<any[]>(
      `
        UPDATE "LocalDeliveryReconciliation"
        SET
          "status" = 'CONFIRMED',
          "confirmedById" = COALESCE($2, "confirmedById"),
          "confirmedByName" = COALESCE($3, "confirmedByName"),
          "confirmedAt" = COALESCE("confirmedAt", NOW()),
          "note" = COALESCE($4, "note"),
          "updatedAt" = NOW()
        WHERE "id" = $1
        RETURNING *
      `,
      id,
      body.confirmedById || user?.id || null,
      body.confirmedByName || user?.name || user?.fullName || null,
      body.note?.trim() || null,
    );

    return rows[0];
  }

  async cancelLocalDeliveryReconciliation(
    id: string,
    body: {
      cancelledById?: string;
      cancelledByName?: string;
      note?: string;
    },
    user?: any
  ) {
    await this.ensureLocalDeliveryReconciliationTable();

    const currentRows = await this.prisma.$queryRawUnsafe<any[]>(
      `SELECT * FROM "LocalDeliveryReconciliation" WHERE "id" = $1 LIMIT 1`,
      id,
    );
    const current = currentRows[0];

    if (!current) {
      throw new NotFoundException("Không tìm thấy phiếu đối soát nội thành.");
    }

    this.ensureBranchScope(user, current.branchId);

    if (String(current.status || "").toUpperCase() === "PAID") {
      throw new BadRequestException("Phiếu đã thanh toán thì không huỷ được.");
    }

    const rows = await this.prisma.$queryRawUnsafe<any[]>(
      `
        UPDATE "LocalDeliveryReconciliation"
        SET
          "status" = 'CANCELLED',
          "cancelledById" = COALESCE($2, "cancelledById"),
          "cancelledByName" = COALESCE($3, "cancelledByName"),
          "cancelledAt" = NOW(),
          "note" = COALESCE($4, "note"),
          "updatedAt" = NOW()
        WHERE "id" = $1
        RETURNING *
      `,
      id,
      body.cancelledById || user?.id || null,
      body.cancelledByName || user?.name || user?.fullName || null,
      body.note?.trim() || null,
    );

    return rows[0];
  }

  async payLocalDeliveryReconciliation(
    id: string,
    body: {
      payments: Array<{ paymentSourceId?: string; amount?: number }>;
      note?: string;
      paidById?: string;
      paidByName?: string;
    },
    user?: any
  ) {
    await this.ensureLocalDeliveryReconciliationTable();

    const paymentRows = Array.isArray(body.payments)
      ? body.payments
          .map((row) => ({
            paymentSourceId: String(row.paymentSourceId || "").trim(),
            amount: Number(row.amount || 0),
          }))
          .filter((row) => row.paymentSourceId && row.amount > 0)
      : [];

    if (!paymentRows.length) {
      throw new BadRequestException("Nhập ít nhất 1 dòng thanh toán.");
    }

    if (paymentRows.length > 3) {
      throw new BadRequestException("Mỗi phiếu chỉ chia tối đa 3 nguồn tiền.");
    }

    const currentRows = await this.prisma.$queryRawUnsafe<any[]>(
      `SELECT * FROM "LocalDeliveryReconciliation" WHERE "id" = $1 LIMIT 1`,
      id,
    );
    const current = currentRows[0];

    if (!current) {
      throw new NotFoundException("Không tìm thấy phiếu đối soát nội thành.");
    }

    this.ensureBranchScope(user, current.branchId);

    if (String(current.status || "").toUpperCase() === "CANCELLED") {
      throw new BadRequestException("Phiếu đã huỷ.");
    }

    if (String(current.status || "").toUpperCase() === "PAID") {
      throw new BadRequestException("Phiếu đã thanh toán.");
    }

    const order = await this.prisma.order.findUnique({
      where: { id: current.orderId },
      include: {
        shipment: true,
        payments: {
          include: { paymentSource: true },
        },
      },
    });

    if (!order || !order.shipment) {
      throw new NotFoundException("Không tìm thấy đơn nội thành của phiếu đối soát.");
    }

    if (this.isGhnCarrier(order.shipment.carrier)) {
      throw new BadRequestException("Đơn GHN dùng màn đối soát GHN.");
    }

    const totalPaid = paymentRows.reduce((sum, row) => sum + row.amount, 0);
    const targetAmount = this.toNumber(current.codAmount || order.finalAmount || 0);

    if (totalPaid <= 0) {
      throw new BadRequestException("Số tiền thanh toán phải lớn hơn 0.");
    }

    if (targetAmount > 0 && totalPaid > targetAmount) {
      throw new BadRequestException("Tổng tiền thanh toán không được lớn hơn COD cần thu.");
    }

    const sourceIds = Array.from(new Set(paymentRows.map((row) => row.paymentSourceId)));
    const sources = await this.prisma.paymentSource.findMany({
      where: { id: { in: sourceIds } },
    });
    const sourceMap = new Map(sources.map((source) => [source.id, source]));

    for (const row of paymentRows) {
      if (!sourceMap.has(row.paymentSourceId)) {
        throw new BadRequestException("Có nguồn tiền không tồn tại.");
      }
    }

    const note = body.note?.trim() || current.note || "Thanh toán đối soát nội thành";
    const now = new Date();

    return this.prisma.$transaction(async (tx) => {
      const pendingCodPayments = order.payments.filter((payment) => payment.status === PaymentStatus.PENDING_COD);

      if (pendingCodPayments.length) {
        for (const payment of pendingCodPayments) {
          await tx.payment.update({
            where: { id: payment.id },
            data: {
              status: PaymentStatus.FAILED,
              note: `${payment.note || "COD chờ thu"} - đã thay bằng thanh toán đối soát nội thành ${current.code || ""}`.trim(),
            },
          });
        }
      }

      for (const row of paymentRows) {
        const source = sourceMap.get(row.paymentSourceId)!;
        await tx.payment.create({
          data: {
            orderId: order.id,
            amount: new Prisma.Decimal(row.amount),
            status: row.amount >= targetAmount ? PaymentStatus.PAID : PaymentStatus.PARTIAL,
            paidAt: now,
            method: source.name,
            paymentSourceId: source.id,
            note,
          },
        });
      }

      const paidAlready = order.payments
        .filter((payment) => payment.status === PaymentStatus.PAID || payment.status === PaymentStatus.PARTIAL)
        .reduce((sum, payment) => sum + this.toNumber(payment.amount), 0);
      const nextPaid = paidAlready + totalPaid;
      const finalAmount = this.toNumber(order.finalAmount);
      const nextPaymentStatus = nextPaid >= finalAmount ? PaymentStatus.PAID : PaymentStatus.PARTIAL;

      await tx.shipment.update({
        where: { id: order.shipment!.id },
        data: {
          shippingStatus: "DELIVERED",
          partnerStatus: "DELIVERED",
          ahamoveStatus: order.shipment!.ahamoveStatus ? "COMPLETED" : order.shipment!.ahamoveStatus,
          codReconciliationStatus: "RECONCILED",
          codReconciledAt: now,
          note,
          lastSyncedAt: now,
        },
      });

      const updatedOrder = await tx.order.update({
        where: { id: order.id },
        data: {
          status: OrderStatus.COMPLETED,
          fulfillmentStatus: FulfillmentStatus.FULFILLED,
          paymentStatus: nextPaymentStatus,
        },
      });

      const rows = await tx.$queryRawUnsafe<any[]>(
        `
          UPDATE "LocalDeliveryReconciliation"
          SET
            "status" = 'PAID',
            "paidAmount" = $2,
            "paymentRows" = $3::jsonb,
            "paidById" = COALESCE($4, "paidById"),
            "paidByName" = COALESCE($5, "paidByName"),
            "paidAt" = NOW(),
            "confirmedAt" = COALESCE("confirmedAt", NOW()),
            "note" = COALESCE($6, "note"),
            "updatedAt" = NOW()
          WHERE "id" = $1
          RETURNING *
        `,
        id,
        totalPaid,
        JSON.stringify(paymentRows.map((row) => ({
          ...row,
          paymentSourceName: sourceMap.get(row.paymentSourceId)?.name || "",
        }))),
        body.paidById || user?.id || null,
        body.paidByName || user?.name || user?.fullName || null,
        note,
      );

      return {
        ok: true,
        reconciliation: rows[0],
        orderId: updatedOrder.id,
        orderCode: updatedOrder.orderCode,
        paymentStatus: updatedOrder.paymentStatus,
      };
    });
  }

  async getLocalDeliveryReconciliation(params: {
    dateFrom?: string;
    dateTo?: string;
    branchId?: string;
    carrier?: string;
    status?: string;
    reconciliationStatus?: string;
    orderStatus?: string;
    paymentStatus?: string;
    q?: string;
  }, user?: any) {
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

    const reconciliationMap = await this.findActiveLocalReconciliationByOrderIds(
      shipments.map((shipment) => shipment.orderId).filter(Boolean) as string[],
    );

    const rows = shipments
      .filter((shipment) => !this.isGhnCarrier(shipment.carrier))
      .map((shipment) => {
        const order = shipment.order;
        const reconciliation = reconciliationMap.get(String(order.id));
        const reconciliationStatus = String(reconciliation?.status || "NONE").toUpperCase();
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
          branchName: (order as any).branch?.name || order.branchId || "—",
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
          orderCreatedAt: order.createdAt,
          orderUpdatedAt: order.updatedAt,
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
          reconciliationId: reconciliation?.id || null,
          reconciliationCode: reconciliation?.code || null,
          reconciliationStatus,
          reconciliationStatusLabel: reconciliation
            ? this.localReconciliationStatusLabel(reconciliationStatus)
            : "Chưa tạo",
          reconciliationCodAmount: this.toNumber(reconciliation?.codAmount || 0),
          reconciliationShippingFee: this.toNumber(reconciliation?.shippingFee || 0),
          reconciliationPaidAmount: this.toNumber(reconciliation?.paidAmount || 0),
          reconciliationPaymentRows: reconciliation?.paymentRows || null,
          reconciliationNote: reconciliation?.note || "",
          reconciliationCreatedAt: reconciliation?.createdAt || null,
          reconciliationUpdatedAt: reconciliation?.updatedAt || null,
          reconciliationConfirmedAt: reconciliation?.confirmedAt || null,
          reconciliationPaidAt: reconciliation?.paidAt || null,
          reconciliationCancelledAt: reconciliation?.cancelledAt || null,
          shipmentCreatedAt: shipment.createdAt,
          shipmentUpdatedAt: shipment.updatedAt,
          createdAt: shipment.createdAt,
          updatedAt: shipment.updatedAt,
        };
      })
      .filter((row) => {
        if (params.status && params.status !== "ALL" && row.localStatus !== params.status) {
          return false;
        }

        if (params.orderStatus && params.orderStatus !== "ALL" && row.orderStatus !== params.orderStatus) {
          return false;
        }

        if (params.paymentStatus && params.paymentStatus !== "ALL" && row.paymentStatus !== params.paymentStatus) {
          return false;
        }

        if (params.reconciliationStatus && params.reconciliationStatus !== "ALL") {
          const requested = String(params.reconciliationStatus || "").toUpperCase();
          const hasProblem =
            row.localStatus === "FAILED" ||
            row.orderStatus === "CANCELLED" ||
            row.reconciliationStatus === "CANCELLED" ||
            (
              this.toNumber(row.reconciliationCodAmount) > 0 &&
              this.toNumber(row.needCollectAmount) > 0 &&
              this.toNumber(row.reconciliationCodAmount) !== this.toNumber(row.needCollectAmount)
            );

          if (requested === "PROBLEM") return hasProblem;
          if (requested === "NONE") return !row.reconciliationId;
          return row.reconciliationStatus === requested;
        }

        return true;
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

        if (row.reconciliationStatus === "DRAFT") acc.reconciliationDraft += 1;
        else if (row.reconciliationStatus === "CONFIRMED") acc.reconciliationConfirmed += 1;
        else if (row.reconciliationStatus === "PAID") acc.reconciliationPaid += 1;
        else if (row.reconciliationStatus === "CANCELLED") acc.reconciliationCancelled += 1;
        else acc.reconciliationNone += 1;

        if (
          row.localStatus === "FAILED" ||
          row.orderStatus === "CANCELLED" ||
          row.reconciliationStatus === "CANCELLED" ||
          (
            this.toNumber(row.reconciliationCodAmount) > 0 &&
            this.toNumber(row.needCollectAmount) > 0 &&
            this.toNumber(row.reconciliationCodAmount) !== this.toNumber(row.needCollectAmount)
          )
        ) {
          acc.problemRows += 1;
        }

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
        reconciliationNone: 0,
        reconciliationDraft: 0,
        reconciliationConfirmed: 0,
        reconciliationPaid: 0,
        reconciliationCancelled: 0,
        problemRows: 0,
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
      ADD COLUMN IF NOT EXISTS "refType" TEXT,
      ADD COLUMN IF NOT EXISTS "refId" TEXT,
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

  private cashVoucherTypeSql(alias = "v") {
    const prefix = alias ? `${alias}.` : "";
    return `COALESCE(${prefix}"type", CASE WHEN UPPER(COALESCE(${prefix}"direction", '')) IN ('OUT', 'PAYMENT', 'CHI', 'CASH_OUT') THEN 'PAYMENT' ELSE 'RECEIPT' END)`;
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
          ${this.cashVoucherTypeSql("v")} AS "type",
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

    const virtualPosValues: any[] = [];
    const virtualPosWhere: string[] = [
      `o."salesChannel" = 'POS'`,
      `o."status" = 'COMPLETED'`,
      `p."status" IN ('PAID', 'PARTIAL')`,
      `p."paymentSourceId" IS NOT NULL`,
      `COALESCE(ps."type", '') != 'COD'`,
      `NOT EXISTS (
        SELECT 1
        FROM "CashVoucher" cv
        WHERE cv."refType" = 'ORDER'
          AND cv."refId" = o."id"
          AND cv."paymentSourceId" = p."paymentSourceId"
          AND ROUND((cv."amount")::numeric, 0) = ROUND((p."amount")::numeric, 0)
          AND COALESCE(cv."status", 'DRAFT') != 'CANCELLED'
      )`,
    ];

    if (params.dateFrom || params.dateTo) {
      const { from, to } = this.makeDateRange(params.dateFrom, params.dateTo);
      const fromBuffer = new Date(from);
      fromBuffer.setDate(fromBuffer.getDate() - 1);
      const toBuffer = new Date(to);
      toBuffer.setDate(toBuffer.getDate() + 1);

      virtualPosValues.push(fromBuffer, toBuffer);
      virtualPosWhere.push(`COALESCE(p."paidAt", p."createdAt") BETWEEN $1 AND $2`);
    }

    if (params.type && params.type !== "ALL" && params.type !== "RECEIPT") {
      virtualPosWhere.push(`1 = 0`);
    }

    const virtualScopedBranchId = this.scopedBranchId(user, params.branchId);
    if (virtualScopedBranchId) {
      virtualPosValues.push(virtualScopedBranchId);
      virtualPosWhere.push(`o."branchId" = $${virtualPosValues.length}`);
    }

    if (params.paymentSourceId && params.paymentSourceId !== "ALL") {
      virtualPosValues.push(params.paymentSourceId);
      virtualPosWhere.push(`p."paymentSourceId" = $${virtualPosValues.length}`);
    }

    if (params.status && params.status !== "ALL" && params.status !== "CONFIRMED") {
      virtualPosWhere.push(`1 = 0`);
    }

    if (params.q?.trim()) {
      virtualPosValues.push(`%${params.q.trim()}%`);
      virtualPosWhere.push(`(
        o."orderCode" ILIKE $${virtualPosValues.length}
        OR COALESCE(o."customerName", '') ILIKE $${virtualPosValues.length}
        OR COALESCE(o."customerPhone", '') ILIKE $${virtualPosValues.length}
        OR COALESCE(p."note", '') ILIKE $${virtualPosValues.length}
        OR COALESCE(ps."name", '') ILIKE $${virtualPosValues.length}
      )`);
    }

    const virtualPosRows = await this.prisma.$queryRawUnsafe<any[]>(
      `
        SELECT
          CONCAT('virtual_pos_', p."id") AS "id",
          CONCAT('POS-', o."orderCode") AS "voucherCode",
          'RECEIPT' AS "type",
          'CONFIRMED' AS "status",
          'IN' AS "direction",
          'Thu bán hàng POS' AS "voucherType",
          'Thu bán hàng POS' AS "category",
          CONCAT('Thu bán lẻ POS ', o."orderCode") AS "title",
          o."customerName" AS "partnerName",
          o."customerPhone" AS "partnerPhone",
          o."customerName" AS "customerName",
          o."customerPhone" AS "customerPhone",
          o."branchId" AS "branchId",
          p."paymentSourceId" AS "paymentSourceId",
          p."amount" AS "amount",
          'ORDER' AS "refType",
          o."id" AS "refId",
          COALESCE(p."note", 'Thanh toán POS') AS "note",
          o."createdByStaffId" AS "createdById",
          o."createdByStaffName" AS "createdByName",
          o."createdByStaffId" AS "staffId",
          o."createdByStaffName" AS "staffName",
          o."createdAt" AS "createdAt",
          p."paidAt" AS "confirmedAt",
          p."createdAt" AS "updatedAt",
          NULL AS "cancelledAt",
          b."name" AS "branchName",
          ps."name" AS "paymentSourceName",
          ps."code" AS "paymentSourceCode",
          ps."type" AS "paymentSourceType",
          true AS "isVirtualPosReceipt"
        FROM "Payment" p
        JOIN "Order" o ON o."id" = p."orderId"
        LEFT JOIN "Branch" b ON b."id" = o."branchId"
        LEFT JOIN "PaymentSource" ps ON ps."id" = p."paymentSourceId"
        WHERE ${virtualPosWhere.join(" AND ")}
        ORDER BY p."createdAt" DESC
      `,
      ...virtualPosValues,
    );

    const allRows = [...rows, ...virtualPosRows];

    let totalReceipt = 0;
    let totalPayment = 0;
    let confirmedReceipt = 0;
    let confirmedPayment = 0;
    let pendingAmount = 0;
    let cancelledAmount = 0;

    for (const row of allRows) {
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
        totalRows: allRows.length,
      },
      rows: allRows.map((row) => ({
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
          ${this.cashVoucherTypeSql("")} AS "type",
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
          ${this.cashVoucherTypeSql("")} AS "type",
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
          ${this.cashVoucherTypeSql("")} AS "type",
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

  async deleteCashVoucher(id: string, user?: any) {
    await this.ensureCashVoucherTable();

    const current = await this.prisma.$queryRawUnsafe<any[]>(
      `SELECT *, COALESCE("status", 'DRAFT') AS "status" FROM "CashVoucher" WHERE "id" = $1 LIMIT 1`,
      id,
    );

    if (!current.length) {
      throw new NotFoundException("Không tìm thấy phiếu thu/chi.");
    }

    this.ensureBranchScope(user, current[0].branchId);

    const roleText = String(
      user?.role ||
      user?.userRole ||
      user?.primaryRole ||
      user?.appRole ||
      user?.type ||
      ""
    ).toUpperCase();

    const permissionKeys = [
      ...(Array.isArray(user?.permissions) ? user.permissions : []),
      ...(Array.isArray(user?.permissionKeys) ? user.permissionKeys : []),
      ...(Array.isArray(user?.effectivePermissions) ? user.effectivePermissions : []),
    ].map((permission: any) =>
      typeof permission === "string"
        ? permission
        : permission?.key || permission?.code || permission?.name || ""
    );

    const canForceDelete =
      roleText.includes("OWNER") ||
      roleText.includes("ADMIN") ||
      permissionKeys.includes("system.manage") ||
      permissionKeys.includes("cash_voucher.delete_confirmed") ||
      permissionKeys.includes("cash_voucher.delete");

    if (current[0].status === "CONFIRMED" && !canForceDelete) {
      throw new BadRequestException("Phiếu đã xác nhận chỉ admin/owner mới được xoá. Nhân viên vui lòng dùng thao tác huỷ phiếu.");
    }

    await this.prisma.$executeRawUnsafe(
      `DELETE FROM "CashVoucher" WHERE "id" = $1`,
      id,
    );

    return {
      ok: true,
      id,
      voucherCode: current[0].voucherCode || current[0].code || id,
    };
  }


  private dateKey(value: Date | string) {
    return this.vnDateKey(value);
  }

  private vnDateKey(value: Date | string | null | undefined) {
    if (!value) return "";
    if (typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value.slice(0, 10))) {
      return value.slice(0, 10);
    }

    const date = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(date.getTime())) return String(value).slice(0, 10);

    // Prisma/Postgres lưu timestamp theo UTC; cộng 7h rồi lấy UTC date để ra đúng ngày vận hành VN.
    // Không dùng SQL AT TIME ZONE ở ledger vì timestamp without time zone dễ bị lệch ngày giữa local/online.
    const vn = new Date(date.getTime() + 7 * 60 * 60 * 1000);
    const y = vn.getUTCFullYear();
    const m = String(vn.getUTCMonth() + 1).padStart(2, "0");
    const d = String(vn.getUTCDate()).padStart(2, "0");
    return `${y}-${m}-${d}`;
  }

  private addDays(dateText: string, days: number) {
    const d = new Date(`${dateText}T00:00:00.000+07:00`);
    d.setDate(d.getDate() + days);
    return this.dateKey(d);
  }

  private daysBetween(dateFrom: string, dateTo: string) {
    const result: string[] = [];
    let cursor = dateFrom;
    let guard = 0;

    while (cursor <= dateTo && guard < 5000) {
      result.push(cursor);
      cursor = this.addDays(cursor, 1);
      guard += 1;
    }

    return result;
  }

  private async ensureDailyCashBalanceTable() {
    await this.prisma.$executeRawUnsafe(`
      CREATE TABLE IF NOT EXISTS "DailyCashBalance" (
        "id" TEXT PRIMARY KEY,
        "date" DATE NOT NULL,
        "branchId" TEXT NOT NULL,
        "paymentSourceId" TEXT NOT NULL,
        "openingBalance" NUMERIC(18,2) NOT NULL DEFAULT 0,
        "totalReceipt" NUMERIC(18,2) NOT NULL DEFAULT 0,
        "totalPayment" NUMERIC(18,2) NOT NULL DEFAULT 0,
        "netAmount" NUMERIC(18,2) NOT NULL DEFAULT 0,
        "closingBalance" NUMERIC(18,2) NOT NULL DEFAULT 0,
        "countedAmount" NUMERIC(18,2),
        "differenceAmount" NUMERIC(18,2),
        "status" TEXT NOT NULL DEFAULT 'OPEN',
        "note" TEXT,
        "lockedAt" TIMESTAMP,
        "lockedById" TEXT,
        "lockedByName" TEXT,
        "createdAt" TIMESTAMP NOT NULL DEFAULT NOW(),
        "updatedAt" TIMESTAMP NOT NULL DEFAULT NOW(),
        CONSTRAINT "DailyCashBalance_date_branchId_paymentSourceId_key" UNIQUE ("date", "branchId", "paymentSourceId")
      );
    `);

    await this.prisma.$executeRawUnsafe(`
      ALTER TABLE "DailyCashBalance"
      ADD COLUMN IF NOT EXISTS "openingBalance" NUMERIC(18,2) NOT NULL DEFAULT 0,
      ADD COLUMN IF NOT EXISTS "totalReceipt" NUMERIC(18,2) NOT NULL DEFAULT 0,
      ADD COLUMN IF NOT EXISTS "totalPayment" NUMERIC(18,2) NOT NULL DEFAULT 0,
      ADD COLUMN IF NOT EXISTS "netAmount" NUMERIC(18,2) NOT NULL DEFAULT 0,
      ADD COLUMN IF NOT EXISTS "closingBalance" NUMERIC(18,2) NOT NULL DEFAULT 0,
      ADD COLUMN IF NOT EXISTS "countedAmount" NUMERIC(18,2),
      ADD COLUMN IF NOT EXISTS "differenceAmount" NUMERIC(18,2),
      ADD COLUMN IF NOT EXISTS "status" TEXT NOT NULL DEFAULT 'OPEN',
      ADD COLUMN IF NOT EXISTS "note" TEXT,
      ADD COLUMN IF NOT EXISTS "lockedAt" TIMESTAMP,
      ADD COLUMN IF NOT EXISTS "lockedById" TEXT,
      ADD COLUMN IF NOT EXISTS "lockedByName" TEXT,
      ADD COLUMN IF NOT EXISTS "createdAt" TIMESTAMP NOT NULL DEFAULT NOW(),
      ADD COLUMN IF NOT EXISTS "updatedAt" TIMESTAMP NOT NULL DEFAULT NOW();
    `);

    await this.prisma.$executeRawUnsafe(`CREATE INDEX IF NOT EXISTS "DailyCashBalance_date_idx" ON "DailyCashBalance" ("date");`);
    await this.prisma.$executeRawUnsafe(`CREATE INDEX IF NOT EXISTS "DailyCashBalance_branchId_idx" ON "DailyCashBalance" ("branchId");`);
    await this.prisma.$executeRawUnsafe(`CREATE INDEX IF NOT EXISTS "DailyCashBalance_paymentSourceId_idx" ON "DailyCashBalance" ("paymentSourceId");`);
    await this.prisma.$executeRawUnsafe(`CREATE INDEX IF NOT EXISTS "DailyCashBalance_status_idx" ON "DailyCashBalance" ("status");`);
  }

  private ledgerKey(date: string, branchId: string, paymentSourceId: string) {
    return `${date}|${branchId}|${paymentSourceId}`;
  }

  async getDailyLedger(params: {
    dateFrom?: string;
    dateTo?: string;
    branchId?: string;
    paymentSourceId?: string;
  }, user?: any) {
    await this.ensureDailyCashBalanceTable();
    await this.ensureCashVoucherTable();

    const { dateFrom, dateTo } = this.makeDateRange(params.dateFrom, params.dateTo);
    const scopedBranchId = this.scopedBranchId(user, params.branchId);
    const paymentSourceFilter = params.paymentSourceId && params.paymentSourceId !== "ALL"
      ? params.paymentSourceId
      : null;

    const days = this.daysBetween(dateFrom, dateTo);
    const requestedDateSet = new Set(days);

    // Core tự tính lại sổ quỹ từ mốc an toàn. UI không được tự cộng số dư.
    // Số dư đầu ngày N luôn bằng số dư cuối ngày N-1 theo đúng cặp branchId + paymentSourceId.
    const calcDateFrom = "2020-01-01";
    const calcRange = this.makeDateRange(calcDateFrom, dateTo);
    const calcFrom = calcRange.from;
    const calcTo = calcRange.to;
    const calcDays = this.daysBetween(calcDateFrom, dateTo);

    // Lấy toàn bộ chi nhánh để suy luận nguồn tiền theo hậu tố TH/QO/CL/XD.
    // Nếu chỉ lấy chi nhánh đang filter thì nguồn global dạng "CHUYỂN KHOẢN - TH"
    // có thể không suy luận được và lại sinh dòng sai chi nhánh.
    const allBranchRows = await this.prisma.branch.findMany({
      select: { id: true, name: true },
      orderBy: { name: "asc" },
    });

    const branchRows = scopedBranchId
      ? allBranchRows.filter((branch) => String(branch.id) === String(scopedBranchId))
      : allBranchRows;

    const sourceRows = await this.prisma.paymentSource.findMany({
      where: {
        isActive: true,
        ...(paymentSourceFilter ? { id: paymentSourceFilter } : {}),
        ...(scopedBranchId ? { OR: [{ branchId: scopedBranchId }, { branchId: null }] } : {}),
      },
      orderBy: [{ sortOrder: "asc" }, { name: "asc" }],
    });

    const normalizeSourceText = (value: unknown) =>
      String(value || "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase()
        .replace(/[_]+/g, " ")
        .replace(/\s+/g, " ")
        .trim();

    const sourceTextHasAlias = (text: string, alias: string) => {
      const escaped = alias.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      return new RegExp(`(^|[\\s\\-_/])${escaped}($|[\\s\\-_/])`, "i").test(text);
    };

    const inferSourceBranch = (source: any) => {
      if (source?.branchId) return String(source.branchId);

      const text = normalizeSourceText([source?.name, source?.code].filter(Boolean).join(" "));
      if (!text) return null;

      const aliasByBranch = allBranchRows.map((branch) => {
        const name = normalizeSourceText(branch.name);
        const aliases = new Set<string>([name]);
        if (name.includes("thai ha")) aliases.add("th");
        if (name.includes("quoc oai")) aliases.add("qo");
        if (name.includes("chua lang")) aliases.add("cl");
        if (name.includes("xa dan")) aliases.add("xd");
        return { branch, aliases: Array.from(aliases).filter(Boolean) };
      });

      for (const { branch, aliases } of aliasByBranch) {
        if (aliases.some((alias) => text === alias || sourceTextHasAlias(text, alias))) {
          return String(branch.id);
        }
      }

      return null;
    };

    const branchMap = new Map(allBranchRows.map((branch) => [String(branch.id), branch.name]));
    const sourceMap = new Map<string, any>();
    const sourceBranchMap = new Map<string, string | null>();

    for (const source of sourceRows) {
      const inferredBranchId = inferSourceBranch(source);
      const enriched = { ...source, inferredBranchId };
      sourceMap.set(String(source.id), enriched);
      sourceBranchMap.set(String(source.id), inferredBranchId);
    }

    const resolveEffectiveBranchId = (paymentSourceId: string, fallbackBranchId: string | null | undefined) => {
      const sourceBranchId = sourceBranchMap.get(String(paymentSourceId)) || null;
      return sourceBranchId || (fallbackBranchId ? String(fallbackBranchId) : null);
    };

    const isAllowedEffectiveBranch = (branchId: string | null | undefined) => {
      if (!branchId) return false;
      if (scopedBranchId && String(branchId) !== String(scopedBranchId)) return false;
      return branchMap.has(String(branchId));
    };

    const shouldKeepSnapshotPair = (branchId: string, paymentSourceId: string) => {
      const sourceBranchId = sourceBranchMap.get(String(paymentSourceId)) || null;
      if (sourceBranchId && String(sourceBranchId) !== String(branchId)) {
        // Bỏ snapshot cũ bị trộn chi nhánh, ví dụ QO + nguồn "CHUYỂN KHOẢN - TH".
        return false;
      }
      return isAllowedEffectiveBranch(branchId);
    };

    const seededSourceRows = Array.from(sourceMap.values()).filter((source: any) => {
      const targetBranchId = resolveEffectiveBranchId(String(source.id), source.branchId);
      if (!targetBranchId) return Boolean(paymentSourceFilter);
      return isAllowedEffectiveBranch(targetBranchId);
    });

    const paymentValues: any[] = [calcFrom, calcTo];
    const paymentWhere = [
      `COALESCE(p."paidAt", p."createdAt") BETWEEN $1 AND $2`,
      `p."paymentSourceId" IS NOT NULL`,
      `p."status" IN ('PAID', 'PARTIAL')`,
      `COALESCE(ps."type", '') != 'COD'`,
      `(COALESCE(o."salesChannel"::text, '') != 'POS' OR o."status" = 'COMPLETED')`,
    ];

    if (scopedBranchId) {
      paymentValues.push(scopedBranchId);
      paymentWhere.push(`(o."branchId" = $${paymentValues.length} OR ps."branchId" = $${paymentValues.length})`);
    }

    if (paymentSourceFilter) {
      paymentValues.push(paymentSourceFilter);
      paymentWhere.push(`p."paymentSourceId" = $${paymentValues.length}`);
    }

    const paymentRowsRaw = await this.prisma.$queryRawUnsafe<any[]>(
      `
        SELECT
          COALESCE(p."paidAt", p."createdAt") AS "moneyAt",
          o."branchId" AS "branchId",
          COALESCE(b."name", o."branchId") AS "branchName",
          p."paymentSourceId" AS "paymentSourceId",
          COALESCE(ps."name", p."method", 'Chưa rõ') AS "paymentSourceName",
          COALESCE(ps."code", p."paymentSourceId") AS "paymentSourceCode",
          COALESCE(ps."type", '') AS "paymentSourceType",
          COALESCE(ps."type", '') AS "sourceType",
          p."amount" AS "amount"
        FROM "Payment" p
        JOIN "Order" o ON o."id" = p."orderId"
        LEFT JOIN "Branch" b ON b."id" = o."branchId"
        LEFT JOIN "PaymentSource" ps ON ps."id" = p."paymentSourceId"
        WHERE ${paymentWhere.join(" AND ")}
      `,
      ...paymentValues,
    );

    const paymentGroupMap = new Map<string, any>();
    for (const row of paymentRowsRaw) {
      const date = this.vnDateKey(row.moneyAt);
      if (!date) continue;
      const key = [
        date,
        row.branchId || "NO_BRANCH",
        row.paymentSourceId || "NO_SOURCE",
        row.paymentSourceName || "",
        row.paymentSourceCode || "",
        row.paymentSourceType || row.sourceType || "",
      ].join("|");

      const current = paymentGroupMap.get(key) || {
        date,
        branchId: row.branchId,
        branchName: row.branchName,
        paymentSourceId: row.paymentSourceId,
        paymentSourceName: row.paymentSourceName,
        paymentSourceCode: row.paymentSourceCode,
        paymentSourceType: row.paymentSourceType || row.sourceType,
        sourceType: row.sourceType || row.paymentSourceType,
        paymentCount: 0,
        amount: 0,
      };

      current.paymentCount += 1;
      current.amount += this.toNumber(row.amount);
      paymentGroupMap.set(key, current);
    }

    const paymentGroupsRaw = Array.from(paymentGroupMap.values());

    const voucherValues: any[] = [calcFrom, calcTo];
    const voucherWhere: string[] = [
      `COALESCE(v."confirmedAt", v."createdAt") BETWEEN $1 AND $2`,
      `COALESCE(v."status", 'DRAFT') = 'CONFIRMED'`,
    ];

    if (scopedBranchId) {
      voucherValues.push(scopedBranchId);
      voucherWhere.push(`v."branchId" = $${voucherValues.length}`);
    }

    if (paymentSourceFilter) {
      voucherValues.push(paymentSourceFilter);
      voucherWhere.push(`v."paymentSourceId" = $${voucherValues.length}`);
    }

    const voucherRowsRaw = await this.prisma.$queryRawUnsafe<any[]>(
      `
        SELECT
          COALESCE(v."confirmedAt", v."createdAt") AS "moneyAt",
          v."branchId" AS "branchId",
          COALESCE(b."name", v."branchId") AS "branchName",
          v."paymentSourceId" AS "paymentSourceId",
          COALESCE(ps."name", ps."code", 'Tiền mặt') AS "paymentSourceName",
          COALESCE(ps."code", v."paymentSourceId") AS "paymentSourceCode",
          COALESCE(ps."type", '') AS "paymentSourceType",
          COALESCE(ps."type", '') AS "sourceType",
          ${this.cashVoucherTypeSql("v")} AS "type",
          v."amount" AS "amount"
        FROM "CashVoucher" v
        LEFT JOIN "Branch" b ON b."id" = v."branchId"
        LEFT JOIN "PaymentSource" ps ON ps."id" = v."paymentSourceId"
        WHERE ${voucherWhere.join(" AND ")}
          AND NOT (
            ${this.cashVoucherTypeSql("v")} = 'RECEIPT'
            AND UPPER(COALESCE(v."refType", '')) = 'ORDER'
            AND EXISTS (
              SELECT 1
              FROM "Payment" p2
              WHERE p2."orderId" = v."refId"
                AND p2."paymentSourceId" = v."paymentSourceId"
                AND ROUND((p2."amount")::numeric, 0) = ROUND((v."amount")::numeric, 0)
                AND p2."status" IN ('PAID', 'PARTIAL')
            )
          )
      `,
      ...voucherValues,
    );

    const voucherGroupMap = new Map<string, any>();
    for (const row of voucherRowsRaw) {
      const date = this.vnDateKey(row.moneyAt);
      if (!date) continue;
      const type = String(row.type || "RECEIPT").toUpperCase() === "PAYMENT" ? "PAYMENT" : "RECEIPT";
      const key = [
        date,
        row.branchId || "NO_BRANCH",
        row.paymentSourceId || "NO_SOURCE",
        row.paymentSourceName || "",
        row.paymentSourceCode || "",
        row.paymentSourceType || row.sourceType || "",
        type,
      ].join("|");

      const current = voucherGroupMap.get(key) || {
        date,
        branchId: row.branchId,
        branchName: row.branchName,
        paymentSourceId: row.paymentSourceId,
        paymentSourceName: row.paymentSourceName,
        paymentSourceCode: row.paymentSourceCode,
        paymentSourceType: row.paymentSourceType || row.sourceType,
        sourceType: row.sourceType || row.paymentSourceType,
        type,
        voucherCount: 0,
        amount: 0,
      };

      current.voucherCount += 1;
      current.amount += this.toNumber(row.amount);
      voucherGroupMap.set(key, current);
    }

    const voucherGroupsRaw = Array.from(voucherGroupMap.values());

    const snapshotValues: any[] = [calcDateFrom, dateTo];
    const snapshotWhere = [`d."date" BETWEEN $1::date AND $2::date`];
    if (scopedBranchId) {
      snapshotValues.push(scopedBranchId);
      snapshotWhere.push(`d."branchId" = $${snapshotValues.length}`);
    }
    if (paymentSourceFilter) {
      snapshotValues.push(paymentSourceFilter);
      snapshotWhere.push(`d."paymentSourceId" = $${snapshotValues.length}`);
    }

    const snapshotsRaw = await this.prisma.$queryRawUnsafe<any[]>(
      `
        SELECT DISTINCT ON (d."date", d."branchId", d."paymentSourceId")
          d."id",
          TO_CHAR(d."date", 'YYYY-MM-DD') AS "date",
          d."branchId",
          d."paymentSourceId",
          d."openingBalance",
          d."totalReceipt",
          d."totalPayment",
          d."netAmount",
          d."closingBalance",
          d."countedAmount",
          d."differenceAmount",
          d."status",
          d."note",
          d."lockedAt",
          d."lockedById",
          d."lockedByName",
          d."createdAt",
          d."updatedAt",
          COALESCE(b."name", d."branchId") AS "branchName",
          COALESCE(ps."name", ps."code", d."paymentSourceId") AS "paymentSourceName",
          COALESCE(ps."code", d."paymentSourceId") AS "paymentSourceCode",
          COALESCE(ps."type", '') AS "paymentSourceType",
          COALESCE(ps."type", '') AS "sourceType"
        FROM "DailyCashBalance" d
        LEFT JOIN "Branch" b ON b."id" = d."branchId"
        LEFT JOIN "PaymentSource" ps ON ps."id" = d."paymentSourceId"
        WHERE ${snapshotWhere.join(" AND ")}
        ORDER BY d."date", d."branchId", d."paymentSourceId", d."updatedAt" DESC, d."createdAt" DESC
      `,
      ...snapshotValues,
    );

    const paymentGroups = paymentGroupsRaw
      .map((row: any) => {
        const effectiveBranchId = resolveEffectiveBranchId(row.paymentSourceId, row.branchId);
        if (!isAllowedEffectiveBranch(effectiveBranchId)) return null;
        return {
          ...row,
          branchId: effectiveBranchId,
          branchName: branchMap.get(String(effectiveBranchId)) || row.branchName || effectiveBranchId,
        };
      })
      .filter(Boolean) as any[];

    const voucherGroups = voucherGroupsRaw
      .map((row: any) => {
        const effectiveBranchId = resolveEffectiveBranchId(row.paymentSourceId, row.branchId);
        if (!isAllowedEffectiveBranch(effectiveBranchId)) return null;
        return {
          ...row,
          branchId: effectiveBranchId,
          branchName: branchMap.get(String(effectiveBranchId)) || row.branchName || effectiveBranchId,
        };
      })
      .filter(Boolean) as any[];

    const snapshots = snapshotsRaw.filter((row: any) => shouldKeepSnapshotPair(row.branchId, row.paymentSourceId));

    const snapshotMap = new Map<string, any>();
    const txnMap = new Map<string, any>();
    const combos = new Map<string, any>();

    const addCombo = (branchId: string, paymentSourceId: string, extra: any = {}) => {
      if (!branchId || !paymentSourceId) return;
      if (!isAllowedEffectiveBranch(branchId)) return;
      if (paymentSourceFilter && String(paymentSourceId) !== String(paymentSourceFilter)) return;

      const key = `${branchId}|${paymentSourceId}`;
      const source = sourceMap.get(String(paymentSourceId));
      const sourceBranchId = sourceBranchMap.get(String(paymentSourceId)) || null;
      if (sourceBranchId && String(sourceBranchId) !== String(branchId)) return;

      if (!combos.has(key)) {
        combos.set(key, {
          branchId,
          branchName: extra.branchName || branchMap.get(String(branchId)) || branchId,
          paymentSourceId,
          paymentSourceName: extra.paymentSourceName || source?.name || paymentSourceId,
          paymentSourceCode: extra.paymentSourceCode || source?.code || paymentSourceId,
          paymentSourceType: extra.paymentSourceType || extra.sourceType || source?.type || "",
        });
      }
    };

    for (const source of seededSourceRows) {
      const targetBranchId = resolveEffectiveBranchId(String(source.id), source.branchId);
      if (!targetBranchId) continue;
      addCombo(String(targetBranchId), String(source.id), {
        branchName: branchMap.get(String(targetBranchId)),
        paymentSourceName: source.name,
        paymentSourceCode: source.code,
        paymentSourceType: source.type,
      });
    }

    for (const row of paymentGroups) {
      const key = this.ledgerKey(row.date, row.branchId, row.paymentSourceId);
      const current = txnMap.get(key) || {
        date: row.date,
        branchId: row.branchId,
        branchName: row.branchName,
        paymentSourceId: row.paymentSourceId,
        paymentSourceName: row.paymentSourceName,
        paymentSourceCode: row.paymentSourceCode,
        paymentSourceType: row.paymentSourceType || row.sourceType,
        posReceiptAmount: 0,
        manualReceiptAmount: 0,
        manualPaymentAmount: 0,
        paymentCount: 0,
        voucherReceiptCount: 0,
        voucherPaymentCount: 0,
      };
      current.posReceiptAmount += this.toNumber(row.amount);
      current.paymentCount += Number(row.paymentCount || 0);
      txnMap.set(key, current);
      addCombo(row.branchId, row.paymentSourceId, row);
    }

    for (const row of voucherGroups) {
      const key = this.ledgerKey(row.date, row.branchId, row.paymentSourceId);
      const current = txnMap.get(key) || {
        date: row.date,
        branchId: row.branchId,
        branchName: row.branchName,
        paymentSourceId: row.paymentSourceId,
        paymentSourceName: row.paymentSourceName,
        paymentSourceCode: row.paymentSourceCode,
        paymentSourceType: row.paymentSourceType || row.sourceType,
        posReceiptAmount: 0,
        manualReceiptAmount: 0,
        manualPaymentAmount: 0,
        paymentCount: 0,
        voucherReceiptCount: 0,
        voucherPaymentCount: 0,
      };
      if (String(row.type).toUpperCase() === "PAYMENT") {
        current.manualPaymentAmount += this.toNumber(row.amount);
        current.voucherPaymentCount += Number(row.voucherCount || 0);
      } else {
        current.manualReceiptAmount += this.toNumber(row.amount);
        current.voucherReceiptCount += Number(row.voucherCount || 0);
      }
      txnMap.set(key, current);
      addCombo(row.branchId, row.paymentSourceId, row);
    }

    for (const snapshot of snapshots) {
      snapshotMap.set(this.ledgerKey(snapshot.date, snapshot.branchId, snapshot.paymentSourceId), snapshot);
      addCombo(snapshot.branchId, snapshot.paymentSourceId, snapshot);
    }

    const previousClosing = new Map<string, number>();
    const rows: any[] = [];

    for (const date of calcDays) {
      const comboList = Array.from(combos.values()).sort((a, b) =>
        String(a.branchName || "").localeCompare(String(b.branchName || ""), "vi") ||
        String(a.paymentSourceName || "").localeCompare(String(b.paymentSourceName || ""), "vi")
      );

      for (const combo of comboList) {
        const key = this.ledgerKey(date, combo.branchId, combo.paymentSourceId);
        const comboKey = `${combo.branchId}|${combo.paymentSourceId}`;
        const txn = txnMap.get(key) || {};
        const snapshot = snapshotMap.get(key);
        const isLocked = String(snapshot?.status || "OPEN").toUpperCase() === "LOCKED";

        // Số dư đầu ngày N = số dư cuối ngày N-1 theo đúng cặp branchId + paymentSourceId.
        // Lưu ý quan trọng: KHÔNG được set previousClosing cho các ngày trống 0đ trước khi nguồn tiền có snapshot/giao dịch.
        // Nếu set 0đ từ năm 2020 cho mọi nguồn, các snapshot opening thật ở ngày 14/15 sẽ bị bỏ qua,
        // dẫn đến ngày sau kéo sai số dư đầu kỳ.
        const hasPreviousClosing = previousClosing.has(comboKey);
        const openingBalance = hasPreviousClosing
          ? this.toNumber(previousClosing.get(comboKey) || 0)
          : this.toNumber(snapshot?.openingBalance || 0);

        const posReceiptAmount = this.toNumber(txn.posReceiptAmount);
        const manualReceiptAmount = this.toNumber(txn.manualReceiptAmount);
        const manualPaymentAmount = this.toNumber(txn.manualPaymentAmount);
        const totalReceipt = posReceiptAmount + manualReceiptAmount;
        const totalPayment = manualPaymentAmount;
        const netAmount = totalReceipt - totalPayment;
        const closingBalance = openingBalance + netAmount;

        const countedAmount = snapshot?.countedAmount == null ? null : this.toNumber(snapshot.countedAmount);
        const differenceAmount = countedAmount == null ? null : countedAmount - closingBalance;

        const hasTransaction = totalReceipt !== 0 || totalPayment !== 0;
        const hasSnapshot = Boolean(snapshot);
        const hasBalance = openingBalance !== 0 || closingBalance !== 0;
        const hasActivity = hasTransaction || hasSnapshot || hasPreviousClosing || hasBalance;

        // Chỉ carry số dư sau khi nguồn đã thật sự xuất hiện trong sổ.
        // Nhờ vậy:
        // - Ngày 15 lấy đúng số dư cuối ngày 14 làm số dư đầu.
        // - Ngày 16 lấy đúng số dư cuối ngày 15 làm số dư đầu.
        // - Các nguồn chưa từng phát sinh không tạo số dư ảo 0đ kéo dài.
        if (hasActivity) {
          previousClosing.set(comboKey, closingBalance);
        }

        if (!requestedDateSet.has(date)) continue;
        if (!hasActivity && days.length > 31) continue;

        rows.push({
          id: snapshot?.id || key,
          date,
          branchId: combo.branchId,
          branchName: combo.branchName,
          paymentSourceId: combo.paymentSourceId,
          paymentSourceName: combo.paymentSourceName,
          paymentSourceCode: combo.paymentSourceCode,
          paymentSourceType: combo.paymentSourceType,
          sourceType: combo.paymentSourceType,
          openingBalance,
          posReceiptAmount,
          manualReceiptAmount,
          manualPaymentAmount,
          totalReceipt,
          totalPayment,
          netAmount,
          closingBalance,
          countedAmount,
          differenceAmount,
          status: isLocked ? "LOCKED" : "OPEN",
          note: snapshot?.note || null,
          lockedAt: snapshot?.lockedAt || null,
          lockedById: snapshot?.lockedById || null,
          lockedByName: snapshot?.lockedByName || null,
          paymentCount: Number(txn.paymentCount || 0),
          voucherReceiptCount: Number(txn.voucherReceiptCount || 0),
          voucherPaymentCount: Number(txn.voucherPaymentCount || 0),
        });
      }
    }

    const firstDate = days[0];
    const lastDate = days[days.length - 1];
    const summary = rows.reduce(
      (acc, row) => {
        if (row.date === firstDate) acc.openingBalance += this.toNumber(row.openingBalance);
        acc.totalReceipt += this.toNumber(row.totalReceipt);
        acc.totalPayment += this.toNumber(row.totalPayment);
        acc.netAmount += this.toNumber(row.netAmount);
        if (row.date === lastDate) acc.closingBalance += this.toNumber(row.closingBalance);
        if (row.status === "LOCKED") acc.lockedRows += 1;
        if (row.differenceAmount !== null && row.differenceAmount !== 0) acc.diffRows += 1;
        return acc;
      },
      {
        openingBalance: 0,
        totalReceipt: 0,
        totalPayment: 0,
        netAmount: 0,
        closingBalance: 0,
        lockedRows: 0,
        diffRows: 0,
        totalRows: rows.length,
      }
    );

    summary.totalRows = rows.length;

    return {
      dateFrom,
      dateTo,
      summary,
      rows,
    };
  }

  async auditDailyLedger(params: {
    dateFrom?: string;
    dateTo?: string;
    branchId?: string;
    paymentSourceId?: string;
  }, user?: any) {
    const ledgerBook: any = await this.getDailyLedger(params, user);
    const rows: any[] = Array.isArray(ledgerBook?.rows) ? ledgerBook.rows : [];
    const summary = ledgerBook?.summary || {};
    const issues: any[] = [];
    const checkedAt = new Date().toISOString();

    const roundMoney = (value: unknown) => Math.round(this.toNumber(value));
    const diffMoney = (actual: unknown, expected: unknown) => roundMoney(actual) - roundMoney(expected);
    const moneyEquals = (actual: unknown, expected: unknown) => diffMoney(actual, expected) === 0;
    const rowLabel = (row: any) =>
      [
        row?.date,
        row?.branchName || row?.branchId,
        row?.paymentSourceName || row?.paymentSourceCode || row?.paymentSourceId,
      ]
        .filter(Boolean)
        .join(" · ");

    const pushIssue = (row: any, field: string, expected: number, actual: number, message: string) => {
      const diff = roundMoney(actual) - roundMoney(expected);
      if (diff === 0) return;
      issues.push({
        level: "ERROR",
        date: row?.date ? String(row.date).slice(0, 10) : undefined,
        branchId: row?.branchId,
        branchName: row?.branchName,
        paymentSourceId: row?.paymentSourceId,
        paymentSourceName: row?.paymentSourceName || row?.paymentSourceCode,
        field,
        expected: roundMoney(expected),
        actual: roundMoney(actual),
        diff,
        message,
      });
    };

    for (const row of rows) {
      const posReceiptAmount = roundMoney(row.posReceiptAmount);
      const manualReceiptAmount = roundMoney(row.manualReceiptAmount);
      const manualPaymentAmount = roundMoney(row.manualPaymentAmount);
      const totalReceipt = roundMoney(row.totalReceipt);
      const totalPayment = roundMoney(row.totalPayment);
      const openingBalance = roundMoney(row.openingBalance);
      const netAmount = roundMoney(row.netAmount);
      const closingBalance = roundMoney(row.closingBalance);
      const countedAmount = row.countedAmount == null ? null : roundMoney(row.countedAmount);
      const differenceAmount = row.differenceAmount == null ? null : roundMoney(row.differenceAmount);

      pushIssue(
        row,
        "totalReceipt",
        posReceiptAmount + manualReceiptAmount,
        totalReceipt,
        `${rowLabel(row)}: tổng thu phải bằng POS/PAYMENT + phiếu thu.`,
      );
      pushIssue(
        row,
        "totalPayment",
        manualPaymentAmount,
        totalPayment,
        `${rowLabel(row)}: tổng chi phải bằng phiếu chi đã xác nhận.`,
      );
      pushIssue(
        row,
        "netAmount",
        totalReceipt - totalPayment,
        netAmount,
        `${rowLabel(row)}: thu - chi đang không khớp.`,
      );
      pushIssue(
        row,
        "closingBalance",
        openingBalance + netAmount,
        closingBalance,
        `${rowLabel(row)}: số dư cuối phải bằng số dư đầu + thu - chi.`,
      );

      if (countedAmount !== null && differenceAmount !== null) {
        pushIssue(
          row,
          "differenceAmount",
          countedAmount - closingBalance,
          differenceAmount,
          `${rowLabel(row)}: lệch phải bằng thực đếm - số dư cuối.`,
        );
      }
    }

    const sortedRows = [...rows].sort((a, b) => {
      const dateDiff = String(a.date || "").localeCompare(String(b.date || ""));
      if (dateDiff !== 0) return dateDiff;
      return String(a.branchId || "").localeCompare(String(b.branchId || "")) ||
        String(a.paymentSourceId || "").localeCompare(String(b.paymentSourceId || ""));
    });
    const previousByPair = new Map<string, any>();
    for (const row of sortedRows) {
      const pairKey = `${row.branchId || ""}|${row.paymentSourceId || ""}`;
      const previous = previousByPair.get(pairKey);
      if (previous && !moneyEquals(row.openingBalance, previous.closingBalance)) {
        pushIssue(
          row,
          "openingBalance",
          roundMoney(previous.closingBalance),
          roundMoney(row.openingBalance),
          `${rowLabel(row)}: số dư đầu phải bằng số dư cuối ngày trước của cùng chi nhánh + nguồn tiền.`,
        );
      }
      previousByPair.set(pairKey, row);
    }

    const days = this.daysBetween(ledgerBook?.dateFrom, ledgerBook?.dateTo);
    const firstDate = days[0];
    const lastDate = days[days.length - 1];
    const expectedSummary = rows.reduce(
      (acc, row) => {
        if (row.date === firstDate) acc.openingBalance += roundMoney(row.openingBalance);
        acc.totalReceipt += roundMoney(row.totalReceipt);
        acc.totalPayment += roundMoney(row.totalPayment);
        acc.netAmount += roundMoney(row.netAmount);
        if (row.date === lastDate) acc.closingBalance += roundMoney(row.closingBalance);
        if (String(row.status || "").toUpperCase() === "LOCKED") acc.lockedRows += 1;
        if (row.differenceAmount !== null && row.differenceAmount !== undefined && roundMoney(row.differenceAmount) !== 0) acc.diffRows += 1;
        return acc;
      },
      {
        openingBalance: 0,
        totalReceipt: 0,
        totalPayment: 0,
        netAmount: 0,
        closingBalance: 0,
        lockedRows: 0,
        diffRows: 0,
        totalRows: rows.length,
      },
    );

    const summaryChecks: Array<[string, number, number, string]> = [
      ["summary.openingBalance", expectedSummary.openingBalance, roundMoney(summary.openingBalance), "Tổng số dư đầu kỳ không khớp chi tiết."],
      ["summary.totalReceipt", expectedSummary.totalReceipt, roundMoney(summary.totalReceipt), "Tổng thu không khớp chi tiết."],
      ["summary.totalPayment", expectedSummary.totalPayment, roundMoney(summary.totalPayment), "Tổng chi không khớp chi tiết."],
      ["summary.netAmount", expectedSummary.netAmount, roundMoney(summary.netAmount), "Thu - chi tổng không khớp chi tiết."],
      ["summary.closingBalance", expectedSummary.closingBalance, roundMoney(summary.closingBalance), "Số dư cuối tổng không khớp chi tiết."],
      ["summary.totalRows", expectedSummary.totalRows, Number(summary.totalRows || 0), "Số dòng sổ không khớp chi tiết."],
    ];

    for (const [field, expected, actual, message] of summaryChecks) {
      const diff = actual - expected;
      if (diff === 0) continue;
      issues.push({
        level: "ERROR",
        date: undefined,
        field,
        expected,
        actual,
        diff,
        message,
      });
    }

    return {
      ok: issues.length === 0,
      checkedAt,
      range: {
        fromDate: ledgerBook?.dateFrom || params.dateFrom,
        toDate: ledgerBook?.dateTo || params.dateTo,
      },
      summary: {
        checkedDays: days.length,
        checkedRows: rows.length,
        issueCount: issues.length,
      },
      issues,
    };
  }

  async closeDailyLedger(body: {
    date: string;
    branchId: string;
    paymentSourceId: string;
    countedAmount?: number;
    note?: string;
    lockedById?: string;
    lockedByName?: string;
  }, user?: any) {
    if (!body.date || !body.branchId || !body.paymentSourceId) {
      throw new BadRequestException("Thiếu ngày, chi nhánh hoặc nguồn tiền cần chốt.");
    }

    this.ensureBranchScope(user, body.branchId);
    await this.ensureDailyCashBalanceTable();

    const data = await this.getDailyLedger({
      dateFrom: body.date,
      dateTo: body.date,
      branchId: body.branchId,
      paymentSourceId: body.paymentSourceId,
    }, user);

    const row = data.rows.find((item: any) =>
      item.date === body.date &&
      String(item.branchId) === String(body.branchId) &&
      String(item.paymentSourceId) === String(body.paymentSourceId)
    );

    let closeRow: any = row;

    if (!closeRow) {
      const [branch, source] = await Promise.all([
        this.prisma.branch.findUnique({ where: { id: body.branchId }, select: { id: true, name: true } }),
        this.prisma.paymentSource.findUnique({ where: { id: body.paymentSourceId } }),
      ]);

      if (!branch || !source) {
        throw new BadRequestException("Không tìm thấy dòng sổ quỹ cần chốt.");
      }

      closeRow = {
        id: `manual_${body.date}_${body.branchId}_${body.paymentSourceId}`,
        date: body.date,
        branchId: body.branchId,
        branchName: branch.name,
        paymentSourceId: body.paymentSourceId,
        paymentSourceName: source.name || source.code || body.paymentSourceId,
        paymentSourceCode: source.code || body.paymentSourceId,
        sourceType: source.type || '',
        openingBalance: 0,
        totalReceipt: 0,
        totalPayment: 0,
        netAmount: 0,
        closingBalance: 0,
      };
    }

    const countedAmount = body.countedAmount === undefined || body.countedAmount === null || body.countedAmount === ("" as any)
      ? null
      : this.toNumber(body.countedAmount);
    const closingBalance = this.toNumber(closeRow.closingBalance);
    const differenceAmount = countedAmount === null ? null : countedAmount - closingBalance;
    const id = closeRow.id && !String(closeRow.id).includes("|") ? closeRow.id : `dcb_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;

    const rows = await this.prisma.$queryRawUnsafe<any[]>(
      `
        INSERT INTO "DailyCashBalance" (
          "id", "date", "branchId", "paymentSourceId",
          "openingBalance", "totalReceipt", "totalPayment", "netAmount", "closingBalance",
          "countedAmount", "differenceAmount", "status", "note",
          "lockedAt", "lockedById", "lockedByName", "createdAt", "updatedAt"
        ) VALUES (
          $1, $2::date, $3, $4,
          $5, $6, $7, $8, $9,
          $10, $11, 'LOCKED', $12,
          NOW(), $13, $14, NOW(), NOW()
        )
        ON CONFLICT ("date", "branchId", "paymentSourceId") DO UPDATE SET
          "openingBalance" = EXCLUDED."openingBalance",
          "totalReceipt" = EXCLUDED."totalReceipt",
          "totalPayment" = EXCLUDED."totalPayment",
          "netAmount" = EXCLUDED."netAmount",
          "closingBalance" = EXCLUDED."closingBalance",
          "countedAmount" = EXCLUDED."countedAmount",
          "differenceAmount" = EXCLUDED."differenceAmount",
          "status" = 'LOCKED',
          "note" = EXCLUDED."note",
          "lockedAt" = NOW(),
          "lockedById" = EXCLUDED."lockedById",
          "lockedByName" = EXCLUDED."lockedByName",
          "updatedAt" = NOW()
        RETURNING *
      `,
      id,
      body.date,
      body.branchId,
      body.paymentSourceId,
      this.toNumber(closeRow.openingBalance),
      this.toNumber(closeRow.totalReceipt),
      this.toNumber(closeRow.totalPayment),
      this.toNumber(closeRow.netAmount),
      closingBalance,
      countedAmount,
      differenceAmount,
      body.note?.trim() || null,
      body.lockedById || user?.id || null,
      body.lockedByName || user?.name || user?.username || user?.email || null,
    );

    const savedRow = rows[0] || {};
    return {
      ...savedRow,
      date: body.date,
      branchId: body.branchId,
      paymentSourceId: body.paymentSourceId,
      countedAmount,
      differenceAmount,
      status: "LOCKED",
    };
  }

  async reopenDailyLedger(body: {
    date: string;
    branchId: string;
    paymentSourceId: string;
    note?: string;
  }, user?: any) {
    if (!body.date || !body.branchId || !body.paymentSourceId) {
      throw new BadRequestException("Thiếu ngày, chi nhánh hoặc nguồn tiền cần mở lại.");
    }

    this.ensureBranchScope(user, body.branchId);
    await this.ensureDailyCashBalanceTable();

    const rows = await this.prisma.$queryRawUnsafe<any[]>(
      `
        UPDATE "DailyCashBalance"
        SET
          "status" = 'OPEN',
          "countedAmount" = NULL,
          "differenceAmount" = NULL,
          "lockedAt" = NULL,
          "lockedById" = NULL,
          "lockedByName" = NULL,
          "note" = COALESCE($4, "note"),
          "updatedAt" = NOW()
        WHERE "date" = $1::date
          AND "branchId" = $2
          AND "paymentSourceId" = $3
        RETURNING *
      `,
      body.date,
      body.branchId,
      body.paymentSourceId,
      body.note?.trim() || null,
    );

    if (!rows.length) {
      throw new NotFoundException("Không tìm thấy dòng sổ quỹ đã chốt để mở lại.");
    }

    return rows[0];
  }

}
