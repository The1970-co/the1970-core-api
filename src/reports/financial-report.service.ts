import { Injectable } from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";

type DateField = "createdAt" | "soldAt";

type Params = {
  fromDate?: string;
  toDate?: string;
  dateField?: DateField;
  branchIds?: string;
  createdByStaffIds?: string;
  assignedStaffIds?: string;
  orderStatuses?: string;
  paymentStatuses?: string;
  fulfillmentStatuses?: string;
  deliveryStatuses?: string;
  salesChannels?: string;
  shippingModes?: string;
  carriers?: string;
  paymentSourceIds?: string;
  trackingFilter?: string;
  codFilter?: string;
  codReconciliationStatuses?: string;
  amountDueFilter?: string;
  itemCountFilter?: string;
};

type Metric = {
  orders: number;
  revenue: number;
  shippingCharged: number;
  shippingCost: number;
  discount: number;
  netRevenue: number;
  cost: number;
  grossProfit: number;
  completedOrders: number;
  cancelledOrders: number;
};

@Injectable()
export class FinancialReportService {
  constructor(private readonly prisma: PrismaService) {}

  private n(value: unknown) {
    const parsed = Number(value || 0);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  private normalize(value: unknown) {
    return String(value || "").trim().toUpperCase();
  }

  private selected(value?: string) {
    return String(value || "")
      .split(/[,\|;]/g)
      .map((item) => item.trim())
      .filter((item) => item && item.toUpperCase() !== "ALL");
  }

  private selectedUpper(value?: string) {
    return this.selected(value).map((item) => item.toUpperCase());
  }

  private matches(values: string[], value: unknown) {
    if (!values.length) return true;
    return values.includes(this.normalize(value));
  }

  private dateKey(date: Date) {
    return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}`;
  }

  private startOfDay(date: Date) {
    return new Date(date.getFullYear(), date.getMonth(), date.getDate());
  }

  private addDays(date: Date, days: number) {
    const next = new Date(date);
    next.setDate(next.getDate() + days);
    return next;
  }

  private parseDate(value: string | undefined, fallback: Date) {
    if (!value) return fallback;
    const parsed = new Date(`${value.slice(0, 10)}T00:00:00`);
    return Number.isNaN(parsed.getTime()) ? fallback : parsed;
  }

  private labelChannel(value: unknown) {
    const key = this.normalize(value || "OTHER");
    const labels: Record<string, string> = {
      POS: "POS",
      SHOWROOM: "Showroom",
      FACEBOOK_MANUAL: "Facebook",
      FACEBOOK: "Facebook",
      VN_WEB: "Website VN",
      INTL_WEB: "Website quốc tế",
      WEBSITE: "Website",
      TIKTOK: "TikTok",
      SHOPEE: "Shopee",
      ZALO: "Zalo",
      OTHER: "Khác",
    };
    return labels[key] || String(value || "Khác");
  }

  private labelShippingMode(value: unknown) {
    const key = this.normalize(value);
    if (["PICKUP", "STORE_PICKUP", "IN_STORE"].includes(key)) return "Nhận tại cửa hàng";
    if (key === "POS") return "Bán tại quầy";
    if (["PARTNER", "SHIP", "DELIVERY", "GHN", "AHAMOVE"].includes(key)) return "Giao hàng";
    return String(value || "Chưa rõ");
  }

  private orderStatusLabel(value: unknown) {
    const key = this.normalize(value);
    const labels: Record<string, string> = {
      NEW: "Mới tạo",
      APPROVED: "Đã duyệt",
      PACKING: "Đang đóng gói",
      SHIPPED: "Đã gửi hàng",
      COMPLETED: "Hoàn thành",
      CANCELLED: "Đã huỷ",
    };
    return labels[key] || String(value || "Chưa rõ");
  }

  private paymentStatusLabel(value: unknown) {
    const key = this.normalize(value);
    const labels: Record<string, string> = {
      UNPAID: "Chưa thanh toán",
      PARTIAL: "Thanh toán một phần",
      PAID: "Đã thanh toán",
      PENDING_COD: "Chờ thu COD",
      REFUNDED: "Đã hoàn tiền",
      FAILED: "Thanh toán lỗi",
    };
    return labels[key] || String(value || "Chưa rõ");
  }

  private fulfillmentStatusLabel(value: unknown) {
    const key = this.normalize(value);
    const labels: Record<string, string> = {
      UNFULFILLED: "Chưa xử lý giao vận",
      PROCESSING: "Đang chuẩn bị hàng",
      PARTIAL: "Giao một phần",
      FULFILLED: "Đã hoàn tất giao vận",
      RETURNED: "Đã trả hàng",
      CANCELLED: "Đã huỷ giao vận",
    };
    return labels[key] || String(value || "Chưa rõ");
  }

  private deliveryStatusLabel(value: unknown) {
    const key = this.normalize(value);
    if (!key || key === "NONE") return "Chưa có vận đơn";
    const labels: Record<string, string> = {
      READY_TO_PICK: "Chờ lấy hàng",
      PICKING: "Đang lấy hàng",
      PICKED: "Đã lấy hàng",
      STORING: "Đang lưu kho",
      TRANSPORTING: "Đang luân chuyển",
      SORTING: "Đang phân loại",
      DELIVERING: "Đang giao hàng",
      DELIVERED: "Giao thành công",
      DELIVERY_SUCCESS: "Giao thành công",
      COMPLETED: "Hoàn thành",
      RETURN: "Đang hoàn hàng",
      RETURNING: "Đang hoàn hàng",
      RETURNED: "Đã hoàn hàng",
      WAITING_TO_RETURN: "Chờ hoàn hàng",
      DELIVERY_FAIL: "Giao không thành công",
      FAILED: "Giao không thành công",
      CANCELLED: "Đã huỷ vận đơn",
      CANCELED: "Đã huỷ vận đơn",
    };
    return labels[key] || String(value || "Chưa rõ");
  }

  private emptyMetric(): Metric {
    return {
      orders: 0,
      revenue: 0,
      shippingCharged: 0,
      shippingCost: 0,
      discount: 0,
      netRevenue: 0,
      cost: 0,
      grossProfit: 0,
      completedOrders: 0,
      cancelledOrders: 0,
    };
  }

  private itemCost(order: any) {
    return (Array.isArray(order.items) ? order.items : []).reduce((sum: number, item: any) => {
      const qty = this.n(item.qty);
      return sum + this.n(item.variant?.costPrice) * qty;
    }, 0);
  }

  private staffLabel(row: any) {
    if (!row) return "Chưa rõ nhân viên";
    const name = row.name || row.username || row.code || "Nhân viên";
    return row.code ? `${name} · ${row.code}` : name;
  }

  private shippingMode(order: any) {
    const channel = this.normalize(order.salesChannel);
    const carrier = this.normalize(order.shipment?.carrier);
    const note = this.normalize(order.note);
    if (channel === "POS") return "POS";
    if (note.includes("PICKUP") || note.includes("NHẬN TẠI CỬA HÀNG") || note.includes("NHAN TAI CUA HANG")) return "PICKUP";
    if (carrier || order.shipment?.trackingCode) return "DELIVERY";
    return "OTHER";
  }

  private trueCodStatus(status: unknown, issue: unknown) {
    const s = this.normalize(status);
    const i = this.normalize(issue);
    return (
      ["PAID", "CONFIRMED", "RECONCILED", "COD_RECONCILED", "COD_RECONCILIATION_PAID", "USER_CONFIRMED"].includes(s) ||
      i.includes("COD_RECONCILIATION_PAID") ||
      i.includes("USER_CONFIRMED")
    );
  }

  private codReconciliationLabel(status: unknown, issue: unknown) {
    const s = this.normalize(status);
    const i = this.normalize(issue);
    if (this.trueCodStatus(s, i)) return "RECONCILED";
    if (s === "MISMATCH" || i.includes("COD_MISMATCH") || i.includes("FEE_MISMATCH") || i.includes("PARTIAL_DELIVERY_AMOUNT_MISMATCH")) return "MISMATCH";
    if (s === "NOT_FOUND" || i.includes("NOT_FOUND_INTERNAL_ORDER")) return "NOT_FOUND";
    if (s === "SAVED" || i.includes("BATCH_SAVED")) return "SAVED";
    return "NOT_RECONCILED";
  }

  private optionMap(rows: Array<{ id: string; name: string }>) {
    const map = new Map<string, string>();
    rows.forEach((row) => {
      if (row.id) map.set(String(row.id), String(row.name || row.id));
    });
    return Array.from(map, ([id, name]) => ({ id, name }));
  }

  async getFinancialReport(params: Params = {}) {
    const now = new Date();
    const today = this.startOfDay(now);
    const from = this.startOfDay(this.parseDate(params.fromDate, this.addDays(today, -9)));
    const to = this.startOfDay(this.parseDate(params.toDate, today));
    const toExclusive = this.addDays(to, 1);
    const dateField: DateField = params.dateField === "soldAt" ? "soldAt" : "createdAt";

    const selected = {
      branchIds: this.selected(params.branchIds),
      createdByStaffIds: this.selected(params.createdByStaffIds),
      assignedStaffIds: this.selected(params.assignedStaffIds),
      orderStatuses: this.selectedUpper(params.orderStatuses),
      paymentStatuses: this.selectedUpper(params.paymentStatuses),
      fulfillmentStatuses: this.selectedUpper(params.fulfillmentStatuses),
      deliveryStatuses: this.selectedUpper(params.deliveryStatuses),
      salesChannels: this.selectedUpper(params.salesChannels),
      shippingModes: this.selectedUpper(params.shippingModes),
      carriers: this.selectedUpper(params.carriers),
      paymentSourceIds: this.selected(params.paymentSourceIds),
      trackingFilter: this.selectedUpper(params.trackingFilter),
      codFilter: this.selectedUpper(params.codFilter),
      codReconciliationStatuses: this.selectedUpper(params.codReconciliationStatuses),
      amountDueFilter: this.selectedUpper(params.amountDueFilter),
      itemCountFilter: this.selectedUpper(params.itemCountFilter),
    };

    const [branches, staff, paymentSources, orderColumnsRows, shipmentColumnsRows] = await Promise.all([
      this.prisma.branch.findMany({
        where: { isActive: true },
        select: { id: true, name: true },
        orderBy: { name: "asc" },
      }),
      this.prisma.staffUser.findMany({
        where: { isActive: true },
        select: {
          id: true,
          code: true,
          name: true,
          username: true,
          branchId: true,
          branchName: true,
          branchRoles: { select: { branchId: true } },
        },
        orderBy: { name: "asc" },
      }).catch(() => [] as any[]),
      this.prisma.paymentSource.findMany({
        where: { isActive: true },
        select: { id: true, code: true, name: true, type: true, branchId: true },
        orderBy: { name: "asc" },
      }).catch(() => [] as any[]),
      this.prisma.$queryRawUnsafe<Array<{ column_name: string }>>(
        `SELECT column_name FROM information_schema.columns WHERE table_name = 'Order'`,
      ).catch(() => []),
      this.prisma.$queryRawUnsafe<Array<{ column_name: string }>>(
        `SELECT column_name FROM information_schema.columns WHERE table_name = 'Shipment'`,
      ).catch(() => []),
    ]);

    const orderColumns = new Set(orderColumnsRows.map((row) => row.column_name));
    const shipmentColumns = new Set(shipmentColumnsRows.map((row) => row.column_name));
    const optionalOrderColumns = ["assignedStaffId", "assignedStaffName"].filter((column) => orderColumns.has(column));
    const optionalShipmentColumns = ["codReconciliationStatus", "codReconciliationIssue"].filter((column) => shipmentColumns.has(column));

    const orders = await this.prisma.order.findMany({
      where: {
        [dateField]: { gte: from, lt: toExclusive },
        ...(selected.branchIds.length ? { branchId: { in: selected.branchIds } } : {}),
      } as any,
      orderBy: { [dateField]: "desc" } as any,
      select: {
        id: true,
        orderCode: true,
        customerName: true,
        customerPhone: true,
        status: true,
        paymentStatus: true,
        fulfillmentStatus: true,
        finalAmount: true,
        totalAmount: true,
        discountAmount: true,
        shippingFee: true,
        salesChannel: true,
        branchId: true,
        createdByStaffId: true,
        createdByStaffName: true,
        note: true,
        createdAt: true,
        soldAt: true,
        items: {
          select: {
            qty: true,
            lineTotal: true,
            unitPrice: true,
            sku: true,
            productName: true,
            variant: { select: { costPrice: true } },
          },
        },
        shipment: {
          select: {
            id: true,
            carrier: true,
            trackingCode: true,
            shippingStatus: true,
            codAmount: true,
            shippingFee: true,
          },
        },
        payments: {
          select: {
            id: true,
            method: true,
            amount: true,
            status: true,
            paymentSourceId: true,
            paymentSource: {
              select: { id: true, code: true, name: true, type: true, branchId: true },
            },
          },
        },
      },
    });

    const orderExtras = new Map<string, any>();
    if (orders.length && optionalOrderColumns.length) {
      const sql = `SELECT "id", ${optionalOrderColumns.map((column) => `"${column}"`).join(", ")} FROM "Order" WHERE "id" = ANY($1)`;
      const rows = await this.prisma.$queryRawUnsafe<any[]>(sql, orders.map((order) => order.id)).catch(() => []);
      rows.forEach((row) => orderExtras.set(String(row.id), row));
    }

    const shipmentExtras = new Map<string, any>();
    if (orders.length && optionalShipmentColumns.length) {
      const sql = `SELECT "orderId", ${optionalShipmentColumns.map((column) => `"${column}"`).join(", ")} FROM "Shipment" WHERE "orderId" = ANY($1)`;
      const rows = await this.prisma.$queryRawUnsafe<any[]>(sql, orders.map((order) => order.id)).catch(() => []);
      rows.forEach((row) => shipmentExtras.set(String(row.orderId), row));
    }

    const branchMap = new Map(branches.map((row) => [row.id, row.name]));
    const staffMap = new Map((staff as any[]).map((row) => [String(row.id), row]));

    const enriched = orders.map((order: any) => {
      const extra = orderExtras.get(String(order.id)) || {};
      const shipmentExtra = shipmentExtras.get(String(order.id)) || {};
      const assignedStaffId = String(extra.assignedStaffId || "");
      const createdByStaffId = String(order.createdByStaffId || "");
      const assignedStaff = staffMap.get(assignedStaffId);
      const createdByStaff = staffMap.get(createdByStaffId);

      const paymentRows = Array.isArray(order.payments) ? order.payments : [];
      const paymentSourceIds = paymentRows.map((row: any) => String(row.paymentSourceId || row.paymentSource?.id || "")).filter(Boolean);
      const paymentSourceNames = paymentRows.map((row: any) => row.paymentSource?.name || row.method).filter(Boolean);
      const paymentSourceTypes = paymentRows.map((row: any) => this.normalize(row.paymentSource?.type)).filter(Boolean);
      const paidAmount = paymentRows
        .filter((row: any) => this.normalize(row.paymentSource?.type) !== "COD" && this.normalize(row.status) !== "FAILED")
        .reduce((sum: number, row: any) => sum + this.n(row.amount), 0);

      const finalAmount = this.n(order.finalAmount ?? order.totalAmount);
      const shippingCharged = this.n(order.shippingFee);
      const shippingCost = this.n(order.shipment?.shippingFee);
      const discount = this.n(order.discountAmount);
      const cost = this.itemCost(order);
      const codAmount = this.n(order.shipment?.codAmount);
      const amountDue = Math.max(0, finalAmount - paidAmount);
      const itemCount = (order.items || []).reduce((sum: number, item: any) => sum + this.n(item.qty), 0);
      const shippingMode = this.shippingMode(order);
      const codReconciliationStatus = this.codReconciliationLabel(
        shipmentExtra.codReconciliationStatus,
        shipmentExtra.codReconciliationIssue,
      );

      return {
        ...order,
        createdByStaffId,
        createdByStaffName: createdByStaff ? this.staffLabel(createdByStaff) : order.createdByStaffName || "Chưa rõ nhân viên",
        assignedStaffId,
        assignedStaffName: assignedStaff
          ? this.staffLabel(assignedStaff)
          : String(extra.assignedStaffName || "").trim(),
        paymentSourceIds,
        paymentSourceNames,
        paymentSourceTypes,
        paidAmount,
        finalAmount,
        shippingCharged,
        shippingCost,
        discount,
        cost,
        codAmount,
        amountDue,
        itemCount,
        shippingMode,
        deliveryStatus: String(order.shipment?.shippingStatus || "NONE"),
        carrier: String(order.shipment?.carrier || "NONE"),
        trackingCode: String(order.shipment?.trackingCode || ""),
        codReconciliationStatus,
      };
    });

    const filtered = enriched.filter((order: any) => {
      if (!this.matches(selected.orderStatuses, order.status)) return false;
      if (!this.matches(selected.paymentStatuses, order.paymentStatus)) return false;
      if (!this.matches(selected.fulfillmentStatuses, order.fulfillmentStatus)) return false;
      if (!this.matches(selected.deliveryStatuses, order.deliveryStatus)) return false;
      if (!this.matches(selected.salesChannels, order.salesChannel)) return false;
      if (!this.matches(selected.shippingModes, order.shippingMode)) return false;
      if (!this.matches(selected.carriers, order.carrier)) return false;
      if (selected.createdByStaffIds.length && !selected.createdByStaffIds.includes(order.createdByStaffId)) return false;

      if (selected.assignedStaffIds.length) {
        const wantsUnassigned = selected.assignedStaffIds.includes("UNASSIGNED");
        const assignedIds = selected.assignedStaffIds.filter((id) => id !== "UNASSIGNED");
        const matched = (wantsUnassigned && !order.assignedStaffId) || assignedIds.includes(order.assignedStaffId);
        if (!matched) return false;
      }

      if (selected.paymentSourceIds.length && !order.paymentSourceIds.some((id: string) => selected.paymentSourceIds.includes(id))) return false;

      if (selected.trackingFilter.includes("HAS") && !order.trackingCode) return false;
      if (selected.trackingFilter.includes("NONE") && order.trackingCode) return false;

      const hasCod = order.codAmount > 0 || order.paymentSourceTypes.includes("COD") || this.normalize(order.paymentStatus) === "PENDING_COD";
      if (selected.codFilter.includes("HAS_COD") && !hasCod) return false;
      if (selected.codFilter.includes("NO_COD") && hasCod) return false;

      if (!this.matches(selected.codReconciliationStatuses, order.codReconciliationStatus)) return false;
      if (selected.amountDueFilter.includes("HAS_DUE") && order.amountDue <= 0) return false;
      if (selected.amountDueFilter.includes("NO_DUE") && order.amountDue > 0) return false;
      if (selected.itemCountFilter.includes("HAS_ITEMS") && order.itemCount <= 0) return false;
      if (selected.itemCountFilter.includes("NO_ITEMS") && order.itemCount > 0) return false;

      return true;
    });

    const add = (metric: Metric, order: any) => {
      metric.orders += 1;
      const cancelled = this.normalize(order.status) === "CANCELLED";
      if (cancelled) {
        metric.cancelledOrders += 1;
        return;
      }
      metric.revenue += order.finalAmount;
      metric.shippingCharged += order.shippingCharged;
      metric.shippingCost += order.shippingCost;
      metric.discount += order.discount;
      metric.cost += order.cost;
      metric.netRevenue += order.finalAmount + order.shippingCharged - order.discount;
      metric.grossProfit += order.finalAmount + order.shippingCharged - order.discount - order.shippingCost - order.cost;
      if (this.normalize(order.status) === "COMPLETED") metric.completedOrders += 1;
    };

    const finalize = (metric: Metric) => ({
      ...metric,
      avgOrderValue: metric.orders ? Math.round(metric.revenue / metric.orders) : 0,
      grossMargin: metric.netRevenue ? Number(((metric.grossProfit / metric.netRevenue) * 100).toFixed(2)) : 0,
    });

    const summary = this.emptyMetric();
    const daily = new Map<string, Metric>();
    const byBranch = new Map<string, Metric>();
    const byStaff = new Map<string, Metric>();
    const byChannel = new Map<string, Metric>();
    const byCarrier = new Map<string, Metric>();
    const byPayment = new Map<string, Metric>();

    for (let cursor = new Date(from); cursor < toExclusive; cursor = this.addDays(cursor, 1)) {
      daily.set(this.dateKey(cursor), this.emptyMetric());
    }

    filtered.forEach((order: any) => {
      add(summary, order);
      const date = this.dateKey(new Date(order[dateField] || order.createdAt));
      const paymentGroup = order.paymentSourceNames.join(", ") || order.paymentStatus || "Chưa rõ";
      const pairs: Array<[Map<string, Metric>, string]> = [
        [daily, date],
        [byBranch, String(order.branchId || "UNKNOWN")],
        [byStaff, String(order.assignedStaffId || order.createdByStaffId || "UNKNOWN")],
        [byChannel, String(order.salesChannel || "OTHER")],
        [byCarrier, String(order.carrier || "NONE")],
        [byPayment, paymentGroup],
      ];
      pairs.forEach(([map, key]) => {
        const metric = map.get(key) || this.emptyMetric();
        add(metric, order);
        map.set(key, metric);
      });
    });

    const rows = (map: Map<string, Metric>, label: (key: string) => string) =>
      Array.from(map.entries())
        .map(([id, metric]) => ({ id, label: label(id), ...finalize(metric) }))
        .sort((a, b) => b.netRevenue - a.netRevenue);

    const createdStaffOptions = (staff as any[]).map((row) => ({ id: row.id, name: this.staffLabel(row) }));
    const assignedStaffOptions = [{ id: "UNASSIGNED", name: "Chưa gán nhân viên" }, ...createdStaffOptions];

    const channelDefaults = [
      { id: "POS", name: "POS" },
      { id: "FACEBOOK_MANUAL", name: "Facebook" },
      { id: "SHOWROOM", name: "Showroom" },
      { id: "VN_WEB", name: "Website VN" },
      { id: "INTL_WEB", name: "Website quốc tế" },
      { id: "TIKTOK", name: "TikTok" },
      { id: "SHOPEE", name: "Shopee" },
      { id: "ZALO", name: "Zalo" },
      { id: "OTHER", name: "Khác" },
      ...enriched.map((order: any) => ({ id: String(order.salesChannel || "OTHER"), name: this.labelChannel(order.salesChannel) })),
    ];

    const deliveryOptions = this.optionMap([
      { id: "NONE", name: "Chưa có vận đơn" },
      ...enriched.map((order: any) => ({ id: order.deliveryStatus, name: order.deliveryStatus === "NONE" ? "Chưa có vận đơn" : order.deliveryStatus })),
    ]);

    const carrierOptions = this.optionMap([
      { id: "NONE", name: "Chưa có đơn vị vận chuyển" },
      { id: "GHN", name: "GHN" },
      { id: "AHAMOVE", name: "AhaMove" },
      { id: "VIETTELPOST", name: "Viettel Post" },
      { id: "GHTK", name: "GHTK" },
      { id: "GRAB", name: "Grab Express" },
      { id: "SHIPPER", name: "Shipper riêng" },
      ...enriched.map((order: any) => ({ id: order.carrier, name: order.carrier === "NONE" ? "Chưa có đơn vị vận chuyển" : order.carrier })),
    ]);

    return {
      success: true,
      generatedAt: now.toISOString(),
      filters: params,
      options: {
        branches: branches.map((row) => ({ id: row.id, name: row.name })),
        createdByStaff: createdStaffOptions,
        assignedStaff: assignedStaffOptions,
        orderStatuses: [
          { id: "NEW", name: "Mới tạo" },
          { id: "APPROVED", name: "Đã duyệt" },
          { id: "PACKING", name: "Đang đóng gói" },
          { id: "SHIPPED", name: "Đã gửi hàng" },
          { id: "COMPLETED", name: "Hoàn thành" },
          { id: "CANCELLED", name: "Đã huỷ" },
        ],
        paymentStatuses: [
          { id: "UNPAID", name: "Chưa thanh toán" },
          { id: "PARTIAL", name: "Thanh toán một phần" },
          { id: "PAID", name: "Đã thanh toán" },
          { id: "PENDING_COD", name: "Chờ đối soát COD" },
          { id: "REFUNDED", name: "Đã hoàn tiền" },
          { id: "FAILED", name: "Thanh toán lỗi" },
        ],
        fulfillmentStatuses: [
          { id: "UNFULFILLED", name: "Chưa giao" },
          { id: "PROCESSING", name: "Đang chuẩn bị" },
          { id: "PARTIAL", name: "Một phần" },
          { id: "FULFILLED", name: "Đã giao vận / hoàn tất" },
          { id: "RETURNED", name: "Trả hàng" },
        ],
        deliveryStatuses: deliveryOptions,
        channels: this.optionMap(channelDefaults),
        shippingModes: [
          { id: "DELIVERY", name: "Giao hàng" },
          { id: "PICKUP", name: "Nhận tại cửa hàng" },
          { id: "POS", name: "Bán tại quầy" },
          { id: "OTHER", name: "Chưa rõ cách giao" },
        ],
        carriers: carrierOptions,
        paymentSources: paymentSources.map((row: any) => ({
          id: row.id,
          name: `${row.name}${row.type ? ` · ${row.type}` : ""}`,
        })),
        trackingOptions: [
          { id: "HAS", name: "Có mã vận đơn" },
          { id: "NONE", name: "Chưa có mã vận đơn" },
        ],
        codOptions: [
          { id: "HAS_COD", name: "Có thu hộ COD" },
          { id: "NO_COD", name: "Không COD" },
        ],
        codReconciliationOptions: [
          { id: "RECONCILED", name: "Đã đối soát COD" },
          { id: "NOT_RECONCILED", name: "Chưa đối soát COD" },
          { id: "MISMATCH", name: "Lệch đối soát" },
          { id: "NOT_FOUND", name: "Không tìm thấy trong phiên GHN" },
          { id: "SAVED", name: "Đã lưu đối soát" },
        ],
        amountDueOptions: [
          { id: "HAS_DUE", name: "Còn phải thu" },
          { id: "NO_DUE", name: "Không còn phải thu" },
        ],
        itemCountOptions: [
          { id: "HAS_ITEMS", name: "Có sản phẩm" },
          { id: "NO_ITEMS", name: "Thiếu sản phẩm" },
        ],
      },
      summary: finalize(summary),
      dailyRows: rows(daily, (id) => id).map((row) => ({ ...row, date: row.id })),
      branchRows: rows(byBranch, (id) => branchMap.get(id) || id),
      staffRows: rows(byStaff, (id) => this.staffLabel(staffMap.get(id))),
      channelRows: rows(byChannel, (id) => this.labelChannel(id)),
      carrierRows: rows(byCarrier, (id) => id === "NONE" ? "Chưa có vận chuyển" : id),
      paymentRows: rows(byPayment, (id) => id),
      orders: filtered.map((order: any) => ({
        id: order.id,
        code: order.orderCode || order.id,
        customerName: order.customerName || "Khách lẻ",
        phone: order.customerPhone || "",
        createdAt: order.createdAt,
        soldAt: order.soldAt,
        branchId: order.branchId,
        branchName: branchMap.get(order.branchId) || order.branchId,
        createdByStaffId: order.createdByStaffId,
        createdByStaffName: order.createdByStaffName,
        assignedStaffId: order.assignedStaffId,
        assignedStaffName: order.assignedStaffName,
        staffName:
          String(order.assignedStaffName || "").trim() ||
          String(order.createdByStaffName || "").trim() ||
          "Chưa rõ nhân viên",
        salesChannel: order.salesChannel,
        channelLabel: this.labelChannel(order.salesChannel),
        status: order.status,
        statusLabel: this.orderStatusLabel(order.status),
        paymentStatus: order.paymentStatus,
        paymentStatusLabel: this.paymentStatusLabel(order.paymentStatus),
        fulfillmentStatus: order.fulfillmentStatus,
        fulfillmentStatusLabel: this.fulfillmentStatusLabel(order.fulfillmentStatus),
        deliveryStatus: order.deliveryStatus,
        deliveryStatusLabel: this.deliveryStatusLabel(order.deliveryStatus),
        paymentMethod: order.paymentSourceNames.join(", ") || order.paymentStatus,
        carrier: order.carrier === "NONE" ? "" : order.carrier,
        shippingMode: this.labelShippingMode(order.shippingMode),
        trackingCode: order.trackingCode,
        codAmount: order.codAmount,
        codReconciliationStatus: order.codReconciliationStatus,
        amountDue: order.amountDue,
        itemCount: order.itemCount,
        finalAmount: order.finalAmount,
        shippingCharged: order.shippingCharged,
        shippingCost: order.shippingCost,
        discount: order.discount,
        netRevenue: order.finalAmount + order.shippingCharged - order.discount,
        cost: order.cost,
        grossProfit: order.finalAmount + order.shippingCharged - order.discount - order.shippingCost - order.cost,
        actionUrl: `/orders/${order.id}`,
      })),
      debug: {
        totalBeforeFilter: enriched.length,
        totalAfterFilter: filtered.length,
        optionalOrderColumns,
        optionalShipmentColumns,
      },
    };
  }
}
