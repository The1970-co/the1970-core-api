import { BadRequestException, Injectable, Logger, OnModuleDestroy, OnModuleInit } from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";
import { GhnClient } from "./ghn.client";
import { AhamoveClient } from "./ahamove.client";
import { ViettelPostClient } from "./viettelpost.client";
import { QuoteShipmentDto } from "./dto/quote-shipment.dto";
import { CreateGhnShipmentDto } from "./dto/create-ghn-shipment.dto";
import { TrackShipmentDto } from "./dto/track-shipment.dto";
import { AuthTotpService } from "../auth-totp/auth-totp.service";

@Injectable()
export class ShipmentService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(ShipmentService.name);
  private readonly trackingCacheMinutes = Number(
    process.env.SHIPMENT_TRACKING_CACHE_MINUTES || 12
  );

  private ghnTrackingSyncTimer: NodeJS.Timeout | null = null;
  private ghnTrackingSyncRunning = false;
  private readonly ghnTrackingSyncCronEnabled = !["0", "false", "off", "no"].includes(
    String(process.env.SHIPMENT_GHN_SYNC_CRON_ENABLED || "true").toLowerCase(),
  );
  private readonly ghnTrackingSyncIntervalMs = Math.max(
    60_000,
    Number(process.env.SHIPMENT_GHN_SYNC_INTERVAL_SECONDS || 300) * 1000,
  );
  private readonly ghnTrackingSyncLimit = Math.min(
    Math.max(Number(process.env.SHIPMENT_GHN_SYNC_LIMIT || 80), 1),
    300,
  );
  private readonly ghnTrackingSyncDays = Math.min(
    Math.max(Number(process.env.SHIPMENT_GHN_SYNC_DAYS || 90), 1),
    365,
  );

  onModuleInit() {
    this.startGhnTrackingSyncCron();
  }

  onModuleDestroy() {
    if (this.ghnTrackingSyncTimer) {
      clearInterval(this.ghnTrackingSyncTimer);
      this.ghnTrackingSyncTimer = null;
    }
  }

  private startGhnTrackingSyncCron() {
    if (!this.ghnTrackingSyncCronEnabled) {
      this.logger.log("[GHN_SYNC_CRON] disabled by SHIPMENT_GHN_SYNC_CRON_ENABLED");
      return;
    }

    if (this.ghnTrackingSyncTimer) return;

    this.logger.log(
      `[GHN_SYNC_CRON] enabled interval=${Math.round(this.ghnTrackingSyncIntervalMs / 1000)}s limit=${this.ghnTrackingSyncLimit} days=${this.ghnTrackingSyncDays}`,
    );

    const firstDelayMs = Math.max(
      20_000,
      Number(process.env.SHIPMENT_GHN_SYNC_FIRST_DELAY_SECONDS || 45) * 1000,
    );

    setTimeout(() => {
      void this.runGhnTrackingSyncCron("startup");
    }, firstDelayMs).unref?.();

    this.ghnTrackingSyncTimer = setInterval(() => {
      void this.runGhnTrackingSyncCron("interval");
    }, this.ghnTrackingSyncIntervalMs);

    this.ghnTrackingSyncTimer.unref?.();
  }

  private async runGhnTrackingSyncCron(trigger: "startup" | "interval" | "manual" = "interval") {
    if (this.ghnTrackingSyncRunning) {
      this.logger.warn(`[GHN_SYNC_CRON] skip ${trigger}: previous run is still running`);
      return { ok: false, skipped: true, reason: "previous_run_still_running" };
    }

    this.ghnTrackingSyncRunning = true;
    const startedAt = Date.now();

    try {
      const result = await this.refreshGhnTrackingBackfill({
        days: this.ghnTrackingSyncDays,
        limit: this.ghnTrackingSyncLimit,
        includeFinal: false,
        dryRun: false,
        onlyDelivered: false,
        source: `cron:${trigger}`,
        preferOldest: true,
      });

      this.logger.log(
        `[GHN_SYNC_CRON] ${trigger} done success=${result.success} correctedOrder=${result.correctedOrderStatus} shipmentChanged=${result.shipmentStatusChanged} skipped=${result.skippedNotDelivered} failed=${result.failedCount} elapsed=${result.elapsedSeconds}s`,
      );

      return result;
    } catch (error) {
      this.logger.error(
        `[GHN_SYNC_CRON] ${trigger} failed after ${Number(((Date.now() - startedAt) / 1000).toFixed(1))}s: ${error instanceof Error ? error.message : String(error)}`,
      );
      return { ok: false, skipped: false, reason: error instanceof Error ? error.message : String(error) };
    } finally {
      this.ghnTrackingSyncRunning = false;
    }
  }

  async runGhnTrackingSyncCronNow() {
    return this.runGhnTrackingSyncCron("manual");
  }

  getGhnTrackingSyncCronStatus() {
    return {
      enabled: this.ghnTrackingSyncCronEnabled,
      running: this.ghnTrackingSyncRunning,
      intervalSeconds: Math.round(this.ghnTrackingSyncIntervalMs / 1000),
      limit: this.ghnTrackingSyncLimit,
      days: this.ghnTrackingSyncDays,
      mode: "sync_all_safe_statuses",
      note: "Cron cập nhật trạng thái vận đơn GHN; không tự huỷ đơn nội bộ và không tự xác nhận shop đã nhận hàng hoàn.",
    };
  }

  constructor(
    private readonly prisma: PrismaService,
    private readonly ghnClient: GhnClient,
    private readonly ahamoveClient: AhamoveClient,
    private readonly viettelPostClient: ViettelPostClient,
    private readonly authTotpService: AuthTotpService
  ) { }

  private readonly fromDistrictId = Number(process.env.GHN_FROM_DISTRICT_ID || 0);
  private readonly fromWardCode = process.env.GHN_FROM_WARD_CODE || "";
  private readonly returnPhone = process.env.GHN_RETURN_PHONE || "";
  private readonly returnAddress = process.env.GHN_RETURN_ADDRESS || "";
  private readonly returnName = process.env.GHN_RETURN_NAME || "The 1970";

  private normalizeCarrierStatusText(input?: string | null) {
    return String(input || "")
      .trim()
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/đ/g, "d")
      .replace(/Đ/g, "D")
      .replace(/[_-]+/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  private mapShippingStatus(input?: string | null) {
    const value = this.normalizeCarrierStatusText(input);

    if (!value) return "NOT_CREATED";

    // Các trạng thái lỗi phải đứng trước "thanh cong" để tránh
    // "giao khong thanh cong" bị nhận nhầm là DELIVERED.
    if (
      value.includes("khong thanh cong") ||
      value.includes("that bai") ||
      value.includes("giao that bai") ||
      value.includes("delivery fail") ||
      value.includes("delivery failed") ||
      value.includes("khong lien lac duoc") ||
      value.includes("khong nghe may") ||
      value.includes("chan so") ||
      value.includes("tu choi nhan") ||
      value.includes("doi y khong mua") ||
      value.includes("fail") ||
      value.includes("exception") ||
      value.includes("lost") ||
      value.includes("damage")
    ) {
      return "FAILED";
    }

    // GHN trả "returned"/"đã hoàn hàng" nghĩa là hãng đã kết thúc chiều hoàn.
    // Không được map nhầm sang DELIVERED chỉ vì có chữ "thành công".
    if (
      value.includes("da hoan hang") ||
      value.includes("don hang da hoan") ||
      value.includes("da hoan") ||
      value.includes("hoan hang thanh cong") ||
      value.includes("hoan thanh cong") ||
      value.includes("return success") ||
      value.includes("return successful") ||
      value.includes("return completed") ||
      value === "returned" ||
      value.includes(" returned")
    ) {
      return "RETURNED";
    }

    // Final states must be checked before generic words like "deliver".
    // GHN may return Vietnamese labels such as "Giao hàng thành công".
    if (
      value.includes("giao hang thanh cong") ||
      value.includes("phat thanh cong") ||
      value.includes("da giao hang") ||
      value.includes("da giao") ||
      value.includes("thanh cong") ||
      value.includes("delivered") ||
      value.includes("delivery success") ||
      value.includes("delivery successful") ||
      value.includes("completed") ||
      value.includes("complete") ||
      value.includes("success")
    ) {
      return "DELIVERED";
    }

    if (
      value.includes("huy") ||
      value.includes("cancel")
    ) {
      return "CANCELLED";
    }

    if (
      value.includes("chuyen hoan") ||
      value.includes("cho hoan") ||
      value.includes("dang hoan") ||
      value.includes("hoan hang") ||
      value.includes("hang hoan") ||
      value.includes("tra hang") ||
      value.includes("waiting to return") ||
      value.includes("waiting return") ||
      value.includes("return")
    ) {
      return "RETURNING";
    }

    if (
      value.includes("dang giao") ||
      value.includes("dang phat") ||
      value.includes("ready to deliver") ||
      value.includes("waiting to deliver") ||
      value.includes("delivering") ||
      value.includes("delivery") ||
      value.includes("in process")
    ) {
      return "DELIVERING";
    }

    if (
      value.includes("lay hang") ||
      value.includes("picking") ||
      value.includes("picked") ||
      value.includes("accepted")
    ) {
      return "PICKING";
    }

    if (
      value.includes("trung chuyen") ||
      value.includes("phan loai") ||
      value.includes("transporting") ||
      value === "transport" ||
      value.includes(" transport ") ||
      value.includes("transit") ||
      value.includes("sorting") ||
      value.includes("storing")
    ) {
      return "IN_TRANSIT";
    }

    if (
      value.includes("tao don") ||
      value.includes("cho lay") ||
      value.includes("san sang") ||
      value.includes("create") ||
      value.includes("created") ||
      value.includes("ready") ||
      value.includes("pending")
    ) {
      return "CREATED";
    }

    return value.toUpperCase().replace(/\s+/g, "_");
  }

  private normalizeGhnRequiredNote(input?: string | null) {
    const value = String(input || "")
      .trim()
      .toUpperCase()
      .replace(/[\s_\-]/g, "");

    if (value === "CHOXEMHANGKHONGTHU" || value === "CHOXEMHANGKHONGCHOTHU") {
      return "CHOXEMHANGKHONGTHU";
    }

    if (value === "CHOXEMHANG" || value === "CHOXEMHANGCHOTHU") {
      return "CHOXEMHANG";
    }

    return "KHONGCHOXEMHANG";
  }

  private normalizeTimelineStatus(status?: string | null) {
    const s = String(status || "").toUpperCase();

    if (!s) return "UNKNOWN";
    const normalized = this.mapShippingStatus(status);
    if (normalized !== "NOT_CREATED") return normalized;

    return s;
  }

  private timelineTitle(status?: string | null, carrier?: string | null) {
    const s = this.normalizeTimelineStatus(status);
    const c = String(carrier || "").toUpperCase();

    if (s === "DELIVERED") return "Giao hàng thành công";
    if (s === "DELIVERING") return "Đang giao hàng";
    if (s === "PICKING") return c.includes("AHAMOVE") ? "Tài xế đã nhận / đang lấy hàng" : "Đang lấy hàng";
    if (s === "CREATED") return c.includes("AHAMOVE") ? "Đã tạo đơn / đang tìm tài xế" : "Đã tạo vận đơn";
    if (s === "CANCELLED") return "Đã huỷ vận đơn";
    if (s === "FAILED") return "Giao hàng thất bại";
    if (s === "RETURNING") return "Đang hoàn hàng";
    if (s === "IN_TRANSIT") return "Đang trung chuyển";

    return status || "Cập nhật vận chuyển";
  }

  private extractAhamoveDriver(raw: any) {
    const data = raw?.data || raw?.order || raw || {};
    const supplier = data?.supplier || data?.driver || data?.shared_link_data?.supplier || {};

    return {
      driverName:
        supplier?.name ||
        supplier?.display_name ||
        data?.driver_name ||
        data?.driverName ||
        null,
      driverPhone:
        supplier?.mobile ||
        supplier?.phone ||
        data?.driver_phone ||
        data?.driverPhone ||
        null,
      driverPlate:
        supplier?.vehicle_plate ||
        supplier?.plate ||
        data?.vehicle_plate ||
        data?.driver_plate ||
        null,
      eta:
        data?.eta ||
        data?.duration ||
        data?.estimated_time ||
        null,
      locationText:
        data?.location?.address ||
        data?.supplier?.location?.address ||
        data?.path?.[1]?.address ||
        null,
    };
  }

  private async appendShipmentTimelineEvent(client: any, input: {
    shipmentId: string;
    orderId?: string | null;
    carrier?: string | null;
    trackingCode?: string | null;
    status: string;
    partnerStatus?: string | null;
    title?: string;
    description?: string | null;
    raw?: any;
    source?: string;
    driverName?: string | null;
    driverPhone?: string | null;
    driverPlate?: string | null;
    eta?: string | null;
    locationText?: string | null;
    eventTime?: string | Date | null;
  }) {
    const status = this.normalizeTimelineStatus(input.status);
    const eventTime = input.eventTime ? new Date(input.eventTime) : new Date();
    const safeEventTime = Number.isNaN(eventTime.getTime()) ? new Date() : eventTime;

    const existing = await (client as any).shipmentTimelineEvent.findFirst({
      where: {
        shipmentId: input.shipmentId,
        status,
        partnerStatus: input.partnerStatus || null,
        eventTime: safeEventTime,
      },
    });

    if (existing) return existing;

    const latest = await (client as any).shipmentTimelineEvent.findFirst({
      where: { shipmentId: input.shipmentId },
      orderBy: { eventTime: "desc" },
    });

    const sameStatus = latest?.status === status;
    const samePartnerStatus =
      String(latest?.partnerStatus || "") === String(input.partnerStatus || "");
    const isSyntheticNow = !input.eventTime;

    if (isSyntheticNow && sameStatus && samePartnerStatus) {
      return latest;
    }

    return (client as any).shipmentTimelineEvent.create({
      data: {
        shipmentId: input.shipmentId,
        orderId: input.orderId || null,
        carrier: input.carrier || null,
        trackingCode: input.trackingCode || null,
        status,
        partnerStatus: input.partnerStatus || null,
        title: input.title || this.timelineTitle(status, input.carrier),
        description: input.description || null,
        raw: input.raw || undefined,
        source: input.source || "system",
        driverName: input.driverName || null,
        driverPhone: input.driverPhone || null,
        driverPlate: input.driverPlate || null,
        eta: input.eta ? String(input.eta) : null,
        locationText: input.locationText || null,
        eventTime: safeEventTime,
      },
    });
  }

  private mapStatusLabel(status: string) {
    const s = this.mapShippingStatus(status);

    if (s === "DELIVERED") return "Giao hàng thành công";
    if (s === "DELIVERING") return "Đang giao hàng";
    if (s === "IN_TRANSIT") return "Đang trung chuyển";
    if (s === "PICKING") return "Đang lấy hàng";
    if (s === "CREATED") return "Chờ lấy hàng";
    if (s === "CANCELLED") return "Đã hủy đơn";
    if (s === "RETURNING") return "Đang hoàn hàng";
    if (s === "FAILED") return "Giao thất bại";

    return "Cập nhật vận đơn";
  }

  private pickFirstText(row: any, keys: string[]) {
    for (const key of keys) {
      const value = row?.[key];
      if (value !== undefined && value !== null && String(value).trim()) {
        return String(value).trim();
      }
    }
    return "";
  }


  private normalizeGhnStatusKey(input?: string | null) {
    return String(input || "")
      .trim()
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/đ/g, "d")
      .replace(/Đ/g, "D")
      .replace(/[\s\-]+/g, "_")
      .replace(/[^a-z0-9_]+/g, "")
      .replace(/_+/g, "_")
      .replace(/^_|_$/g, "");
  }

  private ghnStatusLabel(input?: string | null) {
    const key = this.normalizeGhnStatusKey(input);
    const labels: Record<string, string> = {
      ready_to_pick: "Sẵn sàng lấy hàng",
      picking: "Đang lấy hàng",
      money_collect_picking: "Đang lấy hàng COD",
      picked: "Lấy hàng thành công",
      storing: "Nhập hàng vào kho/bưu cục",
      storing_order: "Nhập hàng vào kho/bưu cục",
      sorting: "Đang phân loại hàng",
      transporting: "Đang trung chuyển hàng",
      transport: "Đang trung chuyển hàng",
      transporting_return: "Đang trung chuyển hàng hoàn",
      transport_return: "Đang trung chuyển hàng hoàn",
      return_transporting: "Đang trung chuyển hàng hoàn",
      return_transport: "Đang trung chuyển hàng hoàn",
      chuyen_hoan: "Chuyển hoàn",
      cho_hoan: "Chờ hoàn hàng",
      dang_hoan_hang: "Đang hoàn hàng",
      dang_trung_chuyen_hang_hoan: "Đang trung chuyển hàng hoàn",
      ready_to_deliver: "Sẵn sàng giao hàng",
      waiting_to_deliver: "Sẵn sàng giao hàng",
      delivering: "Đang giao hàng",
      money_collect_delivering: "Đang giao hàng COD",
      delivered: "Giao hàng thành công",
      delivery_success: "Giao hàng thành công",
      completed: "Giao hàng thành công",
      delivery_fail: "Giao thất bại",
      deliver_fail: "Giao thất bại",
      waiting_to_return: "Chờ hoàn hàng",
      return: "Đang hoàn hàng",
      returning: "Đang hoàn hàng",
      returned: "Đã hoàn hàng",
      cancel: "Đã huỷ vận đơn",
      cancelled: "Đã huỷ vận đơn",
      lost: "Thất lạc hàng",
      damage: "Hàng hư hỏng",
    };

    if (labels[key]) return labels[key];

    const text = this.normalizeCarrierStatusText(input);
    if (
      text.includes("that bai") ||
      text.includes("fail") ||
      text.includes("khong lien lac duoc") ||
      text.includes("khong nghe may") ||
      text.includes("chan so") ||
      text.includes("tu choi nhan") ||
      text.includes("doi y khong mua")
    ) return "Giao thất bại";
    if (
      text.includes("chuyen hoan") ||
      text.includes("cho hoan") ||
      text.includes("dang hoan") ||
      text.includes("hoan hang") ||
      text.includes("hang hoan") ||
      text.includes("return")
    ) return text.includes("trung chuyen") ? "Đang trung chuyển hàng hoàn" : "Đang hoàn hàng";
    if (text.includes("giao hang thanh cong") || text.includes("delivered") || text.includes("success")) return "Giao hàng thành công";
    if (text.includes("dang giao") || text.includes("delivering")) return "Đang giao hàng";
    if (text.includes("trung chuyen") || text.includes("transport")) return "Đang trung chuyển hàng";
    if (text.includes("phan loai") || text.includes("sort")) return "Đang phân loại hàng";
    if (text.includes("nhap") || text.includes("luu") || text.includes("storing")) return "Nhập hàng vào kho/bưu cục";
    if (text.includes("lay hang") || text.includes("pick")) return "Đang lấy hàng";
    if (text.includes("huy") || text.includes("cancel")) return "Đã huỷ vận đơn";

    return String(input || "Cập nhật vận đơn").trim();
  }

  private collectGhnTimelineCandidates(raw: any) {
    const candidates = [
      raw?.log,
      raw?.logs,
      raw?.tracking_logs,
      raw?.trackingLogs,
      raw?.timeline,
      raw?.histories,
      raw?.history,
      raw?.status_logs,
      raw?.sorting_logs,
      raw?.order_logs,
      raw?.data?.log,
      raw?.data?.logs,
      raw?.data?.tracking_logs,
      raw?.data?.trackingLogs,
      raw?.data?.timeline,
      raw?.data?.histories,
      raw?.data?.history,
      raw?.data?.status_logs,
      raw?.data?.sorting_logs,
      raw?.data?.order_logs,
      raw?.publicTracking?.timelines,
      raw?.publicTracking?.raw?.timelines,
      raw?.publicTracking?.raw?.log,
      raw?.publicTracking?.raw?.logs,
      raw?.publicTracking?.raw?.timeline,
      raw?.publicTracking?.raw?.history,
    ];

    const rows: any[] = [];
    for (const item of candidates) {
      if (Array.isArray(item)) rows.push(...item);
    }

    // Public GHN fallback may return nested Next/Nuxt data. Walk it softly.
    const walk = (node: any, depth = 0) => {
      if (!node || depth > 7) return;
      if (Array.isArray(node)) {
        const looksLikeTimeline = node.some((row) => {
          if (!row || typeof row !== "object") return false;
          const keys = Object.keys(row).map((key) => key.toLowerCase());
          const hasStatus = keys.some((key) =>
            ["status", "status_name", "action", "action_name", "current_status"].includes(key)
          );
          const hasTimeOrDetail = keys.some((key) =>
            ["time", "updated_date", "created_date", "updated_at", "created_at", "action_at", "event_time", "description", "detail", "message", "location", "hub_name", "warehouse", "area"].includes(key)
          );
          return hasStatus && hasTimeOrDetail;
        });
        if (looksLikeTimeline) rows.push(...node);
        node.slice(0, 80).forEach((child) => walk(child, depth + 1));
        return;
      }
      if (typeof node === "object") {
        for (const value of Object.values(node)) walk(value, depth + 1);
      }
    };

    walk(raw?.publicTracking?.raw?.jsonObjects);

    const seen = new Set<string>();
    return rows.filter((row) => {
      if (!row || typeof row !== "object") return false;
      const key = JSON.stringify({
        status: row.status || row.status_name || row.action || row.action_name || row.current_status || "",
        time: row.updated_date || row.created_date || row.updated_at || row.created_at || row.action_at || row.event_time || row.time || "",
        detail: row.description || row.detail || row.message || row.reason || row.content || "",
        location: row.location || row.location_text || row.hub_name || row.warehouse || row.area || row.address || "",
      });
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }

  private pickGhnLocation(item: any, shipment?: any) {
    const location = this.pickFirstText(item, [
      "location",
      "location_text",
      "locationText",
      "hub_name",
      "hubName",
      "hub",
      "warehouse",
      "warehouse_name",
      "warehouseName",
      "current_warehouse",
      "currentWarehouse",
      "current_location",
      "currentLocation",
      "area",
      "station_name",
      "stationName",
      "post_office",
      "postOffice",
      "from_location",
      "to_location",
      "address",
    ]);

    if (location) return location;

    const statusKey = this.normalizeGhnStatusKey(
      this.pickFirstText(item, ["status", "status_name", "action", "action_name", "current_status"])
    );

    if (["delivered", "delivery_success", "delivering", "money_collect_delivering"].includes(statusKey)) {
      return String(shipment?.toAddress || shipment?.order?.shippingAddressLine1 || "").trim();
    }

    return "";
  }

  private buildGhnTimelineDescription(input: {
    rawStatus?: string | null;
    title?: string | null;
    explicitDescription?: string | null;
    location?: string | null;
    shipment?: any;
    item?: any;
  }) {
    const key = this.normalizeGhnStatusKey(input.rawStatus || input.title || "");
    const explicit = String(input.explicitDescription || "").trim();
    const location = String(input.location || "").replace(/\s+/g, " ").trim();

    const code = this.pickFirstText(input.item, [
      "order_code",
      "client_order_code",
      "tracking_code",
      "trackingCode",
      "orderCode",
    ]);
    const partialText = /_PR|partial|giao\s*1\s*phan|giao\s*mot\s*phan/i.test(
      [code, input.item?.description, input.item?.reason, input.item?.note, input.item?.message]
        .filter(Boolean)
        .join(" ")
    )
      ? `${code || "Đơn"} - Đơn giao 1 phần`
      : "";

    const normalizedExplicit = this.normalizeGhnStatusKey(explicit);
    const looksRawOnly = explicit && (normalizedExplicit === key || normalizedExplicit === this.normalizeGhnStatusKey(input.rawStatus));
    if (explicit && !looksRawOnly && !/^(delivered|delivering|storing|transporting|picking|picked|sorting|money_collect)/i.test(explicit)) {
      return [explicit, partialText].filter(Boolean).join("\n");
    }

    let detail = "";
    if (["delivered", "delivery_success", "completed"].includes(key)) {
      detail = `Đơn hàng được giao thành công${location ? ` tại ${location}` : ""}.`;
    } else if (["delivering", "money_collect_delivering"].includes(key)) {
      detail = `Đơn hàng đang giao${location ? ` đến ${location}` : ""}.`;
    } else if (["ready_to_deliver", "waiting_to_deliver"].includes(key)) {
      detail = `Đơn hàng sẵn sàng được giao${location ? ` tại ${location}` : ""}.`;
    } else if (["storing", "storing_order"].includes(key)) {
      detail = `Đơn hàng lưu tại ${location || "bưu cục/kho"}.`;
    } else if (key === "sorting") {
      detail = `Đơn hàng đang phân loại${location ? ` tại ${location}` : ""}.`;
    } else if (["transporting_return", "transport_return", "return_transporting", "return_transport", "chuyen_hoan", "dang_trung_chuyen_hang_hoan"].includes(key)) {
      detail = `Đơn hàng đang trung chuyển hàng hoàn${location ? ` tại ${location}` : ""}.`;
    } else if (["transporting", "transport"].includes(key)) {
      const text = this.normalizeCarrierStatusText([input.rawStatus, input.title, explicit, location].filter(Boolean).join(" | "));
      detail = text.includes("hoan") || text.includes("return")
        ? `Đơn hàng đang trung chuyển hàng hoàn${location ? ` tại ${location}` : ""}.`
        : `Đơn hàng đang trung chuyển${location ? ` đến ${location}` : ""}.`;
    } else if (key === "picked") {
      detail = `Đơn hàng lấy thành công${location ? ` tại ${location}` : ""}.`;
    } else if (["picking", "money_collect_picking"].includes(key)) {
      detail = `Nhân viên đang lấy hàng${location ? ` tại ${location}` : ""}.`;
    } else if (key === "ready_to_pick") {
      detail = `Đơn hàng chờ lấy${location ? ` tại ${location}` : ""}.`;
    } else if (["delivery_fail", "deliver_fail"].includes(key)) {
      detail = `Đơn hàng giao thất bại${location ? ` tại ${location}` : ""}.`;
    } else if (["return", "returning", "waiting_to_return", "cho_hoan", "dang_hoan_hang"].includes(key)) {
      detail = `Đơn hàng đang hoàn${location ? ` tại ${location}` : ""}.`;
    } else if (key === "returned") {
      detail = `Đơn hàng đã hoàn${location ? ` về ${location}` : ""}.`;
    } else if (["cancel", "cancelled"].includes(key)) {
      detail = "Vận đơn đã huỷ.";
    } else {
      detail = `${this.ghnStatusLabel(input.rawStatus || input.title)}${location ? ` tại ${location}` : ""}.`;
    }

    return [detail, partialText].filter(Boolean).join("\n");
  }

  private normalizeGhnTimelineEvent(item: any, index: number, shipment?: any) {
    const rawStatus = this.pickFirstText(item, [
      "status",
      "current_status",
      "log_status",
      "order_status",
      "action",
      "action_name",
      "status_name",
    ]);

    const explicitDescription = this.pickFirstText(item, [
      "description",
      "desc",
      "detail",
      "message",
      "reason",
      "note",
      "content",
    ]);

    const location = this.pickGhnLocation(item, shipment);

    const eventCode = this.pickFirstText(item, [
      "code",
      "status_code",
      "action_code",
      "event_code",
      "log_code",
    ]);

    const time = this.pickFirstText(item, [
      "updated_date",
      "action_at",
      "created_date",
      "event_time",
      "time",
      "created_at",
      "updated_at",
      "update_time",
    ]);

    const rawTitle =
      this.pickFirstText(item, ["status_name", "action_name", "title", "name"]) ||
      rawStatus ||
      explicitDescription;

    const title = this.ghnStatusLabel(rawTitle || rawStatus);
    const description = this.buildGhnTimelineDescription({
      rawStatus,
      title: rawTitle,
      explicitDescription,
      location,
      shipment,
      item,
    });

    const mappedStatus = this.mapShippingStatus(
      [title, rawTitle, rawStatus, description].filter(Boolean).join(" | ")
    );

    return {
      id: `${index}-${eventCode || rawStatus || "log"}-${time || "time"}`,
      status: mappedStatus === "NOT_CREATED" ? rawStatus : mappedStatus,
      rawStatus,
      eventCode,
      title,
      description,
      location,
      locationText: location,
      time,
      eventTime: time,
      raw: {
        ...(item || {}),
        rawStatus,
        eventCode,
        description,
        location,
        title,
      },
    };
  }

  private normalizeTimeline(raw: any, shipment?: any) {
    const logs = this.collectGhnTimelineCandidates(raw);

    const sorted = [...logs].sort((a, b) => {
      const ta = new Date(
        this.pickFirstText(a, [
          "updated_date",
          "action_at",
          "created_date",
          "event_time",
          "time",
          "created_at",
          "updated_at",
          "update_time",
        ]) || 0
      ).getTime();
      const tb = new Date(
        this.pickFirstText(b, [
          "updated_date",
          "action_at",
          "created_date",
          "event_time",
          "time",
          "created_at",
          "updated_at",
          "update_time",
        ]) || 0
      ).getTime();
      return tb - ta;
    });

    return sorted.map((item: any, index: number) =>
      this.normalizeGhnTimelineEvent(item, index, shipment)
    );
  }

  private normalizeTracking(raw: any, shipment: any) {
    const timeline = this.normalizeTimeline(raw, shipment);

    const rawStatusText = [
      raw?.status_name,
      raw?.current_status,
      raw?.status,
      raw?.status_code,
      raw?.order_status,
      raw?.data?.status_name,
      raw?.data?.current_status,
      raw?.data?.status,
    ]
      .filter(Boolean)
      .join(" | ");

    // Chỉ tin timeline mới nhất. Không scan toàn bộ timeline để tìm DELIVERED,
    // vì log cũ/metadata public GHN có thể chứa chữ "giao thành công" hoặc "huỷ"
    // trong khi trạng thái hiện tại đang là picking/transporting/delivering.
    // Giữ nguyên flow cũ, chỉ chặn nhận nhầm trạng thái final từ log cũ.
    const latestTimelineStatus = [
      timeline[0]?.title,
      timeline[0]?.description,
      timeline[0]?.status,
      timeline[0]?.rawStatus,
    ]
      .filter(Boolean)
      .join(" | ");

    const rawShippingStatus = this.mapShippingStatus(rawStatusText);
    const timelineShippingStatus = this.mapShippingStatus(latestTimelineStatus);
    const storedShippingStatus = this.mapShippingStatus(shipment?.shippingStatus || "UNKNOWN");
    const activeStatuses = ["CREATED", "PICKING", "DELIVERING", "IN_TRANSIT"];
    const finalStatuses = ["DELIVERED", "RETURNING", "RETURNED", "FAILED", "CANCELLED"];

    // Cực quan trọng: GHN public đôi khi có text/metadata cũ chứa chữ huỷ/hoàn thành,
    // trong khi timeline mới nhất lại đang lấy hàng/trung chuyển/đang giao.
    // Ưu tiên trạng thái active mới nhất để đơn chỉ là ĐÃ XUẤT KHO, không tự nhảy HOÀN THÀNH.
    const shippingStatus = activeStatuses.includes(rawShippingStatus)
      ? rawShippingStatus
      : activeStatuses.includes(timelineShippingStatus)
        ? timelineShippingStatus
        : finalStatuses.includes(rawShippingStatus)
          ? rawShippingStatus
          : finalStatuses.includes(timelineShippingStatus)
            ? timelineShippingStatus
            : rawShippingStatus !== "NOT_CREATED"
              ? rawShippingStatus
              : timelineShippingStatus !== "NOT_CREATED"
                ? timelineShippingStatus
                : storedShippingStatus;

    const partnerStatus =
      raw?.status_name ||
      raw?.current_status ||
      raw?.status ||
      timeline[0]?.title ||
      timeline[0]?.rawStatus ||
      timeline[0]?.status ||
      shipment?.partnerStatus ||
      shipment?.shippingStatus ||
      "UNKNOWN";

    return {
      trackingCode: shipment?.trackingCode || raw?.order_code || "",
      carrier: shipment?.carrier || "GHN",
      shippingStatus,
      partnerStatus: partnerStatus || this.mapStatusLabel(shippingStatus),
      codAmount: Number(raw?.cod_amount ?? shipment?.codAmount ?? 0),
      shippingFee: Number(raw?.total_fee ?? shipment?.shippingFee ?? 0),
      serviceType: raw?.service_type_id ?? null,
      leadtime: raw?.leadtime ?? null,
      updatedAt:
        raw?.updated_date ||
        raw?.updated_at ||
        timeline[0]?.time ||
        shipment?.updatedAt ||
        null,
      from: {
        name: raw?.from_name || shipment?.fromName || "",
        phone: raw?.from_phone || shipment?.fromPhone || "",
        address: raw?.from_address || shipment?.fromAddress || "",
      },
      to: {
        name: raw?.to_name || shipment?.toName || "",
        phone: raw?.to_phone || shipment?.toPhone || "",
        address: raw?.to_address || shipment?.toAddress || "",
      },
      timeline,
      rawSummary: {
        shopId: raw?.shop_id ?? null,
        clientOrderCode: raw?.client_order_code ?? null,
      },
    };
  }

  private assertCanEditCod(order: any) {
    if (!order) {
      throw new BadRequestException("Không tìm thấy order");
    }

    if (!order.shipment?.trackingCode) {
      throw new BadRequestException("Đơn chưa có vận đơn GHN");
    }

    const orderStatus = String(order.status || "").toUpperCase();
    if (orderStatus === "CANCELLED" || orderStatus === "COMPLETED") {
      throw new BadRequestException("Đơn này không được sửa COD");
    }

    const shipmentStatus = String(order.shipment?.shippingStatus || "").toUpperCase();
    if (shipmentStatus.includes("DELIVERED") || shipmentStatus.includes("SUCCESS")) {
      throw new BadRequestException("Đơn đã giao thành công, không thể sửa COD");
    }
  }

  private async appendCarrierTimelineEvents(client: any, input: {
    shipmentId: string;
    orderId?: string | null;
    carrier?: string | null;
    trackingCode?: string | null;
    timeline?: any[];
    source?: string;
  }) {
    const rows = Array.isArray(input.timeline) ? input.timeline : [];

    for (const row of rows) {
      await this.appendShipmentTimelineEvent(client, {
        shipmentId: input.shipmentId,
        orderId: input.orderId,
        carrier: input.carrier,
        trackingCode: input.trackingCode,
        status: row.status || row.rawStatus || row.title || "UNKNOWN",
        partnerStatus: row.rawStatus || row.title || row.status || null,
        title: row.title || this.timelineTitle(row.status, input.carrier),
        description: [row.description, row.location ? `Vị trí/Bưu cục: ${row.location}` : ""]
          .filter(Boolean)
          .join(" · ") || null,
        raw: row.raw || row,
        source: input.source || "carrier_timeline",
        locationText: row.locationText || row.location || null,
        eventTime: row.eventTime || row.time || null,
      });
    }
  }

  async getShipmentDetail(id: string) {
    const shipment = await this.prisma.shipment.findUnique({
      where: { id },
      include: {
        order: {
          include: {
            items: true,
          },
        },
      },
    });

    if (!shipment) {
      throw new BadRequestException("Không tìm thấy phiếu giao hàng");
    }

    return shipment;
  }

  async getShipmentTracking(id: string, force = false) {
    const shipment = await this.prisma.shipment.findUnique({
      where: { id },
      include: {
        trackingCaches: {
          orderBy: { fetchedAt: "desc" },
          take: 1,
        },
      },
    });

    if (!shipment) {
      throw new BadRequestException("Không tìm thấy phiếu giao hàng");
    }

    if (!shipment.trackingCode && !shipment.ahamoveOrderId) {
      return {
        source: "pending",
        cached: false,
        shipment: {
          id: shipment.id,
          carrier: shipment.carrier,
          shippingStatus: "PENDING_SYNC",
          partnerStatus: "Đang đồng bộ vận đơn",
        },
        tracking: null,
        timeline: [],
      };
    }

    const latestCache = shipment.trackingCaches?.[0];
    const now = new Date();

    if (
      !force &&
      latestCache &&
      latestCache.expiresAt &&
      new Date(latestCache.expiresAt).getTime() > now.getTime()
    ) {
      return {
        source: "cache",
        cached: true,
        fetchedAt: latestCache.fetchedAt,
        expiresAt: latestCache.expiresAt,
        shipment: {
          id: shipment.id,
          trackingCode: shipment.trackingCode,
          carrier: shipment.carrier,
          shippingStatus: shipment.shippingStatus,
          partnerStatus: shipment.partnerStatus,
          ahamoveTrackingUrl: shipment.ahamoveTrackingUrl,
        },
        tracking: latestCache.normalizedJson,
        timeline: [],
      };
    }

    const carrierUpper = String(shipment.carrier || "").toUpperCase();
    const isViettelPost = carrierUpper.includes("VIETTEL");

    if (isViettelPost) {
      try {
        await this.trackViettelPostByShipmentId(shipment.id);
      } catch (error) {
        this.logger.warn(
          `[TRACKING_PENDING] ${shipment.id} | ${error instanceof Error ? error.message : error
          }`
        );

        return {
          source: "pending_sync",
          cached: false,
          shipment: {
            id: shipment.id,
            trackingCode: shipment.trackingCode,
            carrier: "VIETTELPOST",
            shippingStatus: shipment.shippingStatus || "CREATED",
            partnerStatus: "ViettelPost đang đồng bộ hành trình",
          },
          tracking: {
            carrier: "VIETTELPOST",
            trackingCode: shipment.trackingCode,
            shippingStatus: shipment.shippingStatus || "CREATED",
            partnerStatus: "ViettelPost đang đồng bộ hành trình",
            timeline: [],
          },
          timeline: [],
        };
      }

      const updated = await this.prisma.shipment.findUnique({ where: { id: shipment.id } });
      const timeline = await (this.prisma as any).shipmentTimelineEvent.findMany({
        where: { shipmentId: shipment.id },
        orderBy: { eventTime: "desc" },
        take: 50,
      });

      const trackingUrl = this.buildViettelPostTrackingUrl(updated?.trackingCode || shipment.trackingCode || "");
      const expiresAt = new Date(now.getTime() + this.trackingCacheMinutes * 60 * 1000);

      return {
        source: "viettelpost_live",
        cached: false,
        fetchedAt: now,
        expiresAt,
        shipment: {
          id: shipment.id,
          trackingCode: updated?.trackingCode || shipment.trackingCode,
          carrier: "VIETTELPOST",
          shippingStatus: updated?.shippingStatus || shipment.shippingStatus,
          partnerStatus: updated?.partnerStatus || shipment.partnerStatus,
          trackingUrl,
        },
        tracking: {
          carrier: "VIETTELPOST",
          trackingCode: updated?.trackingCode || shipment.trackingCode,
          shippingStatus: updated?.shippingStatus || shipment.shippingStatus,
          partnerStatus: updated?.partnerStatus || shipment.partnerStatus,
          codAmount: Number(updated?.codAmount || shipment.codAmount || 0),
          shippingFee: Number(updated?.shippingFee || shipment.shippingFee || 0),
          updatedAt: updated?.lastSyncedAt || updated?.updatedAt || now,
          from: {
            name: updated?.fromName || shipment.fromName || "",
            phone: updated?.fromPhone || shipment.fromPhone || "",
            address: updated?.fromAddress || shipment.fromAddress || "",
          },
          to: {
            name: updated?.toName || shipment.toName || "",
            phone: updated?.toPhone || shipment.toPhone || "",
            address: updated?.toAddress || shipment.toAddress || "",
          },
          timeline: this.mapTimelineEventsForTracking(timeline),
          trackingUrl,
          raw: (updated as any)?.metadata || (shipment as any)?.metadata || {},
        },
        timeline,
      };
    }

    const isAhamove = carrierUpper.includes("AHAMOVE");

    if (isAhamove) {
      const ahamoveOrderId = shipment.ahamoveOrderId || shipment.trackingCode || "";
      const raw = await this.ahamoveClient.getOrderDetail(ahamoveOrderId);
      const ahamoveStatus = this.getAhamoveStatus(raw);
      const trackingUrl = this.getAhamoveTrackingUrl(raw);
      const shippingStatus = this.mapAhamoveShippingStatus(ahamoveStatus);
      const driver = this.extractAhamoveDriver(raw);

      const expiresAt = new Date(
        now.getTime() + this.trackingCacheMinutes * 60 * 1000
      );

      await this.prisma.$transaction(
        async (tx) => {
        await tx.shipmentTrackingCache.create({
          data: {
            shipmentId: shipment.id,
            carrier: "AHAMOVE",
            trackingCode: ahamoveOrderId,
            payloadJson: raw,
            normalizedJson: {
              carrier: "AHAMOVE",
              trackingCode: ahamoveOrderId,
              shippingStatus,
              partnerStatus: ahamoveStatus,
              trackingUrl,
              driver,
            },
            fetchedAt: now,
            expiresAt,
          },
        });

        await tx.shipment.update({
          where: { id: shipment.id },
          data: {
            shippingStatus,
            partnerStatus: ahamoveStatus,
            ahamoveStatus,
            ahamoveSubStatus: raw?.sub_status || raw?.data?.sub_status || null,
            ahamoveTrackingUrl: trackingUrl,
            ahamoveRaw: raw,
            metadata: raw,
            lastSyncedAt: now,
          },
        });

        await this.appendShipmentTimelineEvent(tx, {
          shipmentId: shipment.id,
          orderId: shipment.orderId,
          carrier: "AHAMOVE",
          trackingCode: ahamoveOrderId,
          status: shippingStatus,
          partnerStatus: ahamoveStatus,
          title: this.timelineTitle(shippingStatus, "AHAMOVE"),
          description: trackingUrl ? `Tracking: ${trackingUrl}` : null,
          raw,
          source: force ? "manual_refresh" : "polling",
          ...driver,
        });
      },
        {
          maxWait: Number(process.env.PRISMA_TRANSACTION_MAX_WAIT_MS || 10000),
          timeout: Number(process.env.PRISMA_TRANSACTION_TIMEOUT_MS || 30000),
        },
      );

      const orderForSync = await this.prisma.order.findUnique({
        where: { id: shipment.orderId },
        select: { paymentStatus: true },
      });
      const orderSyncData = this.buildAhamoveOrderSyncData(shippingStatus, {
        codAmount: Number(shipment.codAmount || 0),
        paymentStatus: orderForSync?.paymentStatus,
        codReconciliationStatus: (shipment as any).codReconciliationStatus,
      });
      if (Object.keys(orderSyncData).length > 0) {
        await this.prisma.order.update({
          where: { id: shipment.orderId },
          data: orderSyncData as any,
        });
      }

      const timeline = await (this.prisma as any).shipmentTimelineEvent.findMany({
        where: { shipmentId: shipment.id },
        orderBy: { eventTime: "desc" },
        take: 50,
      });

      return {
        source: "ahamove_live",
        cached: false,
        fetchedAt: now,
        expiresAt,
        shipment: {
          id: shipment.id,
          trackingCode: ahamoveOrderId,
          carrier: "AHAMOVE",
          shippingStatus,
          partnerStatus: ahamoveStatus,
          ahamoveTrackingUrl: trackingUrl,
          driver,
        },
        tracking: {
          carrier: "AHAMOVE",
          trackingCode: ahamoveOrderId,
          shippingStatus,
          partnerStatus: ahamoveStatus,
          codAmount: Number(shipment.codAmount || raw?.cod || raw?.data?.cod || 0),
          shippingFee: Number(
            shipment.shippingFee ||
            raw?.total_fee ||
            raw?.totalFee ||
            raw?.data?.total_fee ||
            raw?.data?.totalFee ||
            0
          ),
          updatedAt:
            raw?.updated_at ||
            raw?.updatedAt ||
            raw?.data?.updated_at ||
            raw?.data?.updatedAt ||
            now,
          from: {
            name: shipment.fromName || raw?.path?.[0]?.name || "",
            phone: shipment.fromPhone || raw?.path?.[0]?.mobile || "",
            address: shipment.fromAddress || raw?.path?.[0]?.address || "",
          },
          to: {
            name: shipment.toName || raw?.path?.[1]?.name || "",
            phone: shipment.toPhone || raw?.path?.[1]?.mobile || "",
            address: shipment.toAddress || raw?.path?.[1]?.address || "",
          },
          timeline: this.mapTimelineEventsForTracking(timeline),
          trackingUrl,
          driver,
          raw,
        },
        timeline,
      };
    }

    const raw = await (this.ghnClient as any).getOrderDetailWithPublicTracking
      ? await (this.ghnClient as any).getOrderDetailWithPublicTracking(shipment.trackingCode || "")
      : await this.ghnClient.getOrderDetail(shipment.trackingCode || "");
    const normalized = this.normalizeTracking(raw, shipment);

    const expiresAt = new Date(
      now.getTime() + this.trackingCacheMinutes * 60 * 1000
    );

    await this.prisma.$transaction(
        async (tx) => {
      await tx.shipmentTrackingCache.create({
        data: {
          shipmentId: shipment.id,
          carrier: shipment.carrier,
          trackingCode: shipment.trackingCode || "",
          payloadJson: raw,
          normalizedJson: normalized,
          fetchedAt: now,
          expiresAt,
        },
      });

      await tx.shipment.update({
        where: { id: shipment.id },
        data: {
          shippingStatus: normalized.shippingStatus,
          partnerStatus: normalized.partnerStatus,
          codAmount: normalized.codAmount,
          shippingFee: normalized.shippingFee,
          fromName: normalized.from.name || null,
          fromPhone: normalized.from.phone || null,
          fromAddress: normalized.from.address || null,
          toName: normalized.to.name || null,
          toPhone: normalized.to.phone || null,
          toAddress: normalized.to.address || null,
          lastSyncedAt: now,
          metadata: this.buildShipmentMetadata(raw, shipment, normalized.shippingStatus),
        },
      });

      await this.appendShipmentTimelineEvent(tx, {
        shipmentId: shipment.id,
        orderId: shipment.orderId,
        carrier: shipment.carrier,
        trackingCode: shipment.trackingCode || "",
        status: normalized.shippingStatus,
        partnerStatus: normalized.partnerStatus,
        title: this.timelineTitle(normalized.shippingStatus, shipment.carrier),
        description:
          normalized.timeline?.[0]?.description ||
          normalized.timeline?.[0]?.location ||
          null,
        raw,
        source: force ? "manual_refresh" : "polling",
      });

      await this.appendCarrierTimelineEvents(tx, {
        shipmentId: shipment.id,
        orderId: shipment.orderId,
        carrier: shipment.carrier,
        trackingCode: shipment.trackingCode || "",
        timeline: normalized.timeline,
        source: force ? "ghn_manual_refresh" : "ghn_polling",
      });

      const orderForSync = await tx.order.findUnique({
        where: { id: shipment.orderId },
        select: { paymentStatus: true },
      });
      const orderSyncData = this.buildCarrierOrderSyncData(normalized.shippingStatus, {
        codAmount: normalized.codAmount ?? shipment.codAmount,
        paymentStatus: orderForSync?.paymentStatus,
        codReconciliationStatus: (shipment as any).codReconciliationStatus,
      });
      if (Object.keys(orderSyncData).length > 0) {
        await tx.order.update({
          where: { id: shipment.orderId },
          data: orderSyncData as any,
        });
      }
    },
        {
          maxWait: Number(process.env.PRISMA_TRANSACTION_MAX_WAIT_MS || 10000),
          timeout: Number(process.env.PRISMA_TRANSACTION_TIMEOUT_MS || 30000),
        },
      );

    const timeline = await (this.prisma as any).shipmentTimelineEvent.findMany({
      where: { shipmentId: shipment.id },
      orderBy: { eventTime: "desc" },
      take: 50,
    });

    return {
      source: "ghn_live",
      cached: false,
      fetchedAt: now,
      expiresAt,
      shipment: {
        id: shipment.id,
        trackingCode: shipment.trackingCode,
        carrier: shipment.carrier,
        shippingStatus: normalized.shippingStatus,
        partnerStatus: normalized.partnerStatus,
      },
      tracking: normalized,
      timeline,
    };
  }

  async getGhnTrackingByCode(trackingCode: string) {
    const code = String(trackingCode || "").trim();

    if (!code) {
      throw new BadRequestException("Thiếu mã vận đơn GHN");
    }

    const raw = await ((this.ghnClient as any).getOrderDetailWithPublicTracking
      ? (this.ghnClient as any).getOrderDetailWithPublicTracking(code)
      : this.ghnClient.getOrderDetail(code));

    const syntheticShipment = {
      id: `external-${code}`,
      orderId: null,
      carrier: "GHN",
      trackingCode: code,
      shippingStatus: raw?.status || raw?.status_name || raw?.current_status || "UNKNOWN",
      partnerStatus: raw?.status_name || raw?.current_status || raw?.status || null,
      codAmount: raw?.cod_amount || 0,
      shippingFee: raw?.total_fee || 0,
      fromName: raw?.from_name || "",
      fromPhone: raw?.from_phone || "",
      fromAddress: raw?.from_address || "",
      toName: raw?.to_name || "",
      toPhone: raw?.to_phone || "",
      toAddress: raw?.to_address || "",
      updatedAt: raw?.updated_date || raw?.updated_at || new Date(),
    };

    const normalized = this.normalizeTracking(raw, syntheticShipment);

    return {
      source: "ghn_live_by_code",
      cached: false,
      shipment: {
        id: syntheticShipment.id,
        trackingCode: code,
        carrier: "GHN",
        shippingStatus: normalized.shippingStatus,
        partnerStatus: normalized.partnerStatus,
      },
      tracking: normalized,
      timeline: normalized.timeline || [],
      raw,
    };
  }



  private buildShipmentMetadata(raw: any, shipment: any, shippingStatus?: string | null) {
    const previous = (shipment as any)?.metadata;
    const previousMeta = previous && typeof previous === "object" && !Array.isArray(previous) ? previous : {};
    const rawMeta = raw && typeof raw === "object" && !Array.isArray(raw) ? raw : { raw };

    const previousReturnReceiveStatus = String(
      previousMeta.returnReceiveStatus ||
        previousMeta.return_receive_status ||
        "",
    ).toUpperCase();

    const previousReceivedAt = previousMeta.returnReceivedAt || previousMeta.return_received_at || null;
    const previousReceivedById = previousMeta.returnReceivedById || previousMeta.return_received_by_id || null;
    const previousReceivedByName = previousMeta.returnReceivedByName || previousMeta.return_received_by_name || null;

    const status = String(shippingStatus || "").toUpperCase();

    if (status === "RETURNED") {
      return {
        ...rawMeta,
        returnReceiveStatus: previousReturnReceiveStatus === "RECEIVED" ? "RECEIVED" : "WAITING_CONFIRM",
        returnReceivedAt: previousReceivedAt,
        returnReceivedById: previousReceivedById,
        returnReceivedByName: previousReceivedByName,
      };
    }

    if (status === "RETURNING") {
      return {
        ...rawMeta,
        returnReceiveStatus: previousReturnReceiveStatus === "RECEIVED" ? "RECEIVED" : "RETURNING",
        returnReceivedAt: previousReceivedAt,
        returnReceivedById: previousReceivedById,
        returnReceivedByName: previousReceivedByName,
      };
    }

    return {
      ...rawMeta,
      ...(previousReturnReceiveStatus ? { returnReceiveStatus: previousReturnReceiveStatus } : {}),
      ...(previousReceivedAt ? { returnReceivedAt: previousReceivedAt } : {}),
      ...(previousReceivedById ? { returnReceivedById: previousReceivedById } : {}),
      ...(previousReceivedByName ? { returnReceivedByName: previousReceivedByName } : {}),
    };
  }

  private getReturnReceiveStatusFromShipment(shipment?: any) {
    const status = String(
      shipment?.metadata?.returnReceiveStatus ||
        shipment?.metadata?.return_receive_status ||
        "",
    ).toUpperCase();

    if (status) return status;

    const shippingStatus = String(shipment?.shippingStatus || "").toUpperCase();
    if (shippingStatus === "RETURNED") return "WAITING_CONFIRM";
    if (shippingStatus === "RETURNING") return "RETURNING";
    return "";
  }

  private getReturnReceiveLabel(status?: string | null) {
    const value = String(status || "").toUpperCase();
    if (value === "RECEIVED") return "Đã nhận hàng hoàn";
    if (value === "WAITING_CONFIRM") return "Chờ xác nhận hàng hoàn";
    if (value === "RETURNING") return "Đang hoàn hàng";
    return "";
  }

  private getSafeShipmentMetadata(shipment?: any) {
    const metadata = shipment?.metadata;
    return metadata && typeof metadata === "object" && !Array.isArray(metadata) ? metadata : {};
  }

  private isGhnSyncIgnoredShipment(shipment?: any) {
    const metadata = this.getSafeShipmentMetadata(shipment);
    return metadata.ghnSyncIgnored === true || metadata.ghnOrderNotFound === true;
  }

  private isGhnOrderNotFoundError(error: any) {
    const raw = [
      error?.message,
      error?.response?.message,
      error?.response?.data?.message,
      error?.cause?.message,
      typeof error === "string" ? error : "",
    ]
      .filter(Boolean)
      .join(" | ");

    const normalized = this.normalizeCarrierStatusText(raw);

    return (
      normalized.includes("don hang khong ton tai") ||
      normalized.includes("order not found") ||
      normalized.includes("corev2 tenant order detail")
    );
  }

  private async markGhnSyncIgnored(shipment: any, reason: string) {
    const currentMetadata = this.getSafeShipmentMetadata(shipment);
    const ignoredAt = new Date().toISOString();

    try {
      await this.prisma.shipment.update({
        where: { id: shipment.id },
        data: {
          metadata: {
            ...currentMetadata,
            ghnSyncIgnored: true,
            ghnSyncIgnoredReason: reason,
            ghnSyncIgnoredAt: ignoredAt,
          } as any,
          lastSyncedAt: new Date(),
        } as any,
      });
    } catch (markError) {
      this.logger.warn(
        `[GHN_SYNC_CRON] cannot mark ignored shipment=${shipment?.id || "unknown"} code=${shipment?.trackingCode || ""}: ${
          markError instanceof Error ? markError.message : String(markError)
        }`,
      );
    }
  }

  private async applyGhnTrackingPreviewToShipment(
    shipment: any,
    preview: { raw: any; normalized: any; orderSyncData?: any },
    source: string,
  ) {
    const now = new Date();
    const raw = preview.raw;
    const normalized = preview.normalized;
    const expiresAt = new Date(now.getTime() + this.trackingCacheMinutes * 60 * 1000);

    await this.prisma.$transaction(
      async (tx) => {
        await tx.shipmentTrackingCache.create({
          data: {
            shipmentId: shipment.id,
            carrier: shipment.carrier,
            trackingCode: shipment.trackingCode || "",
            payloadJson: raw,
            normalizedJson: normalized,
            fetchedAt: now,
            expiresAt,
          },
        });

        await tx.shipment.update({
          where: { id: shipment.id },
          data: {
            shippingStatus: normalized.shippingStatus,
            partnerStatus: normalized.partnerStatus,
            codAmount: normalized.codAmount,
            shippingFee: normalized.shippingFee,
            fromName: normalized.from.name || null,
            fromPhone: normalized.from.phone || null,
            fromAddress: normalized.from.address || null,
            toName: normalized.to.name || null,
            toPhone: normalized.to.phone || null,
            toAddress: normalized.to.address || null,
            lastSyncedAt: now,
            metadata: this.buildShipmentMetadata(raw, shipment, normalized.shippingStatus),
          },
        });

        await this.appendShipmentTimelineEvent(tx, {
          shipmentId: shipment.id,
          orderId: shipment.orderId,
          carrier: shipment.carrier,
          trackingCode: shipment.trackingCode || "",
          status: normalized.shippingStatus,
          partnerStatus: normalized.partnerStatus,
          title: this.timelineTitle(normalized.shippingStatus, shipment.carrier),
          description:
            normalized.timeline?.[0]?.description ||
            normalized.timeline?.[0]?.location ||
            null,
          raw,
          source,
        });

        await this.appendCarrierTimelineEvents(tx, {
          shipmentId: shipment.id,
          orderId: shipment.orderId,
          carrier: shipment.carrier,
          trackingCode: shipment.trackingCode || "",
          timeline: normalized.timeline,
          source,
        });

        const orderSyncData = (preview.orderSyncData || {}) as any;
        if (Object.keys(orderSyncData).length > 0) {
          await tx.order.update({
            where: { id: shipment.orderId },
            data: orderSyncData as any,
          });
        }
      },
      {
        maxWait: Number(process.env.PRISMA_TRANSACTION_MAX_WAIT_MS || 10000),
        timeout: Number(process.env.PRISMA_TRANSACTION_TIMEOUT_MS || 30000),
      },
    );

    return this.prisma.shipment.findUnique({
      where: { id: shipment.id },
      select: {
        shippingStatus: true,
        partnerStatus: true,
        metadata: true,
        order: { select: { status: true, fulfillmentStatus: true, paymentStatus: true } },
      },
    });
  }


  async confirmGhnReturnReceivedByOrderId(orderId: string, user?: any) {
    const order = await this.prisma.order.findUnique({
      where: { id: orderId },
      include: { shipment: true },
    });

    if (!order?.shipment) {
      throw new BadRequestException("Đơn chưa có vận đơn để xác nhận hàng hoàn.");
    }

    const shipment = order.shipment as any;
    const carrier = String(shipment.carrier || "").toUpperCase();
    if (!carrier.includes("GHN")) {
      throw new BadRequestException("Chỉ hỗ trợ xác nhận hàng hoàn cho vận đơn GHN ở bước này.");
    }

    const shippingStatus = String(shipment.shippingStatus || "").toUpperCase();
    const partnerStatus = String(shipment.partnerStatus || "").toUpperCase();
    const statusText = `${shippingStatus} ${partnerStatus}`;

    if (!statusText.includes("RETURNED") && !statusText.includes("DA HOAN") && !statusText.includes("ĐÃ HOÀN")) {
      throw new BadRequestException("GHN chưa báo đơn đã hoàn hàng, chưa thể xác nhận đã nhận hoàn.");
    }

    const branchId = String((order as any).branchId || "").trim();
    if (!branchId) {
      throw new BadRequestException("Đơn chưa có chi nhánh, không thể nhập lại tồn hàng hoàn.");
    }

    const actorId = user?.id || user?.sub || null;
    const actorName = user?.name || user?.fullName || user?.username || user?.email || "system";
    const receivedAt = new Date();
    const currentMetadata = shipment.metadata && typeof shipment.metadata === "object" && !Array.isArray(shipment.metadata)
      ? shipment.metadata
      : {};

    const nextMetadata = {
      ...currentMetadata,
      returnReceiveStatus: "RECEIVED",
      returnReceivedAt: currentMetadata.returnReceivedAt || receivedAt.toISOString(),
      returnReceivedById: currentMetadata.returnReceivedById || actorId,
      returnReceivedByName: currentMetadata.returnReceivedByName || actorName,
    };

    const result = await this.prisma.$transaction(
      async (tx) => {
        const orderWithItems = await tx.order.findUnique({
          where: { id: order.id },
          include: { items: true, shipment: true },
        });

        if (!orderWithItems?.shipment) {
          throw new BadRequestException("Đơn chưa có vận đơn để xác nhận hàng hoàn.");
        }

        const lockedShipment = orderWithItems.shipment as any;
        const lockedMetadata = lockedShipment.metadata && typeof lockedShipment.metadata === "object" && !Array.isArray(lockedShipment.metadata)
          ? lockedShipment.metadata
          : {};

        const lockedNextMetadata = {
          ...lockedMetadata,
          returnReceiveStatus: "RECEIVED",
          returnReceivedAt: lockedMetadata.returnReceivedAt || receivedAt.toISOString(),
          returnReceivedById: lockedMetadata.returnReceivedById || actorId,
          returnReceivedByName: lockedMetadata.returnReceivedByName || actorName,
        };

        const existingReturnMovements = await (tx as any).inventoryMovement.findMany({
          where: {
            refType: "GHN_RETURN_RECEIVED",
            refId: order.id,
          },
          select: { id: true },
          take: 1,
        });

        const inventoryAlreadyApplied = existingReturnMovements.length > 0;
        const restoredItems: Array<{ sku: string; qty: number; beforeQty: number; afterQty: number }> = [];

        if (!inventoryAlreadyApplied) {
          const items = Array.isArray(orderWithItems.items) ? orderWithItems.items : [];
          if (!items.length) {
            throw new BadRequestException("Đơn không có sản phẩm để nhập lại tồn hàng hoàn.");
          }

          for (const item of items as any[]) {
            const qty = Math.max(0, Math.trunc(Number(item?.qty || item?.quantity || 0)));
            if (!qty) continue;

            let variantId = String(item?.variantId || "").trim();
            if (!variantId && item?.sku) {
              const variant = await (tx as any).productVariant.findFirst({
                where: { sku: String(item.sku).trim() },
                select: { id: true },
              });
              variantId = String(variant?.id || "").trim();
            }

            if (!variantId) {
              throw new BadRequestException(`Không tìm thấy variant cho SKU ${item?.sku || item?.productName || item?.id}.`);
            }

            const inventoryItem = await (tx as any).inventoryItem.findUnique({
              where: {
                variantId_branchId: {
                  variantId,
                  branchId,
                },
              },
            });

            const beforeQty = Number(inventoryItem?.availableQty || 0);
            const afterQty = beforeQty + qty;

            if (inventoryItem) {
              await (tx as any).inventoryItem.update({
                where: { id: inventoryItem.id },
                data: { availableQty: afterQty },
              });
            } else {
              await (tx as any).inventoryItem.create({
                data: {
                  variantId,
                  branchId,
                  availableQty: afterQty,
                  reservedQty: 0,
                  incomingQty: 0,
                },
              });
            }

            await (tx as any).inventoryMovement.create({
              data: {
                variantId,
                branchId,
                type: "RETURN",
                qty,
                beforeQty,
                afterQty,
                note: `Nhập lại hàng hoàn GHN từ đơn ${order.orderCode}${shipment.trackingCode ? ` - MVD ${shipment.trackingCode}` : ""}`,
                refType: "GHN_RETURN_RECEIVED",
                refId: order.id,
                createdById: actorId,
                createdAt: receivedAt,
              },
            });

            restoredItems.push({
              sku: String(item?.sku || ""),
              qty,
              beforeQty,
              afterQty,
            });
          }
        }

        const [updatedShipment, updatedOrder] = await Promise.all([
          tx.shipment.update({
            where: { id: lockedShipment.id },
            data: { metadata: lockedNextMetadata as any },
          }),
          tx.order.update({
            where: { id: orderWithItems.id },
            data: { fulfillmentStatus: "RETURNED" as any },
          }),
        ]);

        await this.appendShipmentTimelineEvent(tx, {
          shipmentId: lockedShipment.id,
          orderId: orderWithItems.id,
          carrier: lockedShipment.carrier,
          trackingCode: lockedShipment.trackingCode || "",
          status: "RETURNED",
          partnerStatus: lockedShipment.partnerStatus || null,
          title: inventoryAlreadyApplied ? "Đã xác nhận nhận hàng hoàn" : "Đã xác nhận nhận hàng hoàn và nhập lại kho",
          description: inventoryAlreadyApplied
            ? `Nhân viên ${actorName} đã xác nhận shop nhận hàng hoàn. Kho đã được ghi nhận trước đó, không cộng lại lần 2.`
            : `Nhân viên ${actorName} đã xác nhận shop nhận hàng hoàn và nhập lại ${restoredItems.reduce((sum, item) => sum + item.qty, 0)} sản phẩm vào kho.`,
          raw: {
            ...lockedNextMetadata,
            inventoryAlreadyApplied,
            restoredItems,
          },
          source: "return_receive_confirm",
          eventTime: receivedAt,
        });

        return {
          updatedShipment,
          updatedOrder,
          inventoryAlreadyApplied,
          restoredItems,
        };
      },
      {
        maxWait: Number(process.env.PRISMA_TRANSACTION_MAX_WAIT_MS || 10000),
        timeout: Number(process.env.PRISMA_TRANSACTION_TIMEOUT_MS || 30000),
      },
    );

    const totalRestoredQty = result.restoredItems.reduce((sum, item) => sum + item.qty, 0);

    return {
      ok: true,
      message: result.inventoryAlreadyApplied
        ? "Đã xác nhận shop nhận hàng hoàn. Kho đã được ghi nhận trước đó, không cộng lại lần 2."
        : `Đã xác nhận shop nhận hàng hoàn và nhập lại ${totalRestoredQty} sản phẩm vào kho.`,
      returnReceiveStatus: "RECEIVED",
      returnReceiveLabel: "Đã nhận hàng hoàn",
      returnReceivedAt: nextMetadata.returnReceivedAt,
      returnReceivedByName: nextMetadata.returnReceivedByName,
      inventoryRestored: !result.inventoryAlreadyApplied,
      restoredItems: result.restoredItems,
      shipment: result.updatedShipment,
      order: result.updatedOrder,
    };
  }


  private async previewGhnTrackingForShipment(shipment: any) {
    const raw = await ((this.ghnClient as any).getOrderDetailWithPublicTracking
      ? (this.ghnClient as any).getOrderDetailWithPublicTracking(shipment.trackingCode || "")
      : this.ghnClient.getOrderDetail(shipment.trackingCode || ""));

    const normalized = this.normalizeTracking(raw, shipment);

    const orderSyncData = this.buildCarrierOrderSyncData(normalized.shippingStatus, {
      codAmount: normalized.codAmount ?? shipment.codAmount,
      paymentStatus: shipment.order?.paymentStatus,
      codReconciliationStatus: (shipment as any).codReconciliationStatus,
    });

    return { raw, normalized, orderSyncData };
  }

  async refreshGhnTrackingBackfill(options?: {
    days?: number;
    limit?: number;
    includeFinal?: boolean;
    dryRun?: boolean;
    onlyDelivered?: boolean;
    source?: string;
    preferOldest?: boolean;
  }) {
    const startedAt = Date.now();
    const days = Math.min(Math.max(Number(options?.days || 90), 1), 3650);
    const limit = Math.min(Math.max(Number(options?.limit || 5000), 1), 10000);
    const includeFinal = options?.includeFinal === true;
    const dryRun = options?.dryRun === true;
    const onlyDelivered = options?.onlyDelivered !== false;
    const source = String(options?.source || "manual_refresh_all");
    const preferOldest = options?.preferOldest === true;
    const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

    const shipments = await this.prisma.shipment.findMany({
      where: {
        carrier: { contains: "GHN", mode: "insensitive" },
        trackingCode: { not: null },
        order: { createdAt: { gte: since } },
        ...(includeFinal
          ? {}
          : {
              NOT: [
                { shippingStatus: "DELIVERED" },
                { shippingStatus: "CANCELLED" },
              ],
              order: {
                createdAt: { gte: since },
                NOT: [
                  { status: "COMPLETED" },
                  { status: "CANCELLED" },
                ],
              },
            }),
      } as any,
      select: {
        id: true,
        orderId: true,
        trackingCode: true,
        shippingStatus: true,
        partnerStatus: true,
        codAmount: true,
        codReconciliationStatus: true,
        metadata: true,
        fromName: true,
        fromPhone: true,
        fromAddress: true,
        toName: true,
        toPhone: true,
        toAddress: true,
        updatedAt: true,
        order: {
          select: {
            id: true,
            orderCode: true,
            status: true,
            fulfillmentStatus: true,
            paymentStatus: true,
            createdAt: true,
            shippingAddressLine1: true,
          },
        },
      },
      orderBy: preferOldest ? { updatedAt: "asc" } : { updatedAt: "desc" },
      take: limit,
    });

    const skippedIgnored = shipments.filter(
      (shipment) => String(shipment.trackingCode || "").trim() && this.isGhnSyncIgnoredShipment(shipment),
    ).length;
    const targets = shipments.filter(
      (shipment) => String(shipment.trackingCode || "").trim() && !this.isGhnSyncIgnoredShipment(shipment),
    );

    let refreshed = 0;
    let unchanged = 0;
    let shipmentStatusChangedCount = 0;
    let orderStatusChangedCount = 0;
    let paymentStatusChangedCount = 0;
    let skippedNotDelivered = 0;
    let skippedGhnIgnored = skippedIgnored;
    let skippedGhnNotFound = 0;

    const failed: Array<{ orderId: string | null; orderCode: string | null; trackingCode: string | null; reason: string }> = [];
    const changed: Array<{
      orderId: string | null;
      orderCode: string | null;
      trackingCode: string | null;
      beforeShipmentStatus: string | null;
      afterShipmentStatus: string | null;
      beforeOrderStatus: string | null;
      afterOrderStatus: string | null;
      beforeFulfillmentStatus?: string | null;
      afterFulfillmentStatus?: string | null;
      beforePaymentStatus?: string | null;
      afterPaymentStatus?: string | null;
      partnerStatus?: string | null;
      returnReceiveStatus?: string | null;
      dryRun?: boolean;
      applied?: boolean;
      skippedReason?: string | null;
    }> = [];

    for (const shipment of targets) {
      const beforeShipmentStatus = String(shipment.shippingStatus || "");
      const beforePartnerStatus = String(shipment.partnerStatus || "");
      const beforeOrderStatus = String(shipment.order?.status || "");
      const beforeFulfillmentStatus = String(shipment.order?.fulfillmentStatus || "");
      const beforePaymentStatus = String(shipment.order?.paymentStatus || "");

      try {
        const preview = await this.previewGhnTrackingForShipment(shipment);
        const predictedShipmentStatus = String(preview.normalized?.shippingStatus || "");
        const predictedPartnerStatus = String(preview.normalized?.partnerStatus || "");
        const orderSyncData = (preview.orderSyncData || {}) as any;
        const predictedOrderStatus = String(orderSyncData.status || beforeOrderStatus);
        const predictedFulfillmentStatus = String(orderSyncData.fulfillmentStatus || beforeFulfillmentStatus);
        const predictedPaymentStatus = String(orderSyncData.paymentStatus || beforePaymentStatus);
        const predictedReturnReceiveStatus = predictedShipmentStatus === "RETURNED"
          ? this.getReturnReceiveStatusFromShipment({ ...shipment, shippingStatus: predictedShipmentStatus }) || "WAITING_CONFIRM"
          : predictedShipmentStatus === "RETURNING"
            ? "RETURNING"
            : this.getReturnReceiveStatusFromShipment(shipment);

        const canApply = !onlyDelivered || predictedShipmentStatus === "DELIVERED";

        let afterShipmentStatus = predictedShipmentStatus;
        let afterPartnerStatus = predictedPartnerStatus;
        let afterOrderStatus = predictedOrderStatus;
        let afterFulfillmentStatus = predictedFulfillmentStatus;
        let afterPaymentStatus = predictedPaymentStatus;
        let applied = false;

        if (!dryRun && canApply) {
          // FIX: dùng lại write path cũ đã ổn định của getShipmentTracking().
          // Bản safe cron trước đó dùng applyGhnTrackingPreviewToShipment()
          // làm dry-run vẫn báo sẽ sửa đơn, nhưng write thật fail ở nhóm DELIVERED.
          // getShipmentTracking(force=true) sẽ tự fetch GHN, ghi cache, shipment,
          // timeline và order status theo flow cũ đang chạy ổn định.
          const result: any = await this.getShipmentTracking(shipment.id, true);
          const afterRow = await this.prisma.shipment.findUnique({
            where: { id: shipment.id },
            select: {
              shippingStatus: true,
              partnerStatus: true,
              metadata: true,
              order: { select: { status: true, fulfillmentStatus: true, paymentStatus: true } },
            },
          });

          afterShipmentStatus = String(afterRow?.shippingStatus || result?.shipment?.shippingStatus || "");
          afterPartnerStatus = String(afterRow?.partnerStatus || result?.shipment?.partnerStatus || "");
          afterOrderStatus = String(afterRow?.order?.status || "");
          afterFulfillmentStatus = String(afterRow?.order?.fulfillmentStatus || "");
          afterPaymentStatus = String(afterRow?.order?.paymentStatus || "");
          applied = true;
        } else if (!canApply) {
          skippedNotDelivered += 1;
        }

        refreshed += 1;

        const shipmentChanged = beforeShipmentStatus !== afterShipmentStatus || beforePartnerStatus !== afterPartnerStatus;
        const orderChanged = beforeOrderStatus !== afterOrderStatus || beforeFulfillmentStatus !== afterFulfillmentStatus;
        const paymentChanged = beforePaymentStatus !== afterPaymentStatus;

        if (shipmentChanged) shipmentStatusChangedCount += 1;
        if (orderChanged) orderStatusChangedCount += 1;
        if (paymentChanged) paymentStatusChangedCount += 1;

        if (shipmentChanged || orderChanged || paymentChanged || !canApply) {
          changed.push({
            orderId: shipment.orderId,
            orderCode: shipment.order?.orderCode || null,
            trackingCode: shipment.trackingCode || null,
            beforeShipmentStatus: beforeShipmentStatus || null,
            afterShipmentStatus: afterShipmentStatus || null,
            beforeOrderStatus: beforeOrderStatus || null,
            afterOrderStatus: afterOrderStatus || null,
            beforeFulfillmentStatus: beforeFulfillmentStatus || null,
            afterFulfillmentStatus: afterFulfillmentStatus || null,
            beforePaymentStatus: beforePaymentStatus || null,
            afterPaymentStatus: afterPaymentStatus || null,
            partnerStatus: afterPartnerStatus || null,
            returnReceiveStatus: predictedReturnReceiveStatus || null,
            dryRun,
            applied,
            skippedReason: canApply ? null : "Bỏ qua vì refresh toàn bộ mặc định chỉ tự cập nhật đơn GHN đã DELIVERED.",
          });
        } else {
          unchanged += 1;
        }
      } catch (error) {
        const reason = error instanceof Error ? error.message : String(error);

        if (this.isGhnOrderNotFoundError(error)) {
          skippedGhnNotFound += 1;
          skippedGhnIgnored += 1;

          if (!dryRun) {
            await this.markGhnSyncIgnored(shipment, "GHN_ORDER_NOT_FOUND");
          }

          changed.push({
            orderId: shipment.orderId,
            orderCode: shipment.order?.orderCode || null,
            trackingCode: shipment.trackingCode || null,
            beforeShipmentStatus: beforeShipmentStatus || null,
            afterShipmentStatus: beforeShipmentStatus || null,
            beforeOrderStatus: beforeOrderStatus || null,
            afterOrderStatus: beforeOrderStatus || null,
            beforeFulfillmentStatus: beforeFulfillmentStatus || null,
            afterFulfillmentStatus: beforeFulfillmentStatus || null,
            beforePaymentStatus: beforePaymentStatus || null,
            afterPaymentStatus: beforePaymentStatus || null,
            partnerStatus: beforePartnerStatus || null,
            dryRun,
            applied: false,
            skippedReason: "GHN báo đơn không tồn tại; đã đánh dấu bỏ qua để cron không gọi lại.",
          });
          continue;
        }

        failed.push({
          orderId: shipment.orderId,
          orderCode: shipment.order?.orderCode || null,
          trackingCode: shipment.trackingCode || null,
          reason,
        });
      }
    }

    const elapsedSeconds = Number(((Date.now() - startedAt) / 1000).toFixed(1));
    const progressPercent = targets.length
      ? Number((((refreshed + failed.length) / targets.length) * 100).toFixed(1))
      : 100;

    return {
      ok: failed.length === 0,
      dryRun,
      onlyDelivered,
      source,
      preferOldest,
      rangeDays: days,
      scanned: shipments.length,
      total: targets.length,
      targets: targets.length,
      success: refreshed,
      refreshed,
      unchanged,
      skippedNotDelivered,
      skippedGhnIgnored,
      skippedGhnNotFound,
      corrected: orderStatusChangedCount,
      correctedOrderStatus: orderStatusChangedCount,
      shipmentStatusChanged: shipmentStatusChangedCount,
      paymentStatusChanged: paymentStatusChangedCount,
      changedCount: changed.length,
      failed: failed.length,
      failedCount: failed.length,
      progressPercent,
      elapsedSeconds,
      changed: changed.slice(0, 200),
      failedItems: failed.slice(0, 200),
      message: dryRun
        ? `GHN dry-run xong: kiểm tra ${refreshed}/${targets.length} vận đơn (${progressPercent}%). Sẽ sửa trạng thái đơn ${orderStatusChangedCount}, đổi trạng thái vận đơn ${shipmentStatusChangedCount}, đổi thanh toán ${paymentStatusChangedCount}, bỏ qua chưa delivered ${skippedNotDelivered}, bỏ qua GHN lỗi/đã chặn ${skippedGhnIgnored}, lỗi ${failed.length}. Chưa ghi DB.`
        : `GHN chạy xong: ${refreshed}/${targets.length} vận đơn (${progressPercent}%). Đúng trạng thái ${unchanged}, sửa trạng thái đơn ${orderStatusChangedCount}, đổi trạng thái vận đơn ${shipmentStatusChangedCount}, đổi thanh toán ${paymentStatusChangedCount}, bỏ qua chưa delivered ${skippedNotDelivered}, bỏ qua GHN lỗi/đã chặn ${skippedGhnIgnored}, lỗi ${failed.length}. Thời gian chuẩn hoá ${elapsedSeconds}s.`,
    };
  }


  async getShipmentTrackingByOrder(orderId: string, force = false) {
    const shipment = await this.prisma.shipment.findUnique({
      where: { orderId },
    });

    if (!shipment) {
      throw new BadRequestException("Đơn chưa có vận đơn");
    }

    return this.getShipmentTracking(shipment.id, force);
  }

  async getShipmentTimelineByOrder(orderId: string) {
    const shipment = await this.prisma.shipment.findUnique({
      where: { orderId },
    });

    if (!shipment) {
      return {
        shipment: null,
        timeline: [],
      };
    }

    const timeline = await (this.prisma as any).shipmentTimelineEvent.findMany({
      where: { shipmentId: shipment.id },
      orderBy: { eventTime: "desc" },
      take: 50,
    });

    return {
      shipment: {
        id: shipment.id,
        carrier: shipment.carrier,
        trackingCode: shipment.trackingCode,
        shippingStatus: shipment.shippingStatus,
        partnerStatus: shipment.partnerStatus,
        ahamoveTrackingUrl: shipment.ahamoveTrackingUrl,
      },
      timeline,
    };
  }


  async quote(dto: QuoteShipmentDto) {
    const fromDistrictId = Number((dto as any).fromDistrictId || this.fromDistrictId || 0);
    const fromWardCode = String((dto as any).fromWardCode || this.fromWardCode || "");

    if (!fromDistrictId) {
      throw new BadRequestException("Thiếu GHN_FROM_DISTRICT_ID");
    }

    if (!fromWardCode) {
      throw new BadRequestException("Thiếu GHN_FROM_WARD_CODE");
    }

    const services = await this.ghnClient.getAvailableServices(
      fromDistrictId,
      dto.toDistrictId
    );

    if (!Array.isArray(services) || services.length === 0) {
      throw new BadRequestException("GHN không trả về service khả dụng");
    }

    const quotes = await Promise.all(
      services.map(async (service: any) => {
        const serviceId = Number(service.service_id);
        const serviceTypeId = Number(service.service_type_id);

        const feePayload: any = {
          service_id: serviceId,
          service_type_id: serviceTypeId,
          insurance_value: dto.insuranceValue || 0,
          from_district_id: fromDistrictId,
          from_ward_code: fromWardCode,
          to_district_id: dto.toDistrictId,
          to_ward_code: dto.toWardCode,
          length: dto.length,
          width: dto.width,
          height: dto.height,
          weight: dto.weight,
        };

        if (serviceTypeId === 5) {
          feePayload.items =
            dto.items && dto.items.length > 0
              ? dto.items
              : [
                {
                  name: "Default item",
                  quantity: 1,
                  length: dto.length,
                  width: dto.width,
                  height: dto.height,
                  weight: dto.weight,
                },
              ];
        }

        const fee = await this.ghnClient.calculateFee(feePayload);

        const leadtime = await this.ghnClient.getLeadTime({
          service_id: serviceId,
          from_district_id: fromDistrictId,
          from_ward_code: fromWardCode,
          to_district_id: dto.toDistrictId,
          to_ward_code: dto.toWardCode,
        });

        return {
          serviceId,
          serviceTypeId,
          shortName: service.short_name,
          fee,
          leadtime,
        };
      })
    );

    return quotes;
  }

  async createGhnShipment(orderId: string, dto: CreateGhnShipmentDto) {
    return this.prisma.$transaction(async (tx) => {
      const order = await tx.order.findUnique({
        where: { id: orderId },
        include: { items: true, payments: true },
      });

      if (!order) {
        throw new BadRequestException("Không tìm thấy order");
      }

      const existingShipment = await tx.shipment.findFirst({
        where: { orderId },
      });

      if (existingShipment?.trackingCode) {
        return {
          duplicated: true,
          ghn: existingShipment.metadata || null,
          shipment: existingShipment,
        };
      }

      const fromDistrictId = Number((dto as any).fromDistrictId || this.fromDistrictId || 0);
      const fromWardCode = String((dto as any).fromWardCode || this.fromWardCode || "");
      const fromName = String((dto as any).fromName || this.returnName || "The 1970");
      const fromPhone = String((dto as any).fromPhone || this.returnPhone || "");
      const fromAddress = String((dto as any).fromAddress || this.returnAddress || "");

      if (!fromDistrictId || !fromWardCode || !fromPhone || !fromAddress) {
        throw new BadRequestException("Thiếu cấu hình GHN đầu gửi");
      }

      const services = await this.ghnClient.getAvailableServices(
        fromDistrictId,
        dto.toDistrictId
      );

      if (!services.length) {
        throw new BadRequestException("Không có service GHN");
      }

      const selected =
        services.find((s: any) => Number(s.service_type_id) === 2) ||
        services[0];

      const serviceId = Number(selected.service_id);
      const serviceTypeId = Number(selected.service_type_id);

      const paidAmount = (order.payments || []).reduce(
        (sum: number, payment: any) => sum + Number(payment.amount || 0),
        0
      );
      const remainingCodAmount = Math.max(
        0,
        Math.round(Number(order.finalAmount || 0) - paidAmount)
      );
      const requestedCodAmount = Math.max(
        0,
        Math.round(Number(dto.codAmount || 0))
      );
      const codAmount = Math.min(requestedCodAmount, remainingCodAmount);
      const requiredNote = this.normalizeGhnRequiredNote((dto as any).requiredNote);
      const shipmentNote = String(dto.note || "").trim();
      const created = await this.ghnClient.createOrder({
        payment_type_id: 1,
        note: shipmentNote,
        required_note: requiredNote,
        return_phone: fromPhone,
        return_address: fromAddress,
        return_district_id: fromDistrictId,
        return_ward_code: fromWardCode,
        client_order_code: dto.clientOrderCode,
        from_name: fromName,
        from_phone: fromPhone,
        from_address: fromAddress,
        from_district_id: fromDistrictId,
        from_ward_code: fromWardCode,
        to_name: dto.toName,
        to_phone: dto.toPhone,
        to_address: dto.toAddress,
        to_ward_code: dto.toWardCode,
        to_district_id: dto.toDistrictId,
        cod_amount: codAmount,
        content: `Đơn ${dto.clientOrderCode}`,
        weight: dto.weight,
        length: dto.length,
        width: dto.width,
        height: dto.height,
        service_id: serviceId,
        service_type_id: serviceTypeId,
        items: dto.items.map((i) => ({
          name: i.name,
          quantity: i.quantity,
          price: i.price,
          length: i.length,
          width: i.width,
          height: i.height,
          weight: i.weight,
          category: { level1: "Hàng hóa" },
        })),
      });

      const shipmentMetadata = {
        ...(created || {}),
        note: shipmentNote,
        required_note: requiredNote,
        requiredNote,
      };

      const shipment = await tx.shipment.upsert({
        where: { orderId },
        update: {
          carrier: "GHN",
          trackingCode: created.order_code,
          shippingStatus: this.mapShippingStatus(created.status || "CREATED"),
          partnerStatus: created.status || "CREATED",
          codAmount,
          shippingFee: created.total_fee ?? null,
          weight: dto.weight,
          fromName: this.returnName,
          fromPhone: this.returnPhone,
          fromAddress: this.returnAddress,
          toName: dto.toName,
          toPhone: dto.toPhone,
          toAddress: dto.toAddress,
          metadata: shipmentMetadata,
        },
        create: {
          orderId,
          carrier: "GHN",
          trackingCode: created.order_code,
          shippingStatus: this.mapShippingStatus(created.status || "CREATED"),
          partnerStatus: created.status || "CREATED",
          codAmount,
          shippingFee: created.total_fee ?? null,
          weight: dto.weight,
          fromName: this.returnName,
          fromPhone: this.returnPhone,
          fromAddress: this.returnAddress,
          toName: dto.toName,
          toPhone: dto.toPhone,
          toAddress: dto.toAddress,
          metadata: shipmentMetadata,
        },
      });

      await tx.order.update({
        where: { id: orderId },
        data: this.withPendingCodPayment(
          {
            fulfillmentStatus: "PROCESSING",
            status: "SHIPPED",
          },
          {
            codAmount,
            paymentStatus: order.paymentStatus,
          }
        ) as any,
      });

      return {
        duplicated: false,
        ghn: created,
        shipment,
      };
    });
  }

  async createShipmentFromOrder(orderId: string, user: any) {
    const order = await this.prisma.order.findUnique({
      where: { id: orderId },
      include: {
        items: true,
        shipment: true,
        payments: true,
      },
    });

    if (!order) {
      throw new BadRequestException("Không tìm thấy order");
    }

    if (order.shipment?.trackingCode) {
      throw new BadRequestException("Đơn đã có vận đơn GHN rồi");
    }

    if (!order.shippingRecipientName && !order.customerName) {
      throw new BadRequestException("Thiếu người nhận");
    }

    if (!order.shippingPhone && !order.customerPhone) {
      throw new BadRequestException("Thiếu số điện thoại giao hàng");
    }

    if (!order.shippingAddressLine1) {
      throw new BadRequestException("Thiếu địa chỉ giao hàng");
    }

    const toDistrictId = Number((order as any).shippingGhnDistrictId || 0);
    const toWardCode = String((order as any).shippingGhnWardCode || "");

    if (!toDistrictId || !toWardCode) {
      throw new BadRequestException(
        "Đơn chưa có mã GHN quận/huyện hoặc phường/xã"
      );
    }

    const weight = Math.max(200, Number((order as any).shippingWeight || 500));
    const length = Math.max(10, Number((order as any).shippingLength || 20));
    const width = Math.max(10, Number((order as any).shippingWidth || 20));
    const height = Math.max(1, Number((order as any).shippingHeight || 10));

    const itemCount = Math.max((order.items || []).length, 1);
    const itemWeight = Math.max(50, Math.floor(weight / itemCount));

    const paidAmount = (order.payments || []).reduce(
      (sum: number, payment: any) => sum + Number(payment.amount || 0),
      0
    );
    const remainingCodAmount = Math.max(
      0,
      Math.round(Number(order.finalAmount || 0) - paidAmount)
    );

    const dto: CreateGhnShipmentDto = {
      toName: order.shippingRecipientName || order.customerName || "",
      toPhone: order.shippingPhone || order.customerPhone || "",
      toAddress: [
        order.shippingAddressLine1,
        order.shippingAddressLine2,
        order.shippingWard,
        order.shippingDistrict,
        order.shippingProvince,
      ]
        .filter(Boolean)
        .join(", "),
      toDistrictId,
      toWardCode,
      codAmount:
        order.paymentStatus === "PAID" || order.paymentStatus === "REFUNDED"
          ? 0
          : remainingCodAmount,
      insuranceValue: Number(order.finalAmount || 0),
      note: "",
      clientOrderCode: order.orderCode,
      content: `Đơn ${order.orderCode}`,
      weight,
      length,
      width,
      height,
      items: (order.items || []).map((i: any) => ({
        name: i.productName || i.sku || "Sản phẩm",
        quantity: Number(i.qty || 0),
        price: Number(i.unitPrice || 0),
        length,
        width,
        height,
        weight: itemWeight,
      })),
    } as CreateGhnShipmentDto;

    return this.createGhnShipment(orderId, dto);
  }

  async cancelShipmentByOrderId(orderId: string, user: any) {
    const order = await this.prisma.order.findUnique({
      where: { id: orderId },
      include: {
        shipment: true,
      },
    });

    if (!order) {
      throw new BadRequestException("Không tìm thấy order");
    }

    const shipment = order.shipment;

    if (!shipment?.trackingCode) {
      return this.prisma.order.update({
        where: { id: orderId },
        data: {
          status: "CANCELLED",
          fulfillmentStatus: "UNFULFILLED",
        },
      });
    }

    await this.ghnClient.cancelOrder(shipment.trackingCode);

    await this.prisma.shipment.update({
      where: { id: shipment.id },
      data: {
        shippingStatus: "CANCELLED",
        partnerStatus: "cancel",
      },
    });

    return this.prisma.order.update({
      where: { id: orderId },
      data: {
        status: "CANCELLED",
        fulfillmentStatus: "UNFULFILLED",
      },
    });
  }

  async verifyAndUpdateCod(
    orderId: string,
    codAmount: number,
    code: string,
    user: any
  ) {
    const order = await this.prisma.order.findUnique({
      where: { id: orderId },
      include: {
        shipment: true,
      },
    });

    this.assertCanEditCod(order);

    const nextCod = Math.max(0, Math.round(Number(codAmount || 0)));
    if (Number.isNaN(nextCod)) {
      throw new BadRequestException("COD không hợp lệ");
    }

    const adminActor = await this.prisma.adminUser.findUnique({
      where: { id: user.id || user.sub },
      select: {
        id: true,
        role: true,
        fullName: true,
      },
    });

    let actorRole = "";
    let actorName = "";

    if (adminActor) {
      actorRole = String(adminActor.role || "").toLowerCase();
      actorName = adminActor.fullName || adminActor.id;
    } else {
      const staffActor = await this.prisma.staffUser.findUnique({
        where: { id: user.id || user.sub },
        select: {
          id: true,
          role: true,
          name: true,
        },
      });

      actorRole = String(staffActor?.role || "").toLowerCase();
      actorName = staffActor?.name || staffActor?.id || "";
    }

    console.log("verifyAndUpdateCod actorRole =", actorRole, "user =", user);

    const canEditCod =
      actorRole === "owner" ||
      actorRole === "admin" ||
      actorRole === "fulltime";

    if (!canEditCod) {
      throw new BadRequestException("Bạn không có quyền sửa COD.");
    }

    let approveInfo: any;

    try {
      approveInfo = await this.authTotpService.verifyOwnerCode(code);
      console.log("verifyOwnerCode passed", approveInfo);
    } catch (error) {
      console.error("verifyOwnerCode failed", error);
      throw error;
    }

    const oldCod = Number(order!.shipment!.codAmount || 0);

    try {
      console.log("calling ghnClient.updateCod", {
        trackingCode: order!.shipment!.trackingCode,
        nextCod,
      });

      await this.ghnClient.updateCod(order!.shipment!.trackingCode!, nextCod);

      console.log("ghnClient.updateCod passed");
    } catch (error) {
      console.error("ghnClient.updateCod failed", error);
      throw error;
    }

    await this.prisma.shipment.update({
      where: { id: order!.shipment!.id },
      data: {
        codAmount: nextCod,
      },
    });

    this.logger.warn(
      `[COD_UPDATE] actor=${actorName} approver=${approveInfo.approverName} order=${orderId} oldCod=${oldCod} newCod=${nextCod}`
    );

    return {
      ok: true,
      message: "Đã cập nhật COD.",
      oldCod,
      newCod: nextCod,
      approvedBy: approveInfo.approverName,
    };
  }


  private mapAhamoveShippingStatus(input?: string | null) {
    const value = String(input || "").toUpperCase();

    if (!value) return "NOT_CREATED";
    if (
      value.includes("COMPLETED") ||
      value.includes("DELIVERED") ||
      value.includes("SUCCESS")
    ) {
      return "DELIVERED";
    }

    if (value.includes("CANCEL")) return "CANCELLED";
    if (value.includes("FAIL")) return "FAILED";
    if (value.includes("RETURN")) return "RETURNING";
    if (value.includes("IN PROCESS") || value.includes("IN_PROCESS")) {
      return "DELIVERING";
    }
    if (value.includes("ACCEPTED") || value.includes("PICKED")) return "PICKING";
    if (value.includes("IDLE") || value.includes("ASSIGNING") || value.includes("CREATED")) {
      return "CREATED";
    }

    return value;
  }

  private buildAhamoveOrderSyncData(
    shippingStatus: string,
    context?: {
      codAmount?: number | string | null;
      paymentStatus?: string | null;
      codReconciliationStatus?: string | null;
    }
  ) {
    return this.buildCarrierOrderSyncData(shippingStatus, context);
  }

  private shouldMarkPendingCod(context?: {
    codAmount?: number | string | null;
    paymentStatus?: string | null;
    codReconciliationStatus?: string | null;
  }) {
    const codAmount = Number(context?.codAmount || 0);
    const paymentStatus = String(context?.paymentStatus || "").toUpperCase();
    const reconciliationStatus = String(context?.codReconciliationStatus || "").toUpperCase();

    if (codAmount <= 0) return false;
    if (paymentStatus === "PAID" || paymentStatus === "REFUNDED") return false;
    if (reconciliationStatus === "MATCHED" || reconciliationStatus === "MATCHED_BY_PARTIAL_DELIVERY") {
      return false;
    }

    return true;
  }

  private withPendingCodPayment<T extends Record<string, any>>(
    data: T,
    context?: {
      codAmount?: number | string | null;
      paymentStatus?: string | null;
      codReconciliationStatus?: string | null;
    }
  ) {
    if (!this.shouldMarkPendingCod(context)) return data;

    return {
      ...data,
      paymentStatus: "PENDING_COD",
    };
  }

  private buildCarrierOrderSyncData(
    shippingStatus: string,
    context?: {
      codAmount?: number | string | null;
      paymentStatus?: string | null;
      codReconciliationStatus?: string | null;
    }
  ) {
    const status = String(shippingStatus || "").toUpperCase();

    if (status === "DELIVERED") {
      return this.withPendingCodPayment(
        {
          status: "COMPLETED",
          fulfillmentStatus: "FULFILLED",
        },
        context
      );
    }

    if (status === "CANCELLED") {
      // Tracking refresh chỉ đồng bộ vận đơn, không được tự huỷ đơn nội bộ.
      // Huỷ đơn hệ thống phải đi qua nút Huỷ nội bộ / Huỷ GHN riêng.
      return {};
    }

    if (status === "RETURNED") {
      // GHN đã kết thúc chiều hoàn, nhưng shop vẫn phải bấm xác nhận đã nhận hàng hoàn.
      // Không tự set fulfillmentStatus=RETURNED ở đây để tránh nhập/báo cáo hoàn sai khi shop chưa cầm hàng.
      return this.withPendingCodPayment(
        {
          status: "SHIPPED",
          fulfillmentStatus: "PROCESSING",
        },
        context
      );
    }

    if (status === "RETURNING") {
      return this.withPendingCodPayment(
        {
          status: "SHIPPED",
          fulfillmentStatus: "PROCESSING",
        },
        context
      );
    }

    if (status === "FAILED") {
      return this.withPendingCodPayment(
        {
          status: "SHIPPED",
          fulfillmentStatus: "PROCESSING",
        },
        context
      );
    }

    if (
      status === "CREATED" ||
      status === "PICKING" ||
      status === "DELIVERING" ||
      status === "IN_TRANSIT"
    ) {
      return this.withPendingCodPayment(
        {
          status: "SHIPPED",
          fulfillmentStatus: "PROCESSING",
        },
        context
      );
    }

    return this.withPendingCodPayment({}, context);
  }

  private mapTimelineEventsForTracking(events: any[]) {
    return (Array.isArray(events) ? events : []).map((event: any) => ({
      id: event.id,
      status: event.status || event.partnerStatus || "",
      partnerStatus: event.partnerStatus || "",
      rawStatus: event.raw?.rawStatus || event.partnerStatus || event.status || "",
      eventCode: event.raw?.eventCode || event.raw?.code || event.raw?.status_code || "",
      title: event.title || this.timelineTitle(event.status, event.carrier),
      description: event.description || event.raw?.description || event.raw?.detail || event.raw?.message || "",
      location: event.locationText || event.raw?.location || event.raw?.hub_name || event.raw?.area || "",
      locationText: event.locationText || event.raw?.location || event.raw?.hub_name || event.raw?.area || "",
      time: event.eventTime || event.createdAt || "",
      driverName: event.driverName || null,
      driverPhone: event.driverPhone || null,
      driverPlate: event.driverPlate || null,
      eta: event.eta || null,
      source: event.source || null,
    }));
  }

  private getAhamoveOrderId(raw: any) {
    return (
      raw?.order_id ||
      raw?.id ||
      raw?.data?.order_id ||
      raw?.data?.id ||
      raw?.order?.order_id ||
      raw?.order?.id ||
      ""
    );
  }

  private getAhamoveStatus(raw: any) {
    return (
      raw?.status ||
      raw?.data?.status ||
      raw?.order?.status ||
      raw?.order_status ||
      "CREATED"
    );
  }

  private getAhamoveTrackingUrl(raw: any) {
    return (
      raw?.tracking_url ||
      raw?.shared_link ||
      raw?.data?.tracking_url ||
      raw?.data?.shared_link ||
      raw?.order?.tracking_url ||
      raw?.order?.shared_link ||
      null
    );
  }


  private normalizeViettelName(input?: string | null) {
    return String(input || "")
      .trim()
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/đ/g, "d")
      .replace(/Đ/g, "D")
      .replace(/^tinh\s+/i, "")
      .replace(/^thanh pho\s+/i, "")
      .replace(/^tp\.?\s*/i, "")
      .replace(/^quan\s+/i, "")
      .replace(/^huyen\s+/i, "")
      .replace(/^thi xa\s+/i, "")
      .replace(/^phuong\s+/i, "")
      .replace(/^xa\s+/i, "")
      .replace(/^thi tran\s+/i, "")
      .replace(/\s+/g, " ")
      .trim();
  }

  private pickViettelValue(row: any, keys: string[]) {
    for (const key of keys) {
      const value = row?.[key];
      if (value !== undefined && value !== null && String(value).trim()) {
        return value;
      }
    }

    return undefined;
  }

  private async resolveViettelAddress(input: {
    province?: string | null;
    district?: string | null;
    ward?: string | null;
  }) {
    const provinceName = this.normalizeViettelName(input.province);
    const districtName = this.normalizeViettelName(input.district);
    const wardName = this.normalizeViettelName(input.ward);

    if (!provinceName || !districtName) {
      throw new BadRequestException("Thiếu tỉnh/thành hoặc quận/huyện ViettelPost");
    }

    const provinces = await this.viettelPostClient.listProvinces();
    const province = (Array.isArray(provinces) ? provinces : []).find((item: any) => {
      const name = this.normalizeViettelName(
        item.PROVINCE_NAME || item.name || item.provinceName
      );
      return name === provinceName || name.includes(provinceName) || provinceName.includes(name);
    });

    if (!province) {
      throw new BadRequestException(`Không map được tỉnh/thành ViettelPost: ${input.province}`);
    }

    const provinceId = Number(
      this.pickViettelValue(province, ["PROVINCE_ID", "provinceId", "id"])
    );

    const districts = await this.viettelPostClient.listDistricts(provinceId);
    const district = (Array.isArray(districts) ? districts : []).find((item: any) => {
      const name = this.normalizeViettelName(
        item.DISTRICT_NAME || item.name || item.districtName
      );
      return name === districtName || name.includes(districtName) || districtName.includes(name);
    });

    if (!district) {
      throw new BadRequestException(`Không map được quận/huyện ViettelPost: ${input.district}`);
    }

    const districtId = Number(
      this.pickViettelValue(district, ["DISTRICT_ID", "districtId", "id"])
    );
    const districtValue = this.pickViettelValue(district, [
      "DISTRICT_VALUE",
      "districtValue",
      "value",
      "DISTRICT_ID",
    ]);

    let wardId: number | undefined = undefined;
    let wardNameResult: string | undefined = undefined;

    if (wardName) {
      try {
        const wards = await this.viettelPostClient.listWards(districtId);
        const ward = (Array.isArray(wards) ? wards : []).find((item: any) => {
          const name = this.normalizeViettelName(
            item.WARDS_NAME || item.WARD_NAME || item.name || item.wardName
          );
          return name === wardName || name.includes(wardName) || wardName.includes(name);
        });

        if (ward) {
          wardId = Number(this.pickViettelValue(ward, ["WARDS_ID", "WARD_ID", "wardId", "id"]));
          wardNameResult = String(
            this.pickViettelValue(ward, ["WARDS_NAME", "WARD_NAME", "wardName", "name"]) || ""
          );
        }
      } catch (err) {
        this.logger.warn(
          `Không lấy được ward ViettelPost districtId=${districtId}: ${err instanceof Error ? err.message : String(err)
          }`
        );
      }
    }

    return {
      provinceId,
      provinceName: province.PROVINCE_NAME || province.name || input.province,
      districtId,
      districtValue: String(districtValue || districtId),
      districtName: district.DISTRICT_NAME || district.name || input.district,
      wardId,
      wardName: wardNameResult || input.ward || "",
    };
  }

  async resolveViettelPostAddress(body: any) {
    return this.resolveViettelAddress({
      province: body?.province,
      district: body?.district,
      ward: body?.ward,
    });
  }

  private mapViettelPostShippingStatus(input?: string | null) {
    const status = this.mapShippingStatus(input);
    return status === "NOT_CREATED" ? "CREATED" : status;
  }

  private getViettelPostOrderNumber(raw: any) {
    return (
      raw?.ORDER_NUMBER ||
      raw?.order_number ||
      raw?.orderNumber ||
      raw?.data?.ORDER_NUMBER ||
      raw?.data?.order_number ||
      raw?.data?.orderNumber ||
      raw?.data?.VTPOST_ORDER_CODE ||
      raw?.VTPOST_ORDER_CODE ||
      raw?.trackingCode ||
      ""
    );
  }

  private getViettelPostFee(raw: any) {
    return Number(
      raw?.MONEY_TOTAL ||
      raw?.money_total ||
      raw?.total_fee ||
      raw?.totalFee ||
      raw?.data?.MONEY_TOTAL ||
      raw?.data?.money_total ||
      raw?.data?.total_fee ||
      raw?.data?.totalFee ||
      raw?.price ||
      raw?.data?.price ||
      0
    );
  }

  private getViettelPostStatus(raw: any) {
    return String(
      raw?.ORDER_STATUS ||
      raw?.ORDER_STATUS_NAME ||
      raw?.STATUS_NAME ||
      raw?.status_name ||
      raw?.status ||
      raw?.data?.ORDER_STATUS ||
      raw?.data?.ORDER_STATUS_NAME ||
      raw?.data?.STATUS_NAME ||
      raw?.data?.status_name ||
      raw?.data?.status ||
      "CREATED"
    );
  }

  private buildViettelPostTrackingUrl(orderNumber: string) {
    const base =
      process.env.VIETTELPOST_TRACKING_URL ||
      "https://viettelpost.com.vn/tra-cuu-hanh-trinh-don/";
    return orderNumber ? `${base}?orderNumber=${encodeURIComponent(orderNumber)}` : null;
  }

  private async resolveViettelSenderAddress() {
    const envProvinceId = Number(process.env.VIETTELPOST_SENDER_PROVINCE_ID || 0);
    const envDistrictId = Number(process.env.VIETTELPOST_SENDER_DISTRICT_ID || 0);
    const envWardId = Number(process.env.VIETTELPOST_SENDER_WARD_ID || 0);

    if (envProvinceId && envDistrictId) {
      return {
        provinceId: envProvinceId,
        districtId: envDistrictId,
        wardId: envWardId || undefined,
        address: process.env.VIETTELPOST_SENDER_ADDRESS || this.returnAddress,
        name: process.env.VIETTELPOST_SENDER_NAME || this.returnName,
        phone: process.env.VIETTELPOST_SENDER_PHONE || this.returnPhone,
      };
    }

    const province = process.env.VIETTELPOST_SENDER_PROVINCE || "";
    const district = process.env.VIETTELPOST_SENDER_DISTRICT || "";
    const ward = process.env.VIETTELPOST_SENDER_WARD || "";

    if (!province || !district) {
      throw new BadRequestException(
        "Thiếu cấu hình người gửi ViettelPost. Cần VIETTELPOST_SENDER_PROVINCE_ID/VIETTELPOST_SENDER_DISTRICT_ID hoặc VIETTELPOST_SENDER_PROVINCE/VIETTELPOST_SENDER_DISTRICT"
      );
    }

    const resolved = await this.resolveViettelAddress({ province, district, ward });
    const resolvedAny = resolved as any;

    return {
      provinceId: Number(resolvedAny.provinceId || 0),
      districtId: Number(resolvedAny.districtId || resolvedAny.districtValue || 0),
      wardId: Number(resolvedAny.wardId || 0) || undefined,
      address: process.env.VIETTELPOST_SENDER_ADDRESS || this.returnAddress,
      name: process.env.VIETTELPOST_SENDER_NAME || this.returnName,
      phone: process.env.VIETTELPOST_SENDER_PHONE || this.returnPhone,
    };
  }

  private viettelPostServiceLabel(serviceCode: string) {
    const code = String(serviceCode || "").toUpperCase();

    if (code === "VHT") return "Chuyển phát hỏa tốc";
    if (code === "VTK") return "Chuyển phát tiêu chuẩn";
    if (code === "VCN") return "Chuyển phát nhanh";
    if (code === "PHS") return "Nội tỉnh tiết kiệm";
    if (code === "VCBO") return "Chuyển phát nhanh";
    if (code === "V60") return "Chuyển phát 60 giờ";
    if (code === "V120") return "Chuyển phát 120 giờ";
    if (code === "LCOD") return "Thương mại điện tử";

    return serviceCode || "ViettelPost";
  }

  private viettelPostLeadtimeLabel(serviceCode: string) {
    const code = String(serviceCode || "").toUpperCase();

    if (code === "VHT") return "Hỏa tốc";
    if (code === "VTK") return "Tiêu chuẩn";
    if (code === "VCN") return "Nhanh";
    if (code === "PHS") return "Tiết kiệm";
    if (code === "VCBO") return "Nhanh";

    return "Đang cập nhật";
  }

  private normalizeViettelServiceCodes(body: any) {
    const raw = [
      body?.services,
      body?.serviceCodes,
      process.env.VIETTELPOST_SERVICES,
      process.env.VIETTELPOST_DEFAULT_SERVICE,
      "VHT,VTK,VCN",
    ]
      .filter(Boolean)
      .join(",");

    const inputList = Array.from(
      new Set(
        String(raw || "")
          .split(",")
          .map((item) => item.trim().toUpperCase())
          .filter(Boolean)
      )
    ) as string[];

    // Theo tài liệu VTP: getPriceAllNlp trả danh sách MA_DV_CHINH,
    // nhưng nếu API chỉ trả 1 gói thì vẫn thử getPrice theo 3 mã chính.
    const required = ["VHT", "VTK", "VCN"];
    const merged = Array.from(new Set([...inputList, ...required])) as string[];
    const order = ["VHT", "VTK", "VCN"];

    return [
      ...order.filter((code) => merged.includes(code)),
      ...merged.filter((code) => !order.includes(code)),
    ];
  }

  private shouldTryViettelPostFallbackServices(body: any) {
    return !body?.services && !body?.serviceCodes && !process.env.VIETTELPOST_SERVICES;
  }

  private getViettelPostFallbackServices(body: any) {
    const main = this.normalizeViettelServiceCodes(body);
    const fallback = String(
      process.env.VIETTELPOST_FALLBACK_SERVICES || "VHT"
    )
      .split(",")
      .map((item) => item.trim().toUpperCase())
      .filter(Boolean);

    return Array.from(new Set([...main, ...fallback]));
  }


  private normalizeViettelAddressText(input: {
    province?: string | null;
    district?: string | null;
    ward?: string | null;
    address?: string | null;
  }) {
    return [input.address, input.ward, input.district, input.province]
      .filter(Boolean)
      .map((item) => String(item).trim())
      .filter(Boolean)
      .join(", ");
  }

  private extractViettelPostQuoteRows(raw: any) {
    const candidates = [
      raw,
      raw?.data,
      raw?.DATA,
      raw?.result,
      raw?.RESULT,
      raw?.services,
      raw?.SERVICES,
      raw?.prices,
      raw?.PRICES,
      raw?.list,
      raw?.LIST,
      raw?.data?.RESULT,
      raw?.data?.result,
      raw?.data?.services,
      raw?.data?.SERVICES,
      raw?.data?.prices,
      raw?.data?.PRICES,
      raw?.data?.data,
      raw?.DATA?.data,
      raw?.Data,
      raw?.DATA?.RESULT,
      raw?.RESULT?.DATA,
      raw?.RESULT?.data,
      raw?.data?.DATA,
    ];

    for (const item of candidates) {
      if (Array.isArray(item)) return item;
    }

    // Một số response Viettel trả object nhưng chính nó là 1 giá.
    if (
      raw &&
      typeof raw === "object" &&
      (raw.MONEY_TOTAL ||
        raw.money_total ||
        raw.GIA_CUOC ||
        raw.TOTAL_FEE ||
        raw.total_fee ||
        raw.MA_DV_CHINH ||
        raw.ORDER_SERVICE)
    ) {
      return [raw];
    }

    return [];
  }

  private getViettelPostServiceCode(raw: any) {
    return String(
      raw?.MA_DV_CHINH ||
      raw?.ORDER_SERVICE ||
      raw?.SERVICE_CODE ||
      raw?.service_code ||
      raw?.serviceCode ||
      raw?.code ||
      raw?.MA_DICH_VU ||
      raw?.SERVICE_ID ||
      raw?.service_id ||
      raw?.id ||
      raw?.data?.MA_DV_CHINH ||
      raw?.data?.ORDER_SERVICE ||
      raw?.data?.SERVICE_CODE ||
      ""
    ).toUpperCase();
  }

  private getViettelPostServiceName(raw: any) {
    return String(
      raw?.TEN_DICHVU ||
      raw?.TEN_DICH_VU ||
      raw?.SERVICE_NAME ||
      raw?.service_name ||
      raw?.serviceName ||
      raw?.name ||
      raw?.shortName ||
      raw?.data?.TEN_DICHVU ||
      raw?.data?.SERVICE_NAME ||
      ""
    );
  }

  private getViettelPostQuoteFee(raw: any) {
    return Number(
      raw?.GIA_CUOC ||
      raw?.MONEY_TOTAL ||
      raw?.money_total ||
      raw?.MONEY_TOTAL_OLD ||
      raw?.money_total_old ||
      raw?.MONEY_TOTAL_FEE ||
      raw?.money_total_fee ||
      raw?.TOTAL_FEE ||
      raw?.total_fee ||
      raw?.totalFee ||
      raw?.fee ||
      raw?.price ||
      raw?.data?.GIA_CUOC ||
      raw?.data?.MONEY_TOTAL ||
      raw?.data?.TOTAL_FEE ||
      0
    );
  }

  private getViettelPostQuoteLeadtime(raw: any) {
    return String(
      raw?.THOI_GIAN ||
      raw?.LEADTIME ||
      raw?.leadtime ||
      raw?.time ||
      raw?.data?.THOI_GIAN ||
      ""
    );
  }

  private normalizeViettelPostInventory(raw: any) {
    const pick = (...keys: string[]) => {
      for (const key of keys) {
        const value = key.split(".").reduce((acc: any, part) => acc?.[part], raw);
        if (value !== undefined && value !== null && String(value).trim()) {
          return String(value).trim();
        }
      }
      return "";
    };

    const groupAddressId = Number(
      pick(
        "group_address_id",
        "groupAddressId",
        "groupaddressId",
        "groupAddressID",
        "GROUPADDRESS_ID",
        "GROUP_ADDRESS_ID",
        "SENDER_GROUP_ADDRESS_ID",
        "sender_group_address_id",
        "senderGroupAddressId",
        "senderAddressId",
        "sender_address_id",
        "address_id",
        "addressId",
        "ADDRESS_ID",
        "id",
        "ID",
        "_id"
      ) || 0
    );

    const provinceId = Number(
      pick(
        "province_id",
        "provinceId",
        "PROVINCE_ID",
        "SENDER_PROVINCE",
        "SENDER_PROVINCE_ID",
        "senderProvinceId",
        "province.value",
        "province.id"
      ) || 0
    );

    const districtId = Number(
      pick(
        "district_id",
        "districtId",
        "DISTRICT_ID",
        "SENDER_DISTRICT",
        "SENDER_DISTRICT_ID",
        "senderDistrictId",
        "district.value",
        "district.id"
      ) || 0
    );

    const wardId = Number(
      pick(
        "wards_id",
        "ward_id",
        "wardId",
        "WARDS_ID",
        "WARD_ID",
        "SENDER_WARD",
        "SENDER_WARD_ID",
        "senderWardId",
        "ward.value",
        "ward.id"
      ) || 0
    );

    const name = pick(
      "name",
      "NAME",
      "full_name",
      "fullName",
      "SENDER_FULLNAME",
      "SENDER_NAME",
      "senderName",
      "contact_name",
      "contactName",
      "customer_name",
      "customerName"
    );

    const phone = pick(
      "phone",
      "PHONE",
      "tel",
      "TEL",
      "mobile",
      "MOBILE",
      "SENDER_PHONE",
      "senderPhone",
      "contact_phone",
      "contactPhone"
    );

    const address = pick(
      "address",
      "ADDRESS",
      "SENDER_ADDRESS",
      "senderAddress",
      "full_address",
      "fullAddress",
      "ADDRESS_FULL",
      "address_full"
    );

    return {
      groupAddressId,
      cusId: Number(pick("cus_id", "cusId", "CUS_ID", "customer_id", "customerId") || 0) || undefined,
      name,
      phone,
      address,
      provinceId,
      districtId,
      wardId: wardId || undefined,
      raw,
    };
  }

  private extractViettelPostInventories(raw: any) {
    const directCandidates = [
      raw,
      raw?.data,
      raw?.DATA,
      raw?.result,
      raw?.RESULT,
      raw?.inventories,
      raw?.inventory,
      raw?.listInventory,
      raw?.listInventories,
      raw?.list,
      raw?.LIST,
      raw?.rows,
      raw?.items,
      raw?.data?.data,
      raw?.data?.result,
      raw?.data?.RESULT,
      raw?.data?.list,
      raw?.data?.LIST,
      raw?.data?.rows,
      raw?.data?.items,
      raw?.DATA?.data,
      raw?.DATA?.result,
      raw?.DATA?.RESULT,
      raw?.DATA?.list,
      raw?.DATA?.LIST,
    ];

    for (const item of directCandidates) {
      if (Array.isArray(item)) return item;
    }

    const rows: any[] = [];
    const walk = (node: any, depth = 0) => {
      if (!node || depth > 6) return;

      if (Array.isArray(node)) {
        const looksLikeInventory = node.some((item) => {
          if (!item || typeof item !== "object") return false;
          const keys = Object.keys(item).map((key) => key.toLowerCase());
          return keys.some((key) =>
            [
              "group_address_id",
              "groupaddressid",
              "groupaddress_id",
              "sender_group_address_id",
              "senderaddressid",
              "address",
              "sender_address",
              "full_address",
            ].includes(key)
          );
        });

        if (looksLikeInventory) rows.push(...node);
        for (const child of node.slice(0, 80)) walk(child, depth + 1);
        return;
      }

      if (typeof node === "object") {
        for (const value of Object.values(node)) walk(value, depth + 1);
      }
    };

    walk(raw);

    const seen = new Set<string>();
    return rows.filter((item) => {
      const key = JSON.stringify({
        id:
          item?.group_address_id ||
          item?.groupAddressId ||
          item?.GROUPADDRESS_ID ||
          item?.GROUP_ADDRESS_ID ||
          item?.id ||
          item?.ID ||
          "",
        address: item?.address || item?.ADDRESS || item?.SENDER_ADDRESS || item?.full_address || "",
      });
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }

  async getPickupLocations() {
    const locations: any[] = [];

    const ghnShopId = Number(process.env.GHN_SHOP_ID || 0);
    const ghnDistrictId = Number(process.env.GHN_FROM_DISTRICT_ID || 0);
    const ghnWardCode = String(process.env.GHN_FROM_WARD_CODE || "");
    const ghnAddress = String(process.env.GHN_RETURN_ADDRESS || "");
    const ghnName = String(process.env.GHN_RETURN_NAME || "The 1970");
    const ghnPhone = String(process.env.GHN_RETURN_PHONE || "");

    if (ghnDistrictId && ghnWardCode) {
      locations.push({
        id: `ghn-${ghnShopId || "default"}-${ghnDistrictId}-${ghnWardCode}`,
        carrier: "ghn",
        label: `${ghnName}${ghnAddress ? ` · ${ghnAddress}` : ""}`,
        name: ghnName,
        phone: ghnPhone,
        address: ghnAddress,
        ghnShopId: ghnShopId || undefined,
        ghnFromDistrictId: ghnDistrictId,
        ghnFromWardCode: ghnWardCode,
      });
    }

    const ahamoveName = String(process.env.AHAMOVE_FROM_NAME || this.returnName || "The 1970");
    const ahamovePhone = String(process.env.AHAMOVE_FROM_PHONE || this.returnPhone || "");
    const ahamoveAddress = String(process.env.AHAMOVE_FROM_ADDRESS || this.returnAddress || "");

    if (ahamovePhone && ahamoveAddress) {
      locations.push({
        id: `ahamove-default-${ahamovePhone}`,
        carrier: "ahamove",
        label: `${ahamoveName}${ahamoveAddress ? ` · ${ahamoveAddress}` : ""}`,
        name: ahamoveName,
        phone: ahamovePhone,
        address: ahamoveAddress,
      });
    }

    try {
      const inventories = await this.listViettelPostInventories();
      for (const item of inventories as any[]) {
        locations.push({
          id: `viettelpost-${item.groupAddressId || item.phone || item.address || item.name}`,
          carrier: "viettelpost",
          label: `${item.name || process.env.VIETTELPOST_SENDER_NAME || "Kho ViettelPost"}${item.address ? ` · ${item.address}` : ""}`,
          name: item.name || process.env.VIETTELPOST_SENDER_NAME || this.returnName,
          phone: item.phone || process.env.VIETTELPOST_SENDER_PHONE || this.returnPhone,
          address: item.address || process.env.VIETTELPOST_SENDER_ADDRESS || this.returnAddress,
          viettelGroupAddressId: Number(item.groupAddressId || 0) || undefined,
          groupAddressId: Number(item.groupAddressId || 0) || undefined,
          viettelProvinceId: Number(item.provinceId || 0) || undefined,
          viettelDistrictId: Number(item.districtId || 0) || undefined,
          viettelWardId: Number(item.wardId || 0) || undefined,
        });
      }
    } catch (error) {
      this.logger.warn(
        `[PICKUP_LOCATIONS] Không tải được kho ViettelPost: ${
          error instanceof Error ? error.message : error
        }`
      );
    }

    const envVtpGroupId = Number(
      process.env.VIETTELPOST_SENDER_GROUP_ADDRESS_ID ||
        process.env.VIETTELPOST_GROUPADDRESS_ID ||
        process.env.VIETTELPOST_GROUP_ADDRESS_ID ||
        process.env.VIETTELPOST_SENDER_ADDRESS_ID ||
        0
    );
    const hasEnvVtp = locations.some(
      (item) =>
        item.carrier === "viettelpost" &&
        Number(item.groupAddressId || item.viettelGroupAddressId || 0) === envVtpGroupId,
    );

    if (!hasEnvVtp && envVtpGroupId) {
      locations.push({
        id: `viettelpost-env-${envVtpGroupId}`,
        carrier: "viettelpost",
        label: `${process.env.VIETTELPOST_SENDER_NAME || this.returnName}${
          process.env.VIETTELPOST_SENDER_ADDRESS
            ? ` · ${process.env.VIETTELPOST_SENDER_ADDRESS}`
            : ""
        }`,
        name: process.env.VIETTELPOST_SENDER_NAME || this.returnName,
        phone: process.env.VIETTELPOST_SENDER_PHONE || this.returnPhone,
        address: process.env.VIETTELPOST_SENDER_ADDRESS || this.returnAddress,
        viettelGroupAddressId: envVtpGroupId,
        groupAddressId: envVtpGroupId,
        viettelProvinceId: Number(process.env.VIETTELPOST_SENDER_PROVINCE_ID || 0) || undefined,
        viettelDistrictId: Number(process.env.VIETTELPOST_SENDER_DISTRICT_ID || 0) || undefined,
        viettelWardId: Number(process.env.VIETTELPOST_SENDER_WARD_ID || 0) || undefined,
      });
    }

    return locations;
  }

  async listViettelPostInventories() {
    const raw = await this.viettelPostClient.listInventories();
    const rows = this.extractViettelPostInventories(raw)
      .map((item: any) => this.normalizeViettelPostInventory(item))
      // ViettelPost /user/listInventory có tài khoản chỉ trả groupAddressId + address,
      // không luôn trả provinceId/districtId. Vẫn phải đưa ra UI để map kho giống Sapo,
      // lúc create/quote sẽ fallback tỉnh/huyện/xã từ env nếu inventory thiếu mã.
      .filter((item: any) => item.groupAddressId || item.address || item.name || item.phone);

    return rows;
  }

  private async getViettelSenderConfig(input?: any) {
    const senderGroupAddressId = Number(
      input?.senderGroupAddressId ||
      input?.groupAddressId ||
      input?.GROUPADDRESS_ID ||
      process.env.VIETTELPOST_SENDER_GROUP_ADDRESS_ID ||
      process.env.VIETTELPOST_GROUPADDRESS_ID ||
      0
    );

    if (senderGroupAddressId) {
      const inventories = await this.listViettelPostInventories().catch(() => [] as any[]);
      const found = inventories.find(
        (item: any) => Number(item.groupAddressId) === senderGroupAddressId
      );

      if (found) {
        return {
          groupAddressId: Number(found.groupAddressId),
          cusId: Number(found.cusId || 0) || undefined,
          provinceId: Number(found.provinceId || 0) || Number((await this.getViettelSenderConfigFromEnv()).provinceId),
          districtId: Number(found.districtId || 0) || Number((await this.getViettelSenderConfigFromEnv()).districtId),
          wardId: Number(found.wardId || 0) || Number((await this.getViettelSenderConfigFromEnv()).wardId || 0) || undefined,
          name: input?.fromName || found.name || process.env.VIETTELPOST_SENDER_NAME || this.returnName,
          phone: input?.fromPhone || found.phone || process.env.VIETTELPOST_SENDER_PHONE || this.returnPhone,
          address: input?.fromAddress || found.address || process.env.VIETTELPOST_SENDER_ADDRESS || this.returnAddress,
          inventory: found,
        };
      }
    }

    if (input?.senderProvinceId && input?.senderDistrictId) {
      return {
        groupAddressId: senderGroupAddressId || undefined,
        provinceId: Number(input.senderProvinceId),
        districtId: Number(input.senderDistrictId),
        wardId: Number(input.senderWardId || 0) || undefined,
        name: input?.fromName || process.env.VIETTELPOST_SENDER_NAME || this.returnName,
        phone: input?.fromPhone || process.env.VIETTELPOST_SENDER_PHONE || this.returnPhone,
        address: input?.fromAddress || process.env.VIETTELPOST_SENDER_ADDRESS || this.returnAddress,
      };
    }

    const envSender = await this.getViettelSenderConfigFromEnv();
    return {
      ...envSender,
      groupAddressId: senderGroupAddressId || undefined,
    };
  }

  private async getViettelSenderConfigFromEnv() {
    const rawProvinceId = Number(process.env.VIETTELPOST_SENDER_PROVINCE_ID || 0);
    const rawDistrictId = Number(process.env.VIETTELPOST_SENDER_DISTRICT_ID || 0);
    const wardIdRaw =
      process.env.VIETTELPOST_SENDER_WARD_ID ||
      process.env.VIETTELPOST_SENDER_WARD_CODE ||
      "";
    const rawWardId = Number(wardIdRaw || 0);

    const looksLikeGhnId =
      rawProvinceId >= 100 || rawDistrictId >= 1000 || String(wardIdRaw || "").includes("B");

    if (rawProvinceId && rawDistrictId && !looksLikeGhnId) {
      return {
        provinceId: rawProvinceId,
        districtId: rawDistrictId,
        wardId: rawWardId || undefined,
        name: process.env.VIETTELPOST_SENDER_NAME || this.returnName,
        phone: process.env.VIETTELPOST_SENDER_PHONE || this.returnPhone,
        address: process.env.VIETTELPOST_SENDER_ADDRESS || this.returnAddress,
      };
    }

    const fallback = this.normalizeViettelReceiverForOldCarrier({
      province: process.env.VIETTELPOST_SENDER_PROVINCE || "Hà Nội",
      district: process.env.VIETTELPOST_SENDER_DISTRICT || "Quốc Oai",
      ward: process.env.VIETTELPOST_SENDER_WARD || "Sài Sơn",
      address:
        process.env.VIETTELPOST_SENDER_ADDRESS ||
        this.returnAddress ||
        "Ngõ chợ Thầy, Làng Đa Phúc, Xã Sài Sơn, Huyện Quốc Oai, Hà Nội",
    });

    const resolved = await this.resolveViettelAddress({
      province: fallback.province,
      district: fallback.district,
      ward: fallback.ward,
    });
    const resolvedAny = resolved as any;

    const provinceId = Number(resolvedAny.provinceId || 0);
    const districtId = Number(resolvedAny.districtId || resolvedAny.districtValue || 0);
    const wardId = Number(resolvedAny.wardId || 0) || undefined;

    if (!provinceId || !districtId) {
      throw new BadRequestException(
        "Thiếu hoặc sai mã ViettelPost người gửi. Không dùng mã GHN cho VIETTELPOST_SENDER_PROVINCE_ID / DISTRICT_ID"
      );
    }

    this.logger.warn(
      `[VIETTELPOST_SENDER] ENV đang giống mã GHN hoặc thiếu mã ViettelPost. Đã resolve lại sender=${provinceId}/${districtId}/${wardId || ""}`
    );

    return {
      provinceId,
      districtId,
      wardId,
      name: process.env.VIETTELPOST_SENDER_NAME || this.returnName,
      phone: process.env.VIETTELPOST_SENDER_PHONE || this.returnPhone,
      address: process.env.VIETTELPOST_SENDER_ADDRESS || this.returnAddress,
    };
  }

  private normalizeViettelReceiverForOldCarrier(input: {
    province?: string | null;
    district?: string | null;
    ward?: string | null;
    address?: string | null;
  }) {
    const rawText = [
      input.address,
      input.ward,
      input.district,
      input.province,
    ]
      .filter(Boolean)
      .join(" ")
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/đ/g, "d")
      .replace(/Đ/g, "D")
      .toLowerCase();

    let province = String(input.province || "").trim();
    let district = String(input.district || "").trim();
    let ward = String(input.ward || "").trim();

    // Hà Nội sau sáp nhập dễ parse thành "Xã Quốc Oai" thay vì Huyện Quốc Oai.
    // Với Viettel/GHN/Sapo carrier cũ, tuyến cần old district/ward.
    if (
      rawText.includes("quoc oai") &&
      (rawText.includes("sai son") || rawText.includes("chợ thầy") || rawText.includes("cho thay"))
    ) {
      province = province || "Hà Nội";
      district = "Quốc Oai";
      ward = "Sài Sơn";
    }

    // Đắk Nông / Đắk R'Lấp / Kiến Đức hay bị mất dấu hoặc viết Đăk/Dak.
    if (
      (rawText.includes("dak r") || rawText.includes("dac r")) &&
      rawText.includes("kien duc")
    ) {
      province = province || "Đắk Nông";
      district = "Đắk R'Lấp";
      ward = "Kiến Đức";
    }

    return { province, district, ward };
  }

  async quoteViettelPost(body: any) {
    const sender = await this.getViettelSenderConfig(body);

    const receiverInput = this.normalizeViettelReceiverForOldCarrier({
      province: body?.province || body?.toProvince,
      district: body?.district || body?.toDistrict,
      ward: body?.ward || body?.toWard,
      address: body?.toAddress || body?.address,
    });

    const resolved = await this.resolveViettelAddress({
      province: receiverInput.province,
      district: receiverInput.district,
      ward: receiverInput.ward,
    });

    const resolvedAny = resolved as any;

    const weight = Math.max(1, Number(body?.weight || body?.PRODUCT_WEIGHT || 200));
    const productPrice = Math.max(
      0,
      Number(body?.productPrice || body?.insuranceValue || body?.PRODUCT_PRICE || 0)
    );
    const codAmount = Math.max(0, Number(body?.codAmount || body?.MONEY_COLLECTION || 0));
    const productLength = Math.max(1, Number(body?.length || body?.PRODUCT_LENGTH || 10));
    const productWidth = Math.max(1, Number(body?.width || body?.PRODUCT_WIDTH || 10));
    const productHeight = Math.max(1, Number(body?.height || body?.PRODUCT_HEIGHT || 10));
    const productType =
      body?.productType || process.env.VIETTELPOST_PRODUCT_TYPE || "HH";

    const senderAddressText = this.normalizeViettelAddressText({
      address: sender.address,
      ward: sender.wardId ? "" : process.env.VIETTELPOST_SENDER_WARD,
      district: process.env.VIETTELPOST_SENDER_DISTRICT,
      province: process.env.VIETTELPOST_SENDER_PROVINCE,
    });

    const receiverAddressText = this.normalizeViettelAddressText({
      address: body?.toAddress || body?.address,
      ward: receiverInput.ward,
      district: receiverInput.district,
      province: receiverInput.province,
    });

    const rows: any[] = [];
    const failedMessages: string[] = [];

    const pushQuoteRow = (raw: any, fallbackServiceCode?: string) => {
      const fee = this.getViettelPostQuoteFee(raw) || this.getViettelPostFee(raw);
      if (!fee) return;

      const serviceCode =
        this.getViettelPostServiceCode(raw) ||
        String(fallbackServiceCode || "VHT").toUpperCase();
      const serviceName = this.getViettelPostServiceName(raw);
      const leadtime = this.getViettelPostQuoteLeadtime(raw);

      const quoteKey = `viettelpost-${serviceCode || rows.length}`;

      if (rows.some((item) => item._quoteKey === quoteKey)) {
        return;
      }

      rows.push({
        serviceId: 0,
        serviceTypeId: rows.length + 1,
        shortName: `Viettel Post - ${serviceName || this.viettelPostServiceLabel(serviceCode)
          }`,
        fee: {
          total: fee,
          total_fee: fee,
          service_fee: fee,
        },
        leadtime: {
          label: leadtime || this.viettelPostLeadtimeLabel(serviceCode),
        },
        _carrier: "viettelpost",
        _quoteKey: quoteKey,
        _serviceName: serviceCode,
        _viettelServiceCode: serviceCode,
        _viettelReceiverProvinceId: Number(resolvedAny.provinceId || 0),
        _viettelReceiverDistrictId: Number(
          resolvedAny.districtId || resolvedAny.districtValue || 0
        ),
        _viettelReceiverWardId: Number(resolvedAny.wardId || 0) || undefined,
        _viettelSenderGroupAddressId: Number(sender.groupAddressId || 0) || undefined,
        _applyFeeToInput: true,
        _raw: raw,
      });
    };

    const nlpPayload = {
      PRODUCT_WEIGHT: weight,
      PRODUCT_PRICE: productPrice,
      PRODUCT_LENGTH: productLength,
      PRODUCT_WIDTH: productWidth,
      PRODUCT_HEIGHT: productHeight,
      PRODUCT_TYPE: productType,
      MONEY_COLLECTION: codAmount,
      SENDER_PROVINCE: Number(sender.provinceId || 0),
      SENDER_DISTRICT: Number(sender.districtId || 0),
      SENDER_WARD: Number(sender.wardId || 0) || undefined,
      RECEIVER_PROVINCE: Number(resolvedAny.provinceId || 0),
      RECEIVER_DISTRICT: Number(resolvedAny.districtId || resolvedAny.districtValue || 0),
      RECEIVER_WARD: Number(resolvedAny.wardId || 0) || undefined,
      SENDER_ADDRESS: senderAddressText || sender.address,
      RECEIVER_ADDRESS: receiverAddressText,
      ORDER_SERVICE_ADD:
        body?.serviceAdd || process.env.VIETTELPOST_SERVICE_ADD || "",
      NATIONAL_TYPE: Number(body?.nationalType || 1),
    };

    this.logger.log(
      `[VIETTELPOST_QUOTE_NLP] sender=${Number(sender.provinceId || 0)}/${Number(
        sender.districtId || 0
      )}/${Number(sender.wardId || 0) || ""} receiver=${Number(
        resolvedAny.provinceId || 0
      )}/${Number(resolvedAny.districtId || resolvedAny.districtValue || 0)}/${Number(resolvedAny.wardId || 0) || ""
      } text="${receiverAddressText}"`
    );

    const priceAllPayload = {
      PRODUCT_WEIGHT: weight,
      PRODUCT_PRICE: productPrice,
      PRODUCT_LENGTH: productLength,
      PRODUCT_WIDTH: productWidth,
      PRODUCT_HEIGHT: productHeight,
      PRODUCT_TYPE: productType,
      MONEY_COLLECTION: codAmount,
      SENDER_PROVINCE: Number(sender.provinceId || 0),
      SENDER_DISTRICT: Number(sender.districtId || 0),
      RECEIVER_PROVINCE: Number(resolvedAny.provinceId || 0),
      RECEIVER_DISTRICT: Number(resolvedAny.districtId || resolvedAny.districtValue || 0),
      TYPE: Number(body?.type || body?.TYPE || 1),
      NATIONAL_TYPE: Number(body?.nationalType || 1),
    };

    this.logger.log(
      `[VIETTELPOST_QUOTE_PRICE_ALL] sender=${priceAllPayload.SENDER_PROVINCE}/${priceAllPayload.SENDER_DISTRICT} receiver=${priceAllPayload.RECEIVER_PROVINCE}/${priceAllPayload.RECEIVER_DISTRICT}`
    );

    try {
      const rawPriceAll = await this.viettelPostClient.getPriceAll(priceAllPayload);
      const priceAllRows = this.extractViettelPostQuoteRows(rawPriceAll);

      for (const item of priceAllRows) {
        pushQuoteRow(item);
      }

      if (!priceAllRows.length) {
        pushQuoteRow(rawPriceAll);
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      failedMessages.push(`getPriceAll: ${message}`);
      this.logger.warn(`ViettelPost getPriceAll failed: ${message}`);
    }

    try {
      const rawAll = await this.viettelPostClient.getPriceAllNlp(nlpPayload);
      const allRows = this.extractViettelPostQuoteRows(rawAll);

      for (const item of allRows) {
        pushQuoteRow(item);
      }

      if (!rows.length) {
        // Một số tenant trả object đơn thay vì array.
        pushQuoteRow(rawAll);
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      failedMessages.push(`getPriceAllNlp: ${message}`);
      this.logger.warn(`ViettelPost getPriceAllNlp failed: ${message}`);
    }

    // getPriceAllNlp theo tài liệu VTP có thể trả danh sách dịch vụ.
    // Nhưng thực tế một số tuyến/account chỉ trả gói hệ thống tự chọn.
    // Vì vậy vẫn gọi thêm getPrice cho từng mã chính để bắt đủ gói nào khả dụng.
    const services = this.normalizeViettelServiceCodes(body);

    this.logger.log(
      `[VIETTELPOST_QUOTE_GETPRICE] services=${services.join(",")}`
    );

    for (const serviceCode of services) {
      if (rows.some((item) => item._viettelServiceCode === serviceCode)) {
        continue;
      }

      try {
        const payload = {
          PRODUCT_WEIGHT: weight,
          PRODUCT_PRICE: productPrice,
          PRODUCT_LENGTH: productLength,
          PRODUCT_WIDTH: productWidth,
          PRODUCT_HEIGHT: productHeight,
          MONEY_COLLECTION: codAmount,
          ORDER_SERVICE: serviceCode,
          ORDER_SERVICE_ADD:
            body?.serviceAdd || process.env.VIETTELPOST_SERVICE_ADD || "",
          SENDER_PROVINCE: Number(sender.provinceId || 0),
          SENDER_DISTRICT: Number(sender.districtId || 0),
          SENDER_WARD: Number(sender.wardId || 0) || undefined,
          RECEIVER_PROVINCE: Number(resolvedAny.provinceId || 0),
          RECEIVER_DISTRICT: Number(
            resolvedAny.districtId || resolvedAny.districtValue || 0
          ),
          RECEIVER_WARD: Number(resolvedAny.wardId || 0) || undefined,
          PRODUCT_TYPE: productType,
          NATIONAL_TYPE: Number(body?.nationalType || 1),
        };

        const raw = await this.viettelPostClient.getPrice(payload);
        pushQuoteRow(raw, serviceCode);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        failedMessages.push(`${serviceCode}: ${message}`);
        this.logger.warn(`ViettelPost quote failed service=${serviceCode}: ${message}`);
      }
    }

    if (!rows.length) {
      this.logger.warn(
        `ViettelPost không trả về gói cước phù hợp. ${failedMessages.join(" | ")}`
      );

      return [
        {
          serviceId: 0,
          serviceTypeId: 0,
          shortName: "Viettel Post - Chưa có gói phù hợp",
          fee: {
            total: 0,
            total_fee: 0,
            service_fee: 0,
          },
          leadtime: {
            label: "Không khả dụng",
          },
          _carrier: "viettelpost",
          _quoteKey: "viettelpost-unavailable",
          _serviceName: "VHT",
          _viettelServiceCode: "VHT",
          _viettelReceiverProvinceId: Number(resolvedAny.provinceId || 0),
          _viettelReceiverDistrictId: Number(
            resolvedAny.districtId || resolvedAny.districtValue || 0
          ),
          _viettelReceiverWardId: Number(resolvedAny.wardId || 0) || undefined,
          _disabled: true,
          _disabledReason:
            failedMessages[0] ||
            "ViettelPost chưa trả về gói cước phù hợp cho tuyến này.",
          _applyFeeToInput: false,
        },
      ];
    }

    return rows.sort((a, b) => Number(a.fee?.total || 0) - Number(b.fee?.total || 0));
  }

  async createViettelPostShipment(orderId: string, dto: any) {
    return this.prisma.$transaction(async (tx) => {
      const order = await tx.order.findUnique({
        where: { id: orderId },
        include: { items: true },
      });

      if (!order) {
        throw new BadRequestException("Không tìm thấy order");
      }

      const existingShipment = await tx.shipment.findFirst({
        where: { orderId },
      });

      if (existingShipment?.trackingCode) {
        return {
          duplicated: true,
          viettelpost: existingShipment.metadata || null,
          shipment: existingShipment,
        };
      }

      const sender = await this.getViettelSenderConfig(dto);
      const senderProvinceId = Number(dto?.senderProvinceId || sender.provinceId || 0);
      const senderDistrictId = Number(dto?.senderDistrictId || sender.districtId || 0);
      const senderWardId = Number(dto?.senderWardId || sender.wardId || 0);

      if (!senderProvinceId || !senderDistrictId) {
        throw new BadRequestException("Thiếu VIETTELPOST_SENDER_PROVINCE_ID / VIETTELPOST_SENDER_DISTRICT_ID");
      }

      const resolved =
        dto?.receiverProvinceId && dto?.receiverDistrictId
          ? {
            provinceId: Number(dto.receiverProvinceId),
            districtId: Number(dto.receiverDistrictId),
            wardId: Number(dto.receiverWardId || 0) || undefined,
          }
          : await this.resolveViettelAddress({
            province: dto?.toProvince || order.shippingProvince,
            district: dto?.toDistrict || order.shippingDistrict,
            ward: dto?.toWard || order.shippingWard,
          });

      const serviceCode = String(
        dto?.serviceCode ||
        dto?.orderService ||
        process.env.VIETTELPOST_DEFAULT_SERVICE ||
        "VCN"
      );

      const codAmount = Math.max(0, Math.round(Number(dto?.codAmount || 0)));
      const orderAny = order as any;
      const weight = Math.max(1, Number(dto?.weight || orderAny.shippingWeight || 200));
      const length = Math.max(1, Number(dto?.length || orderAny.shippingLength || 10));
      const width = Math.max(1, Number(dto?.width || orderAny.shippingWidth || 10));
      const height = Math.max(1, Number(dto?.height || orderAny.shippingHeight || 10));

      const toName = dto?.toName || order.shippingRecipientName || order.customerName || "";
      const toPhone = dto?.toPhone || order.shippingPhone || order.customerPhone || "";
      const toAddress =
        dto?.toAddress ||
        [
          order.shippingAddressLine1,
          order.shippingAddressLine2,
          order.shippingWard,
          order.shippingDistrict,
          order.shippingProvince,
        ]
          .filter(Boolean)
          .join(", ");

      const items =
        Array.isArray(dto?.items) && dto.items.length
          ? dto.items
          : (order.items || []).map((item: any) => ({
            name: item.productName || item.sku || "Sản phẩm",
            quantity: Number(item.qty || 1),
            price: Number(item.unitPrice || 0),
            weight,
          }));

      const payload = {
        ORDER_NUMBER: dto?.clientOrderCode || dto?.orderCode || order.orderCode,
        GROUPADDRESS_ID:
          Number(dto?.senderGroupAddressId || dto?.groupAddressId || sender.groupAddressId || process.env.VIETTELPOST_GROUPADDRESS_ID || 0) || undefined,
        CUS_ID: Number(sender.cusId || process.env.VIETTELPOST_CUS_ID || 0) || undefined,
        SENDER_FULLNAME: dto?.fromName || sender.name || this.returnName,
        SENDER_ADDRESS: dto?.fromAddress || sender.address || this.returnAddress,
        SENDER_PHONE: dto?.fromPhone || sender.phone || this.returnPhone,
        SENDER_WARD: senderWardId || undefined,
        SENDER_DISTRICT: senderDistrictId,
        SENDER_PROVINCE: senderProvinceId,
        RECEIVER_FULLNAME: toName,
        RECEIVER_ADDRESS: toAddress,
        RECEIVER_PHONE: toPhone,
        RECEIVER_WARD: Number(resolved.wardId || 0) || undefined,
        RECEIVER_DISTRICT: Number(resolved.districtId),
        RECEIVER_PROVINCE: Number(resolved.provinceId),
        PRODUCT_NAME: dto?.content || `Đơn ${order.orderCode}`,
        PRODUCT_DESCRIPTION: dto?.note || "",
        PRODUCT_QUANTITY: items.reduce(
          (sum: number, item: any) => sum + Number(item.quantity || item.qty || item.num || 1),
          0
        ),
        PRODUCT_PRICE: Number(dto?.insuranceValue || order.finalAmount || 0),
        PRODUCT_WEIGHT: weight,
        PRODUCT_LENGTH: length,
        PRODUCT_WIDTH: width,
        PRODUCT_HEIGHT: height,
        PRODUCT_TYPE: dto?.productType || process.env.VIETTELPOST_PRODUCT_TYPE || "HH",
        ORDER_PAYMENT: Number(dto?.orderPayment || process.env.VIETTELPOST_ORDER_PAYMENT || 3),
        ORDER_SERVICE: serviceCode,
        ORDER_SERVICE_ADD: dto?.serviceAdd || process.env.VIETTELPOST_SERVICE_ADD || "",
        MONEY_COLLECTION: codAmount,
        ORDER_NOTE: dto?.note || "",
        LIST_ITEM: items.map((item: any) => ({
          PRODUCT_NAME: item.name || item.productName || item.sku || "Sản phẩm",
          PRODUCT_PRICE: Number(item.price || 0),
          PRODUCT_WEIGHT: Number(item.weight || weight),
          PRODUCT_QUANTITY: Number(item.quantity || item.qty || item.num || 1),
        })),
      };

      const created = await this.viettelPostClient.createOrder(payload);
      const orderNumber = this.getViettelPostOrderNumber(created) || payload.ORDER_NUMBER;
      const fee = this.getViettelPostFee(created);
      const partnerStatus = this.getViettelPostStatus(created);
      const shippingStatus = this.mapViettelPostShippingStatus(partnerStatus);
      const trackingUrl = this.buildViettelPostTrackingUrl(orderNumber);

      const shipmentMetadata = {
        ...(created || {}),
        note: String(dto?.note || "").trim(),
      };

      const shipment = await tx.shipment.upsert({
        where: { orderId },
        update: {
          carrier: "VIETTELPOST",
          trackingCode: orderNumber,
          shippingStatus,
          partnerStatus,
          codAmount,
          shippingFee: fee || null,
          fromName: payload.SENDER_FULLNAME,
          fromPhone: payload.SENDER_PHONE,
          fromAddress: payload.SENDER_ADDRESS,
          toName,
          toPhone,
          toAddress,
          metadata: {
            carrier: "VIETTELPOST",
            trackingUrl,
            serviceCode,
            payload,
            response: created,
          },
          lastSyncedAt: new Date(),
        },
        create: {
          orderId,
          carrier: "VIETTELPOST",
          trackingCode: orderNumber,
          shippingStatus,
          partnerStatus,
          codAmount,
          shippingFee: fee || null,
          fromName: payload.SENDER_FULLNAME,
          fromPhone: payload.SENDER_PHONE,
          fromAddress: payload.SENDER_ADDRESS,
          toName,
          toPhone,
          toAddress,
          metadata: {
            carrier: "VIETTELPOST",
            trackingUrl,
            serviceCode,
            payload,
            response: created,
          },
          lastSyncedAt: new Date(),
        },
      });

      await this.appendShipmentTimelineEvent(tx, {
        shipmentId: shipment.id,
        orderId,
        carrier: "VIETTELPOST",
        trackingCode: orderNumber,
        status: shippingStatus,
        partnerStatus,
        title: this.timelineTitle(shippingStatus, "VIETTELPOST"),
        description: trackingUrl ? `Tracking: ${trackingUrl}` : null,
        raw: created,
        source: "create",
      });

      await tx.order.update({
        where: { id: orderId },
        data: this.withPendingCodPayment(
          {
            fulfillmentStatus: "PROCESSING",
            status: "SHIPPED",
          },
          {
            codAmount,
            paymentStatus: order.paymentStatus,
          }
        ) as any,
      });

      return {
        duplicated: false,
        viettelpost: created,
        shipment,
      };
    });
  }

  async trackViettelPostByShipmentId(id: string) {
    const shipment = await this.prisma.shipment.findUnique({
      where: { id },
    });

    if (!shipment) {
      throw new BadRequestException("Không tìm thấy phiếu giao hàng");
    }

    const trackingCode = shipment.trackingCode || "";

    if (!trackingCode) {
      throw new BadRequestException("Phiếu chưa có mã vận đơn ViettelPost");
    }

    const raw = await this.viettelPostClient.trackOrder(trackingCode);
    const partnerStatus = this.getViettelPostStatus(raw);
    const shippingStatus = this.mapViettelPostShippingStatus(partnerStatus);
    const trackingUrl = this.buildViettelPostTrackingUrl(trackingCode);

    const updated = await this.prisma.shipment.update({
      where: { id: shipment.id },
      data: {
        shippingStatus,
        partnerStatus,
        metadata: {
          carrier: "VIETTELPOST",
          trackingUrl,
          response: raw,
        },
        lastSyncedAt: new Date(),
      },
    });

    await this.appendShipmentTimelineEvent(this.prisma, {
      shipmentId: shipment.id,
      orderId: shipment.orderId,
      carrier: "VIETTELPOST",
      trackingCode,
      status: shippingStatus,
      partnerStatus,
      title: this.timelineTitle(shippingStatus, "VIETTELPOST"),
      description: trackingUrl ? `Tracking: ${trackingUrl}` : null,
      raw,
      source: "manual_refresh",
    });

    const orderForSync = await this.prisma.order.findUnique({
      where: { id: shipment.orderId },
      select: { paymentStatus: true },
    });
    const orderSyncData = this.buildCarrierOrderSyncData(shippingStatus, {
      codAmount: Number(shipment.codAmount || 0),
      paymentStatus: orderForSync?.paymentStatus,
      codReconciliationStatus: (shipment as any).codReconciliationStatus,
    });
    if (Object.keys(orderSyncData).length > 0) {
      await this.prisma.order.update({
        where: { id: shipment.orderId },
        data: orderSyncData as any,
      });
    }

    return {
      source: "viettelpost_live",
      shipment: updated,
      tracking: raw,
    };
  }

  async cancelViettelPostShipmentByOrderId(orderId: string, user: any) {
    const order = await this.prisma.order.findUnique({
      where: { id: orderId },
      include: { shipment: true },
    });

    if (!order) {
      throw new BadRequestException("Không tìm thấy order");
    }

    const shipment = order.shipment;

    if (!shipment?.trackingCode) {
      return this.prisma.order.update({
        where: { id: orderId },
        data: {
          status: "CANCELLED",
          fulfillmentStatus: "UNFULFILLED",
        },
      });
    }

    const raw = await this.viettelPostClient.cancelOrder(shipment.trackingCode);

    await this.prisma.shipment.update({
      where: { id: shipment.id },
      data: {
        shippingStatus: "CANCELLED",
        partnerStatus: "cancel",
        metadata: {
          carrier: "VIETTELPOST",
          cancelResponse: raw,
        },
        lastSyncedAt: new Date(),
      },
    });

    await this.appendShipmentTimelineEvent(this.prisma, {
      shipmentId: shipment.id,
      orderId,
      carrier: "VIETTELPOST",
      trackingCode: shipment.trackingCode,
      status: "CANCELLED",
      partnerStatus: "cancel",
      title: "Đã huỷ vận đơn",
      raw,
      source: "cancel",
    });

    return this.prisma.order.update({
      where: { id: orderId },
      data: {
        status: "CANCELLED",
        fulfillmentStatus: "UNFULFILLED",
      },
    });
  }

  private getAhamoveDefaultServices() {
    const cityPrefix = String(process.env.AHAMOVE_CITY_PREFIX || "HAN")
      .trim()
      .toUpperCase() || "HAN";

    const raw =
      process.env.AHAMOVE_SERVICES ||
      [
        `${cityPrefix}-BIKE`,
        `${cityPrefix}-2H`,
        `${cityPrefix}-TRUCK-1000`,
        `${cityPrefix}-TRUCK-2000`,
        `${cityPrefix}-TRUCK-5000`,
      ].join(",");

    return Array.from(
      new Set(
        String(raw)
          .split(",")
          .map((item) => item.trim().toUpperCase())
          .filter(Boolean)
      )
    );
  }

  private ahamoveServiceLabel(serviceId: string) {
    const code = String(serviceId || "").toUpperCase();

    if (code.includes("TRUCK-1000")) return "Xe tải 1000kg";
    if (code.includes("TRUCK-2000")) return "Xe tải 2000kg";
    if (code.includes("TRUCK-5000")) return "Xe tải 5000kg";
    if (code.includes("2H") || code.includes("SAVING") || code.includes("ECONOMY")) {
      return "Siêu Tốc - Tiết Kiệm";
    }
    if (code.includes("BIKE") || code.includes("EXPRESS")) return "Siêu Tốc";

    return serviceId || "AhaMove";
  }

  private ahamoveServiceLeadtime(serviceId: string) {
    const code = String(serviceId || "").toUpperCase();

    if (code.includes("TRUCK")) return "Xe tải";
    if (code.includes("2H") || code.includes("SAVING") || code.includes("ECONOMY")) {
      return "Giao trong 1 giờ";
    }
    if (code.includes("BIKE") || code.includes("EXPRESS")) return "Ưu tiên, giao hoả tốc";

    return "Nội thành realtime";
  }

  async quoteAhamove(body: any) {
    const fromName = body?.fromName || process.env.AHAMOVE_FROM_NAME || this.returnName;
    const fromPhone =
      body?.fromPhone || process.env.AHAMOVE_FROM_PHONE || this.returnPhone;
    const fromAddress =
      body?.fromAddress ||
      process.env.AHAMOVE_FROM_ADDRESS ||
      this.returnAddress;

    if (!fromPhone || !fromAddress) {
      throw new BadRequestException("Thiếu cấu hình AhaMove đầu gửi");
    }

    if (!body?.toName || !body?.toPhone || !body?.toAddress) {
      throw new BadRequestException("Thiếu thông tin người nhận AhaMove");
    }

    const requestedServices = Array.isArray(body?.services)
      ? body.services
      : String(
        body?.services ||
        body?.serviceIds ||
        body?.serviceId ||
        this.getAhamoveDefaultServices().join(",")
      )
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean);

    const services = Array.from(
      new Set(
        requestedServices
          .map((item: any) => String(item || "").trim().toUpperCase())
          .filter(Boolean)
      )
    ) as string[];

    const codAmount = Math.max(0, Math.round(Number(body?.codAmount || 0)));
    const itemValue = Math.max(0, Math.round(Number(body?.itemValue || codAmount || 0)));
    const weightGram = Math.max(100, Number(body?.weightGram || body?.weight || 200));
    const lengthCm = Math.max(1, Number(body?.lengthCm || body?.length || 10));
    const widthCm = Math.max(1, Number(body?.widthCm || body?.width || 10));
    const heightCm = Math.max(1, Number(body?.heightCm || body?.height || 10));

    const buildPayload = (serviceId: string) => ({
      payment_method:
        body?.payment_method ||
        body?.paymentMethod ||
        process.env.AHAMOVE_PAYMENT_METHOD ||
        "BALANCE",
      order_time: Number(body?.order_time ?? body?.orderTime ?? 0),
      path: [
        {
          address: fromAddress,
          name: fromName,
          mobile: fromPhone,
          remarks: body?.fromNote || "",
        },
        {
          address: body.toAddress,
          name: body.toName,
          mobile: body.toPhone,
          cod: codAmount,
          item_value: itemValue,
          tracking_number: body?.clientOrderCode || body?.orderCode || "",
          remarks: body?.note || "",
        },
      ],
      services: [
        {
          _id: serviceId,
          requests: Array.isArray(body?.requests) ? body.requests : [],
        },
      ],
      remarks: body?.note || "",
      items: Array.isArray(body?.items)
        ? body.items.map((item: any, index: number) => ({
          _id: String(item?._id || item?.id || index + 1),
          name: item?.name || "Sản phẩm",
          num: Number(item?.num || item?.quantity || 1),
          price: Number(item?.price || 0),
        }))
        : [],
      package_detail: [
        {
          weight: Math.max(0.1, weightGram / 1000),
          length: Math.max(0.01, lengthCm / 100),
          width: Math.max(0.01, widthCm / 100),
          height: Math.max(0.01, heightCm / 100),
          description: "Thời trang",
        },
      ],
    });

    const rows: any[] = [];

    for (const serviceId of services) {
      try {
        const raw = await this.ahamoveClient.estimate(buildPayload(serviceId));
        const items = Array.isArray(raw) ? raw : [raw];

        for (const item of items) {
          const data = item?.data || item || {};
          const rawServiceId =
            item?.service_id ||
            item?.serviceId ||
            data?.service_id ||
            data?.serviceId ||
            serviceId;

          rows.push({
            ...(item || {}),
            service_id: rawServiceId,
            serviceId: rawServiceId,
            shortName: this.ahamoveServiceLabel(String(rawServiceId)),
            leadtime: {
              label: this.ahamoveServiceLeadtime(String(rawServiceId)),
            },
            _carrier: "ahamove",
            _quoteKey: `ahamove-${rawServiceId}`,
            _serviceName: rawServiceId,
            _ahamoveServiceId: rawServiceId,
          });
        }
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        this.logger.warn(`AhaMove quote failed service=${serviceId}: ${message}`);

        rows.push({
          service_id: serviceId,
          serviceId: serviceId,
          shortName: this.ahamoveServiceLabel(serviceId),
          leadtime: {
            label: this.ahamoveServiceLeadtime(serviceId),
          },
          fee: {
            total: 0,
            total_fee: 0,
            service_fee: 0,
          },
          _carrier: "ahamove",
          _quoteKey: `ahamove-unavailable-${serviceId}`,
          _serviceName: serviceId,
          _ahamoveServiceId: serviceId,
          _disabled: true,
          _disabledReason: message || "AhaMove chưa trả về gói cước phù hợp.",
          _applyFeeToInput: false,
        });
      }
    }

    return rows;
  }


  async createAhamoveShipment(orderId: string, dto: any) {
    return this.prisma.$transaction(async (tx) => {
      const order = await tx.order.findUnique({
        where: { id: orderId },
        include: { items: true },
      });

      if (!order) {
        throw new BadRequestException("Không tìm thấy order");
      }

      const existingShipment = await tx.shipment.findFirst({
        where: { orderId },
      });

      if (existingShipment?.ahamoveOrderId || existingShipment?.trackingCode) {
        return {
          duplicated: true,
          ahamove: existingShipment.ahamoveRaw || existingShipment.metadata || null,
          shipment: existingShipment,
        };
      }

      const fromName = dto?.fromName || process.env.AHAMOVE_FROM_NAME || this.returnName;
      const fromPhone =
        dto?.fromPhone || process.env.AHAMOVE_FROM_PHONE || this.returnPhone;
      const fromAddress =
        dto?.fromAddress ||
        process.env.AHAMOVE_FROM_ADDRESS ||
        this.returnAddress;

      const serviceId = String(
        dto?.serviceId || process.env.AHAMOVE_DEFAULT_SERVICE_ID || "HAN-BIKE"
      );

      if (!fromPhone || !fromAddress) {
        throw new BadRequestException("Thiếu cấu hình AhaMove đầu gửi");
      }

      if (!dto?.toName || !dto?.toPhone || !dto?.toAddress) {
        throw new BadRequestException("Thiếu thông tin người nhận AhaMove");
      }

      const codAmount = Math.max(0, Math.round(Number(dto?.codAmount || 0)));

      const items =
        Array.isArray(dto?.items) && dto.items.length
          ? dto.items.map((item: any, index: number) => ({
            _id: String(item?._id || item?.id || index + 1),
            name: item?.name || "Sản phẩm",
            num: Number(item?.num || item?.quantity || 1),
            price: Number(item?.price || 0),
          }))
          : (order.items || []).map((item: any, index: number) => ({
            _id: String(item?.sku || item?.variantId || index + 1),
            name: item.productName || item.sku || "Sản phẩm",
            num: Number(item.qty || 1),
            price: Number(item.unitPrice || 0),
          }));

      const payload = {
        service_id: serviceId,
        requests: Array.isArray(dto?.requests) ? dto.requests : [],
        payment_method:
          dto?.payment_method ||
          dto?.paymentMethod ||
          process.env.AHAMOVE_PAYMENT_METHOD ||
          "BALANCE",
        order_time: Number(dto?.order_time ?? dto?.orderTime ?? 0),
        path: [
          {
            address: fromAddress,
            name: fromName,
            mobile: fromPhone,
          },
          {
            address: dto.toAddress,
            name: dto.toName,
            mobile: dto.toPhone,
            cod: codAmount,
          },
        ],
        remarks: dto?.note || `Đơn ${order.orderCode}`,
        items,
      };

      const created = await this.ahamoveClient.createOrder(payload);
      const ahamoveOrderId = this.getAhamoveOrderId(created);
      const ahamoveStatus = this.getAhamoveStatus(created);
      const trackingUrl = this.getAhamoveTrackingUrl(created);

      if (!ahamoveOrderId) {
        throw new BadRequestException("AhaMove không trả về order_id");
      }

      const shipmentMetadata = {
        ...(created || {}),
        note: String(dto?.note || "").trim(),
      };

      const shipment = await tx.shipment.upsert({
        where: { orderId },
        update: {
          carrier: "AHAMOVE",
          trackingCode: ahamoveOrderId,
          shippingStatus: this.mapAhamoveShippingStatus(ahamoveStatus),
          partnerStatus: ahamoveStatus,
          codAmount,
          shippingFee:
            created?.fee ||
            created?.total_fee ||
            created?.data?.fee ||
            created?.data?.total_fee ||
            null,
          fromName,
          fromPhone,
          fromAddress,
          toName: dto.toName,
          toPhone: dto.toPhone,
          toAddress: dto.toAddress,
          ahamoveOrderId,
          ahamoveTrackingUrl: trackingUrl,
          ahamoveStatus,
          ahamoveSubStatus: created?.sub_status || created?.data?.sub_status || null,
          ahamoveRaw: created,
          metadata: shipmentMetadata,
          lastSyncedAt: new Date(),
        },
        create: {
          orderId,
          carrier: "AHAMOVE",
          trackingCode: ahamoveOrderId,
          shippingStatus: this.mapAhamoveShippingStatus(ahamoveStatus),
          partnerStatus: ahamoveStatus,
          codAmount,
          shippingFee:
            created?.fee ||
            created?.total_fee ||
            created?.data?.fee ||
            created?.data?.total_fee ||
            null,
          fromName,
          fromPhone,
          fromAddress,
          toName: dto.toName,
          toPhone: dto.toPhone,
          toAddress: dto.toAddress,
          ahamoveOrderId,
          ahamoveTrackingUrl: trackingUrl,
          ahamoveStatus,
          ahamoveSubStatus: created?.sub_status || created?.data?.sub_status || null,
          ahamoveRaw: created,
          metadata: shipmentMetadata,
          lastSyncedAt: new Date(),
        },
      });

      await this.appendShipmentTimelineEvent(tx, {
        shipmentId: shipment.id,
        orderId,
        carrier: "AHAMOVE",
        trackingCode: ahamoveOrderId,
        status: this.mapAhamoveShippingStatus(ahamoveStatus) || "CREATED",
        partnerStatus: ahamoveStatus,
        title: this.timelineTitle(this.mapAhamoveShippingStatus(ahamoveStatus) || "CREATED", "AHAMOVE"),
        description: trackingUrl ? `Tracking: ${trackingUrl}` : null,
        raw: created,
        source: "create",
      });

      await tx.order.update({
        where: { id: orderId },
        data: this.withPendingCodPayment(
          {
            fulfillmentStatus: "PROCESSING",
            status: "SHIPPED",
          },
          {
            codAmount,
            paymentStatus: order.paymentStatus,
          }
        ) as any,
      });

      return {
        duplicated: false,
        ahamove: created,
        shipment,
      };
    }, { timeout: 20000, maxWait: 10000 });
  }

  async trackAhamoveByShipmentId(id: string) {
    const shipment = await this.prisma.shipment.findUnique({
      where: { id },
    });

    if (!shipment) {
      throw new BadRequestException("Không tìm thấy phiếu giao hàng");
    }

    const ahamoveOrderId = shipment.ahamoveOrderId || shipment.trackingCode;

    if (!ahamoveOrderId) {
      throw new BadRequestException("Phiếu chưa có mã đơn AhaMove");
    }

    const raw = await this.ahamoveClient.getOrderDetail(ahamoveOrderId);
    const ahamoveStatus = this.getAhamoveStatus(raw);
    const trackingUrl = this.getAhamoveTrackingUrl(raw);

    const shippingStatus = this.mapAhamoveShippingStatus(ahamoveStatus);
    const driver = this.extractAhamoveDriver(raw);

    const updated = await this.prisma.shipment.update({
      where: { id: shipment.id },
      data: {
        shippingStatus,
        partnerStatus: ahamoveStatus,
        ahamoveStatus,
        ahamoveSubStatus: raw?.sub_status || raw?.data?.sub_status || null,
        ahamoveTrackingUrl: trackingUrl,
        ahamoveRaw: raw,
        metadata: raw,
        lastSyncedAt: new Date(),
      },
    });

    await this.appendShipmentTimelineEvent(this.prisma, {
      shipmentId: shipment.id,
      orderId: shipment.orderId,
      carrier: "AHAMOVE",
      trackingCode: ahamoveOrderId,
      status: shippingStatus,
      partnerStatus: ahamoveStatus,
      title: this.timelineTitle(shippingStatus, "AHAMOVE"),
      description: trackingUrl ? `Tracking: ${trackingUrl}` : null,
      raw,
      source: "manual_refresh",
      ...driver,
    });

    const orderForSync = await this.prisma.order.findUnique({
      where: { id: shipment.orderId },
      select: { paymentStatus: true },
    });
    const orderSyncData = this.buildAhamoveOrderSyncData(shippingStatus, {
      codAmount: Number(shipment.codAmount || 0),
      paymentStatus: orderForSync?.paymentStatus,
      codReconciliationStatus: (shipment as any).codReconciliationStatus,
    });
    if (Object.keys(orderSyncData).length > 0) {
      await this.prisma.order.update({
        where: { id: shipment.orderId },
        data: orderSyncData as any,
      });
    }

    return {
      source: "ahamove_live",
      shipment: updated,
      tracking: raw,
    };
  }

  async cancelAhamoveShipmentByOrderId(orderId: string, user: any) {
    const order = await this.prisma.order.findUnique({
      where: { id: orderId },
      include: {
        shipment: true,
      },
    });

    if (!order) {
      throw new BadRequestException("Không tìm thấy order");
    }

    const shipment = order.shipment;

    if (!shipment?.ahamoveOrderId && !shipment?.trackingCode) {
      return this.prisma.order.update({
        where: { id: orderId },
        data: {
          status: "CANCELLED",
          fulfillmentStatus: "UNFULFILLED",
        },
      });
    }

    const ahamoveOrderId = shipment.ahamoveOrderId || shipment.trackingCode || "";
    const raw = await this.ahamoveClient.cancelOrder(ahamoveOrderId);

    await this.prisma.shipment.update({
      where: { id: shipment.id },
      data: {
        shippingStatus: "CANCELLED",
        partnerStatus: "cancel",
        ahamoveStatus: "CANCELLED",
        ahamoveRaw: raw,
        metadata: raw,
        lastSyncedAt: new Date(),
      },
    });

    return this.prisma.order.update({
      where: { id: orderId },
      data: {
        status: "CANCELLED",
        fulfillmentStatus: "UNFULFILLED",
      },
    });
  }



  async handleAhamoveWebhook(body: any, headers?: any) {
    const ahamoveOrderId =
      body?.order_id ||
      body?.id ||
      body?.data?.order_id ||
      body?.data?.id ||
      body?.order?.order_id ||
      body?.order?.id ||
      body?.trackingCode ||
      "";

    if (!ahamoveOrderId) {
      this.logger.warn(
        `[AHAMOVE_WEBHOOK] missing order id body=${JSON.stringify(body || {})}`
      );

      return {
        ok: true,
        ignored: true,
        reason: "missing_ahamove_order_id",
      };
    }

    const ahamoveStatus = this.getAhamoveStatus(body);
    const trackingUrl = this.getAhamoveTrackingUrl(body);
    const shippingStatus = this.mapAhamoveShippingStatus(ahamoveStatus);
    const subStatus =
      body?.sub_status ||
      body?.subStatus ||
      body?.data?.sub_status ||
      body?.data?.subStatus ||
      null;

    const shipment = await this.prisma.shipment.findFirst({
      where: {
        OR: [
          { ahamoveOrderId },
          { trackingCode: ahamoveOrderId },
        ],
      },
      include: {
        order: true,
      },
    });

    if (!shipment) {
      this.logger.warn(
        `[AHAMOVE_WEBHOOK] shipment not found ahamoveOrderId=${ahamoveOrderId}`
      );

      return {
        ok: true,
        ignored: true,
        reason: "shipment_not_found",
        ahamoveOrderId,
      };
    }

    const updatedShipment = await this.prisma.shipment.update({
      where: { id: shipment.id },
      data: {
        carrier: "AHAMOVE",
        shippingStatus,
        partnerStatus: ahamoveStatus,
        ahamoveOrderId,
        ahamoveTrackingUrl: trackingUrl,
        ahamoveStatus,
        ahamoveSubStatus: subStatus,
        ahamoveRaw: body,
        metadata: body,
        lastSyncedAt: new Date(),
      },
    });

    const driver = this.extractAhamoveDriver(body);

    await this.appendShipmentTimelineEvent(this.prisma, {
      shipmentId: shipment.id,
      orderId: shipment.orderId,
      carrier: "AHAMOVE",
      trackingCode: ahamoveOrderId,
      status: shippingStatus,
      partnerStatus: ahamoveStatus,
      title: this.timelineTitle(shippingStatus, "AHAMOVE"),
      description: trackingUrl ? `Tracking: ${trackingUrl}` : null,
      raw: body,
      source: "webhook",
      ...driver,
    });

    const orderForSync = await this.prisma.order.findUnique({
      where: { id: shipment.orderId },
      select: { paymentStatus: true },
    });
    const nextOrderData: any = this.buildAhamoveOrderSyncData(shippingStatus, {
      codAmount: Number(shipment.codAmount || 0),
      paymentStatus: orderForSync?.paymentStatus,
      codReconciliationStatus: (shipment as any).codReconciliationStatus,
    });

    if (Object.keys(nextOrderData).length > 0) {
      await this.prisma.order.update({
        where: { id: shipment.orderId },
        data: nextOrderData,
      });
    }

    try {
      await this.prisma.ahamoveShipment.upsert({
        where: { shipmentId: shipment.id },
        update: {
          ahamoveOrderId,
          serviceId: String(
            body?.service_id ||
            body?.serviceId ||
            body?.data?.service_id ||
            body?.data?.serviceId ||
            ""
          ) || null,
          status: ahamoveStatus,
          subStatus,
          trackingUrl,
          sharedLink:
            body?.shared_link ||
            body?.sharedLink ||
            body?.data?.shared_link ||
            body?.data?.sharedLink ||
            null,
          raw: body,
          lastSyncedAt: new Date(),
        },
        create: {
          shipmentId: shipment.id,
          orderId: shipment.orderId,
          ahamoveOrderId,
          serviceId: String(
            body?.service_id ||
            body?.serviceId ||
            body?.data?.service_id ||
            body?.data?.serviceId ||
            ""
          ) || null,
          status: ahamoveStatus,
          subStatus,
          trackingUrl,
          sharedLink:
            body?.shared_link ||
            body?.sharedLink ||
            body?.data?.shared_link ||
            body?.data?.sharedLink ||
            null,
          raw: body,
          lastSyncedAt: new Date(),
        },
      });
    } catch (error) {
      this.logger.warn(
        `[AHAMOVE_WEBHOOK] cannot sync AhamoveShipment table: ${error instanceof Error ? error.message : String(error)
        }`
      );
    }

    return {
      ok: true,
      ahamoveOrderId,
      status: ahamoveStatus,
      shippingStatus,
      shipment: updatedShipment,
    };
  }


  async getByOrder(orderId: string) {
    return this.prisma.shipment.findFirst({
      where: { orderId },
    });
  }

  async getWarRoomDeliveryRevenue(query: any = {}) {
    const range = String(query?.range || "today");
    const now = new Date();
    const pad = (n: number) => String(n).padStart(2, "0");
    const ymd = (date: Date) => `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;

    const defaultRange = (() => {
      const today = new Date(now);
      if (range === "yesterday") {
        const d = new Date(now);
        d.setDate(d.getDate() - 1);
        return { fromDate: ymd(d), toDate: ymd(d) };
      }
      if (range === "7d" || range === "10d" || range === "30d") {
        const days = range === "7d" ? 7 : range === "10d" ? 10 : 30;
        const from = new Date(now);
        from.setDate(from.getDate() - (days - 1));
        return { fromDate: ymd(from), toDate: ymd(today) };
      }
      return { fromDate: ymd(today), toDate: ymd(today) };
    })();

    const fromDate = String(query?.fromDate || query?.dateFrom || query?.startDate || defaultRange.fromDate);
    const toDate = String(query?.toDate || query?.dateTo || query?.endDate || defaultRange.toDate);
    const start = new Date(`${fromDate}T00:00:00.000`);
    const end = new Date(`${toDate}T23:59:59.999`);
    const safeStart = Number.isNaN(start.getTime()) ? new Date(`${defaultRange.fromDate}T00:00:00.000`) : start;
    const safeEnd = Number.isNaN(end.getTime()) ? new Date(`${defaultRange.toDate}T23:59:59.999`) : end;

    const normalize = (input?: string | null) =>
      String(input || "")
        .trim()
        .toLowerCase()
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .replace(/đ/g, "d")
        .replace(/Đ/g, "D")
        .replace(/[\s_-]+/g, " ");

    const isPosOrder = (order: any) => {
      const raw = normalize(`${order?.salesChannel || ""} ${order?.channel || ""} ${order?.orderType || ""} ${order?.paymentMethod || ""}`);
      return raw.includes("pos") || raw.includes("ban le") || raw.includes("retail") || raw.includes("quay");
    };

    const isFacebookOrder = (order: any) => {
      const raw = normalize(`${order?.salesChannel || ""} ${order?.channel || ""} ${order?.orderType || ""} ${order?.paymentMethod || ""} ${order?.paymentType || ""}`);
      return raw.includes("facebook") || raw.includes("fb") || raw.includes("meta") || raw.includes("cod");
    };

    const amountOf = (order: any) => Number(order?.finalAmount ?? order?.totalAmount ?? order?.amount ?? 0) || 0;
    const isPaid = (order: any) => {
      const raw = normalize(`${order?.paymentStatus || ""} ${order?.status || ""} ${order?.fulfillmentStatus || ""}`);
      return raw.includes("paid") || raw.includes("completed") || raw.includes("fulfilled") || raw.includes("da thanh toan");
    };

    const isCancelled = (order: any) => {
      const raw = normalize([
        order?.status,
        order?.fulfillmentStatus,
        order?.deliveryStatus,
        order?.shippingStatus,
        order?.shipmentStatus,
        order?.trackingStatus,
        order?.carrierStatus,
        order?.carrierStatusName,
        order?.ghnStatus,
        order?.shipment?.shippingStatus,
        order?.shipment?.partnerStatus,
      ].filter(Boolean).join(" "));
      return (
        raw.includes("cancel") ||
        raw.includes("cancelled") ||
        raw.includes("canceled") ||
        raw.includes("huy") ||
        raw.includes("da huy")
      );
    };

    const shippingSignal = (order: any) =>
      normalize([
        order?.status,
        order?.fulfillmentStatus,
        order?.deliveryStatus,
        order?.shippingStatus,
        order?.shipmentStatus,
        order?.trackingStatus,
        order?.carrierStatus,
        order?.carrierStatusName,
        order?.ghnStatus,
        order?.codStatus,
        order?.shipment?.shippingStatus,
        order?.shipment?.partnerStatus,
        order?.shipment?.ahamoveStatus,
        order?.shipment?.ahamoveSubStatus,
      ].filter(Boolean).join(" "));

    const isDelivered = (order: any) => {
      const raw = shippingSignal(order);
      if (
        raw.includes("khong thanh cong") ||
        raw.includes("that bai") ||
        raw.includes("failed") ||
        raw.includes("fail") ||
        raw.includes("return") ||
        raw.includes("hoan")
      ) {
        return false;
      }
      return (
        raw.includes("delivered") ||
        raw.includes("delivery success") ||
        raw.includes("completed") ||
        raw.includes("complete") ||
        raw.includes("success") ||
        raw.includes("giao hang thanh cong") ||
        raw.includes("giao thanh cong") ||
        raw.includes("da giao") ||
        raw.includes("fulfilled")
      );
    };

    const inRange = (value?: Date | string | null) => {
      if (!value) return false;
      const d = new Date(value);
      if (Number.isNaN(d.getTime())) return false;
      return d >= safeStart && d <= safeEnd;
    };

    const orderSelectInclude: any = {
      shipment: true,
      items: {
        include: {
          variant: { select: { id: true, sku: true, costPrice: true, price: true } },
        },
      },
    };

    const orderClient = (this.prisma as any).order;
    const timelineClient = (this.prisma as any).shipmentTimelineEvent;

    const createdOrders = await orderClient.findMany({
      where: { createdAt: { gte: safeStart, lte: safeEnd } },
      include: orderSelectInclude,
      orderBy: { createdAt: "desc" },
      take: 5000,
    });

    const rawDeliveredEvents = timelineClient?.findMany
      ? await timelineClient.findMany({
          where: { status: "DELIVERED", eventTime: { gte: safeStart, lte: safeEnd } },
          select: { orderId: true, shipmentId: true, eventTime: true, source: true },
          orderBy: { eventTime: "desc" },
          take: 10000,
        })
      : [];

    const deliveredEvents = (rawDeliveredEvents || []).filter((event: any) => {
      const source = String(event?.source || "").toLowerCase();
      const isSynthetic =
        source.includes("manual_refresh") ||
        source.includes("refresh_all") ||
        source.includes("backfill") ||
        source.includes("cron") ||
        source === "system";

      // Loại event tổng hợp do refresh/backfill/cron tạo tại thời điểm đồng bộ.
      // Timeline chỉ dùng làm fallback khi đơn không có finish_date từ GHN.
      return !isSynthetic && (source.includes("carrier") || source.includes("ghn_") || source.includes("webhook"));
    });

    const deliveredOrderIds = Array.from(
      new Set(
        (deliveredEvents || [])
          .map((event: any) => event?.orderId)
          .filter((id: any) => typeof id === "string" && id.length > 0)
      )
    );
    const deliveredOrderIdSet = new Set(deliveredOrderIds);

    const deliveredShipmentIds = Array.from(
      new Set(
        (deliveredEvents || [])
          .map((event: any) => event?.shipmentId)
          .filter((id: any) => typeof id === "string" && id.length > 0)
      )
    );
    const deliveredShipmentIdSet = new Set(deliveredShipmentIds);

    const deliveredEventByOrderId = new Map<string, any>();
    const deliveredEventByShipmentId = new Map<string, any>();
    for (const event of deliveredEvents || []) {
      const orderId = String(event?.orderId || "");
      const shipmentId = String(event?.shipmentId || "");
      if (orderId && !deliveredEventByOrderId.has(orderId)) deliveredEventByOrderId.set(orderId, event);
      if (shipmentId && !deliveredEventByShipmentId.has(shipmentId)) deliveredEventByShipmentId.set(shipmentId, event);
    }

    const pickFromObject = (source: any, keys: string[]) => {
      if (!source || typeof source !== "object") return null;
      for (const key of keys) {
        const value = source?.[key];
        if (value) return value;
      }
      return null;
    };

    const explicitDeliveryDateCandidates = (order: any) => {
      const shipment = order?.shipment || {};
      const metadata = shipment?.metadata && typeof shipment.metadata === "object" ? shipment.metadata : {};
      const publicTracking = metadata?.publicTracking || metadata?.public_tracking || {};
      const publicRaw = publicTracking?.raw || {};

      return [
        { value: order?.deliveredAt, source: "order.deliveredAt" },
        { value: order?.deliveryCompletedAt, source: "order.deliveryCompletedAt" },
        { value: order?.deliverySuccessAt, source: "order.deliverySuccessAt" },
        { value: order?.shippingCompletedAt, source: "order.shippingCompletedAt" },
        { value: order?.shippedSuccessAt, source: "order.shippedSuccessAt" },
        { value: shipment?.deliveredAt, source: "shipment.deliveredAt" },
        { value: shipment?.deliveryCompletedAt, source: "shipment.deliveryCompletedAt" },
        { value: shipment?.deliverySuccessAt, source: "shipment.deliverySuccessAt" },
        { value: shipment?.shippingCompletedAt, source: "shipment.shippingCompletedAt" },
        { value: shipment?.completedAt, source: "shipment.completedAt" },
        { value: shipment?.deliveredTime, source: "shipment.deliveredTime" },
        { value: shipment?.completedTime, source: "shipment.completedTime" },
        {
          value: pickFromObject(metadata, [
            "finish_date",
            "finishDate",
            "delivered_at",
            "deliveredAt",
            "delivery_completed_at",
            "deliveryCompletedAt",
            "delivery_success_at",
            "deliverySuccessAt",
            "completed_at",
            "completedAt",
          ]),
          source: "shipment.metadata.finish_date",
        },
        {
          value: pickFromObject(publicRaw, [
            "finish_date",
            "finishDate",
            "delivered_at",
            "deliveredAt",
            "delivery_completed_at",
            "deliveryCompletedAt",
            "completed_at",
            "completedAt",
          ]),
          source: "shipment.metadata.publicTracking.raw.finish_date",
        },
      ];
    };

    const normalizeDeliverySource = (source: string) =>
      source === "shipment.metadata.finish_date" ? "ghn.finish_date" : source;

    const getExplicitDeliveryInfo = (order: any) => {
      // Ưu tiên tuyệt đối mốc giao thành công thật của GHN/carrier.
      // Nếu finish_date nằm ngoài khoảng đang xem thì đơn đó KHÔNG được fallback về timeline manual_refresh trong ngày khác.
      for (const candidate of explicitDeliveryDateCandidates(order)) {
        if (!candidate.value) continue;
        const date = new Date(candidate.value);
        if (Number.isNaN(date.getTime())) continue;
        return {
          deliveryDate: candidate.value,
          deliveryDateSource: normalizeDeliverySource(candidate.source),
        };
      }
      return null;
    };

    const getTimelineDeliveryInfo = (order: any) => {
      const orderId = String(order?.id || "");
      const shipmentId = String(order?.shipment?.id || "");
      const event =
        (orderId && deliveredEventByOrderId.get(orderId)) ||
        (shipmentId && deliveredEventByShipmentId.get(shipmentId));

      if (event?.eventTime) {
        return {
          deliveryDate: event.eventTime,
          deliveryDateSource: `timeline:${event.source || "carrier"}`,
        };
      }

      return null;
    };

    const getDeliveryDebugInfo = (order: any) => {
      return (
        getExplicitDeliveryInfo(order) ||
        getTimelineDeliveryInfo(order) ||
        { deliveryDate: null, deliveryDateSource: null }
      );
    };

    const hasDeliveryDateInSelectedRange = (order: any) => {
      const info = getDeliveryDebugInfo(order);
      return Boolean(info.deliveryDate && inRange(info.deliveryDate));
    };

    const recentStart = new Date(safeStart);
    recentStart.setDate(recentStart.getDate() - 90);

    const recentOrders = await orderClient.findMany({
      where: { createdAt: { gte: recentStart, lte: safeEnd } },
      include: orderSelectInclude,
      orderBy: { createdAt: "desc" },
      take: 8000,
    });

    const successOrders = recentOrders.filter((order: any) => {
      if (isCancelled(order)) return false;
      if (!isDelivered(order)) return false;

      // Chỉ tính doanh thu giao thành công vào đúng ngày giao thật.
      // Ưu tiên finish_date của GHN; timeline carrier chỉ là fallback.
      // Không dùng updatedAt/lastSyncedAt và không cho timeline manual_refresh kéo đơn sang sai ngày.
      return hasDeliveryDateInSelectedRange(order);
    });

    const posCreated = createdOrders.filter(isPosOrder);
    const facebookCreated = createdOrders.filter((order: any) => !isPosOrder(order) && isFacebookOrder(order));
    const otherCreated = createdOrders.filter((order: any) => !isPosOrder(order) && !isFacebookOrder(order));

    const posSuccess = createdOrders.filter((order: any) => !isCancelled(order) && isPosOrder(order) && (isPaid(order) || isDelivered(order)));
    const facebookDelivered = successOrders.filter((order: any) => !isPosOrder(order) && isFacebookOrder(order) && isDelivered(order));
    const otherDelivered = successOrders.filter((order: any) => !isPosOrder(order) && !isFacebookOrder(order) && isDelivered(order));

    const sumAmount = (orders: any[]) => orders.reduce((sum, order) => sum + amountOf(order), 0);

    const qtyOfItem = (item: any) => Number(item?.qty ?? item?.quantity ?? 0) || 0;
    const unitCostOfItem = (item: any) => {
      const candidates = [
        item?.costPrice,
        item?.unitCost,
        item?.cost,
        item?.purchasePrice,
        item?.variant?.costPrice,
        item?.variant?.purchasePrice,
        item?.variant?.importPrice,
      ];
      for (const value of candidates) {
        const parsed = Number(value || 0);
        if (Number.isFinite(parsed) && parsed > 0) return parsed;
      }
      return 0;
    };
    const costOfOrder = (order: any) => {
      const items = Array.isArray(order?.items) ? order.items : [];
      return items.reduce((sum: number, item: any) => sum + unitCostOfItem(item) * qtyOfItem(item), 0);
    };
    const sumCost = (orders: any[]) => orders.reduce((sum, order) => sum + costOfOrder(order), 0);

    const normalizeOrder = (order: any) => ({
      ...order,
      shippingStatus: order?.shipment?.shippingStatus || order?.shippingStatus || null,
      shipmentStatus: order?.shipment?.shippingStatus || order?.shipmentStatus || null,
      carrierStatus: order?.shipment?.partnerStatus || order?.carrierStatus || null,
      carrierStatusName: order?.shipment?.partnerStatus || order?.carrierStatusName || null,
      ahamoveStatus: order?.shipment?.ahamoveStatus || null,
      ahamoveSubStatus: order?.shipment?.ahamoveSubStatus || null,
    });

    const debugDeliveredSamples = facebookDelivered.slice(0, 20).map((order: any) => {
      const debug = getDeliveryDebugInfo(order);
      return {
        orderId: order?.id || null,
        orderCode: order?.orderCode || order?.code || null,
        salesChannel: order?.salesChannel || order?.channel || null,
        trackingCode: order?.shipment?.trackingCode || null,
        amount: amountOf(order),
        cost: costOfOrder(order),
        costRate: amountOf(order) > 0 ? Number(((costOfOrder(order) / amountOf(order)) * 100).toFixed(1)) : 0,
        status: order?.status || null,
        fulfillmentStatus: order?.fulfillmentStatus || null,
        shippingStatus: order?.shipment?.shippingStatus || null,
        partnerStatus: order?.shipment?.partnerStatus || null,
        deliveryDate: debug.deliveryDate,
        deliveryDateSource: debug.deliveryDateSource,
        shipmentUpdatedAt: order?.shipment?.updatedAt || null,
        lastSyncedAt: order?.shipment?.lastSyncedAt || null,
        metadataFinishDate: order?.shipment?.metadata?.finish_date || order?.shipment?.metadata?.finishDate || null,
      };
    });

    const localDayKey = (value?: Date | string | null) => {
      if (!value) return "";
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return "";
      return ymd(date);
    };

    const upsertDailySuccessRow = (
      map: Map<string, any>,
      dateKey: string,
      bucket: "pos" | "facebook" | "other",
      order: any,
    ) => {
      if (!dateKey) return;
      const current =
        map.get(dateKey) || {
          date: dateKey,
          successOrders: 0,
          successAmount: 0,
          successCost: 0,
          posOrders: 0,
          posAmount: 0,
          posCost: 0,
          facebookDeliveredOrders: 0,
          facebookDeliveredAmount: 0,
          facebookDeliveredCost: 0,
          otherDeliveredOrders: 0,
          otherDeliveredAmount: 0,
          otherDeliveredCost: 0,
        };

      const amount = amountOf(order);
      const cost = costOfOrder(order);
      current.successOrders += 1;
      current.successAmount += amount;
      current.successCost += cost;

      if (bucket === "pos") {
        current.posOrders += 1;
        current.posAmount += amount;
        current.posCost += cost;
      } else if (bucket === "facebook") {
        current.facebookDeliveredOrders += 1;
        current.facebookDeliveredAmount += amount;
        current.facebookDeliveredCost += cost;
      } else {
        current.otherDeliveredOrders += 1;
        current.otherDeliveredAmount += amount;
        current.otherDeliveredCost += cost;
      }

      map.set(dateKey, current);
    };

    const dailySuccessMap = new Map<string, any>();

    for (const order of posSuccess) {
      const posDate =
        order?.soldAt ||
        order?.paidAt ||
        order?.completedAt ||
        order?.updatedAt ||
        order?.createdAt;
      const dateKey = localDayKey(posDate);
      if (dateKey && inRange(posDate)) {
        upsertDailySuccessRow(dailySuccessMap, dateKey, "pos", order);
      }
    }

    for (const order of facebookDelivered) {
      const debug = getDeliveryDebugInfo(order);
      const dateKey = localDayKey(debug.deliveryDate);
      if (dateKey) {
        upsertDailySuccessRow(dailySuccessMap, dateKey, "facebook", order);
      }
    }

    for (const order of otherDelivered) {
      const debug = getDeliveryDebugInfo(order);
      const dateKey = localDayKey(debug.deliveryDate);
      if (dateKey) {
        upsertDailySuccessRow(dailySuccessMap, dateKey, "other", order);
      }
    }

    const dailySuccessRows = Array.from(dailySuccessMap.values()).sort((a, b) =>
      String(a.date) < String(b.date) ? 1 : -1,
    );

    return {
      range,
      fromDate: ymd(safeStart),
      toDate: ymd(safeEnd),
      generatedAt: new Date().toISOString(),
      orderCreated: {
        total: createdOrders.length,
        amount: sumAmount(createdOrders),
        pos: { orders: posCreated.length, amount: sumAmount(posCreated) },
        facebook: { orders: facebookCreated.length, amount: sumAmount(facebookCreated) },
        other: { orders: otherCreated.length, amount: sumAmount(otherCreated) },
      },
      revenueSuccess: {
        totalOrders: posSuccess.length + facebookDelivered.length + otherDelivered.length,
        totalAmount: sumAmount(posSuccess) + sumAmount(facebookDelivered) + sumAmount(otherDelivered),
        totalCost: sumCost(posSuccess) + sumCost(facebookDelivered) + sumCost(otherDelivered),
        pos: { orders: posSuccess.length, amount: sumAmount(posSuccess), cost: sumCost(posSuccess) },
        facebookDelivered: { orders: facebookDelivered.length, amount: sumAmount(facebookDelivered), cost: sumCost(facebookDelivered) },
        otherDelivered: { orders: otherDelivered.length, amount: sumAmount(otherDelivered), cost: sumCost(otherDelivered) },
      },
      dailySuccessRows,
      createdOrders: createdOrders.map(normalizeOrder),
      successOrders: [...posSuccess, ...facebookDelivered, ...otherDelivered].map(normalizeOrder),
      debugDeliveredSamples,
      deliverySource: {
        deliveredTimelineEvents: deliveredEvents?.length || 0,
        ignoredSyntheticDeliveredEvents: Math.max((rawDeliveredEvents?.length || 0) - (deliveredEvents?.length || 0), 0),
        note: "POS thành công lấy từ đơn POS đã paid/completed. Facebook giao thành công lấy theo mốc giao thật từ GHN/timeline carrier hoặc finish_date, không lấy updatedAt/lastSyncedAt của cron/backfill. Giá vốn lấy theo đúng tập đơn thành công trong ngày.",
      },
    };
  }


  async track(dto: TrackShipmentDto) {
    return this.ghnClient.getOrderDetail(dto.orderCode, dto.clientOrderCode);
  }

  async ghnProvinces() {
    return this.ghnClient.getProvinces();
  }

  async ghnDistricts(provinceId?: number) {
    return this.ghnClient.getDistricts(provinceId);
  }

  async ghnWards(districtId: number) {
    return this.ghnClient.getWards(districtId);
  }
}