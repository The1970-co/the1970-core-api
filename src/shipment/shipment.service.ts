import { BadRequestException, Injectable, Logger } from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";
import { GhnClient } from "./ghn.client";
import { AhamoveClient } from "./ahamove.client";
import { ViettelPostClient } from "./viettelpost.client";
import { QuoteShipmentDto } from "./dto/quote-shipment.dto";
import { CreateGhnShipmentDto } from "./dto/create-ghn-shipment.dto";
import { TrackShipmentDto } from "./dto/track-shipment.dto";
import { AuthTotpService } from "../auth-totp/auth-totp.service";

@Injectable()
export class ShipmentService {
  private readonly logger = new Logger(ShipmentService.name);
  private readonly trackingCacheMinutes = Number(
    process.env.SHIPMENT_TRACKING_CACHE_MINUTES || 12
  );

  constructor(
    private readonly prisma: PrismaService,
    private readonly ghnClient: GhnClient,
    private readonly ahamoveClient: AhamoveClient,
    private readonly viettelPostClient: ViettelPostClient,
    private readonly authTotpService: AuthTotpService
  ) {}

  private readonly fromDistrictId = Number(process.env.GHN_FROM_DISTRICT_ID || 0);
  private readonly fromWardCode = process.env.GHN_FROM_WARD_CODE || "";
  private readonly returnPhone = process.env.GHN_RETURN_PHONE || "";
  private readonly returnAddress = process.env.GHN_RETURN_ADDRESS || "";
  private readonly returnName = process.env.GHN_RETURN_NAME || "The 1970";

  private mapShippingStatus(input?: string | null) {
    const value = String(input || "").toLowerCase();

    if (!value) return "NOT_CREATED";
    if (value.includes("cancel")) return "CANCELLED";
    if (value.includes("deliver")) return "DELIVERING";
    if (value.includes("pick")) return "PICKING";
    if (value.includes("transit") || value.includes("sorting")) return "IN_TRANSIT";
    if (value.includes("complete") || value.includes("success")) return "DELIVERED";
    if (value.includes("fail") || value.includes("return")) return "FAILED";
    if (value.includes("create") || value.includes("ready")) return "CREATED";

    return value.toUpperCase();
  }

  private normalizeTimelineStatus(status?: string | null) {
    const s = String(status || "").toUpperCase();

    if (!s) return "UNKNOWN";
    if (s.includes("DELIVERED") || s.includes("COMPLETED") || s.includes("SUCCESS")) return "DELIVERED";
    if (s.includes("DELIVERING") || s.includes("IN_PROCESS") || s.includes("IN PROCESS")) return "DELIVERING";
    if (s.includes("PICKING") || s.includes("ACCEPTED") || s.includes("PICKED")) return "PICKING";
    if (s.includes("CREATED") || s.includes("READY") || s.includes("ASSIGNING") || s.includes("IDLE")) return "CREATED";
    if (s.includes("CANCEL")) return "CANCELLED";
    if (s.includes("FAIL")) return "FAILED";
    if (s.includes("RETURN")) return "RETURNING";
    if (s.includes("TRANSIT") || s.includes("SORT")) return "IN_TRANSIT";

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
  }) {
    const status = this.normalizeTimelineStatus(input.status);
    const latest = await (client as any).shipmentTimelineEvent.findFirst({
      where: { shipmentId: input.shipmentId },
      orderBy: { eventTime: "desc" },
    });

    const sameStatus = latest?.status === status;
    const samePartnerStatus =
      String(latest?.partnerStatus || "") === String(input.partnerStatus || "");

    if (sameStatus && samePartnerStatus) {
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
        eventTime: new Date(),
      },
    });
  }

  private mapStatusLabel(status: string) {
    const s = String(status || "").toUpperCase();

    if (s.includes("DELIVERED") || s.includes("SUCCESS")) {
      return "Giao hàng thành công";
    }

    if (s.includes("DELIVERING")) {
      return "Đang giao hàng";
    }

    if (s.includes("TRANSIT") || s.includes("SORT")) {
      return "Đang trung chuyển";
    }

    if (s.includes("PICKING")) {
      return "Đang lấy hàng";
    }

    if (s.includes("READY")) {
      return "Chờ lấy hàng";
    }

    if (s.includes("CANCEL")) {
      return "Đã hủy đơn";
    }

    if (s.includes("RETURN")) {
      return "Đang hoàn hàng";
    }

    if (s.includes("FAIL")) {
      return "Giao thất bại";
    }

    return "Cập nhật vận đơn";
  }

  private normalizeTimeline(raw: any) {
    const logs = Array.isArray(raw?.log) ? raw.log : [];

    const sorted = [...logs].sort((a, b) => {
      const ta = new Date(a?.updated_date || a?.action_at || 0).getTime();
      const tb = new Date(b?.updated_date || b?.action_at || 0).getTime();
      return tb - ta;
    });

    return sorted.map((item: any, index: number) => {
      const status = String(item?.status || "");

      return {
        id: `${index}-${status || "log"}`,
        status,
        title: item?.status_name || this.mapStatusLabel(status),
        description: item?.description || item?.reason || "",
        location: item?.location || item?.hub_name || item?.area || "",
        time:
          item?.updated_date ||
          item?.action_at ||
          item?.created_date ||
          "",
      };
    });
  }

  private normalizeTracking(raw: any, shipment: any) {
    const timeline = this.normalizeTimeline(raw);

    const partnerStatus =
      raw?.status ||
      raw?.status_name ||
      raw?.current_status ||
      shipment?.shippingStatus ||
      "UNKNOWN";

    return {
      trackingCode: shipment?.trackingCode || raw?.order_code || "",
      carrier: shipment?.carrier || "GHN",
      shippingStatus: this.mapShippingStatus(partnerStatus),
      partnerStatus: raw?.status_name || this.mapStatusLabel(partnerStatus),
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
      throw new BadRequestException("Phiếu giao hàng chưa có mã vận đơn");
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
      await this.trackViettelPostByShipmentId(shipment.id);

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

      await this.prisma.$transaction(async (tx) => {
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
      });

      const orderSyncData = this.buildAhamoveOrderSyncData(shippingStatus);
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

    const raw = await this.ghnClient.getOrderDetail(shipment.trackingCode || "");
    const normalized = this.normalizeTracking(raw, shipment);

    const expiresAt = new Date(
      now.getTime() + this.trackingCacheMinutes * 60 * 1000
    );

    await this.prisma.$transaction(async (tx) => {
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
          metadata: raw,
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
        raw,
        source: force ? "manual_refresh" : "polling",
      });
    });

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
    if (!this.fromDistrictId) {
      throw new BadRequestException("Thiếu GHN_FROM_DISTRICT_ID");
    }

    if (!this.fromWardCode) {
      throw new BadRequestException("Thiếu GHN_FROM_WARD_CODE");
    }

    const services = await this.ghnClient.getAvailableServices(
      this.fromDistrictId,
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
          from_district_id: this.fromDistrictId,
          from_ward_code: this.fromWardCode,
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
          from_district_id: this.fromDistrictId,
          from_ward_code: this.fromWardCode,
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

      if (
        !this.fromDistrictId ||
        !this.fromWardCode ||
        !this.returnPhone ||
        !this.returnAddress
      ) {
        throw new BadRequestException("Thiếu cấu hình GHN đầu gửi");
      }

      const services = await this.ghnClient.getAvailableServices(
        this.fromDistrictId,
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

      const created = await this.ghnClient.createOrder({
        payment_type_id: 1,
        note: dto.note || "",
        required_note: "KHONGCHOXEMHANG",
        return_phone: this.returnPhone,
        return_address: this.returnAddress,
        return_district_id: this.fromDistrictId,
        return_ward_code: this.fromWardCode,
        client_order_code: dto.clientOrderCode,
        from_name: this.returnName,
        from_phone: this.returnPhone,
        from_address: this.returnAddress,
        from_district_id: this.fromDistrictId,
        from_ward_code: this.fromWardCode,
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
          metadata: created,
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
          metadata: created,
        },
      });

      await tx.order.update({
        where: { id: orderId },
        data: {
          fulfillmentStatus: "PROCESSING",
          status: "SHIPPED",
        },
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

  private buildAhamoveOrderSyncData(shippingStatus: string) {
    if (shippingStatus === "DELIVERED") {
      return {
        status: "COMPLETED",
        fulfillmentStatus: "FULFILLED",
      };
    }

    if (shippingStatus === "CANCELLED") {
      return {
        status: "CANCELLED",
        fulfillmentStatus: "UNFULFILLED",
      };
    }

    if (shippingStatus === "FAILED" || shippingStatus === "RETURNING") {
      return {
        fulfillmentStatus: "RETURNED",
      };
    }

    if (
      shippingStatus === "PICKING" ||
      shippingStatus === "DELIVERING" ||
      shippingStatus === "CREATED"
    ) {
      return {
        status: "SHIPPED",
        fulfillmentStatus: "PROCESSING",
      };
    }

    return {};
  }

  private mapTimelineEventsForTracking(events: any[]) {
    return (Array.isArray(events) ? events : []).map((event: any) => ({
      id: event.id,
      status: event.status || event.partnerStatus || "",
      title: event.title || this.timelineTitle(event.status, event.carrier),
      description: event.description || "",
      location: event.locationText || "",
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
          `Không lấy được ward ViettelPost districtId=${districtId}: ${
            err instanceof Error ? err.message : String(err)
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
    const s = String(input || "").toUpperCase();

    if (!s) return "CREATED";
    if (s.includes("HUY") || s.includes("CANCEL")) return "CANCELLED";
    if (s.includes("THANH CONG") || s.includes("DELIVERED") || s.includes("SUCCESS")) {
      return "DELIVERED";
    }
    if (s.includes("DANG PHAT") || s.includes("DELIVERING")) return "DELIVERING";
    if (s.includes("DANG LAY") || s.includes("PICK")) return "PICKING";
    if (s.includes("HOAN") || s.includes("RETURN")) return "RETURNING";
    if (s.includes("FAIL") || s.includes("THAT BAI")) return "FAILED";
    if (s.includes("TRUNG CHUYEN") || s.includes("TRANSIT")) return "IN_TRANSIT";

    return "CREATED";
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
    if (code === "VTK") return "Chuyển phát tiết kiệm";
    if (code === "V60") return "Giao trong 60 phút";
    if (code === "V90") return "Giao trong 90 phút";
    if (code === "LCOD") return "COD tiêu chuẩn";
    if (code === "SCOD") return "COD nhanh";
    if (code === "PHS") return "Phát hẹn giờ";
    if (code === "PTN") return "Phát tận nơi";
    if (code === "VCN") return "Chuyển phát nhanh";

    return `Dịch vụ ${code}`;
  }

  private viettelPostLeadtimeLabel(serviceCode: string) {
    const code = String(serviceCode || "").toUpperCase();

    if (code === "VHT") return "Hỏa tốc";
    if (code === "V60") return "Trong 60 phút";
    if (code === "V90") return "Trong 90 phút";
    if (code === "VTK") return "2-5 ngày";
    if (code === "VCN") return "1-3 ngày";
    if (code === "SCOD") return "COD nhanh";
    if (code === "LCOD") return "COD tiêu chuẩn";

    return "Đang cập nhật";
  }

  private normalizeViettelServiceCodes(body: any) {
    const raw =
      body?.services ||
      body?.serviceCodes ||
      process.env.VIETTELPOST_SERVICES ||
      process.env.VIETTELPOST_DEFAULT_SERVICE ||
      "VHT";

    return Array.from(
      new Set(
        String(raw)
          .split(",")
          .map((item) => item.trim().toUpperCase())
          .filter(Boolean)
      )
    );
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
      raw?.RESULT,
      raw?.result,
      raw?.services,
      raw?.data?.RESULT,
      raw?.data?.result,
      raw?.data?.services,
      raw?.data?.data,
    ];

    for (const item of candidates) {
      if (Array.isArray(item)) return item;
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
        raw?.data?.MA_DV_CHINH ||
        raw?.data?.ORDER_SERVICE ||
        raw?.data?.SERVICE_CODE ||
        ""
    );
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
        raw?.MONEY_TOTAL_OLD ||
        raw?.MONEY_TOTAL_FEE ||
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
    const sender = await this.getViettelSenderConfigFromEnv();
    const senderProvinceId = Number(body?.senderProvinceId || sender.provinceId || 0);
    const senderDistrictId = Number(body?.senderDistrictId || sender.districtId || 0);

    if (!senderProvinceId || !senderDistrictId) {
      throw new BadRequestException(
        "Thiếu VIETTELPOST_SENDER_PROVINCE_ID / VIETTELPOST_SENDER_DISTRICT_ID"
      );
    }

    const receiverInput = this.normalizeViettelReceiverForOldCarrier({
      province: body?.province || body?.toProvince,
      district: body?.district || body?.toDistrict,
      ward: body?.ward || body?.toWard,
      address: body?.toAddress || body?.address,
    });

    // Không dùng receiverProvinceId/receiverDistrictId từ frontend cho ViettelPost.
    // Lý do: sau sáp nhập địa giới, frontend có thể gửi sai wardId/districtId
    // như Quốc Oai/Sài Sơn -> wardId 424. Viettel cần old-carrier address.
    const resolved = await this.resolveViettelAddress({
      province: receiverInput.province,
      district: receiverInput.district,
      ward: receiverInput.ward,
    });

    const resolvedAny = resolved as any;

    const rawServices =
      body?.services ||
      body?.serviceCodes ||
      process.env.VIETTELPOST_SERVICES ||
      process.env.VIETTELPOST_DEFAULT_SERVICE ||
      "VHT";

    const services = Array.from(
      new Set(
        String(rawServices)
          .split(",")
          .map((item) => item.trim().toUpperCase())
          .filter(Boolean)
      )
    );

    const weight = Math.max(1, Number(body?.weight || body?.PRODUCT_WEIGHT || 200));
    const productPrice = Math.max(
      0,
      Number(body?.productPrice || body?.insuranceValue || body?.PRODUCT_PRICE || 0)
    );
    const codAmount = Math.max(0, Number(body?.codAmount || body?.MONEY_COLLECTION || 0));

    const rows: any[] = [];
    const failedMessages: string[] = [];

    this.logger.log(
      `[VIETTELPOST_QUOTE] senderProvince=${senderProvinceId} senderDistrict=${senderDistrictId} receiverProvince=${Number(resolvedAny.provinceId || 0)} receiverDistrict=${Number(resolvedAny.districtId || resolvedAny.districtValue || 0)} receiverWard=${Number(resolvedAny.wardId || 0) || ""} receiverText=${receiverInput.province}/${receiverInput.district}/${receiverInput.ward} services=${services.join(",")}`
    );

    for (const serviceCode of services) {
      try {
        const payload = {
          PRODUCT_WEIGHT: weight,
          PRODUCT_PRICE: productPrice,
          MONEY_COLLECTION: codAmount,
          ORDER_SERVICE: serviceCode,
          ORDER_SERVICE_ADD:
            body?.serviceAdd || process.env.VIETTELPOST_SERVICE_ADD || "",
          SENDER_PROVINCE: senderProvinceId,
          SENDER_DISTRICT: senderDistrictId,
          RECEIVER_PROVINCE: Number(resolvedAny.provinceId || 0),
          RECEIVER_DISTRICT: Number(
            resolvedAny.districtId || resolvedAny.districtValue || 0
          ),
          PRODUCT_TYPE:
            body?.productType || process.env.VIETTELPOST_PRODUCT_TYPE || "HH",
          NATIONAL_TYPE: Number(body?.nationalType || 1),
        };

        const raw = await this.viettelPostClient.getPrice(payload);
        const fee = this.getViettelPostFee(raw);

        if (!fee) {
          failedMessages.push(`${serviceCode}: ViettelPost trả phí = 0`);
          continue;
        }

        rows.push({
          serviceId: 0,
          serviceTypeId: 0,
          shortName: `Viettel Post - ${this.viettelPostServiceLabel(serviceCode)}`,
          fee: {
            total: fee,
            total_fee: fee,
            service_fee: fee,
          },
          leadtime: {
            label: this.viettelPostLeadtimeLabel(serviceCode),
          },
          _carrier: "viettelpost",
          _quoteKey: `viettelpost-${serviceCode}`,
          _serviceName: serviceCode,
          _viettelServiceCode: serviceCode,
          _viettelReceiverProvinceId: Number(resolvedAny.provinceId || 0),
          _viettelReceiverDistrictId: Number(
            resolvedAny.districtId || resolvedAny.districtValue || 0
          ),
          _viettelReceiverWardId: Number(resolvedAny.wardId || 0) || undefined,
          _applyFeeToInput: true,
          _raw: raw,
        });
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        failedMessages.push(`${serviceCode}: ${message}`);
        this.logger.warn(`ViettelPost quote failed service=${serviceCode}: ${message}`);
      }
    }

    if (!rows.length) {
      this.logger.warn(
        `ViettelPost không trả về gói cước phù hợp. services=${services.join(",")} | ${failedMessages.join(" | ")}`
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
          _quoteKey: `viettelpost-unavailable-${services.join("-") || "none"}`,
          _serviceName: services[0] || "VHT",
          _viettelServiceCode: services[0] || "VHT",
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

    return rows;
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

      const sender = await this.getViettelSenderConfigFromEnv();
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
        GROUPADDRESS_ID: Number(process.env.VIETTELPOST_GROUPADDRESS_ID || 0) || undefined,
        CUS_ID: Number(process.env.VIETTELPOST_CUS_ID || 0) || undefined,
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
        data: {
          fulfillmentStatus: "PROCESSING",
          status: "SHIPPED",
        },
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

  async quoteAhamove(body: any) {
    const fromName = body?.fromName || process.env.AHAMOVE_FROM_NAME || this.returnName;
    const fromPhone =
      body?.fromPhone || process.env.AHAMOVE_FROM_PHONE || this.returnPhone;
    const fromAddress =
      body?.fromAddress ||
      process.env.AHAMOVE_FROM_ADDRESS ||
      this.returnAddress;

    const serviceId = String(
      body?.serviceId || process.env.AHAMOVE_DEFAULT_SERVICE_ID || "HAN-BIKE"
    );

    if (!fromPhone || !fromAddress) {
      throw new BadRequestException("Thiếu cấu hình AhaMove đầu gửi");
    }

    if (!body?.toName || !body?.toPhone || !body?.toAddress) {
      throw new BadRequestException("Thiếu thông tin người nhận AhaMove");
    }

    const codAmount = Math.max(0, Math.round(Number(body?.codAmount || 0)));
    const itemValue = Math.max(0, Math.round(Number(body?.itemValue || codAmount || 0)));

    const payload = {
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
      payment_method:
        body?.payment_method ||
        body?.paymentMethod ||
        process.env.AHAMOVE_PAYMENT_METHOD ||
        "BALANCE",
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
          weight: Math.max(0.1, Number(body?.weightKg || body?.weight || 200) / 1000),
          length: Math.max(0.01, Number(body?.lengthM || body?.length || 10) / 100),
          width: Math.max(0.01, Number(body?.widthM || body?.width || 10) / 100),
          height: Math.max(0.01, Number(body?.heightM || body?.height || 10) / 100),
          description: "Thời trang",
        },
      ],
    };

    return this.ahamoveClient.estimate(payload);
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
          metadata: created,
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
          metadata: created,
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
        data: {
          fulfillmentStatus: "PROCESSING",
          status: "SHIPPED",
        },
      });

      return {
        duplicated: false,
        ahamove: created,
        shipment,
      };
    });
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

    const orderSyncData = this.buildAhamoveOrderSyncData(shippingStatus);
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

    const nextOrderData: any = this.buildAhamoveOrderSyncData(shippingStatus);

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
        `[AHAMOVE_WEBHOOK] cannot sync AhamoveShipment table: ${
          error instanceof Error ? error.message : String(error)
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