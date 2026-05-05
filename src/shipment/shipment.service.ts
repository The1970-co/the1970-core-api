import { BadRequestException, Injectable, Logger } from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";
import { GhnClient } from "./ghn.client";
import { AhamoveClient } from "./ahamove.client";
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

    if (!shipment.trackingCode) {
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
        },
        tracking: latestCache.normalizedJson,
      };
    }

    const raw = await this.ghnClient.getOrderDetail(shipment.trackingCode);
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
        cod_amount: dto.codAmount,
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
          codAmount: dto.codAmount,
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
          codAmount: dto.codAmount,
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
          : Number(order.finalAmount || 0),
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
    if (value.includes("CANCEL")) return "CANCELLED";
    if (value.includes("COMPLETED")) return "DELIVERED";
    if (value.includes("IDLE") || value.includes("ASSIGNING")) return "CREATED";
    if (value.includes("ACCEPTED")) return "PICKING";
    if (value.includes("IN PROCESS") || value.includes("IN_PROCESS")) {
      return "DELIVERING";
    }

    return value;
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

  async quoteAhamove(body: any) {
    const fromName = body?.fromName || process.env.AHAMOVE_FROM_NAME || this.returnName;
    const fromPhone =
      body?.fromPhone || process.env.AHAMOVE_FROM_PHONE || this.returnPhone;
    const fromAddress =
      body?.fromAddress ||
      process.env.AHAMOVE_FROM_ADDRESS ||
      this.returnAddress;

    const serviceId =
      body?.serviceId || process.env.AHAMOVE_DEFAULT_SERVICE_ID || "HAN-BIKE";

    if (!fromPhone || !fromAddress) {
      throw new BadRequestException("Thiếu cấu hình AhaMove đầu gửi");
    }

    if (!body?.toName || !body?.toPhone || !body?.toAddress) {
      throw new BadRequestException("Thiếu thông tin người nhận AhaMove");
    }

    const payload = {
      service_id: serviceId,
      path: [
        {
          address: fromAddress,
          name: fromName,
          mobile: fromPhone,
        },
        {
          address: body.toAddress,
          name: body.toName,
          mobile: body.toPhone,
          cod: Math.max(0, Math.round(Number(body?.codAmount || 0))),
        },
      ],
      remarks: body?.note || "",
      items: Array.isArray(body?.items) ? body.items : [],
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

      const serviceId =
        dto?.serviceId || process.env.AHAMOVE_DEFAULT_SERVICE_ID || "HAN-BIKE";

      if (!fromPhone || !fromAddress) {
        throw new BadRequestException("Thiếu cấu hình AhaMove đầu gửi");
      }

      if (!dto?.toName || !dto?.toPhone || !dto?.toAddress) {
        throw new BadRequestException("Thiếu thông tin người nhận AhaMove");
      }

      const codAmount = Math.max(0, Math.round(Number(dto?.codAmount || 0)));

      const items =
        Array.isArray(dto?.items) && dto.items.length
          ? dto.items
          : (order.items || []).map((item: any) => ({
              name: item.productName || item.sku || "Sản phẩm",
              num: Number(item.qty || 1),
              price: Number(item.unitPrice || 0),
            }));

      const payload = {
        service_id: serviceId,
        order_time: new Date().toISOString(),
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

    const updated = await this.prisma.shipment.update({
      where: { id: shipment.id },
      data: {
        shippingStatus: this.mapAhamoveShippingStatus(ahamoveStatus),
        partnerStatus: ahamoveStatus,
        ahamoveStatus,
        ahamoveSubStatus: raw?.sub_status || raw?.data?.sub_status || null,
        ahamoveTrackingUrl: trackingUrl,
        ahamoveRaw: raw,
        metadata: raw,
        lastSyncedAt: new Date(),
      },
    });

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