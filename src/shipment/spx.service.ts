import { BadRequestException, Injectable, Logger } from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";
import { SpxClient } from "./spx.client";
import { CarrierInventoryService } from "./carrier-inventory.service";

type SpxAdminAddressItem = { label: string; value: number };

type SpxResolvedAddress = {
  detailAddress: string;
  adminAddress: SpxAdminAddressItem[];
  state: string;
  city: string;
  district: string;
  stateLocationId: number;
  cityLocationId: number;
  districtLocationId: number;
  placeId?: string;
  addressId?: string;
};

@Injectable()
export class SpxService {
  private readonly logger = new Logger(SpxService.name);
  private readonly locationCache = new Map<string, any[]>();

  constructor(
    private readonly prisma: PrismaService,
    private readonly spxClient: SpxClient,
    private readonly carrierInventoryService: CarrierInventoryService
  ) {}

  private normalizeText(input?: string | null) {
    return String(input || "")
      .trim()
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/đ/g, "d")
      .replace(/Đ/g, "D")
      .replace(/[,_-]+/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  private stripAdministrativePrefix(value?: string | null) {
    return this.normalizeText(value)
      .replace(/^(tinh|thanh pho|tp\.?|quan|huyen|thi xa|xa|phuong|thi tran)\s+/, "")
      .trim();
  }

  private mapSpxShippingStatus(input?: string | null) {
    const value = this.normalizeText(input);
    if (!value) return "CREATED";
    if (value.includes("cancel") || value.includes("huy")) return "CANCELLED";
    if (value.includes("return") || value.includes("hoan")) {
      if (value.includes("complete") || value.includes("success") || value.includes("da hoan")) return "RETURNED";
      return "RETURNING";
    }
    if (value.includes("fail") || value.includes("that bai") || value.includes("exception")) return "FAILED";
    if (value.includes("delivered") || value.includes("success") || value.includes("da giao") || value.includes("giao thanh cong")) return "DELIVERED";
    if (value.includes("deliver") || value.includes("dang giao") || value.includes("out for delivery")) return "DELIVERING";
    if (value.includes("transit") || value.includes("transport") || value.includes("sorting") || value.includes("phan loai") || value.includes("trung chuyen")) return "IN_TRANSIT";
    if (value.includes("pick") || value.includes("lay hang")) return "PICKING";
    if (value.includes("created") || value.includes("pending") || value.includes("cho lay")) return "CREATED";
    return String(input || "CREATED").toUpperCase().replace(/\s+/g, "_");
  }

  private timelineTitle(status?: string | null) {
    const s = this.mapSpxShippingStatus(status);
    if (s === "DELIVERED") return "Giao hàng thành công";
    if (s === "DELIVERING") return "Đang giao hàng";
    if (s === "IN_TRANSIT") return "Đang trung chuyển";
    if (s === "PICKING") return "Đang lấy hàng";
    if (s === "CREATED") return "Đã tạo vận đơn";
    if (s === "CANCELLED") return "Đã huỷ vận đơn";
    if (s === "FAILED") return "Giao hàng thất bại";
    if (s === "RETURNING") return "Đang hoàn hàng";
    if (s === "RETURNED") return "Đã hoàn hàng";
    return "Cập nhật vận chuyển";
  }

  private pickFirst(row: any, keys: string[]) {
    for (const key of keys) {
      const value = row?.[key];
      if (value !== undefined && value !== null && String(value).trim()) return value;
    }
    return undefined;
  }

  private getRootLocationId() {
    return Number(process.env.SPX_ROOT_LOCATION_ID || 6000407);
  }

  private getProductId() {
    return Number(process.env.SPX_PRODUCT_ID || 53001);
  }

  private getPickupTime() {
    // SPX web gửi Unix timestamp theo slot lấy hàng. Mặc định: ngày mai 10:00 local-ish.
    const env = Number(process.env.SPX_PICKUP_TIME || 0);
    if (env > 0) return env;
    const now = new Date();
    const pickup = new Date(now);
    pickup.setDate(now.getDate() + 1);
    pickup.setHours(10, 0, 0, 0);
    return Math.floor(pickup.getTime() / 1000);
  }

  private getPickupRangeId() {
    return Number(process.env.SPX_PICKUP_TIME_RANGE_ID || 1);
  }

  private getStaticLocationFallback(level: 1 | 2 | 3, parentId: number, name?: string | null) {
    const target = this.stripAdministrativePrefix(name);
    if (!target) return null;

    const provinceRows = [
      { label: "Hà Nội", value: 6000403 },
      { label: "Hồ Chí Minh", value: 6000404 },
      { label: "Bắc Ninh", value: 6000405 },
    ];

    const districtRowsByProvince: Record<number, Array<{ label: string; value: number }>> = {
      6000403: [
        { label: "Quận Đống Đa", value: 6000424 },
        { label: "Huyện Quốc Oai", value: 6000414 },
      ],
    };

    const wardRowsByDistrict: Record<number, Array<{ label: string; value: number }>> = {
      6000414: [{ label: "Xã Sài Sơn", value: 6006462 }],
      6000424: [{ label: "Phường Trung Liệt", value: 6006668 }],
    };

    const rows =
      level === 1
        ? provinceRows
        : level === 2
          ? districtRowsByProvince[parentId] || []
          : wardRowsByDistrict[parentId] || [];

    return rows.find((row) => this.scoreLocationName(target, row.label) > 0) || null;
  }

  private normalizeSpxLocationRows(raw: any): any[] {
    const candidates: any[] = [];
    const visited = new Set<any>();

    const walk = (node: any) => {
      if (!node || typeof node !== "object") return;
      if (visited.has(node)) return;
      visited.add(node);

      if (Array.isArray(node)) {
        node.forEach(walk);
        return;
      }

      const label = String(
        node.label ||
          node.name ||
          node.location_name ||
          node.locationName ||
          node.display_name ||
          node.displayName ||
          node.region_name ||
          node.regionName ||
          "",
      ).trim();
      const value = Number(
        node.value ||
          node.id ||
          node.location_id ||
          node.locationId ||
          node.region_id ||
          node.regionId ||
          0,
      );

      if (label && value) {
        candidates.push({
          ...node,
          label,
          value,
        });
      }

      for (const value of Object.values(node)) {
        if (value && typeof value === "object") walk(value);
      }
    };

    walk(raw?.data ?? raw?.result ?? raw);

    const seen = new Set<string>();
    return candidates.filter((row) => {
      const key = `${row.value}:${this.normalizeText(row.label)}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }

  private async getSpxSubLocations(locationId: number, subLevel: number) {
    const cacheKey = `${locationId}:${subLevel}`;
    if (this.locationCache.has(cacheKey)) return this.locationCache.get(cacheKey) || [];

    const raw = await this.spxClient.getSubLocationLayerInfo({ locationId, subLevel });
    const rows = this.normalizeSpxLocationRows(raw);
    this.locationCache.set(cacheKey, rows);
    return rows;
  }

  private async getSpxSubLocationsAny(locationId: number, subLevels: number[]) {
    const merged: any[] = [];
    const seen = new Set<string>();

    for (const subLevel of subLevels) {
      try {
        const rows = await this.getSpxSubLocations(locationId, subLevel);
        for (const row of rows) {
          const key = `${row.value}:${this.normalizeText(row.label)}`;
          if (seen.has(key)) continue;
          seen.add(key);
          merged.push(row);
        }
      } catch (err) {
        this.logger.warn(
          `[SPX] Không load được location layer locationId=${locationId} subLevel=${subLevel}: ${
            err instanceof Error ? err.message : String(err)
          }`,
        );
      }
    }

    return merged;
  }

  private scoreLocationName(input: string, candidate: string) {
    const a = this.stripAdministrativePrefix(input);
    const b = this.stripAdministrativePrefix(candidate);
    const fullA = this.normalizeText(input);
    const fullB = this.normalizeText(candidate);

    if (!a || !b) return 0;
    if (a === b || fullA === fullB) return 100;
    if (fullA.includes(fullB) || fullB.includes(fullA)) return 95;
    if (a.includes(b) || b.includes(a)) return 90;

    const aw = a.split(" ").filter(Boolean);
    const bw = b.split(" ").filter(Boolean);
    if (aw.length && aw.every((word) => b.includes(word))) return 80;
    if (bw.length && bw.every((word) => a.includes(word))) return 78;
    return 0;
  }

  private findBestLocation(rows: any[], name?: string | null) {
    const target = String(name || "").trim();
    if (!target) return null;

    const best = rows
      .map((row) => ({ row, score: this.scoreLocationName(target, row.label) }))
      .filter((item) => item.score > 0)
      .sort((a, b) => b.score - a.score || String(a.row.label).localeCompare(String(b.row.label), "vi"))[0];

    return best?.row || null;
  }

  private splitAddressFallback(address?: string | null) {
    const parts = String(address || "")
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean);

    if (parts.length >= 4) {
      return {
        detail: parts.slice(0, -3).join(", "),
        ward: parts[parts.length - 3],
        district: parts[parts.length - 2],
        province: parts[parts.length - 1],
      };
    }

    if (parts.length === 3) {
      return { detail: parts[0], ward: "", district: parts[1], province: parts[2] };
    }

    return { detail: parts[0] || String(address || ""), ward: "", district: "", province: "" };
  }


  private pickDeepFirst(raw: any, keys: string[]) {
    const visited = new Set<any>();
    const walk = (node: any): any => {
      if (!node || typeof node !== "object") return undefined;
      if (visited.has(node)) return undefined;
      visited.add(node);
      if (Array.isArray(node)) {
        for (const item of node) {
          const found = walk(item);
          if (found !== undefined && found !== null && String(found).trim()) return found;
        }
        return undefined;
      }
      for (const key of keys) {
        const value = node[key];
        if (value !== undefined && value !== null && String(value).trim()) return value;
      }
      for (const value of Object.values(node)) {
        const found = walk(value);
        if (found !== undefined && found !== null && String(found).trim()) return found;
      }
      return undefined;
    };
    return walk(raw);
  }

  private async resolveSpxPlaceByText(input: {
    name?: string | null;
    phone?: string | null;
    detailAddress?: string | null;
    ward?: string | null;
    district?: string | null;
    province?: string | null;
  }) {
    const rawText = [
      input.detailAddress,
      input.ward,
      input.district,
      input.province,
    ]
      .filter(Boolean)
      .join(", ");

    const fullText = [
      input.name,
      input.phone,
      input.detailAddress,
      input.ward,
      input.district,
      input.province,
    ]
      .filter(Boolean)
      .join(", ");

    if (!rawText.trim() && !fullText.trim()) {
      return {} as { placeId?: string; addressId?: string; raw?: any };
    }

    const extract = (raw: any) => {
      const placeId = String(
        this.pickDeepFirst(raw, [
          "place_id",
          "placeId",
          "placeID",
          "google_place_id",
          "googlePlaceId",
          "id",
        ]) || "",
      ).trim();
      const addressId = String(
        this.pickDeepFirst(raw, [
          "address_id",
          "addressId",
          "address_pk_id",
          "addressPkId",
          "pk_id",
          "pkId",
        ]) || "",
      ).trim();
      return { placeId: placeId || undefined, addressId: addressId || undefined };
    };

    const attempts = [rawText, fullText].filter(Boolean);
    const raws: any[] = [];

    for (const text of attempts) {
      try {
        const raw = await this.spxClient.globalAutocomplete({ input: text, size: 10, country: "VN", language: "vi" });
        raws.push({ type: "global_autocomplete", input: text, raw });
        const found = extract(raw);
        if (found.placeId || found.addressId) return { ...found, raw };
      } catch (err) {
        this.logger.warn(`[SPX] globalAutocomplete failed: ${err instanceof Error ? err.message : String(err)}`);
      }
    }

    for (const text of attempts) {
      try {
        const raw = await this.spxClient.addressSegmentation({ input: text, country: "VN", language: "vi" });
        raws.push({ type: "address_segmentation", input: text, raw });
        const found = extract(raw);
        if (found.placeId || found.addressId) return { ...found, raw };
      } catch (err) {
        this.logger.warn(`[SPX] addressSegmentation failed: ${err instanceof Error ? err.message : String(err)}`);
      }
    }

    return { raw: raws } as { placeId?: string; addressId?: string; raw?: any };
  }

  private async resolveSpxAddress(input: {
    detailAddress?: string | null;
    address?: string | null;
    province?: string | null;
    district?: string | null;
    ward?: string | null;
    placeId?: string | null;
    addressId?: string | null;
    name?: string | null;
    phone?: string | null;
    prefix?: "sender" | "deliver";
  }): Promise<SpxResolvedAddress> {
    const fallback = this.splitAddressFallback(input.address);
    const provinceName = String(input.province || fallback.province || "").trim();
    const districtName = String(input.district || fallback.district || "").trim();
    const wardName = String(input.ward || fallback.ward || "").trim();
    const detailAddress = String(input.detailAddress || fallback.detail || input.address || "").trim();

    if (!provinceName || !districtName || !wardName) {
      throw new BadRequestException(
        `SPX thiếu tỉnh/huyện/xã để resolve địa chỉ ${input.prefix || ""}. Hãy gửi province/district/ward rõ ràng.`
      );
    }

    const provinceRows = await this.getSpxSubLocationsAny(this.getRootLocationId(), [1]);
    const province =
      this.findBestLocation(provinceRows, provinceName) ||
      this.getStaticLocationFallback(1, this.getRootLocationId(), provinceName);
    if (!province) {
      throw new BadRequestException(
        `SPX không tìm thấy tỉnh/thành: ${provinceName}. SPX trả ${provinceRows.length} tỉnh/thành từ root ${this.getRootLocationId()}.`,
      );
    }

    const districtRows = await this.getSpxSubLocationsAny(Number(province.value), [2, 1]);
    const district =
      this.findBestLocation(districtRows, districtName) ||
      this.getStaticLocationFallback(2, Number(province.value), districtName);
    if (!district) {
      throw new BadRequestException(
        `SPX không tìm thấy quận/huyện: ${districtName} trong ${province.label}. SPX trả ${districtRows.length} quận/huyện.`,
      );
    }

    const wardRows = await this.getSpxSubLocationsAny(Number(district.value), [3, 1, 2]);
    const ward =
      this.findBestLocation(wardRows, wardName) ||
      this.getStaticLocationFallback(3, Number(district.value), wardName);
    if (!ward) {
      throw new BadRequestException(
        `SPX không tìm thấy phường/xã: ${wardName} trong ${district.label}. SPX trả ${wardRows.length} phường/xã.`,
      );
    }

    const adminAddress = [
      { label: String(province.label), value: Number(province.value) },
      { label: String(district.label), value: Number(district.value) },
      { label: String(ward.label), value: Number(ward.value) },
    ];

    let placeId = String(input.placeId || "").trim() || undefined;
    let addressId = String(input.addressId || "").trim() || undefined;

    if (!placeId || !addressId) {
      const segmented = await this.resolveSpxPlaceByText({
        name: input.name,
        phone: input.phone,
        detailAddress,
        ward: String(ward.label),
        district: String(district.label),
        province: String(province.label),
      });
      placeId = placeId || segmented.placeId;
      addressId = addressId || segmented.addressId;
    }

    return {
      detailAddress,
      adminAddress,
      state: String(province.label),
      city: String(district.label),
      district: String(ward.label),
      stateLocationId: Number(province.value),
      cityLocationId: Number(district.value),
      districtLocationId: Number(ward.value),
      placeId,
      addressId,
    };
  }

  private getSpxFirstListItem(raw: any) {
    const data = raw?.data || raw?.result || raw || {};
    if (Array.isArray(data?.list) && data.list.length) return data.list[0];
    if (Array.isArray(raw?.list) && raw.list.length) return raw.list[0];
    return data?.order || data;
  }

  private getSpxTrackingCode(raw: any) {
    const row = this.getSpxFirstListItem(raw);
    return String(
      this.pickFirst(row, [
        "spx_tn", "sls_tn", "tracking_code", "trackingCode", "tracking_number", "trackingNumber",
        "awb_number", "awbNumber", "waybill_no", "waybillNo", "order_code", "orderCode", "spx_tracking_number",
      ]) || ""
    ).trim();
  }

  private getSpxOrderSn(raw: any) {
    const row = this.getSpxFirstListItem(raw);
    return String(
      this.pickFirst(row, ["order_sn", "orderSn", "order_id", "orderId", "external_order_code", "externalOrderCode"]) || ""
    ).trim();
  }

  private getSpxStatus(raw: any) {
    const row = this.getSpxFirstListItem(raw);
    return String(
      this.pickFirst(row, ["status", "status_name", "statusName", "current_status", "currentStatus", "shipment_status", "shipmentStatus"]) || "CREATED"
    );
  }

  private getSpxFee(raw: any) {
    const row = this.getSpxFirstListItem(raw);
    const feeInfoFirst = Array.isArray(row?.fee_info) ? row.fee_info[0] : row?.fee_info;
    return Number(
      feeInfoFirst?.estimated_shipping_fee ||
        row?.estimated_shipping_fee ||
        feeInfoFirst?.shipping_fee ||
        feeInfoFirst?.basic_shipping_fee ||
        row?.esf_info?.shipping_fee ||
        row?.esf_info?.estimated_shipping_fee ||
        row?.fee || row?.total_fee || row?.totalFee || row?.shipping_fee || row?.shippingFee ||
        row?.price || row?.total_price || row?.totalPrice || row?.user_price_details?.total_fee || 0
    );
  }

  private getSpxServiceCode(row: any, fallback = "STANDARD") {
    const data = row?.data || row?.result || row || {};
    return String(data?.service_code || data?.serviceCode || data?.service_id || data?.serviceId || data?.code || data?.product_id || data?.base_info?.product_id || fallback).trim();
  }

  private getSpxServiceName(row: any, fallback = "SPX Standard") {
    const data = row?.data || row?.result || row || {};
    return String(data?.service_name || data?.serviceName || data?.shortName || data?.name || data?.product_name || data?.base_info?.product_name || fallback).trim();
  }

  private normalizeQuoteRows(raw: any) {
    if (Array.isArray(raw)) return raw;
    if (Array.isArray(raw?.data)) return raw.data;
    if (Array.isArray(raw?.result)) return raw.result;
    if (Array.isArray(raw?.list)) return raw.list;
    if (Array.isArray(raw?.data?.list)) return raw.data.list;
    if (Array.isArray(raw?.services)) return raw.services;
    if (Array.isArray(raw?.rates)) return raw.rates;
    if (Array.isArray(raw?.quotes)) return raw.quotes;
    if (raw?.data && typeof raw.data === "object") return [raw.data];
    if (raw?.result && typeof raw.result === "object") return [raw.result];
    return raw ? [raw] : [];
  }

  private buildReceiverAddress(dto: any, order?: any) {
    return (
      dto?.toAddress ||
      [order?.shippingAddressLine1, order?.shippingAddressLine2, order?.shippingWard, order?.shippingDistrict, order?.shippingProvince]
        .filter(Boolean)
        .join(", ")
    );
  }

  private normalizePhoneForSpx(value?: string | null) {
    const digits = String(value || "").replace(/\D/g, "");
    if (digits.startsWith("84")) return digits;
    if (digits.startsWith("0")) return `84${digits.slice(1)}`;
    return digits;
  }

  private getItems(dto: any, order?: any, totalWeightGram = 1000) {
    const rawItems = Array.isArray(dto?.items) && dto.items.length
      ? dto.items
      : (order?.items || []).map((item: any) => ({
          name: item.productName || item.sku || "Sản phẩm",
          sku: item.sku,
          quantity: Number(item.qty || 1),
          price: Number(item.unitPrice || item.price || 0),
          weight: Math.max(1, Math.floor(totalWeightGram / Math.max((order?.items || []).length, 1))),
        }));

    return rawItems.map((item: any) => ({
      name: String(item.name || item.productName || item.sku || "Sản phẩm"),
      sku: item.sku ? String(item.sku) : undefined,
      quantity: Math.max(1, Number(item.quantity || item.qty || item.num || 1)),
      price: Math.max(0, Math.round(Number(item.price || 0))),
      weight: Math.max(1, Math.round(Number(item.weight || Math.floor(totalWeightGram / Math.max(rawItems.length, 1))))),
    }));
  }

  private async buildSpxBatchOrderPayload(dto: any, order?: any, mode: "quote" | "create" = "quote") {
    const weightGram = Math.max(1, Math.round(Number(dto?.weight || 1000)));
    const weightKg = Math.max(1, Math.ceil(weightGram / 1000));
    const length = Math.max(1, Number(dto?.length || 10));
    const width = Math.max(1, Number(dto?.width || 10));
    const height = Math.max(1, Number(dto?.height || 10));
    const codAmount = Math.max(0, Math.round(Number(dto?.codAmount || 0)));
    const insuranceValue = Math.max(0, Math.round(Number(dto?.insuranceValue || dto?.productPrice || order?.finalAmount || 0)));
    const productPrice = Math.max(0, Math.round(Number(dto?.productPrice || dto?.insuranceValue || order?.finalAmount || 0)));
    const items = this.getItems(dto, order, weightGram);
    const firstItem = items[0] || { name: "Sản phẩm", quantity: 1, price: productPrice, weight: weightGram };
    const toAddress = this.buildReceiverAddress(dto, order);

    // SPX nên dùng địa chỉ gửi cố định theo shop/kho. Tạm thời lấy từ env để tránh
    // bị ảnh hưởng bởi pickup mapping động của AhaMove/GHN trên màn tạo đơn.
    // Nếu sau này muốn cho SPX dùng sender động theo chi nhánh, bật SPX_USE_DTO_SENDER=true.
    const useDtoSender = ["1", "true", "yes", "on"].includes(
      String(process.env.SPX_USE_DTO_SENDER || "").trim().toLowerCase(),
    );

    const senderProvince = useDtoSender
      ? dto?.fromProvince || process.env.SPX_SENDER_PROVINCE || process.env.GHN_RETURN_PROVINCE || ""
      : process.env.SPX_SENDER_PROVINCE || process.env.GHN_RETURN_PROVINCE || dto?.fromProvince || "";
    const senderDistrict = useDtoSender
      ? dto?.fromDistrict || process.env.SPX_SENDER_DISTRICT || process.env.GHN_RETURN_DISTRICT || ""
      : process.env.SPX_SENDER_DISTRICT || process.env.GHN_RETURN_DISTRICT || dto?.fromDistrict || "";
    const senderWard = useDtoSender
      ? dto?.fromWard || process.env.SPX_SENDER_WARD || process.env.GHN_RETURN_WARD || ""
      : process.env.SPX_SENDER_WARD || process.env.GHN_RETURN_WARD || dto?.fromWard || "";
    const senderDetail = useDtoSender
      ? dto?.fromAddress || process.env.SPX_SENDER_ADDRESS || process.env.GHN_RETURN_ADDRESS || ""
      : process.env.SPX_SENDER_ADDRESS || process.env.GHN_RETURN_ADDRESS || dto?.fromAddress || "";

    const senderName = useDtoSender
      ? dto?.fromName || process.env.SPX_SENDER_NAME || process.env.GHN_RETURN_NAME || "The 1970"
      : process.env.SPX_SENDER_NAME || process.env.GHN_RETURN_NAME || dto?.fromName || "The 1970";
    const senderPhone = this.normalizePhoneForSpx(
      useDtoSender
        ? dto?.fromPhone || process.env.SPX_SENDER_PHONE || process.env.GHN_RETURN_PHONE
        : process.env.SPX_SENDER_PHONE || process.env.GHN_RETURN_PHONE || dto?.fromPhone,
    );
    const receiverName = dto?.toName || order?.shippingRecipientName || order?.customerName || "Khách hàng";
    const receiverPhone = this.normalizePhoneForSpx(dto?.toPhone || order?.shippingPhone || order?.customerPhone);

    const sender = await this.resolveSpxAddress({
      detailAddress: senderDetail,
      address: senderDetail,
      province: senderProvince,
      district: senderDistrict,
      ward: senderWard,
      name: senderName,
      phone: senderPhone,
      placeId: useDtoSender ? dto?.fromPlaceId || process.env.SPX_SENDER_PLACE_ID : process.env.SPX_SENDER_PLACE_ID || dto?.fromPlaceId,
      addressId: useDtoSender ? dto?.fromAddressId || process.env.SPX_SENDER_ADDRESS_ID : process.env.SPX_SENDER_ADDRESS_ID || dto?.fromAddressId,
      prefix: "sender",
    });

    const receiver = await this.resolveSpxAddress({
      detailAddress: toAddress,
      address: toAddress,
      province: dto?.toProvince || dto?.province || order?.shippingProvince,
      district: dto?.toDistrict || dto?.district || order?.shippingDistrict,
      ward: dto?.toWard || dto?.ward || order?.shippingWard,
      name: receiverName,
      phone: receiverPhone,
      placeId: dto?.toPlaceId,
      addressId: dto?.toAddressId || dto?.deliverAddressId || dto?.deliver_address_id,
      prefix: "deliver",
    });

    const productId = this.getProductId();
    const pickupTime = this.getPickupTime();
    const pickupRangeId = this.getPickupRangeId();

    // Web SPX quote/create schema thường yêu cầu address_id. Trên portal,
    // deliver_address_id ở bước quote có thể trùng với address id shop gửi.
    // Nếu không có address id riêng cho người nhận, fallback tạm về SPX_SENDER_ADDRESS_ID
    // để tránh lỗi parse param failed ở batch_check_order.
    const senderAddressId = String(
      sender.addressId ||
        process.env.SPX_SENDER_ADDRESS_ID ||
        process.env.SPX_ADDRESS_ID ||
        "",
    ).trim();
    const receiverAddressId = String(
      receiver.addressId ||
        dto?.toAddressId ||
        dto?.deliverAddressId ||
        dto?.deliver_address_id ||
        process.env.SPX_DELIVER_ADDRESS_ID ||
        process.env.SPX_RECEIVER_ADDRESS_ID ||
        senderAddressId ||
        "",
    ).trim();

    const common: any = {
      base_info: {
        product_id: productId,
        order_type: 1,
        ...(mode === "create" ? { product_name: "Standard Service", three_pl_name: "SPX" } : {}),
      },
      from: {
        place_id: sender.placeId || "",
        detail_address: sender.detailAddress,
        admin_address: sender.adminAddress,
      },
      to: {
        place_id: receiver.placeId || "",
        detail_address: receiver.detailAddress,
        admin_address: receiver.adminAddress,
      },
      sender_info: {
        sender_country: "VN",
        sender_post_code: "",
        sender_place_id: sender.placeId || "",
        sender_address_version: 0,
        sender_admin_address: sender.adminAddress,
        sender_state: sender.state,
        sender_city: sender.city,
        sender_district: sender.district,
        sender_state_location_id: sender.stateLocationId,
        sender_city_location_id: sender.cityLocationId,
        sender_district_location_id: sender.districtLocationId,
        sender_detail_address: sender.detailAddress,
        ...(senderAddressId ? { sender_address_id: senderAddressId } : {}),
        sender_name: senderName,
        sender_phone: senderPhone,
      },
      deliver_info: {
        deliver_country: "VN",
        deliver_post_code: "",
        deliver_place_id: receiver.placeId || "",
        deliver_address_version: 0,
        deliver_admin_address: receiver.adminAddress,
        deliver_state: receiver.state,
        deliver_city: receiver.city,
        deliver_district: receiver.district,
        deliver_state_location_id: receiver.stateLocationId,
        deliver_city_location_id: receiver.cityLocationId,
        deliver_district_location_id: receiver.districtLocationId,
        deliver_detail_address: receiver.detailAddress,
        ...(receiverAddressId ? { deliver_address_id: receiverAddressId } : {}),
        deliver_name: receiverName,
        deliver_phone: receiverPhone,
        deliver_instruction: dto?.note || dto?.shippingNote || "",
      },
      fulfillment_info: {
        pickup_time: pickupTime,
        pickup_time_range_id: pickupRangeId,
        collect_type: 1,
        cod_collection: codAmount > 0 ? 1 : 0,
        cod_amount: codAmount,
        payment_role: 1,
        insurance_collection: insuranceValue > 0 ? 1 : 0,
        is_pickup_weight: 1,
        deliver_type: 1,
        allow_mutual_check: 0,
        allow_try_on: 0,
        allow_partial_delivery: 0,
        parcel_dimension: [length, width, height],
      },
      parcel_info: {
        parcel_weight: [weightKg],
        parcel_length: length,
        parcel_width: width,
        parcel_height: height,
        parcel_category: 0,
        parcel_item_name: firstItem.name,
        parcel_item_quantity: items.reduce((sum, item) => sum + Number(item.quantity || 0), 0) || 1,
        express_insured_value: insuranceValue || productPrice || 0,
        parcel_item_type: "",
        ...(mode === "create" ? {
          item_list: items.map((item, index) => ({
            item_id: Number(item.sku?.replace(/\D/g, "").slice(0, 8) || 0) || index + 1,
            item_name: item.name,
            item_weight: String(item.weight),
            item_price: String(item.price),
            item_quantity: item.quantity,
          })),
        } : {}),
      },
      product_id: productId,
      payment_type: 1,
      paynow_role: 1,
      coin_tag: 0,
      remote_area_fee: 0,
      insurance_service_fee: 0,
      cod_service_fee: 0,
      mutual_check_service_fee: 0,
      try_on_service_fee: 0,
      additional_service_fee: "0",
      account_voucher_id: "0",
      shipping_fee_voucher: "0",
      voucher_instance_id: "0",
    };

    if (mode === "quote") {
      // Endpoint batch_check_order của SPX khá kén format. Dùng payload tối thiểu
      // giống web SPX: không gửi các field dành riêng cho create order.
      const quoteItem = {
        // Web SPX có parcel_weight ở top-level và parcel_info.parcel_weight dạng mảng.
        parcel_weight: weightKg,
        base_info: {
          product_id: productId,
          order_type: 1,
        },
        from: {
          place_id: sender.placeId || "",
          detail_address: sender.detailAddress,
          admin_address: sender.adminAddress,
        },
        to: {
          place_id: receiver.placeId || "",
          detail_address: receiver.detailAddress,
          admin_address: receiver.adminAddress,
        },
        sender_info: {
          sender_country: "VN",
          sender_post_code: "undefined",
          sender_place_id: sender.placeId || "",
          sender_address_version: 0,
          sender_admin_address: sender.adminAddress,
          sender_state: sender.state,
          sender_city: sender.city,
          sender_district: sender.district,
          sender_state_location_id: sender.stateLocationId,
          sender_city_location_id: sender.cityLocationId,
          sender_district_location_id: sender.districtLocationId,
          sender_detail_address: sender.detailAddress,
          ...(senderAddressId ? { sender_address_id: senderAddressId } : {}),
        },
        deliver_info: {
          deliver_country: "VN",
          deliver_post_code: "undefined",
          deliver_place_id: receiver.placeId || "",
          deliver_address_version: 0,
          deliver_admin_address: receiver.adminAddress,
          deliver_state: receiver.state,
          deliver_city: receiver.city,
          deliver_district: receiver.district,
          deliver_state_location_id: receiver.stateLocationId,
          deliver_city_location_id: receiver.cityLocationId,
          deliver_district_location_id: receiver.districtLocationId,
          deliver_detail_address: receiver.detailAddress,
          ...(receiverAddressId ? { deliver_address_id: receiverAddressId } : {}),
        },
        fulfillment_info: {
          pickup_time: pickupTime,
          pickup_time_range_id: pickupRangeId,
          collect_type: 1,
          // Web quote SPX gửi 0 ở bước báo giá, COD thật sẽ gửi ở create order.
          cod_collection: 0,
          payment_role: 1,
          // Web quote thường gửi insurance_collection=1. Giá trị bảo hiểm thật vẫn nằm ở create order.
          insurance_collection: 1,
          is_pickup_weight: 1,
          deliver_type: 1,
          parcel_dimension: [length, width, height],
        },
        parcel_info: {
          parcel_weight: [weightKg],
          parcel_length: length,
          parcel_width: width,
          parcel_height: height,
          parcel_category: 0,
          parcel_item_quantity: items.reduce((sum, item) => sum + Number(item.quantity || 0), 0) || 1,
        },
      };

      return {
        list: [quoteItem],
        meta: { sender, receiver, senderAddressId, receiverAddressId, weightGram, weightKg, length, width, height, codAmount, insuranceValue, productPrice, items },
      };
    }

    if (mode === "create") {
      // Lấy phí bằng đúng schema quote của web SPX, không dùng common create payload.
      const quoteItemForFee = {
        parcel_weight: weightKg,
        base_info: {
          product_id: productId,
          order_type: 1,
        },
        from: {
          place_id: sender.placeId || "",
          detail_address: sender.detailAddress,
          admin_address: sender.adminAddress,
        },
        to: {
          place_id: receiver.placeId || "",
          detail_address: receiver.detailAddress,
          admin_address: receiver.adminAddress,
        },
        sender_info: {
          sender_country: "VN",
          sender_post_code: "undefined",
          sender_place_id: sender.placeId || "",
          sender_address_version: 0,
          sender_admin_address: sender.adminAddress,
          sender_state: sender.state,
          sender_city: sender.city,
          sender_district: sender.district,
          sender_state_location_id: sender.stateLocationId,
          sender_city_location_id: sender.cityLocationId,
          sender_district_location_id: sender.districtLocationId,
          sender_detail_address: sender.detailAddress,
          ...(senderAddressId ? { sender_address_id: senderAddressId } : {}),
        },
        deliver_info: {
          deliver_country: "VN",
          deliver_post_code: "undefined",
          deliver_place_id: receiver.placeId || "",
          deliver_address_version: 0,
          deliver_admin_address: receiver.adminAddress,
          deliver_state: receiver.state,
          deliver_city: receiver.city,
          deliver_district: receiver.district,
          deliver_state_location_id: receiver.stateLocationId,
          deliver_city_location_id: receiver.cityLocationId,
          deliver_district_location_id: receiver.districtLocationId,
          deliver_detail_address: receiver.detailAddress,
          ...(receiverAddressId ? { deliver_address_id: receiverAddressId } : {}),
        },
        fulfillment_info: {
          pickup_time: pickupTime,
          pickup_time_range_id: pickupRangeId,
          collect_type: 1,
          cod_collection: 0,
          payment_role: 1,
          insurance_collection: 1,
          is_pickup_weight: 1,
          deliver_type: 1,
          parcel_dimension: [length, width, height],
        },
        parcel_info: {
          parcel_weight: [weightKg],
          parcel_length: length,
          parcel_width: width,
          parcel_height: height,
          parcel_category: 0,
          parcel_item_quantity: items.reduce((sum, item) => sum + Number(item.quantity || 0), 0) || 1,
        },
      };

      const quoteRaw = await this.spxClient.quote({ list: [quoteItemForFee] });
      const quoteRow = this.getSpxFirstListItem(quoteRaw);
      const fee = this.getSpxFee(quoteRaw);
      const senderPkId = Number(dto?.senderAddressPkId || dto?.sender_address_pk_id || process.env.SPX_SENDER_ADDRESS_PK_ID || process.env.SPX_SENDER_ADDRESS_ID || 0);

      if (senderPkId > 0) {
        common.sender_info.sender_address_pk_id = senderPkId;
      }

      const feeInfo = Array.isArray(quoteRow?.fee_info) ? quoteRow.fee_info[0] : quoteRow?.fee_info || {};
      common.esf_info = {
        ...(quoteRow?.esf_info || {}),
        rate_channel_id: quoteRow?.esf_info?.rate_channel_id || 12813,
        charged_weight: quoteRow?.esf_info?.charged_weight || weightGram,
        volumetric_factor: quoteRow?.esf_info?.volumetric_factor || 6000,
        estimated_shipping_fee: fee,
        shipping_fee: fee,
        estimated_shipping_fee_without_voucher: String(quoteRow?.estimated_shipping_fee_without_voucher || feeInfo?.basic_shipping_fee || fee || 0),
        estimated_total_discount: Number(quoteRow?.estimated_total_discount || 0),
      };
      common.estimated_shipping_fee = fee;
      common.estimated_shipping_fee_without_voucher = String(quoteRow?.estimated_shipping_fee_without_voucher || feeInfo?.basic_shipping_fee || fee || 0);
      common.estimated_total_discount = Number(quoteRow?.estimated_total_discount || 0);
      common.edt_info = quoteRow?.edt_info || {};
      common.service_fee = fee;
      common.total_bsf = String(feeInfo?.basic_shipping_fee || common.estimated_shipping_fee_without_voucher || fee || 0);
      common.base_shipping_fee_without_discount = String(feeInfo?.basic_shipping_fee || common.total_bsf || fee || 0);
      common.shipping_fee_voucher = String(feeInfo?.shipping_fee_voucher || quoteRow?.shipping_fee_voucher || 0);
      common.vat_fee = Number(feeInfo?.vat_fee || quoteRow?.vat_fee || 0);
      common.need_peak_time_fee = 0;
      common.vas_info = {};
      common.scfs_order_id = 0;
    }

    return { list: [common], meta: { sender, receiver, senderAddressId, receiverAddressId, weightGram, weightKg, length, width, height, codAmount, insuranceValue, productPrice, items } };
  }

  async quoteSpx(dto: any) {
    try {
      const built = await this.buildSpxBatchOrderPayload(dto, undefined, "quote");
      const spxDebugEnabled = ["1", "true", "yes", "on"].includes(
        String(process.env.SPX_DEBUG || "").trim().toLowerCase(),
      );
      const raw = await this.spxClient.quote({ list: built.list });

      if (raw?.retcode !== undefined && raw.retcode !== 0 && raw.retcode !== "0") {
        return [{
          serviceId: 0,
          serviceTypeId: 0,
          shortName: "SPX chưa khả dụng",
          fee: { total: 0, total_fee: 0, service_fee: 0 },
          leadtime: { label: "Không khả dụng" },
          _carrier: "spx",
          _quoteKey: "spx-error",
          _serviceName: "STANDARD",
          _spxServiceCode: "STANDARD",
          _raw: raw,
          ...(spxDebugEnabled ? { _debug: { resolved: built.meta, spxPayload: { list: built.list } } } : {}),
          _disabled: true,
          _disabledReason: raw?.message || raw?.detail || "SPX chưa trả về phí cho tuyến này.",
          _applyFeeToInput: false,
        }];
      }

      const rows = this.normalizeQuoteRows(raw?.data || raw);
      const mapped = rows.map((row: any, index: number) => {
        const serviceCode = this.getSpxServiceCode(row, index === 0 ? "STANDARD" : `SPX-${index + 1}`);
        const serviceName = this.getSpxServiceName(row, serviceCode || "SPX Standard");
        const fee = this.getSpxFee(row);
        const edt = row?.edt_info || {};
        const leadtimeLabel = edt?.edt_min !== undefined || edt?.edt_max !== undefined
          ? `${edt.edt_min ?? ""}-${edt.edt_max ?? ""} ngày`.replace(/^-/, "").replace(/- ngày$/, " ngày")
          : "Đang cập nhật";

        return {
          serviceId: index + 1,
          serviceTypeId: index + 1,
          shortName: serviceName,
          fee: { total: fee, total_fee: fee, service_fee: fee },
          leadtime: { label: leadtimeLabel },
          _carrier: "spx",
          _quoteKey: `spx-${serviceCode || index}`,
          _serviceName: serviceName,
          _spxServiceCode: serviceCode,
          _raw: row,
          _applyFeeToInput: fee > 0,
          ...(fee > 0 ? {} : {
            _disabled: true,
            _disabledReason: row?.message || row?.detail || "SPX chưa trả về phí cho tuyến này.",
            _applyFeeToInput: false,
          }),
        };
      });

      return mapped.length ? mapped.sort((a, b) => Number(a.fee?.total || 0) - Number(b.fee?.total || 0)) : [{
        serviceId: 0,
        serviceTypeId: 0,
        shortName: "Shopee Express - Chưa có gói phù hợp",
        fee: { total: 0, total_fee: 0, service_fee: 0 },
        leadtime: { label: "Không khả dụng" },
        _carrier: "spx",
        _quoteKey: "spx-unavailable",
        _serviceName: "STANDARD",
        _spxServiceCode: "STANDARD",
        _disabled: true,
        _disabledReason: "SPX không trả về gói cước phù hợp.",
        _applyFeeToInput: false,
      }];
    } catch (err) {
      const message = err instanceof Error ? err.message : "SPX quote lỗi";
      return [{
        serviceId: 0,
        serviceTypeId: 0,
        shortName: "SPX chưa khả dụng",
        fee: { total: 0, total_fee: 0, service_fee: 0 },
        leadtime: { label: "Không khả dụng" },
        _carrier: "spx",
        _quoteKey: "spx-error",
        _serviceName: "STANDARD",
        _spxServiceCode: "STANDARD",
        _raw: { error: message },
        _disabled: true,
        _disabledReason: message,
        _applyFeeToInput: false,
      }];
    }
  }

  async createSpxShipment(orderId: string, dto: any, user?: any) {
    return this.prisma.$transaction(async (tx) => {
      const order = await tx.order.findUnique({
        where: { id: orderId },
        include: { items: true, shipment: true },
      });

      if (!order) throw new BadRequestException("Không tìm thấy order");

      if (order.shipment?.trackingCode) {
        if (String(order.shipment.carrier || "").toUpperCase() === "SPX") {
          return { duplicated: true, spx: order.shipment.metadata || null, shipment: order.shipment };
        }
        throw new BadRequestException(`Đơn đã có vận đơn ${order.shipment.carrier || "khác"}`);
      }

      const built = await this.buildSpxBatchOrderPayload(dto, order, "create");
      const created = await this.spxClient.createOrder({ list: built.list });
      const trackingCode = this.getSpxTrackingCode(created);
      const orderSn = this.getSpxOrderSn(created);

      if (!trackingCode) throw new BadRequestException("SPX không trả về mã vận đơn");

      const partnerStatus = this.getSpxStatus(created);
      const shippingStatus = this.mapSpxShippingStatus(partnerStatus);
      const fee = this.getSpxFee(created) || built.list[0]?.estimated_shipping_fee || 0;
      const codAmount = built.meta.codAmount;
      const payload = built.list[0];

      const shipment = await tx.shipment.upsert({
        where: { orderId },
        update: {
          carrier: "SPX",
          trackingCode,
          shippingStatus,
          partnerStatus,
          codAmount,
          shippingFee: fee || null,
          fromName: payload.sender_info.sender_name,
          fromPhone: payload.sender_info.sender_phone,
          fromAddress: payload.sender_info.sender_detail_address,
          toName: payload.deliver_info.deliver_name,
          toPhone: payload.deliver_info.deliver_phone,
          toAddress: payload.deliver_info.deliver_detail_address,
          weight: built.meta.weightGram,
          note: dto?.note || dto?.shippingNote || null,
          metadata: { carrier: "SPX", serviceCode: "STANDARD", orderSn, payload: { list: built.list }, response: created },
          lastSyncedAt: new Date(),
        },
        create: {
          orderId,
          carrier: "SPX",
          trackingCode,
          shippingStatus,
          partnerStatus,
          codAmount,
          shippingFee: fee || null,
          fromName: payload.sender_info.sender_name,
          fromPhone: payload.sender_info.sender_phone,
          fromAddress: payload.sender_info.sender_detail_address,
          toName: payload.deliver_info.deliver_name,
          toPhone: payload.deliver_info.deliver_phone,
          toAddress: payload.deliver_info.deliver_detail_address,
          weight: built.meta.weightGram,
          note: dto?.note || dto?.shippingNote || null,
          metadata: { carrier: "SPX", serviceCode: "STANDARD", orderSn, payload: { list: built.list }, response: created },
          lastSyncedAt: new Date(),
        },
      });

      const stockOutResult = await this.carrierInventoryService.ensureOrderStockOutForShipment(tx, order, {
        carrier: "SPX",
        trackingCode,
        actorName: this.carrierInventoryService.getActorName(user),
      });

      await (tx as any).shipmentTimelineEvent.create({
        data: {
          shipmentId: shipment.id,
          orderId,
          carrier: "SPX",
          trackingCode,
          status: shippingStatus,
          partnerStatus,
          title: this.timelineTitle(shippingStatus),
          description: trackingCode ? `Mã vận đơn SPX: ${trackingCode}` : null,
          raw: created,
          source: "create",
          eventTime: new Date(),
        },
      });

      await tx.order.update({
        where: { id: orderId },
        data: {
          fulfillmentStatus: "PROCESSING" as any,
          status: "SHIPPED" as any,
          paymentStatus: codAmount > 0 && order.paymentStatus === "UNPAID" ? "COD_PENDING" as any : order.paymentStatus,
        },
      });

      return { duplicated: false, spx: created, shipment, stockOut: stockOutResult };
    }, { timeout: 30000, maxWait: 10000 });
  }

  async trackSpxByShipmentId(id: string) {
    const shipment = await this.prisma.shipment.findUnique({ where: { id } });
    if (!shipment) throw new BadRequestException("Không tìm thấy phiếu giao hàng");
    if (!shipment.trackingCode) throw new BadRequestException("Phiếu chưa có mã vận đơn SPX");

    const metadata = (shipment.metadata || {}) as any;
    const orderSn = this.getSpxOrderSn(metadata?.response) || metadata?.orderSn || shipment.trackingCode;
    const raw = await this.spxClient.trackOrder(orderSn);
    const partnerStatus = this.getSpxStatus(raw);
    const shippingStatus = this.mapSpxShippingStatus(partnerStatus);

    const updated = await this.prisma.shipment.update({
      where: { id: shipment.id },
      data: {
        shippingStatus,
        partnerStatus,
        metadata: { ...(metadata || {}), carrier: "SPX", trackingResponse: raw },
        lastSyncedAt: new Date(),
      },
    });

    await (this.prisma as any).shipmentTimelineEvent.create({
      data: {
        shipmentId: shipment.id,
        orderId: shipment.orderId,
        carrier: "SPX",
        trackingCode: shipment.trackingCode,
        status: shippingStatus,
        partnerStatus,
        title: this.timelineTitle(shippingStatus),
        raw,
        source: "manual_refresh",
        eventTime: new Date(),
      },
    });

    if (["DELIVERED", "RETURNING", "RETURNED", "FAILED", "CANCELLED"].includes(shippingStatus)) {
      await this.prisma.order.update({
        where: { id: shipment.orderId },
        data: {
          ...(shippingStatus === "DELIVERED" ? { status: "COMPLETED" as any, fulfillmentStatus: "FULFILLED" as any } : {}),
          ...(shippingStatus === "CANCELLED" ? { status: "CANCELLED" as any, fulfillmentStatus: "UNFULFILLED" as any } : {}),
          ...(shippingStatus === "RETURNING" || shippingStatus === "RETURNED" || shippingStatus === "FAILED" ? { fulfillmentStatus: "PROCESSING" as any } : {}),
        },
      });
    }

    return { source: "spx_live", shipment: updated, tracking: raw };
  }

  async cancelSpxShipmentByOrderId(orderId: string, user?: any) {
    const order = await this.prisma.order.findUnique({ where: { id: orderId }, include: { shipment: true } });
    if (!order) throw new BadRequestException("Không tìm thấy order");

    const shipment = order.shipment;
    const actorName = this.carrierInventoryService.getActorName(user);

    if (!shipment?.trackingCode) {
      const localResult = await this.prisma.$transaction(async (tx) => {
        const inventoryRestore = await this.carrierInventoryService.restoreOrderStockForShipmentCancel(tx, order, shipment, {
          carrier: "SPX",
          actorName,
        });

        const updatedOrder = await tx.order.update({
          where: { id: orderId },
          data: { status: "CANCELLED" as any, fulfillmentStatus: "UNFULFILLED" as any },
        });

        return { order: updatedOrder, inventoryRestore };
      });

      return localResult.order;
    }

    if (String(shipment.carrier || "").toUpperCase() !== "SPX") {
      throw new BadRequestException(`Vận đơn hiện tại không phải SPX (${shipment.carrier || "không rõ"})`);
    }

    const metadata = (shipment.metadata || {}) as any;
    const orderSn = this.getSpxOrderSn(metadata?.response) || metadata?.orderSn || metadata?.spxOrderSn || metadata?.externalOrderCode || "";
    if (!orderSn) {
      throw new BadRequestException("Thiếu order_sn SPX để huỷ đơn. Mở metadata.response.data.list[0].order_sn hoặc đồng bộ lại đơn trước.");
    }

    const raw = await this.spxClient.cancelOrder(orderSn);

    const result = await this.prisma.$transaction(async (tx) => {
      const inventoryRestore = await this.carrierInventoryService.restoreOrderStockForShipmentCancel(tx, order, shipment, {
        carrier: "SPX",
        actorName,
      });

      const updatedShipment = await tx.shipment.update({
        where: { id: shipment.id },
        data: {
          shippingStatus: "CANCELLED",
          partnerStatus: "cancel",
          metadata: { ...(metadata || {}), carrier: "SPX", cancelResponse: raw },
          lastSyncedAt: new Date(),
        },
      });

      await (tx as any).shipmentTimelineEvent.create({
        data: {
          shipmentId: shipment.id,
          orderId,
          carrier: "SPX",
          trackingCode: shipment.trackingCode,
          status: "CANCELLED",
          partnerStatus: "cancel",
          title: "Đã huỷ vận đơn",
          raw,
          source: "cancel",
          eventTime: new Date(),
        },
      });

      const updatedOrder = await tx.order.update({
        where: { id: orderId },
        data: { status: "CANCELLED" as any, fulfillmentStatus: "UNFULFILLED" as any },
      });

      return { order: updatedOrder, shipment: updatedShipment, inventoryRestore };
    });

    return result.order;
  }

}
