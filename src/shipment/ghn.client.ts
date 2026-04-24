import { BadRequestException, Injectable, Logger } from "@nestjs/common";

@Injectable()
export class GhnClient {
  private readonly logger = new Logger(GhnClient.name);

  private readonly token = process.env.GHN_TOKEN || "";
  private readonly shopId = process.env.GHN_SHOP_ID || "";
  private readonly useDev = (process.env.GHN_USE_DEV || "true") === "true";

  private readonly baseUrl = this.useDev
    ? "https://dev-online-gateway.ghn.vn/shiip/public-api"
    : "https://online-gateway.ghn.vn/shiip/public-api";

  private getHeaders(includeShopId = false) {
    if (!this.token) {
      throw new BadRequestException("Thiếu GHN_TOKEN");
    }

    const headers: Record<string, string> = {
      Token: this.token,
      "Content-Type": "application/json",
    };

    if (includeShopId) {
      if (!this.shopId) {
        throw new BadRequestException("Thiếu GHN_SHOP_ID");
      }
      headers.ShopId = this.shopId;
    }

    return headers;
  }

  private async request(
    method: "GET" | "POST",
    path: string,
    body?: Record<string, unknown>,
    includeShopId = false
  ) {
    const url = `${this.baseUrl}${path}`;

    this.logger.log(
      `[GHN] ${method} ${path} | shopId=${this.shopId} | body=${JSON.stringify(
        body || {}
      )}`
    );

    const res = await fetch(url, {
      method,
      headers: this.getHeaders(includeShopId),
      ...(body ? { body: JSON.stringify(body) } : {}),
    });

    const json = await res.json().catch(() => null);

    if (!res.ok || json?.code !== 200) {
      this.logger.error(
        `[GHN ERROR] ${path} | status=${res.status} | response=${JSON.stringify(
          json
        )}`
      );

      throw new BadRequestException(
        json?.message || `GHN request failed: ${path}`
      );
    }

    return json.data;
  }

  private async get(path: string, includeShopId = false) {
    return this.request("GET", path, undefined, includeShopId);
  }

  private async post(
    path: string,
    body: Record<string, unknown>,
    includeShopId = false
  ) {
    return this.request("POST", path, body, includeShopId);
  }

  // ========================
  // MASTER DATA
  // ========================
  async getProvinces() {
    return this.get("/master-data/province");
  }

  async getDistricts(provinceId?: number) {
    const query =
      typeof provinceId === "number" ? `?province_id=${provinceId}` : "";
    return this.get(`/master-data/district${query}`);
  }

  async getWards(districtId: number) {
    return this.post("/master-data/ward", { district_id: districtId });
  }

  // ========================
  // SERVICES
  // ========================
  async getAvailableServices(fromDistrict: number, toDistrict: number) {
    return this.post(
      "/v2/shipping-order/available-services",
      {
        shop_id: Number(this.shopId),
        from_district: fromDistrict,
        to_district: toDistrict,
      },
      false
    );
  }

  // ========================
  // FEE
  // ========================
  async calculateFee(body: Record<string, unknown>) {
    return this.post("/v2/shipping-order/fee", body, true);
  }

  // ========================
  // LEADTIME
  // ========================
  async getLeadTime(body: {
    service_id: number;
    from_district_id: number;
    from_ward_code?: string;
    to_district_id: number;
    to_ward_code: string;
  }) {
    return this.post("/v2/shipping-order/leadtime", body, true);
  }

  // ========================
  // CREATE ORDER
  // ========================
  async createOrder(body: Record<string, unknown>) {
    return this.post("/v2/shipping-order/create", body, true);
  }

  // ========================
  // CANCEL ORDER
  // ========================
  async cancelOrder(orderCode: string) {
    if (!orderCode) {
      throw new BadRequestException("Thiếu orderCode GHN");
    }

    return this.post(
      "/v2/switch-status/cancel",
      {
        order_codes: [orderCode],
      },
      true
    );
  }

  // ========================
  // TRACK ORDER
  // ========================
  async getOrderDetail(orderCode?: string, clientOrderCode?: string) {
    if (!orderCode && !clientOrderCode) {
      throw new BadRequestException("Thiếu orderCode hoặc clientOrderCode");
    }

    return this.post(
      "/v2/shipping-order/detail",
      {
        ...(orderCode ? { order_code: orderCode } : {}),
        ...(clientOrderCode ? { client_order_code: clientOrderCode } : {}),
      },
      true
    );
  }

  // ========================
  // UPDATE COD
  // ========================
  async updateCod(orderCode: string, codAmount: number) {
    if (!orderCode) {
      throw new BadRequestException("Thiếu orderCode GHN");
    }

    if (Number.isNaN(Number(codAmount)) || Number(codAmount) < 0) {
      throw new BadRequestException("codAmount không hợp lệ");
    }

    return this.post(
      "/v2/shipping-order/updateCOD",
      {
        order_code: orderCode,
        cod_amount: Math.round(Number(codAmount)),
      },
      true
    );
  }

  // ========================
  // REDELIVERY
  // ========================
  async redelivery(orderCode: string) {
    if (!orderCode) {
      throw new BadRequestException("Thiếu orderCode GHN");
    }

    return this.post(
      "/v2/switch-status/re-delivery",
      {
        order_codes: [orderCode],
      },
      true
    );
  }

  // ========================
  // UPDATE ORDER INFO
  // Dùng sau nếu m muốn sửa tên / sđt / địa chỉ phía GHN
  // ========================
  async updateOrderInfo(body: {
    order_code: string;
    to_name?: string;
    to_phone?: string;
    to_address?: string;
    to_ward_code?: string;
    to_district_id?: number;
    note?: string;
  }) {
    if (!body?.order_code) {
      throw new BadRequestException("Thiếu order_code GHN");
    }

    return this.post("/v2/shipping-order/update", body, true);
  }
}