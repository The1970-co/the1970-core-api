import { BadRequestException, Injectable, Logger } from "@nestjs/common";

type SpxRequestOptions = {
  method?: "GET" | "POST" | "PUT" | "DELETE";
  body?: any;
  query?: Record<string, any>;
  allowBusinessError?: boolean;
};

@Injectable()
export class SpxClient {
  private readonly logger = new Logger(SpxClient.name);

  private readonly baseUrl = (
    process.env.SPX_BASE_URL ||
    process.env.SPX_API_BASE_URL ||
    "https://spx.vn"
  ).replace(/\/$/, "");

  private endpoint(name: string, fallback: string) {
    const raw = process.env[`SPX_${name}_PATH`] || fallback;
    return raw.startsWith("/") ? raw : `/${raw}`;
  }

  private get userId() {
    return String(
      process.env.SPX_USER_ID ||
        process.env.SPX_CLIENT_ID ||
        process.env.SPX_SHOP_ID ||
        ""
    ).trim();
  }

  private get userSecret() {
    return String(
      process.env.SPX_USER_SECRET ||
        process.env.SPX_SECRET_KEY ||
        process.env.SPX_API_TOKEN ||
        ""
    ).trim();
  }

  private get accountId() {
    return String(process.env.SPX_ACCOUNT_ID || process.env.SPX_ACCOUNT || "").trim();
  }

  private get isApiEnabled() {
    const value = String(process.env.SPX_API_ENABLED || "").trim().toLowerCase();
    return value === "true" || value === "1" || value === "yes";
  }

  private assertCredentials() {
    if (!this.userId) throw new BadRequestException("Thiếu SPX_USER_ID");
    if (!this.userSecret) throw new BadRequestException("Thiếu SPX_USER_SECRET");
  }

  private assertApiCreateEnabled() {
    if (!this.isApiEnabled) {
      throw new BadRequestException(
        "SPX chưa được bật quyền API tạo vận đơn. Hiện chỉ dùng được báo giá; vui lòng tạo đơn SPX thủ công hoặc chờ SPX bật quyền kết nối API."
      );
    }

    if (!String(process.env.SPX_CREATE_ORDER_PATH || "").trim()) {
      throw new BadRequestException(
        "Thiếu SPX_CREATE_ORDER_PATH chính thức. Sau khi SPX bật quyền API, cần cấu hình endpoint tạo vận đơn họ cấp."
      );
    }
  }

  private buildQuery(query?: Record<string, any>) {
    const pairs = Object.entries(query || {}).filter(
      ([, value]) => value !== undefined && value !== null && String(value) !== ""
    );

    if (!pairs.length) return "";

    const search = new URLSearchParams();
    for (const [key, value] of pairs) search.set(key, String(value));
    return `?${search.toString()}`;
  }

  private buildHeaders() {
    this.assertCredentials();

    const headers: Record<string, string> = {
      Accept: "application/json, text/plain, */*",
      "Content-Type": "application/json;charset=UTF-8",
      "X-SPX-USER-ID": this.userId,
      "X-SPX-USER-SECRET": this.userSecret,
      "X-User-Id": this.userId,
      "X-User-Secret": this.userSecret,
      user_id: this.userId,
      user_secret: this.userSecret,
    };

    if (this.accountId) {
      headers["X-SPX-Account-Id"] = this.accountId;
      headers.account_id = this.accountId;
    }

    return headers;
  }

  private normalizeResponse(data: any, allowBusinessError = false) {
    if (!data) return data;

    // SPX web/open endpoint dùng retcode=0 là success. Với quote, retcode != 0 vẫn trả về
    // để service/UI tự disable gói, không làm hỏng toàn bộ bảng báo giá.
    if (data?.retcode !== undefined && data?.retcode !== 0 && data?.retcode !== "0") {
      if (allowBusinessError) return data;
      throw new BadRequestException(data?.message || data?.detail || "SPX request failed");
    }

    const code = data?.code ?? data?.error_code ?? data?.errorCode ?? data?.status_code;
    const hasExplicitFailure =
      data?.success === false ||
      data?.ok === false ||
      data?.error ||
      (code !== undefined &&
        code !== null &&
        ![0, 200, "0", "200", "success", "SUCCESS"].includes(code));

    if (hasExplicitFailure) {
      if (allowBusinessError) return data;
      throw new BadRequestException(
        data?.message || data?.msg || data?.error_message || data?.error || "SPX request failed"
      );
    }

    return data;
  }

  private async request<T = any>(path: string, options: SpxRequestOptions = {}) {
    const method = options.method || "GET";
    const queryString = this.buildQuery(options.query);
    const bodyText = method === "GET" || options.body === undefined ? "" : JSON.stringify(options.body);
    const url = `${this.baseUrl}${path}${queryString}`;

    this.logger.log(`[SPX] ${method} ${path}`);

    const res = await fetch(url, {
      method,
      headers: this.buildHeaders(),
      body: bodyText || undefined,
    });

    const text = await res.text();
    let data: any = null;
    try {
      data = text ? JSON.parse(text) : null;
    } catch {
      data = { raw: text };
    }

    if (!res.ok) {
      this.logger.error(`[SPX ERROR] ${method} ${path} | status=${res.status} | response=${text}`);
      throw new BadRequestException(
        data?.message || data?.msg || data?.error_message || data?.error || `SPX request failed: ${path}`
      );
    }

    return this.normalizeResponse(data, options.allowBusinessError) as T;
  }

  getSubLocationLayerInfo(input: { locationId: number; subLevel: number; country?: string; addressVersion?: number }) {
    return this.request(this.endpoint("LOCATION_LAYER", "/shipment/merchant/open/location/get_sub_location_layer_info"), {
      method: "GET",
      query: {
        country: input.country || "VN",
        location_id: input.locationId,
        sub_level: input.subLevel,
        address_version: input.addressVersion ?? 0,
      },
      allowBusinessError: true,
    });
  }

  globalAutocomplete(input: { input: string; size?: number; country?: string; language?: string }) {
    return this.request(this.endpoint("GLOBAL_AUTOCOMPLETE", "/shipment/merchant/open/location/global_autocomplete"), {
      method: "GET",
      query: {
        size: input.size || 10,
        input: input.input,
        country: input.country || "VN",
        language: input.language || "vi",
      },
      allowBusinessError: true,
    });
  }

  addressSegmentation(input: { input: string; country?: string; language?: string }) {
    return this.request(this.endpoint("ADDRESS_SEGMENTATION", "/shipment/merchant/open/location/address_segmentation"), {
      method: "POST",
      body: {
        input: input.input,
        country: input.country || "VN",
        language: input.language || "vi",
      },
      allowBusinessError: true,
    });
  }

  getLocationId(input: { locationName: string; country?: string; addressVersion?: number }) {
    return this.request(this.endpoint("GET_LOCATION_ID", "/shipment/merchant/open/location/getLocationId"), {
      method: "GET",
      query: {
        location_name: input.locationName,
        country: input.country || "VN",
        address_version: input.addressVersion ?? 0,
      },
      allowBusinessError: true,
    });
  }

  quote(body: any) {
    return this.request(this.endpoint("QUOTE", "/shipment/order/open/order/batch_check_order"), {
      method: "POST",
      body,
      allowBusinessError: true,
    });
  }

  createOrder(body: any) {
    this.assertApiCreateEnabled();
    return this.request(this.endpoint("CREATE_ORDER", process.env.SPX_CREATE_ORDER_PATH || ""), {
      method: "POST",
      body,
    });
  }

  trackOrder(orderSnOrTrackingCode: string) {
    if (!this.isApiEnabled) {
      throw new BadRequestException("SPX chưa được bật quyền API tracking. Vui lòng tra cứu trên SPX/Sapo thủ công.");
    }

    return this.request(this.endpoint("TRACK", "/shipment/order/open/order/get_order_info"), {
      method: String(process.env.SPX_TRACK_METHOD || "GET").toUpperCase() as any,
      query: {
        order_sn: orderSnOrTrackingCode,
        orderSn: orderSnOrTrackingCode,
        tracking_code: orderSnOrTrackingCode,
      },
      body: {
        order_sn: orderSnOrTrackingCode,
        orderSn: orderSnOrTrackingCode,
        tracking_code: orderSnOrTrackingCode,
      },
      allowBusinessError: true,
    });
  }

  cancelOrder(orderSn: string) {
    if (!this.isApiEnabled) {
      throw new BadRequestException("SPX chưa được bật quyền API hủy vận đơn. Vui lòng hủy trên SPX/Sapo thủ công.");
    }

    return this.request(this.endpoint("CANCEL", "/shipment/order/logistic/order/batch_cancel_order"), {
      method: "POST",
      body: { list: [orderSn] },
    });
  }
}
