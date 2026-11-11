import { BadRequestException, Injectable, Logger } from "@nestjs/common";

@Injectable()
export class AhamoveClient {
  private readonly logger = new Logger(AhamoveClient.name);

  private readonly apiKey = String(process.env.AHAMOVE_API_KEY || process.env.AHAMOVE_TOKEN || "").trim();

  private readonly accountPhone = String(
    process.env.AHAMOVE_ACCOUNT_PHONE || process.env.AHAMOVE_PHONE || ""
  ).trim();

  private readonly baseUrl = this.resolveBaseUrl();

  private accessToken: string | null = null;

  private resolveBaseUrl() {
    const explicit = String(process.env.AHAMOVE_BASE_URL || "").trim();
    if (explicit) return explicit.replace(/\/$/, "");

    const env = String(process.env.AHAMOVE_ENV || process.env.AHAMOVE_MODE || "").trim().toLowerCase();
    if (env === "sandbox" || env === "staging" || env === "test") {
      return "https://partner-apistg.ahamove.com";
    }

    return "https://partner-api.ahamove.com";
  }

  private maskPhone(phone: string) {
    const clean = String(phone || "").replace(/\D/g, "");
    if (clean.length <= 4) return clean || "missing";
    return `${"*".repeat(Math.max(0, clean.length - 4))}${clean.slice(-4)}`;
  }

  private async getAccessToken() {
    if (this.accessToken) {
      return this.accessToken;
    }

    if (!this.apiKey) {
      throw new BadRequestException("Thiếu AHAMOVE_API_KEY hoặc AHAMOVE_TOKEN");
    }

    if (!this.accountPhone) {
      throw new BadRequestException(
        "Thiếu AHAMOVE_ACCOUNT_PHONE"
      );
    }

    const url = `${this.baseUrl}/v3/accounts/token`;

    this.logger.log(
      `[AHAMOVE AUTH] request token | baseUrl=${this.baseUrl} | phone=${this.maskPhone(this.accountPhone)}`
    );

    const res = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        api_key: this.apiKey,
        mobile: this.accountPhone,
      }),
    });

    const rawText = await res.text().catch(() => "");
    const json = rawText
      ? (() => {
          try {
            return JSON.parse(rawText);
          } catch {
            return { raw: rawText };
          }
        })()
      : null;

    if (!res.ok) {
      this.logger.error(
        `[AHAMOVE AUTH ERROR] status=${res.status} | response=${JSON.stringify(
          json
        )}`
      );

      throw new BadRequestException(
        json?.message ||
          json?.description ||
          "Không lấy được token AhaMove"
      );
    }

    const token =
      json?.token ||
      json?.access_token ||
      json?.data?.token ||
      null;

    if (!token) {
      throw new BadRequestException(
        "AhaMove không trả về token"
      );
    }

    this.accessToken = token;

    return token;
  }

  private async getHeaders() {
    const token = await this.getAccessToken();

    return {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    };
  }

  private async request(
    method: "GET" | "POST" | "DELETE",
    path: string,
    body?: Record<string, unknown>
  ) {
    const url = `${this.baseUrl}${path}`;

    this.logger.log(
      `[AHAMOVE] ${method} ${this.baseUrl}${path} | body=${JSON.stringify(body || {})}`
    );

    const headers = await this.getHeaders();

    const res = await fetch(url, {
      method,
      headers,
      ...(body ? { body: JSON.stringify(body) } : {}),
    });

    const rawText = await res.text().catch(() => "");
    const json = rawText
      ? (() => {
          try {
            return JSON.parse(rawText);
          } catch {
            return { raw: rawText };
          }
        })()
      : null;

    if (!res.ok) {
      this.logger.error(
        `[AHAMOVE ERROR] ${path} | status=${res.status} | response=${JSON.stringify(
          json
        )}`
      );

      const message =
        json?.message ||
        json?.description ||
        json?.error ||
        json?.code ||
        json?.raw ||
        (json ? JSON.stringify(json) : rawText) ||
        `Ahamove request failed: ${path}`;

      throw new BadRequestException(
        `${message} | status=${res.status} | path=${path} | baseUrl=${this.baseUrl} | phone=${this.maskPhone(this.accountPhone)}`
      );
    }

    return json;
  }

  // Diagnostic only: the consumer promotion endpoint is not part of the
  // documented Partner API. Never expose the bearer token or raw headers.
  async probePromotions(query: {
    lat: number; lng: number; serviceId: string; serviceFee: number;
    distance: number; orderFee: number; paymentMethod: string;
    deliveryTypes?: string;
  }) {
    if (process.env.AHAMOVE_PROMOTION_PROBE_ENABLED !== "true") {
      return { supported: false, reason: "probe_disabled", message: "Set AHAMOVE_PROMOTION_PROBE_ENABLED=true to test." };
    }

    const params = new URLSearchParams({
      lat: String(query.lat), lng: String(query.lng),
      payment_method: query.paymentMethod,
      service_id: query.serviceId,
      service_fee: String(query.serviceFee),
      distance: String(query.distance),
      order_fee: String(query.orderFee),
      delivery_types: query.deliveryTypes || "INSTANT",
      app_source: "web_user",
      page: "0",
    });
    const token = await this.getAccessToken();
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 8000);
    try {
      const response = await fetch(
        `https://api.ahamove.com/api/v3/private/promotion/list?${params.toString()}`,
        { method: "GET", headers: { Authorization: `Bearer ${token}`, Accept: "application/json" }, signal: controller.signal },
      );
      const raw = await response.text();
      let payload: any = null;
      try { payload = raw ? JSON.parse(raw) : null; } catch { /* Do not expose upstream HTML or unexpected responses. */ }
      const items = Array.isArray(payload)
        ? payload
        : Array.isArray(payload?.data) ? payload.data : null;
      return {
        supported: response.ok && items !== null,
        httpStatus: response.status,
        itemCount: items?.length ?? null,
        // Only display non-sensitive promotion fields; never dump internal payloads.
        promotions: items?.slice(0, 50).map((item: any) => ({
          code: String(item?.promo_code || item?.code || ""),
          title: String(item?.title || item?.name || ""),
          description: String(item?.description || ""),
          discount: item?.discount ?? item?.discount_amount ?? null,
        })) ?? [],
        reason: response.status === 401 || response.status === 403
          ? "partner_token_not_authorized_for_consumer_promotions"
          : response.status === 404 ? "endpoint_unavailable" :
            response.ok && items === null ? "unexpected_response_shape" :
            !response.ok ? "upstream_request_failed" : null,
      };
    } catch (error) {
      return { supported: false, reason: error instanceof Error && error.name === "AbortError" ? "timeout" : "network_error" };
    } finally { clearTimeout(timeout); }
  }

  async listServices(cityId = "HAN", deliveryType?: "INSTANT" | "TRUCK") {
    const params = new URLSearchParams({ city_id: cityId });
    if (deliveryType) params.set("delivery_type", deliveryType);
    return this.request("GET", `/v3/services?${params.toString()}`);
  }

  async estimate(body: Record<string, unknown>) {
    return this.request("POST", "/v3/orders/estimates", body);
  }

  async createOrder(body: Record<string, unknown>) {
    return this.request("POST", "/v3/orders", body);
  }

  async getOrderDetail(orderId: string) {
    if (!orderId) {
      throw new BadRequestException("Thiếu ahamoveOrderId");
    }

    return this.request("GET", `/v3/orders/${orderId}`);
  }

  async cancelOrder(orderId: string) {
    if (!orderId) {
      throw new BadRequestException("Thiếu ahamoveOrderId");
    }

    const comment = String(
      process.env.AHAMOVE_CANCEL_COMMENT || "Khách hàng muốn hủy đơn"
    ).trim();

    return this.request("DELETE", `/v3/orders/${orderId}`, {
      comment,
    });
  }
}