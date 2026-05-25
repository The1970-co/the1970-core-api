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
        `Ahamove request failed: ${path}`;

      throw new BadRequestException(
        `${message} | baseUrl=${this.baseUrl} | phone=${this.maskPhone(this.accountPhone)}`
      );
    }

    return json;
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

    return this.request("DELETE", `/v3/orders/${orderId}`);
  }
}