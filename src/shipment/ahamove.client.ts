import { BadRequestException, Injectable, Logger } from "@nestjs/common";

@Injectable()
export class AhamoveClient {
  private readonly logger = new Logger(AhamoveClient.name);

  private readonly apiKey = process.env.AHAMOVE_API_KEY || "";

  private readonly accountPhone =
    process.env.AHAMOVE_ACCOUNT_PHONE || "";

  private readonly baseUrl =
    process.env.AHAMOVE_BASE_URL ||
    "https://partner-apistg.ahamove.com";

  private accessToken: string | null = null;

  private async getAccessToken() {
    if (this.accessToken) {
      return this.accessToken;
    }

    if (!this.apiKey) {
      throw new BadRequestException("Thiếu AHAMOVE_API_KEY");
    }

    if (!this.accountPhone) {
      throw new BadRequestException(
        "Thiếu AHAMOVE_ACCOUNT_PHONE"
      );
    }

    const url = `${this.baseUrl}/v3/accounts/token`;

    this.logger.log(
      `[AHAMOVE AUTH] request token | phone=${this.accountPhone}`
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

    const json = await res.json().catch(() => null);

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
    method: "GET" | "POST",
    path: string,
    body?: Record<string, unknown>
  ) {
    const url = `${this.baseUrl}${path}`;

    this.logger.log(
      `[AHAMOVE] ${method} ${path} | body=${JSON.stringify(body || {})}`
    );

    const headers = await this.getHeaders();

    const res = await fetch(url, {
      method,
      headers,
      ...(body ? { body: JSON.stringify(body) } : {}),
    });

    const json = await res.json().catch(() => null);

    if (!res.ok) {
      this.logger.error(
        `[AHAMOVE ERROR] ${path} | status=${res.status} | response=${JSON.stringify(
          json
        )}`
      );

      throw new BadRequestException(
        json?.message ||
          json?.description ||
          `Ahamove request failed: ${path}`
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

    return this.request("POST", `/v3/orders/${orderId}/cancel`);
  }
}