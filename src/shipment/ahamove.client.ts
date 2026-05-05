import { BadRequestException, Injectable, Logger } from "@nestjs/common";

@Injectable()
export class AhamoveClient {
  private readonly logger = new Logger(AhamoveClient.name);

  private readonly apiKey = process.env.AHAMOVE_API_KEY || "";
  private readonly baseUrl =
    process.env.AHAMOVE_API_BASE_URL || "https://partner-apistg.ahamove.com";

  private getHeaders() {
    if (!this.apiKey) {
      throw new BadRequestException("Thiếu AHAMOVE_API_KEY");
    }

    return {
      Authorization: `Bearer ${this.apiKey}`,
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

    const res = await fetch(url, {
      method,
      headers: this.getHeaders(),
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
        json?.message || json?.description || `Ahamove request failed: ${path}`
      );
    }

    return json;
  }

  async estimate(body: Record<string, unknown>) {
    return this.request("POST", "/v3/orders/estimate", body);
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
