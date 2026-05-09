import { BadRequestException, Injectable, Logger } from "@nestjs/common";

type ViettelPostRequestOptions = {
  method?: "GET" | "POST" | "PUT" | "DELETE";
  body?: any;
  auth?: boolean;
};

@Injectable()
export class ViettelPostClient {
  private readonly logger = new Logger(ViettelPostClient.name);

  private readonly baseUrl = (
    process.env.VIETTELPOST_BASE_URL ||
    "https://partner.viettelpost.vn/v2"
  ).replace(/\/$/, "");

  private tokenCache: {
    token: string;
    expiresAt: number;
  } | null = null;

  private endpoint(name: string, fallback: string) {
    const key = `VIETTELPOST_${name}_PATH`;
    const raw = process.env[key] || fallback;
    return raw.startsWith("/") ? raw : `/${raw}`;
  }

  private get username() {
    return process.env.VIETTELPOST_USERNAME || "";
  }

  private get password() {
    return process.env.VIETTELPOST_PASSWORD || "";
  }

  private get staticToken() {
    return process.env.VIETTELPOST_TOKEN || "";
  }

  private normalizeResponse(data: any) {
    if (data?.error) {
      throw new BadRequestException(data?.message || "ViettelPost request failed");
    }

    if (data?.status === 500 || data?.statusCode >= 400) {
      throw new BadRequestException(data?.message || "ViettelPost request failed");
    }

    return data?.data ?? data;
  }

  private async request<T = any>(
    path: string,
    options: ViettelPostRequestOptions = {}
  ): Promise<T> {
    const method = options.method || "GET";
    const headers: Record<string, string> = {
      Accept: "application/json",
      "Content-Type": "application/json",
    };

    if (options.auth !== false) {
      const token = await this.getToken();
      if (token) {
        headers.Token = token;
        headers.Authorization = `Bearer ${token}`;
      }
    }

    const url = `${this.baseUrl}${path}`;

    const res = await fetch(url, {
      method,
      headers,
      body:
        method === "GET" || options.body === undefined
          ? undefined
          : JSON.stringify(options.body),
    });

    const text = await res.text();
    let data: any = null;

    try {
      data = text ? JSON.parse(text) : null;
    } catch {
      data = text;
    }

    if (!res.ok) {
      this.logger.error(
        `[VIETTELPOST ERROR] ${path} | status=${res.status} | response=${text}`
      );

      throw new BadRequestException(
        data?.message ||
          data?.Message ||
          data?.error ||
          `ViettelPost request failed: ${path}`
      );
    }

    return this.normalizeResponse(data) as T;
  }

  async login() {
    if (this.staticToken) return this.staticToken;

    if (!this.username || !this.password) {
      throw new BadRequestException(
        "Thiếu VIETTELPOST_USERNAME / VIETTELPOST_PASSWORD hoặc VIETTELPOST_TOKEN"
      );
    }

    const loginPath = this.endpoint("LOGIN", "/user/Login");

    const res: any = await this.request(loginPath, {
      method: "POST",
      auth: false,
      body: {
        USERNAME: this.username,
        PASSWORD: this.password,
        username: this.username,
        password: this.password,
      },
    });

    const token =
      res?.token ||
      res?.TOKEN ||
      res?.access_token ||
      res?.data?.token ||
      res?.data?.TOKEN ||
      "";

    if (!token) {
      throw new BadRequestException("ViettelPost không trả về token đăng nhập");
    }

    this.tokenCache = {
      token,
      expiresAt: Date.now() + 23 * 60 * 60 * 1000,
    };

    return token;
  }

  async getToken() {
    if (this.staticToken) return this.staticToken;

    if (this.tokenCache && this.tokenCache.expiresAt > Date.now()) {
      return this.tokenCache.token;
    }

    return this.login();
  }

  listProvinces() {
    return this.request<any[]>(this.endpoint("PROVINCES", "/categories/listProvince"));
  }

  listDistricts(provinceId: number) {
    return this.request<any[]>(
      `${this.endpoint("DISTRICTS", "/categories/listDistrict")}?provinceId=${provinceId}`
    );
  }

  listWards(districtId: number) {
    return this.request<any[]>(
      `${this.endpoint("WARDS", "/categories/listWards")}?districtId=${districtId}`
    );
  }

  getPrice(payload: any) {
    return this.request<any>(this.endpoint("PRICE", "/order/getPrice"), {
      method: "POST",
      body: payload,
    });
  }

  getPriceAllNlp(payload: any) {
    return this.request<any>(
      this.endpoint("PRICE_ALL_NLP", "/order/getPriceAllNlp"),
      {
        method: "POST",
        body: payload,
      }
    );
  }

  listServices(payload: any) {
    return this.request<any>(
      this.endpoint("SERVICES", "/categories/listService"),
      {
        method: "POST",
        body: payload,
      }
    );
  }

  createOrder(payload: any) {
    return this.request<any>(this.endpoint("CREATE_ORDER", "/order/createOrder"), {
      method: "POST",
      body: payload,
    });
  }

  trackOrder(orderNumber: string) {
    const path = this.endpoint("TRACK", "/order/getOrderStatus");
    const separator = path.includes("?") ? "&" : "?";

    return this.request<any>(
      `${path}${separator}ORDER_NUMBER=${encodeURIComponent(orderNumber)}`
    );
  }

  cancelOrder(orderNumber: string) {
    const path = this.endpoint("CANCEL", "/order/UpdateOrder");

    return this.request<any>(path, {
      method: "POST",
      body: {
        ORDER_NUMBER: orderNumber,
        TYPE: 4,
        NOTE: "Cancel from The 1970 Operations",
      },
    });
  }
}
