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
    source: string;
  } | null = null;

  private endpoint(name: string, fallback: string) {
    const key = `VIETTELPOST_${name}_PATH`;
    const raw = process.env[key] || fallback;
    return raw.startsWith("/") ? raw : `/${raw}`;
  }

  private stripBearer(raw?: string | null) {
    return String(raw || "")
      .replace(/^Bearer\s+/i, "")
      .trim();
  }

  private looksLikeJwt(token: string) {
    return token.split(".").length === 3;
  }

  private get username() {
    return process.env.VIETTELPOST_USERNAME || "";
  }

  private get password() {
    return process.env.VIETTELPOST_PASSWORD || "";
  }

  private get longLivedToken() {
    return this.stripBearer(
      process.env.VIETTELPOST_ACCOUNT_TOKEN ||
        process.env.VIETTELPOST_ACCESS_TOKEN ||
        process.env.VIETTELPOST_LONG_TOKEN ||
        ""
    );
  }

  private get webToken() {
    return this.stripBearer(
      process.env.VIETTELPOST_WEB_TOKEN ||
        process.env.VIETTELPOST_SECRET_TOKEN ||
        process.env.VIETTELPOST_PARTNER_TOKEN ||
        process.env.VIETTELPOST_TOKEN ||
        ""
    );
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

  private extractToken(data: any) {
    const normalized = data?.data ?? data;

    return this.stripBearer(
      normalized?.token ||
        normalized?.TOKEN ||
        normalized?.access_token ||
        normalized?.accessToken ||
        data?.token ||
        data?.TOKEN ||
        data?.access_token ||
        data?.accessToken ||
        ""
    );
  }

  private async rawRequest<T = any>(
    path: string,
    options: ViettelPostRequestOptions & { token?: string } = {}
  ): Promise<T> {
    const method = options.method || "GET";
    const headers: Record<string, string> = {
      Accept: "application/json",
      "Content-Type": "application/json",
    };

    const token = this.stripBearer(options.token);
    if (token) {
      headers.Token = token;
    }

    const url = `${this.baseUrl}${path}`;
    const body =
      method === "GET" || options.body === undefined
        ? undefined
        : JSON.stringify(options.body);

    const res = await fetch(url, {
      method,
      headers,
      body,
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
        `[VIETTELPOST ERROR] ${path} | status=${res.status} | tokenTail=${
          token ? token.slice(-6) : "none"
        } | response=${text}`
      );

      throw new BadRequestException(
        data?.message ||
          data?.Message ||
          data?.error ||
          `ViettelPost request failed: ${path}`
      );
    }

    return data as T;
  }

  private async request<T = any>(
    path: string,
    options: ViettelPostRequestOptions = {}
  ): Promise<T> {
    const token = options.auth === false ? "" : await this.getToken();

    const data = await this.rawRequest<any>(path, {
      ...options,
      token,
    });

    return this.normalizeResponse(data) as T;
  }

  private async loginVtpWithWebToken() {
    const tokenFromWeb = this.webToken;

    if (!tokenFromWeb) {
      throw new BadRequestException(
        "Thiếu VIETTELPOST_WEB_TOKEN hoặc VIETTELPOST_TOKEN"
      );
    }

    const loginVtpPath = this.endpoint("LOGIN_VTP", "/user/LoginVTP");

    const data: any = await this.rawRequest(loginVtpPath, {
      method: "POST",
      auth: false,
      body: {
        token: tokenFromWeb,
      },
    });

    const apiToken = this.extractToken(data);

    if (!apiToken) {
      throw new BadRequestException("ViettelPost LoginVTP không trả về token");
    }

    this.logger.log(
      `[VIETTELPOST_LOGIN_VTP] ok | webTail=${tokenFromWeb.slice(
        -6
      )} | apiTail=${apiToken.slice(-6)}`
    );

    return apiToken;
  }

  private async loginByUsernamePassword() {
    if (!this.username || !this.password) {
      throw new BadRequestException(
        "Thiếu VIETTELPOST_USERNAME / VIETTELPOST_PASSWORD"
      );
    }

    const loginPath = this.endpoint("LOGIN", "/user/Login");

    const loginData: any = await this.rawRequest(loginPath, {
      method: "POST",
      auth: false,
      body: {
        USERNAME: this.username,
        PASSWORD: this.password,
        username: this.username,
        password: this.password,
      },
    });

    const shortToken = this.extractToken(loginData);

    if (!shortToken) {
      throw new BadRequestException("ViettelPost Login không trả về token ngắn hạn");
    }

    const ownerConnectPath = this.endpoint("OWNERCONNECT", "/user/ownerconnect");

    const ownerData: any = await this.rawRequest(ownerConnectPath, {
      method: "POST",
      auth: false,
      token: shortToken,
      body: {
        USERNAME: this.username,
        PASSWORD: this.password,
        username: this.username,
        password: this.password,
      },
    });

    const longToken = this.extractToken(ownerData);

    if (!longToken) {
      throw new BadRequestException(
        "ViettelPost ownerconnect không trả về token dài hạn"
      );
    }

    this.logger.log(
      `[VIETTELPOST_OWNERCONNECT] ok | shortTail=${shortToken.slice(
        -6
      )} | longTail=${longToken.slice(-6)}`
    );

    return longToken;
  }

  async login() {
    if (this.longLivedToken) {
      return this.longLivedToken;
    }

    const legacyToken = this.webToken;

    if (legacyToken && this.looksLikeJwt(legacyToken)) {
      return legacyToken;
    }

    if (legacyToken) {
      return this.loginVtpWithWebToken();
    }

    return this.loginByUsernamePassword();
  }

  async getToken() {
    if (this.longLivedToken) return this.longLivedToken;

    if (this.tokenCache && this.tokenCache.expiresAt > Date.now()) {
      return this.tokenCache.token;
    }

    const token = await this.login();

    this.tokenCache = {
      token,
      source: this.looksLikeJwt(this.webToken) ? "env-jwt" : "login",
      expiresAt: Date.now() + 12 * 60 * 60 * 1000,
    };

    return token;
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

  private hasInventoryLikePayload(input: any): boolean {
    const stack: any[] = [input];
    let scanned = 0;

    while (stack.length && scanned < 500) {
      const node = stack.shift();
      scanned += 1;

      if (!node) continue;

      if (Array.isArray(node)) {
        if (
          node.some((item: any) => {
            if (!item || typeof item !== "object") return false;
            const keys = Object.keys(item).map((key) => key.toLowerCase());
            return keys.some((key) =>
              [
                "groupaddressid",
                "groupaddress_id",
                "group_address_id",
                "sender_group_address_id",
                "senderaddressid",
                "address",
                "sender_address",
                "full_address",
                "phone",
              ].includes(key)
            );
          })
        ) {
          return true;
        }

        stack.push(...node.slice(0, 80));
        continue;
      }

      if (typeof node === "object") {
        stack.push(...Object.values(node));
      }
    }

    return false;
  }

  async listInventories() {
    const configuredPath = this.endpoint("INVENTORIES", "/user/listInventory");
    const paths = Array.from(
      new Set([
        configuredPath,
        "/user/listInventory",
        "/user/listInventoryV2",
        "/user/getListInventory",
        "/setting/listInventory",
        "/setting/listAddress",
        "/setting/listAllInventory",
      ])
    );

    const token = await this.getToken();
    let lastError: any = null;
    let lastPayload: any = null;

    for (const path of paths) {
      for (const method of ["GET", "POST"] as const) {
        try {
          const payload = await this.rawRequest<any>(path, {
            method,
            token,
            body: method === "POST" ? {} : undefined,
          });

          lastPayload = payload;

          if (this.hasInventoryLikePayload(payload)) {
            this.logger.log(`[VIETTELPOST_INVENTORY] ok ${method} ${path}`);
            return payload;
          }

          this.logger.warn(`[VIETTELPOST_INVENTORY] empty ${method} ${path}`);
        } catch (error) {
          lastError = error;
          this.logger.warn(
            `[VIETTELPOST_INVENTORY] failed ${method} ${path}: ${
              error instanceof Error ? error.message : JSON.stringify(error)
            }`
          );
        }
      }
    }

    if (lastPayload) return lastPayload;

    throw new BadRequestException(
      lastError instanceof Error
        ? lastError.message
        : "Không lấy được danh sách kho ViettelPost"
    );
  }

  getPrice(payload: any) {
    return this.request<any>(this.endpoint("PRICE", "/order/getPrice"), {
      method: "POST",
      body: payload,
    });
  }

  getPriceAll(payload: any) {
    return this.request<any>(this.endpoint("PRICE_ALL", "/order/getPriceAll"), {
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

  createOrder(payload: any) {
    return this.request<any>(this.endpoint("CREATE_ORDER", "/order/createOrder"), {
      method: "POST",
      body: payload,
    });
  }

  trackOrder(orderNumber: string) {
    const cleanOrderNumber = String(orderNumber || "").trim();

    if (!cleanOrderNumber) {
      throw new BadRequestException("Thiếu mã vận đơn ViettelPost");
    }

    return this.request<any>(this.endpoint("TRACK", "/order/getOrderStatus"), {
      method: "POST",
      body: {
        ORDER_NUMBER: cleanOrderNumber,
      },
    });
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
