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
  // PUBLIC TRACKING FALLBACK
  // ========================
  private safeJsonParse(input?: string | null) {
    if (!input) return null;
    try {
      return JSON.parse(input);
    } catch {
      return null;
    }
  }

  private decodeHtmlEntities(input: string) {
    return String(input || "")
      .replace(/&quot;/g, '"')
      .replace(/&#34;/g, '"')
      .replace(/&amp;/g, "&")
      .replace(/&lt;/g, "<")
      .replace(/&gt;/g, ">");
  }

  private extractJsonObjectsFromHtml(html: string) {
    const results: any[] = [];
    const text = this.decodeHtmlEntities(html || "");

    const nextData = text.match(
      /<script[^>]+id=["']__NEXT_DATA__["'][^>]*>([\s\S]*?)<\/script>/i
    );
    const nextJson = this.safeJsonParse(nextData?.[1]?.trim());
    if (nextJson) results.push(nextJson);

    const nuxtData = text.match(
      /<script[^>]*>\s*window\.__NUXT__\s*=\s*([\s\S]*?)<\/script>/i
    );
    const nuxtJson = this.safeJsonParse(
      nuxtData?.[1]?.replace(/;\s*$/, "").trim()
    );
    if (nuxtJson) results.push(nuxtJson);

    const assignmentPatterns = [
      /window\.__INITIAL_STATE__\s*=\s*({[\s\S]*?});/i,
      /window\.__APP_STATE__\s*=\s*({[\s\S]*?});/i,
      /window\.__PRELOADED_STATE__\s*=\s*({[\s\S]*?});/i,
    ];

    for (const pattern of assignmentPatterns) {
      const match = text.match(pattern);
      const parsed = this.safeJsonParse(match?.[1]?.trim());
      if (parsed) results.push(parsed);
    }

    return results;
  }

  private looksLikeTimelineRow(row: any) {
    if (!row || typeof row !== "object" || Array.isArray(row)) return false;

    const keys = Object.keys(row).map((key) => key.toLowerCase());
    const hasStatus = keys.some((key) =>
      ["status", "status_name", "action", "action_name", "current_status"].includes(key)
    );
    const hasTime = keys.some((key) =>
      ["time", "updated_date", "created_date", "updated_at", "created_at", "action_at", "event_time"].includes(key)
    );
    const hasDetail = keys.some((key) =>
      ["description", "detail", "message", "reason", "location", "hub_name", "warehouse", "area", "content"].includes(key)
    );

    return hasStatus && (hasTime || hasDetail);
  }

  private collectTimelineArrays(root: any, depth = 0): any[][] {
    if (!root || depth > 8) return [];
    const arrays: any[][] = [];

    if (Array.isArray(root)) {
      if (root.some((row) => this.looksLikeTimelineRow(row))) {
        arrays.push(root);
      }

      for (const item of root.slice(0, 50)) {
        arrays.push(...this.collectTimelineArrays(item, depth + 1));
      }

      return arrays;
    }

    if (typeof root === "object") {
      for (const [key, value] of Object.entries(root)) {
        const lower = key.toLowerCase();
        const likelyTimelineKey =
          lower.includes("log") ||
          lower.includes("timeline") ||
          lower.includes("history") ||
          lower.includes("status") ||
          lower.includes("tracking");

        if (Array.isArray(value)) {
          if (likelyTimelineKey || value.some((row) => this.looksLikeTimelineRow(row))) {
            arrays.push(value);
          }
          arrays.push(...this.collectTimelineArrays(value, depth + 1));
        } else if (value && typeof value === "object") {
          arrays.push(...this.collectTimelineArrays(value, depth + 1));
        }
      }
    }

    return arrays;
  }

  private dedupeTimelineRows(rows: any[]) {
    const seen = new Set<string>();
    const output: any[] = [];

    for (const row of rows) {
      const key = JSON.stringify({
        status: row?.status || row?.status_name || row?.action || row?.current_status || "",
        time: row?.updated_date || row?.created_date || row?.updated_at || row?.created_at || row?.action_at || row?.time || "",
        detail: row?.description || row?.detail || row?.message || row?.reason || row?.content || "",
        location: row?.location || row?.hub_name || row?.warehouse || row?.area || "",
      });

      if (seen.has(key)) continue;
      seen.add(key);
      output.push(row);
    }

    return output;
  }

  async getPublicTracking(orderCode: string) {
    if (!orderCode) {
      throw new BadRequestException("Thiếu orderCode GHN");
    }

    const publicUrl = `https://donhang.ghn.vn/?order_code=${encodeURIComponent(orderCode)}`;

    try {
      const res = await fetch(publicUrl, {
        method: "GET",
        headers: {
          Accept: "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
          "User-Agent":
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari",
        },
      });

      const text = await res.text().catch(() => "");
      if (!res.ok || !text) {
        return {
          ok: false,
          source: "ghn_public_html",
          url: publicUrl,
          status: res.status,
          timelines: [],
          raw: null,
        };
      }

      const jsonObjects = this.extractJsonObjectsFromHtml(text);
      const rows = this.dedupeTimelineRows(
        jsonObjects.flatMap((obj) => this.collectTimelineArrays(obj)).flat()
      );

      return {
        ok: true,
        source: "ghn_public_html",
        url: publicUrl,
        timelines: rows,
        raw: {
          jsonObjects,
          htmlLength: text.length,
        },
      };
    } catch (error) {
      this.logger.warn(
        `[GHN PUBLIC TRACKING ERROR] orderCode=${orderCode} | ${
          error instanceof Error ? error.message : String(error)
        }`
      );

      return {
        ok: false,
        source: "ghn_public_html",
        url: publicUrl,
        timelines: [],
        raw: null,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }

  async getOrderDetailWithPublicTracking(orderCode?: string, clientOrderCode?: string) {
    const detail = await this.getOrderDetail(orderCode, clientOrderCode);
    const code = orderCode || detail?.order_code || detail?.orderCode || clientOrderCode || "";
    const publicTracking = code ? await this.getPublicTracking(code) : null;

    return {
      ...(detail || {}),
      publicTracking,
    };
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