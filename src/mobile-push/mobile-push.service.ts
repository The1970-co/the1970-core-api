import { Injectable, OnModuleDestroy } from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";
import * as apn from "apn";
import * as fs from "fs";

type NewOrderPushPayload = {
  id: string;
  orderCode?: string | null;
  finalAmount?: number | string | null;
  customerName?: string | null;
  customerPhone?: string | null;
  branchId?: string | null;
  salesChannel?: string | null;
  createdByStaffName?: string | null;
};

@Injectable()
export class MobilePushService implements OnModuleDestroy {
  private apnProvider: apn.Provider | null = null;

  constructor(private readonly prisma: PrismaService) {}

  private n(value: unknown) {
    const parsed = Number(value || 0);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  private getApnsKey() {
    const keyContent = process.env.APNS_KEY_CONTENT || process.env.APNS_KEY;
    const keyBase64 = process.env.APNS_KEY_BASE64;
    const keyPath = process.env.APNS_KEY_PATH;

    if (keyBase64) {
      return Buffer.from(keyBase64, "base64").toString("utf8");
    }

    if (keyContent) {
      return keyContent.replace(/\\n/g, "\n");
    }

    if (keyPath) {
      return fs.readFileSync(keyPath, "utf8");
    }

    return "";
  }

  private provider() {
    if (this.apnProvider) return this.apnProvider;

    const key = this.getApnsKey();
    const keyId = process.env.APNS_KEY_ID;
    const teamId = process.env.APNS_TEAM_ID;

    if (!key || !keyId || !teamId) {
      throw new Error(
        "Missing APNS env: APNS_KEY_CONTENT/APNS_KEY_BASE64/APNS_KEY_PATH, APNS_KEY_ID, APNS_TEAM_ID",
      );
    }

    this.apnProvider = new apn.Provider({
      token: { key, keyId, teamId },
      production: process.env.APNS_PRODUCTION === "true",
    });

    return this.apnProvider;
  }

  async registerToken(params: {
    userId?: string | null;
    staffId?: string | null;
    branchId?: string | null;
    platform?: string | null;
    provider?: string | null;
    token: string;
    deviceId?: string | null;
    appVersion?: string | null;
  }) {
    const token = String(params.token || "").trim();
    if (!token) return { success: false, message: "Thiếu device token." };

    const platform = String(params.platform || "ios").toLowerCase();
    const provider = String(params.provider || "apns").toLowerCase();

    await (this.prisma as any).mobilePushToken.upsert({
      where: { token },
      update: {
        userId: params.userId || null,
        staffId: params.staffId || null,
        branchId: params.branchId || null,
        platform,
        provider,
        deviceId: params.deviceId || null,
        appVersion: params.appVersion || null,
        isActive: true,
        lastSeenAt: new Date(),
      },
      create: {
        userId: params.userId || null,
        staffId: params.staffId || null,
        branchId: params.branchId || null,
        platform,
        provider,
        token,
        deviceId: params.deviceId || null,
        appVersion: params.appVersion || null,
        isActive: true,
        lastSeenAt: new Date(),
      },
    });

    return { success: true };
  }

  private formatMoney(value: unknown) {
    return `${new Intl.NumberFormat("vi-VN").format(Math.round(this.n(value)))}đ`;
  }

  async notifyNewOrder(order: NewOrderPushPayload) {
    console.log("[MOBILE_PUSH_NEW_ORDER_START]", {
      orderId: order?.id || null,
      orderCode: order?.orderCode || null,
      finalAmount: order?.finalAmount || 0,
      branchId: order?.branchId || null,
      salesChannel: order?.salesChannel || null,
    });

    if (!order?.id) {
      console.warn("[MOBILE_PUSH_NEW_ORDER_SKIPPED] missing order id", order);
      return { success: false, sent: 0, failed: 0 };
    }

    let tokens: Array<{ id: string; token: string }> = [];
    try {
      tokens = await (this.prisma as any).mobilePushToken.findMany({
        where: {
          isActive: true,
          platform: "ios",
          provider: "apns",
        },
        select: { id: true, token: true },
        take: 500,
      });
    } catch (error) {
      console.error("[MOBILE_PUSH_TOKEN_QUERY_FAILED]", error);
      return { success: false, sent: 0, failed: 0 };
    }

    if (!tokens.length) {
      console.warn("[MOBILE_PUSH_NO_ACTIVE_TOKENS]");
      return { success: true, sent: 0, failed: 0 };
    }

    console.log("[MOBILE_PUSH_TOKENS_FOUND]", {
      count: tokens.length,
      production: process.env.APNS_PRODUCTION === "true",
      topic: process.env.APNS_BUNDLE_ID || "co.the1970.operations",
    });

    const note = new apn.Notification();
    note.topic = process.env.APNS_BUNDLE_ID || "co.the1970.operations";
    note.alert = {
      title: "Có đơn mới",
      body: `${order.orderCode || "Đơn mới"} · ${this.formatMoney(order.finalAmount)}`,
    };
    note.sound = "default";
    note.payload = {
      type: "new_order",
      orderId: order.id,
      orderCode: order.orderCode || "",
      branchId: order.branchId || "",
      salesChannel: order.salesChannel || "",
    };

    try {
      const result = await this.provider().send(
        note,
        tokens.map((item) => item.token),
      );

      console.log("[MOBILE_PUSH_APNS_RESULT]", {
        sent: result.sent.length,
        failed: result.failed.length,
        failedReasons: result.failed.map((item: any) => ({
          status: item?.status || null,
          reason: item?.response?.reason || null,
          devicePrefix: item?.device ? String(item.device).slice(0, 12) : null,
        })),
      });

      const deadTokens = result.failed
        .filter((item: any) => {
          const reason = String(item?.response?.reason || "");
          return item?.device && ["BadDeviceToken", "Unregistered"].includes(reason);
        })
        .map((item: any) => item.device);

      if (deadTokens.length) {
        await (this.prisma as any).mobilePushToken.updateMany({
          where: { token: { in: deadTokens } },
          data: { isActive: false },
        });
      }

      return {
        success: true,
        sent: result.sent.length,
        failed: result.failed.length,
      };
    } catch (error) {
      console.error("[MOBILE_PUSH_SEND_FAILED]", error);
      return { success: false, sent: 0, failed: tokens.length };
    }
  }

  onModuleDestroy() {
    this.apnProvider?.shutdown();
  }
}
