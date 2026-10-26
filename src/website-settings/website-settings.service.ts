import { Injectable } from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";

@Injectable()
export class WebsiteSettingsService {
  constructor(private readonly prisma: PrismaService) {}

  private market(value?: string) {
    return String(value || "VN").toUpperCase() === "INTERNATIONAL"
      ? "INTERNATIONAL"
      : "VN";
  }

  async get(market?: string) {
    const key = this.market(market);
    return this.prisma.websiteSettings.upsert({
      where: { market: key },
      create: { market: key },
      update: {},
    });
  }

  async update(market: string | undefined, body: any) {
    const key = this.market(market);
    const pct = Math.max(
      0,
      Math.min(100, Number(body?.newCustomerDiscountPercent || 0)),
    );

    return this.prisma.websiteSettings.upsert({
      where: { market: key },
      create: {
        market: key,
        cartEnabled: body?.cartEnabled === true,
        checkoutEnabled: body?.checkoutEnabled === true,
        newCustomerDiscountEnabled: body?.newCustomerDiscountEnabled === true,
        newCustomerDiscountPercent: pct,
        messengerEnabled: body?.messengerEnabled === true,
        messengerUrl: String(body?.messengerUrl || "").trim() || null,
        zaloEnabled: body?.zaloEnabled === true,
        zaloUrl: String(body?.zaloUrl || "").trim() || null,
        defaultBranchId: String(body?.defaultBranchId || "").trim() || null,
      },
      update: {
        cartEnabled: body?.cartEnabled === true,
        checkoutEnabled: body?.checkoutEnabled === true,
        newCustomerDiscountEnabled: body?.newCustomerDiscountEnabled === true,
        newCustomerDiscountPercent: pct,
        messengerEnabled: body?.messengerEnabled === true,
        messengerUrl: String(body?.messengerUrl || "").trim() || null,
        zaloEnabled: body?.zaloEnabled === true,
        zaloUrl: String(body?.zaloUrl || "").trim() || null,
        defaultBranchId: String(body?.defaultBranchId || "").trim() || null,
      },
    });
  }

  async getPublic(market?: string) {
    const row = await this.get(market);
    return {
      cartEnabled: row.cartEnabled,
      checkoutEnabled: row.checkoutEnabled,
      newCustomerDiscountEnabled: row.newCustomerDiscountEnabled,
      newCustomerDiscountPercent: Number(row.newCustomerDiscountPercent || 0),
      messengerEnabled: row.messengerEnabled,
      messengerUrl: row.messengerUrl,
      zaloEnabled: row.zaloEnabled,
      zaloUrl: row.zaloUrl,
    };
  }
}
