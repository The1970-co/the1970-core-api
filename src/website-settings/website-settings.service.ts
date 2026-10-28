import { Injectable } from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";

@Injectable()
export class WebsiteSettingsService {
  constructor(private readonly prisma: PrismaService) {}

  private market(value?: string) {
    return String(value || "VN").trim().toUpperCase() || "VN";
  }

  private nullable(value: unknown) {
    const text = String(value ?? "").trim();
    return text || null;
  }

  private percent(value: unknown) {
    const number = Number(value || 0);
    if (!Number.isFinite(number)) return 0;
    return Math.min(100, Math.max(0, number));
  }

  async getAdmin(market?: string) {
    const key = this.market(market);

    return this.prisma.websiteSettings.upsert({
      where: { market: key },
      create: { market: key },
      update: {},
    });
  }

  // Backward compatibility với website-commerce cũ.
  async get(market?: string) {
    return this.getAdmin(market);
  }

  async update(market: string | undefined, body: any) {
    const key = this.market(market);

    return this.prisma.websiteSettings.upsert({
      where: { market: key },
      create: {
        market: key,
        cartEnabled: Boolean(body?.cartEnabled),
        checkoutEnabled: Boolean(body?.checkoutEnabled),
        newCustomerDiscountEnabled: Boolean(
          body?.newCustomerDiscountEnabled,
        ),
        newCustomerDiscountPercent: this.percent(
          body?.newCustomerDiscountPercent,
        ),
        messengerEnabled: Boolean(body?.messengerEnabled),
        messengerUrl: this.nullable(body?.messengerUrl),
        zaloEnabled: Boolean(body?.zaloEnabled),
        zaloUrl: this.nullable(body?.zaloUrl),
        defaultBranchId: this.nullable(body?.defaultBranchId),
      },
      update: {
        ...(typeof body?.cartEnabled === "boolean"
          ? { cartEnabled: body.cartEnabled }
          : {}),
        ...(typeof body?.checkoutEnabled === "boolean"
          ? { checkoutEnabled: body.checkoutEnabled }
          : {}),
        ...(typeof body?.newCustomerDiscountEnabled === "boolean"
          ? {
              newCustomerDiscountEnabled:
                body.newCustomerDiscountEnabled,
            }
          : {}),
        ...(body?.newCustomerDiscountPercent !== undefined
          ? {
              newCustomerDiscountPercent: this.percent(
                body.newCustomerDiscountPercent,
              ),
            }
          : {}),
        ...(typeof body?.messengerEnabled === "boolean"
          ? { messengerEnabled: body.messengerEnabled }
          : {}),
        ...(body?.messengerUrl !== undefined
          ? {
              messengerUrl: this.nullable(body.messengerUrl),
            }
          : {}),
        ...(typeof body?.zaloEnabled === "boolean"
          ? { zaloEnabled: body.zaloEnabled }
          : {}),
        ...(body?.zaloUrl !== undefined
          ? { zaloUrl: this.nullable(body.zaloUrl) }
          : {}),
        ...(body?.defaultBranchId !== undefined
          ? {
              defaultBranchId: this.nullable(
                body.defaultBranchId,
              ),
            }
          : {}),
      },
    });
  }

  async publicSettings(market?: string) {
    const row = await this.getAdmin(market);

    return {
      market: row.market,
      cartEnabled: row.cartEnabled,
      checkoutEnabled: row.checkoutEnabled,
      newCustomerDiscountEnabled:
        row.newCustomerDiscountEnabled,
      newCustomerDiscountPercent: Number(
        row.newCustomerDiscountPercent || 0,
      ),
      messengerEnabled: row.messengerEnabled,
      messengerUrl: row.messengerUrl,
      zaloEnabled: row.zaloEnabled,
      zaloUrl: row.zaloUrl,
    };
  }

  // Backward compatibility với controller cũ.
  async getPublic(market?: string) {
    return this.publicSettings(market);
  }
}
