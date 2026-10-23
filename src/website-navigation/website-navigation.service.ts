import { Injectable } from "@nestjs/common";
import type { Prisma } from "@prisma/client";
import { PrismaService } from "../prisma/prisma.service";
import { DEFAULT_NAVIGATION_CONFIG } from "./default-navigation";

@Injectable()
export class WebsiteNavigationService {
  constructor(private readonly prisma: PrismaService) {}

  private market(value?: string) {
    return String(value || "VN").trim().toUpperCase() || "VN";
  }

  private clean(value: any) {
    const items = Array.isArray(value?.items) ? value.items : [];
    return {
      version: Number(value?.version || 1),
      items: items.map((item: any, itemIndex: number) => ({
        id: String(item?.id || `menu-${itemIndex + 1}`),
        enabled: item?.enabled !== false,
        type: item?.type === "MEGA" ? "MEGA" : "LINK",
        labelVi: String(item?.labelVi || ""),
        labelEn: String(item?.labelEn || ""),
        link: String(item?.link || ""),
        columns: Array.isArray(item?.columns)
          ? item.columns.map((column: any, columnIndex: number) => ({
              id: String(column?.id || `column-${itemIndex + 1}-${columnIndex + 1}`),
              titleVi: String(column?.titleVi || ""),
              titleEn: String(column?.titleEn || ""),
              links: Array.isArray(column?.links)
                ? column.links.map((link: any, linkIndex: number) => ({
                    id: String(link?.id || `link-${itemIndex + 1}-${columnIndex + 1}-${linkIndex + 1}`),
                    labelVi: String(link?.labelVi || ""),
                    labelEn: String(link?.labelEn || ""),
                    link: String(link?.link || ""),
                    enabled: link?.enabled !== false,
                  }))
                : [],
            }))
          : [],
        promo: {
          enabled: item?.promo?.enabled === true,
          imageUrl: String(item?.promo?.imageUrl || ""),
          titleVi: String(item?.promo?.titleVi || ""),
          titleEn: String(item?.promo?.titleEn || ""),
          link: String(item?.promo?.link || ""),
        },
      })),
    };
  }

  async getAdmin(market?: string) {
    const key = this.market(market);
    const row = await this.prisma.websiteNavigation.findUnique({ where: { market: key } });
    if (row) return row;
    return this.prisma.websiteNavigation.create({
      data: {
        market: key,
        draftConfig: DEFAULT_NAVIGATION_CONFIG as unknown as Prisma.InputJsonValue,
      },
    });
  }

  async saveDraft(market: string | undefined, config: any) {
    const key = this.market(market);
    const cleaned = this.clean(config);
    return this.prisma.websiteNavigation.upsert({
      where: { market: key },
      create: { market: key, draftConfig: cleaned as unknown as Prisma.InputJsonValue },
      update: { draftConfig: cleaned as unknown as Prisma.InputJsonValue },
    });
  }

  async publish(market?: string) {
    const key = this.market(market);
    const row = await this.getAdmin(key);
    return this.prisma.websiteNavigation.update({
      where: { market: key },
      data: {
        publishedConfig: row.draftConfig as Prisma.InputJsonValue,
        publishedAt: new Date(),
      },
    });
  }

  async publicConfig(market?: string) {
    const key = this.market(market);
    const row = await this.prisma.websiteNavigation.findUnique({ where: { market: key } });
    return row?.publishedConfig || DEFAULT_NAVIGATION_CONFIG;
  }
}
