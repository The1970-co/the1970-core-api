import { Injectable } from "@nestjs/common";
import type { Prisma } from "@prisma/client";
import { PrismaService } from "../prisma/prisma.service";
import { DEFAULT_HOMEPAGE_CONFIG } from "./default-homepage";

@Injectable()
export class WebsiteHomepageService {
  constructor(private readonly prisma: PrismaService) {}

  private market(value?: string) {
    return String(value || "VN").trim().toUpperCase() || "VN";
  }

  private normalizeConfig(value: any) {
    if (!value || typeof value !== "object") return DEFAULT_HOMEPAGE_CONFIG;
    const blocks = Array.isArray(value.blocks) ? value.blocks : [];
    return {
      version: Number(value.version || 1),
      blocks: blocks.map((block: any, blockIndex: number) => ({
        id: String(block?.id || `block-${blockIndex + 1}`),
        type: String(block?.type || "CARD_ROW"),
        enabled: block?.enabled !== false,
        titleVi: String(block?.titleVi || ""),
        titleEn: String(block?.titleEn || ""),
        layout: String(block?.layout || ""),
        settings: block?.settings && typeof block.settings === "object" ? block.settings : {},
        items: Array.isArray(block?.items)
          ? block.items.map((item: any, itemIndex: number) => ({
              id: String(item?.id || `item-${blockIndex + 1}-${itemIndex + 1}`),
              titleVi: String(item?.titleVi || ""),
              titleEn: String(item?.titleEn || ""),
              subtitleVi: String(item?.subtitleVi || ""),
              subtitleEn: String(item?.subtitleEn || ""),
              bodyVi: String(item?.bodyVi || ""),
              bodyEn: String(item?.bodyEn || ""),
              ctaVi: String(item?.ctaVi || ""),
              ctaEn: String(item?.ctaEn || ""),
              link: String(item?.link || ""),
              mediaMode: item?.mediaMode === "slider" ? "slider" : "single",
              autoplayMs: Math.max(1500, Number(item?.autoplayMs || 5000)),
              images: Array.isArray(item?.images)
                ? item.images.map((image: any, imageIndex: number) => ({
                    id: String(image?.id || `image-${blockIndex + 1}-${itemIndex + 1}-${imageIndex + 1}`),
                    url: String(image?.url || "").trim(),
                    altVi: String(image?.altVi || ""),
                    altEn: String(image?.altEn || ""),
                    link: String(image?.link || ""),
                  })).filter((image: any) => image.url)
                : [],
            }))
          : [],
      })),
    };
  }

  async getAdmin(market?: string) {
    const key = this.market(market);
    const row = await this.prisma.websiteHomepage.findUnique({ where: { market: key } });
    if (row) return row;
    return this.prisma.websiteHomepage.create({
      data: {
        market: key,
        draftConfig: DEFAULT_HOMEPAGE_CONFIG as unknown as Prisma.InputJsonValue,
      },
    });
  }

  async saveDraft(market: string | undefined, config: any) {
    const key = this.market(market);
    const normalized = this.normalizeConfig(config);
    return this.prisma.websiteHomepage.upsert({
      where: { market: key },
      create: { market: key, draftConfig: normalized as unknown as Prisma.InputJsonValue },
      update: { draftConfig: normalized as unknown as Prisma.InputJsonValue },
    });
  }

  async publish(market?: string) {
    const key = this.market(market);
    const row = await this.getAdmin(key);
    return this.prisma.websiteHomepage.update({
      where: { market: key },
      data: {
        publishedConfig: row.draftConfig as Prisma.InputJsonValue,
        publishedAt: new Date(),
      },
    });
  }

  async publicConfig(market?: string) {
    const key = this.market(market);
    const row = await this.prisma.websiteHomepage.findUnique({ where: { market: key } });
    return row?.publishedConfig || DEFAULT_HOMEPAGE_CONFIG;
  }
}
