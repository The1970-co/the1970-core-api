import { Injectable } from "@nestjs/common";
import type { Prisma } from "@prisma/client";
import { PrismaService } from "../prisma/prisma.service";

type AnyObject = Record<string, any>;

const ALLOWED_BLOCK_TYPES = new Set([
  "FULL_BLEED_HERO",
  "HERO_SPLIT",
  "CARD_ROW",
  "STORY_WIDE",
  "STORY_SPLIT",
  "PRODUCT_GRID",
]);

const EMPTY_CONFIG = {
  version: 2,
  blocks: [],
};

@Injectable()
export class WebsiteHomepageService {
  constructor(private readonly prisma: PrismaService) {}

  private market(value?: string) {
    return String(value || "VN").trim().toUpperCase() || "VN";
  }

  private text(value: unknown) {
    return String(value ?? "").trim();
  }

  private number(value: unknown, min: number, max: number, fallback: number) {
    const number = Number(value);
    if (!Number.isFinite(number)) return fallback;
    return Math.min(max, Math.max(min, number));
  }

  private normalizeImage(raw: unknown, index: number) {
    const image =
      raw && typeof raw === "object"
        ? (raw as AnyObject)
        : {};

    return {
      ...image,
      id: this.text(image.id) || `image-${index}`,
      url: this.text(image.url),
      altVi: this.text(image.altVi),
      altEn: this.text(image.altEn),
      link: this.text(image.link),
    };
  }

  private normalizeItem(raw: unknown, blockIndex: number, itemIndex: number) {
    const item =
      raw && typeof raw === "object"
        ? (raw as AnyObject)
        : {};

    const images = Array.isArray(item.images)
      ? item.images
          .map((image: unknown, imageIndex: number) =>
            this.normalizeImage(image, imageIndex),
          )
          .filter((image: AnyObject) => Boolean(image.url))
      : [];

    return {
      ...item,
      id:
        this.text(item.id) ||
        `item-${blockIndex}-${itemIndex}`,
      titleVi: this.text(item.titleVi),
      titleEn: this.text(item.titleEn),
      subtitleVi: this.text(item.subtitleVi),
      subtitleEn: this.text(item.subtitleEn),
      bodyVi: this.text(item.bodyVi),
      bodyEn: this.text(item.bodyEn),
      ctaVi: this.text(item.ctaVi),
      ctaEn: this.text(item.ctaEn),
      link: this.text(item.link),
      mediaMode:
        item.mediaMode === "slider" ? "slider" : "single",
      autoplayMs: this.number(
        item.autoplayMs,
        1500,
        60000,
        5000,
      ),
      mediaType:
        item.mediaType === "video" ? "video" : "image",
      videoUrl: this.text(item.videoUrl),
      images,
    };
  }

  private normalizeBlock(raw: unknown, blockIndex: number) {
    const block =
      raw && typeof raw === "object"
        ? (raw as AnyObject)
        : {};

    const rawType = this.text(block.type);
    const type = ALLOWED_BLOCK_TYPES.has(rawType)
      ? rawType
      : "STORY_WIDE";

    const settingsSource =
      block.settings && typeof block.settings === "object"
        ? { ...(block.settings as AnyObject) }
        : {};

    const settings =
      type === "FULL_BLEED_HERO"
        ? {
            ...settingsSource,
            heightVh: this.number(
              settingsSource.heightVh,
              45,
              100,
              82,
            ),
            contentAlign:
              settingsSource.contentAlign === "CENTER"
                ? "CENTER"
                : settingsSource.contentAlign === "RIGHT"
                  ? "RIGHT"
                  : "LEFT",
            overlayOpacity: this.number(
              settingsSource.overlayOpacity,
              0,
              0.7,
              0.24,
            ),
          }
        : type === "PRODUCT_GRID"
          ? {
              ...settingsSource,
              limit: this.number(
                settingsSource.limit,
                1,
                48,
                8,
              ),
              onlyFeatured:
                settingsSource.onlyFeatured === true,
            }
          : settingsSource;

    const items = Array.isArray(block.items)
      ? block.items.map(
          (item: unknown, itemIndex: number) =>
            this.normalizeItem(
              item,
              blockIndex,
              itemIndex,
            ),
        )
      : [];

    return {
      ...block,
      id:
        this.text(block.id) ||
        `block-${blockIndex}`,
      type,
      enabled: block.enabled !== false,
      titleVi: this.text(block.titleVi),
      titleEn: this.text(block.titleEn),
      layout:
        this.text(block.layout) ||
        type.toLowerCase(),
      settings,
      items,
    };
  }

  private normalizeConfig(raw: unknown) {
    const source =
      raw && typeof raw === "object"
        ? (raw as AnyObject)
        : EMPTY_CONFIG;

    const blocks = Array.isArray(source.blocks)
      ? source.blocks.map(
          (block: unknown, blockIndex: number) =>
            this.normalizeBlock(block, blockIndex),
        )
      : [];

    return {
      ...source,
      version: Math.max(
        2,
        Number(source.version || 2),
      ),
      blocks,
    };
  }

  private json(value: unknown) {
    return this.normalizeConfig(
      value,
    ) as Prisma.InputJsonValue;
  }

  async getAdmin(market?: string) {
    const key = this.market(market);

    return this.prisma.websiteHomepage.upsert({
      where: { market: key },
      create: {
        market: key,
        draftConfig: this.json(EMPTY_CONFIG),
      },
      update: {},
    });
  }

  async saveDraft(
    market: string | undefined,
    config: unknown,
  ) {
    const key = this.market(market);
    const normalized = this.json(config);

    return this.prisma.websiteHomepage.upsert({
      where: { market: key },
      create: {
        market: key,
        draftConfig: normalized,
      },
      update: {
        draftConfig: normalized,
      },
    });
  }

  async publish(market?: string) {
    const key = this.market(market);
    const row = await this.getAdmin(key);

    const normalized = this.json(
      row.draftConfig,
    );

    return this.prisma.websiteHomepage.update({
      where: { market: key },
      data: {
        draftConfig: normalized,
        publishedConfig: normalized,
        publishedAt: new Date(),
      },
    });
  }

  async publicConfig(market?: string) {
    const key = this.market(market);

    const row =
      await this.prisma.websiteHomepage.findUnique({
        where: { market: key },
      });

    return this.normalizeConfig(
      row?.publishedConfig ||
        row?.draftConfig ||
        EMPTY_CONFIG,
    );
  }
}
