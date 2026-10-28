import { Injectable } from "@nestjs/common";
import type { Prisma } from "@prisma/client";
import { PrismaService } from "../prisma/prisma.service";

type AnyObject = Record<string, any>;

const DEFAULT_SETTINGS = {
  menuFontFamily: "SERIF",
  menuFontSize: 12,
  menuGap: 28,
  megaMenuHeightVh: 48,
  megaOverlayOpacity: 0.28,
  headerSticky: true,
  announcementEnabled: true,
  announcementTextVi: "THE 1970 — EST. 2015",
  announcementLink: "",
  headerLogoUrl: "",
  headerLogoWidth: 118,
  headerLogoAlign: "LEFT",
};

const EMPTY_CONFIG = {
  version: 2,
  settings: DEFAULT_SETTINGS,
  items: [],
};

@Injectable()
export class WebsiteNavigationService {
  constructor(private readonly prisma: PrismaService) {}

  private market(value?: string) {
    return String(value || "VN").trim().toUpperCase() || "VN";
  }

  private text(value: unknown) {
    return String(value ?? "").trim();
  }

  private number(
    value: unknown,
    min: number,
    max: number,
    fallback: number,
  ) {
    const number = Number(value);
    if (!Number.isFinite(number)) return fallback;
    return Math.min(max, Math.max(min, number));
  }

  private normalizeConfig(raw: unknown) {
    const source =
      raw && typeof raw === "object"
        ? (raw as AnyObject)
        : EMPTY_CONFIG;

    const settingsSource =
      source.settings && typeof source.settings === "object"
        ? source.settings
        : {};

    const settings = {
      menuFontFamily:
        settingsSource.menuFontFamily === "SANS" ? "SANS" : "SERIF",
      menuFontSize: this.number(
        settingsSource.menuFontSize,
        10,
        18,
        DEFAULT_SETTINGS.menuFontSize,
      ),
      menuGap: this.number(
        settingsSource.menuGap,
        12,
        60,
        DEFAULT_SETTINGS.menuGap,
      ),
      megaMenuHeightVh: this.number(
        settingsSource.megaMenuHeightVh,
        35,
        65,
        DEFAULT_SETTINGS.megaMenuHeightVh,
      ),
      megaOverlayOpacity: this.number(
        settingsSource.megaOverlayOpacity,
        0,
        0.7,
        DEFAULT_SETTINGS.megaOverlayOpacity,
      ),
      headerSticky:
        typeof settingsSource.headerSticky === "boolean"
          ? settingsSource.headerSticky
          : DEFAULT_SETTINGS.headerSticky,
      announcementEnabled:
        typeof settingsSource.announcementEnabled === "boolean"
          ? settingsSource.announcementEnabled
          : DEFAULT_SETTINGS.announcementEnabled,
      announcementTextVi:
        this.text(settingsSource.announcementTextVi) ||
        DEFAULT_SETTINGS.announcementTextVi,
      announcementLink: this.text(
        settingsSource.announcementLink,
      ),
      headerLogoUrl: this.text(
        settingsSource.headerLogoUrl,
      ),
      headerLogoWidth: this.number(
        settingsSource.headerLogoWidth,
        40,
        260,
        DEFAULT_SETTINGS.headerLogoWidth,
      ),
      headerLogoAlign:
        settingsSource.headerLogoAlign === "CENTER"
          ? "CENTER"
          : settingsSource.headerLogoAlign === "RIGHT"
            ? "RIGHT"
            : "LEFT",
    };

    const items = Array.isArray(source.items)
      ? source.items.map((item: AnyObject, itemIndex: number) => {
          const rawPromos = Array.isArray(item?.promos)
            ? item.promos
            : item?.promo
              ? [item.promo]
              : [];

          return {
            id: this.text(item?.id) || `menu-${itemIndex}`,
            enabled: item?.enabled !== false,
            type: item?.type === "MEGA" ? "MEGA" : "LINK",
            labelVi: this.text(item?.labelVi),
            labelEn: this.text(item?.labelEn),
            link: this.text(item?.link) || "/",
            columns: Array.isArray(item?.columns)
              ? item.columns.map(
                  (column: AnyObject, columnIndex: number) => ({
                    id:
                      this.text(column?.id) ||
                      `column-${itemIndex}-${columnIndex}`,
                    titleVi: this.text(column?.titleVi),
                    titleEn: this.text(column?.titleEn),
                    links: Array.isArray(column?.links)
                      ? column.links.map(
                          (
                            link: AnyObject,
                            linkIndex: number,
                          ) => ({
                            id:
                              this.text(link?.id) ||
                              `link-${itemIndex}-${columnIndex}-${linkIndex}`,
                            labelVi: this.text(link?.labelVi),
                            labelEn: this.text(link?.labelEn),
                            link:
                              this.text(link?.link) || "/",
                            enabled:
                              link?.enabled !== false,
                          }),
                        )
                      : [],
                  }),
                )
              : [],
            promos: rawPromos
              .slice(0, 2)
              .map(
                (
                  promo: AnyObject,
                  promoIndex: number,
                ) => ({
                  id:
                    this.text(promo?.id) ||
                    `promo-${itemIndex}-${promoIndex}`,
                  enabled: promo?.enabled !== false,
                  imageUrl: this.text(promo?.imageUrl),
                  titleVi: this.text(promo?.titleVi),
                  titleEn: this.text(promo?.titleEn),
                  link:
                    this.text(promo?.link) ||
                    this.text(item?.link) ||
                    "/",
                }),
              ),
          };
        })
      : [];

    return {
      version: Math.max(2, Number(source.version || 2)),
      settings,
      items,
    };
  }

  private json(value: unknown) {
    return this.normalizeConfig(value) as Prisma.InputJsonValue;
  }

  async getAdmin(market?: string) {
    const key = this.market(market);

    return this.prisma.websiteNavigation.upsert({
      where: { market: key },
      create: {
        market: key,
        draftConfig: this.json(EMPTY_CONFIG),
      },
      update: {},
    });
  }

  async saveDraft(market: string | undefined, config: unknown) {
    const key = this.market(market);
    const normalized = this.json(config);

    return this.prisma.websiteNavigation.upsert({
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
    const normalized = this.json(row.draftConfig);

    return this.prisma.websiteNavigation.update({
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

    const row = await this.prisma.websiteNavigation.findUnique({
      where: { market: key },
    });

    return this.normalizeConfig(
      row?.publishedConfig || row?.draftConfig || EMPTY_CONFIG,
    );
  }
}
