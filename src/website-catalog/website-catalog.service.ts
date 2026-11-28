import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from "@nestjs/common";
import { ProductStatus, VariantStatus, WebsitePublishStatus } from "@prisma/client";
import type { Prisma } from "@prisma/client";
import { PrismaService } from "../prisma/prisma.service";

@Injectable()
export class WebsiteCatalogService {
  constructor(private readonly prisma: PrismaService) {}

  private text(value: unknown) {
    return String(value ?? "").trim();
  }

  private nullable(value: unknown) {
    const text = this.text(value);
    return text || null;
  }

  private slugify(value: unknown) {
    return this.text(value)
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/đ/g, "d")
      .replace(/Đ/g, "D")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "");
  }

  private colorKey(color: unknown) {
    return `color:${this.text(color).normalize("NFC").toLowerCase()}`;
  }

  private async resolveColor(productId: string, requested: unknown) {
    const variants = await this.prisma.productVariant.findMany({
      where: { productId, status: VariantStatus.ACTIVE },
      select: { color: true },
    });
    const colors = new Map(variants.map(v => [this.colorKey(v.color), this.text(v.color)]));
    const key = typeof requested === "string" && requested.startsWith("color:")
      ? this.colorKey(requested.slice(6)) : "";
    if (!key && colors.size === 1 && (requested === undefined || requested === "")) {
      const only = [...colors.entries()][0];
      return { key: only[0], label: only[1] };
    }
    if (!key || !colors.has(key)) {
      throw new BadRequestException("Chọn một màu có biến thể đang hoạt động của sản phẩm nội bộ.");
    }
    return { key, label: colors.get(key) || "" };
  }

  private automaticSlug(title: string, label: string, fallback: string) {
    const base = this.slugify(title) || this.slugify(fallback) || "san-pham";
    const color = this.slugify(label);
    return color && base !== color && !base.endsWith(`-${color}`) ? `${base}-${color}` : base;
  }

  // The unique indexes are the final authority, including concurrent requests.
  private async withUniqueSlug<T>(base: string, productId: string, colorKey: string,
    currentId: string | undefined, write: (slug: string) => Promise<T>, locked = false): Promise<T> {
    for (let n = 1; n <= 100; n++) {
      const candidate = n === 1 ? base : `${base}-${n}`;
      try {
        return await write(candidate);
      } catch (error) {
        if ((error as { code?: string })?.code !== "P2002") throw error;
        const sameColor = await this.prisma.websiteProduct.findUnique({
          where: { productId_colorKey: { productId, colorKey } }, select: { id: true },
        });
        if (sameColor && sameColor.id !== currentId) {
          throw new BadRequestException("Màu này đã có bài đăng website. Hãy sửa bài hiện có hoặc chọn màu khác.");
        }
        const sameSlug = await this.prisma.websiteProduct.findUnique({
          where: { slug: candidate }, select: { id: true },
        });
        if (!sameSlug || sameSlug.id === currentId) {
          // A concurrent delete may have removed the conflicting row. Do not hide unknown constraints.
          throw error;
        }
        if (locked) throw new BadRequestException("Đường dẫn đã xuất bản không thể thay đổi.");
      }
    }
    throw new BadRequestException("Không tạo được đường dẫn duy nhất. Hãy thử tên đường dẫn khác.");
  }

  private status(value: unknown): WebsitePublishStatus {
    const status = this.text(value).toUpperCase();
    if (status === "PUBLISHED") return WebsitePublishStatus.PUBLISHED;
    if (status === "ARCHIVED") return WebsitePublishStatus.ARCHIVED;
    return WebsitePublishStatus.DRAFT;
  }

  private normalizeCategorySlugs(value: unknown) {
    if (!Array.isArray(value)) return [];

    return Array.from(
      new Set(
        value
          .map((item) => this.text(item).normalize("NFD")
            .replace(/[\u0300-\u036f]/g, "").replace(/[đĐ]/g, "d")
            .toLowerCase().replace(/[^a-z0-9\s-]/g, "").trim()
            .replace(/\s+/g, "-").replace(/-+/g, "-"))
          .filter(Boolean),
      ),
    );
  }

  private normalizeImages(value: unknown) {
    if (!Array.isArray(value)) return [];
    return value
      .map((item: any, index) => ({
        url: this.text(item?.url || item?.imageUrl),
        altVi: this.nullable(item?.altVi),
        altEn: this.nullable(item?.altEn),
        sortOrder: Number.isFinite(Number(item?.sortOrder))
          ? Number(item.sortOrder)
          : index,
      }))
      .filter((item) => item.url);
  }

  private include(): Prisma.WebsiteProductInclude {
    return {
      product: {
        include: {
          categoryRel: true,
          variants: {
            orderBy: { createdAt: "asc" },
            include: { inventoryItems: true },
          },
        },
      },
      images: {
        orderBy: [{ sortOrder: "asc" }, { createdAt: "asc" }],
      },
    };
  }

  async list(params: { q?: string; status?: string }) {
    const q = this.text(params.q);
    const status = this.text(params.status).toUpperCase();
    return this.prisma.websiteProduct.findMany({
      where: {
        ...(status && ["DRAFT", "PUBLISHED", "ARCHIVED"].includes(status)
          ? { status: status as WebsitePublishStatus }
          : {}),
        ...(q
          ? {
              OR: [
                { titleVi: { contains: q, mode: "insensitive" } },
                { titleEn: { contains: q, mode: "insensitive" } },
                { slug: { contains: q, mode: "insensitive" } },
                { product: { name: { contains: q, mode: "insensitive" } } },
                { product: { variants: { some: { sku: { contains: q, mode: "insensitive" } } } } },
              ],
            }
          : {}),
      },
      orderBy: [{ sortOrder: "asc" }, { updatedAt: "desc" }],
      include: this.include(),
    });
  }

  async get(id: string) {
    const row = await this.prisma.websiteProduct.findUnique({
      where: { id },
      include: this.include(),
    });
    if (!row) throw new NotFoundException("Không tìm thấy sản phẩm website.");
    return row;
  }

  async create(body: any) {
    const productId = this.text(body?.productId);
    if (!productId) throw new BadRequestException("Chưa chọn sản phẩm trong hệ thống.");

    const product = await this.prisma.product.findUnique({
      where: { id: productId },
      select: { id: true, name: true, slug: true, imageUrl: true },
    });
    if (!product) throw new BadRequestException("Sản phẩm master không tồn tại.");

    const color = await this.resolveColor(productId, body?.colorKey);
    const colorKey = color.key;
    const titleVi = this.text(body?.titleVi) || product.name;
    const baseSlug = this.slugify(body?.slug) || this.automaticSlug(titleVi, color.label, product.slug);

    const nextStatus = this.status(body?.status);
    const images = this.normalizeImages(body?.images);

    return this.withUniqueSlug(baseSlug, productId, colorKey, undefined, slug => this.prisma.websiteProduct.create({
      data: {
        productId,
        colorKey,
        slug,
        status: nextStatus,
        marketVn: body?.marketVn !== false,
        marketInternational: body?.marketInternational === true,
        featured: body?.featured === true,
        sortOrder: Number(body?.sortOrder || 0),
        websiteCategorySlugs: this.normalizeCategorySlugs(
          body?.websiteCategorySlugs,
        ),
        titleVi,
        titleEn: this.nullable(body?.titleEn),
        shortDescriptionVi: this.nullable(body?.shortDescriptionVi),
        shortDescriptionEn: this.nullable(body?.shortDescriptionEn),
        descriptionVi: this.nullable(body?.descriptionVi),
        descriptionEn: this.nullable(body?.descriptionEn),
        coverImageUrl: this.nullable(body?.coverImageUrl) || images[0]?.url || null,
        seoTitleVi: this.nullable(body?.seoTitleVi),
        seoTitleEn: this.nullable(body?.seoTitleEn),
        seoDescriptionVi: this.nullable(body?.seoDescriptionVi),
        seoDescriptionEn: this.nullable(body?.seoDescriptionEn),
        publishedAt: nextStatus === WebsitePublishStatus.PUBLISHED ? new Date() : null,
        images: images.length ? { create: images } : undefined,
      },
      include: this.include(),
    }));
  }

  async update(id: string, body: any) {
    const current = await this.prisma.websiteProduct.findUnique({ where: { id } });
    if (!current) throw new NotFoundException("Không tìm thấy sản phẩm website.");

    const nextStatus = this.status(body?.status ?? current.status);
    const images = this.normalizeImages(body?.images);
    const replaceImages = Array.isArray(body?.images);
    const titleVi = this.text(body?.titleVi) || current.titleVi;
    if (body?.productId !== undefined && this.text(body.productId) !== current.productId) {
      throw new BadRequestException("Không thể đổi sản phẩm nội bộ của bài đăng đã tạo.");
    }
    // Existing listings without a color remain all-color until explicitly assigned.
    // Do not allow converting a color-specific listing back to all colors.
    const requestedColor = body?.colorKey;
    const color = requestedColor === undefined || requestedColor === current.colorKey
      ? { key: current.colorKey, label: current.colorKey.startsWith("color:") ? current.colorKey.slice(6) : "" }
      : await this.resolveColor(current.productId, requestedColor);
    const colorKey = color.key;
    const locked = !!current.publishedAt || current.status === WebsitePublishStatus.PUBLISHED;
    const baseSlug = locked && current.slug ? current.slug : this.slugify(body?.slug ?? current.slug) || this.automaticSlug(titleVi, color.label, current.slug);

    return this.withUniqueSlug(baseSlug, current.productId, colorKey, id, slug => this.prisma.$transaction(async (tx) => {
      if (replaceImages) {
        await tx.websiteProductImage.deleteMany({ where: { websiteProductId: id } });
      }

      await tx.websiteProduct.update({
        where: { id },
        data: {
          slug,
          colorKey,
          status: nextStatus,
          marketVn: body?.marketVn === undefined ? current.marketVn : body.marketVn === true,
          marketInternational:
            body?.marketInternational === undefined
              ? current.marketInternational
              : body.marketInternational === true,
          featured: body?.featured === undefined ? current.featured : body.featured === true,
          sortOrder:
            body?.sortOrder === undefined
              ? current.sortOrder
              : Number(body.sortOrder || 0),
          websiteCategorySlugs:
            body?.websiteCategorySlugs === undefined
              ? current.websiteCategorySlugs
              : this.normalizeCategorySlugs(body.websiteCategorySlugs),
          titleVi,
          titleEn: body?.titleEn === undefined ? current.titleEn : this.nullable(body.titleEn),
          shortDescriptionVi:
            body?.shortDescriptionVi === undefined
              ? current.shortDescriptionVi
              : this.nullable(body.shortDescriptionVi),
          shortDescriptionEn:
            body?.shortDescriptionEn === undefined
              ? current.shortDescriptionEn
              : this.nullable(body.shortDescriptionEn),
          descriptionVi:
            body?.descriptionVi === undefined
              ? current.descriptionVi
              : this.nullable(body.descriptionVi),
          descriptionEn:
            body?.descriptionEn === undefined
              ? current.descriptionEn
              : this.nullable(body.descriptionEn),
          coverImageUrl:
            body?.coverImageUrl === undefined
              ? current.coverImageUrl
              : this.nullable(body.coverImageUrl),
          seoTitleVi:
            body?.seoTitleVi === undefined ? current.seoTitleVi : this.nullable(body.seoTitleVi),
          seoTitleEn:
            body?.seoTitleEn === undefined ? current.seoTitleEn : this.nullable(body.seoTitleEn),
          seoDescriptionVi:
            body?.seoDescriptionVi === undefined
              ? current.seoDescriptionVi
              : this.nullable(body.seoDescriptionVi),
          seoDescriptionEn:
            body?.seoDescriptionEn === undefined
              ? current.seoDescriptionEn
              : this.nullable(body.seoDescriptionEn),
          publishedAt:
            nextStatus === WebsitePublishStatus.PUBLISHED
              ? current.publishedAt || new Date()
              : current.publishedAt,
          ...(replaceImages && images.length
            ? { images: { create: images } }
            : {}),
        },
      });

      return tx.websiteProduct.findUnique({
        where: { id },
        include: this.include(),
      });
    }), locked && !!current.slug);
  }

  async remove(id: string) {
    await this.prisma.websiteProduct.delete({ where: { id } }).catch(() => null);
    return { success: true };
  }

  async publicList(market: "VN" | "INTERNATIONAL" = "VN") {
    const branchId = this.text(process.env.STOREFRONT_VN_BRANCH_ID);
    const rows = await this.prisma.websiteProduct.findMany({
      where: {
        status: WebsitePublishStatus.PUBLISHED,
        ...(market === "INTERNATIONAL"
          ? { marketInternational: true }
          : { marketVn: true }),
        product: { status: ProductStatus.ACTIVE },
      },
      orderBy: [{ sortOrder: "asc" }, { publishedAt: "desc" }],
      include: {
        images: { orderBy: { sortOrder: "asc" } },
        product: {
          include: {
            categoryRel: true,
            variants: {
              where: { status: VariantStatus.ACTIVE },
              orderBy: { createdAt: "asc" },
              include: {
                inventoryItems: {
                  where: branchId ? { branchId } : undefined,
                },
              },
            },
          },
        },
      },
    });

    return rows.map((row) => {
      const variants = row.product.variants.filter(v => !row.colorKey || this.colorKey(v.color) === row.colorKey);
      const displayColor = this.text(variants[0]?.color);
      return {
        id: row.id,
        productId: row.productId,
        colorKey: row.colorKey,
        slug: row.slug,
        name: market === "INTERNATIONAL" ? row.titleEn || row.titleVi : row.titleVi,
        titleVi: row.titleVi,
        titleEn: row.titleEn,
        description:
          market === "INTERNATIONAL"
            ? row.descriptionEn || row.descriptionVi
            : row.descriptionVi,
        shortDescription:
          market === "INTERNATIONAL"
            ? row.shortDescriptionEn || row.shortDescriptionVi
            : row.shortDescriptionVi,
        imageUrl: row.coverImageUrl || row.images[0]?.url || row.product.imageUrl,
        images: row.images.map((x) => ({ url: x.url, altVi: x.altVi, altEn: x.altEn })),
        category: row.product.categoryRel?.name || row.product.category || null,
        websiteCategorySlugs: Array.isArray(row.websiteCategorySlugs)
          ? row.websiteCategorySlugs
          : [],
        featured: row.featured,
        variants: variants.map((v) => ({
          id: v.id,
          sku: v.sku,
          color: row.colorKey ? displayColor : v.color || "",
          size: v.size || "",
          price: Number(v.price || 0),
          compareAtPrice: Number(v.compareAtPrice || 0),
          imageUrl: v.imageUrl,
          stock: v.inventoryItems.reduce(
            (sum, x) =>
              sum + Math.max(0, Number(x.availableQty || 0) - Number(x.reservedQty || 0)),
            0,
          ),
        })),
      };
    });
  }

  async publicGet(slug: string, market: "VN" | "INTERNATIONAL" = "VN") {
    const rows = await this.publicList(market);
    return rows.find((x) => x.slug.toLowerCase() === this.text(slug).toLowerCase()) || null;
  }
}
