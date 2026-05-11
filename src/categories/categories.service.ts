import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";

type CreateCategoryInput = {
  name: string;
  code: string;
  slug: string;
  description?: string;
  sortOrder?: number;
};

type UpdateCategoryInput = {
  name?: string;
  code?: string;
  slug?: string;
  description?: string;
  sortOrder?: number;
  isActive?: boolean;
};

@Injectable()
export class CategoriesService {
  constructor(private prisma: PrismaService) {}

  async findAll() {
    return this.prisma.category.findMany({
      orderBy: [{ sortOrder: "asc" }, { name: "asc" }],
      include: {
        _count: {
          select: { products: true },
        },
      },
    });
  }

  private slugify(input: string) {
    return String(input || "")
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/đ/g, "d")
      .replace(/Đ/g, "D")
      .toLowerCase()
      .trim()
      .replace(/[^a-z0-9\s-]/g, "")
      .replace(/\s+/g, "-")
      .replace(/-+/g, "-");
  }

  private toCode(input: string) {
    return this.slugify(input).replace(/-/g, "_").toUpperCase();
  }

  private normalizeText(input: string) {
    return String(input || "").replace(/\s+/g, " ").trim();
  }

  private capitalizeVietnameseName(input: string) {
    const lowerWords = new Set(["và", "hoặc", "của", "cho", "the", "vn"]);

    return this.normalizeText(input)
      .split(" ")
      .map((word, index) => {
        const lower = word.toLocaleLowerCase("vi-VN");
        if (index > 0 && lowerWords.has(lower)) return lower;
        return lower.charAt(0).toLocaleUpperCase("vi-VN") + lower.slice(1);
      })
      .join(" ");
  }

  private categoryCompare(a: { name: string }, b: { name: string }) {
    return String(a.name || "").localeCompare(String(b.name || ""), "vi", {
      sensitivity: "base",
      numeric: true,
    });
  }

  async normalize(options: {
    capitalizeNames?: boolean;
    sortAlphabetically?: boolean;
    hideInactiveFromPickers?: boolean;
  }) {
    const categories = await this.prisma.category.findMany({
      orderBy: [{ sortOrder: "asc" }, { name: "asc" }],
      include: {
        _count: {
          select: { products: true },
        },
      },
    });

    const groups = new Map<string, typeof categories>();

    for (const category of categories) {
      const finalName = options.capitalizeNames
        ? this.capitalizeVietnameseName(category.name)
        : this.normalizeText(category.name);

      const key = this.slugify(finalName || category.name);
      groups.set(key, [...(groups.get(key) || []), category]);
    }

    let updated = 0;

    const sortedGroups = Array.from(groups.entries()).sort(([, aRows], [, bRows]) => {
      const aName = options.capitalizeNames
        ? this.capitalizeVietnameseName(aRows[0]?.name || "")
        : aRows[0]?.name || "";

      const bName = options.capitalizeNames
        ? this.capitalizeVietnameseName(bRows[0]?.name || "")
        : bRows[0]?.name || "";

      return this.categoryCompare({ name: aName }, { name: bName });
    });

    for (let index = 0; index < sortedGroups.length; index += 1) {
      const [key, groupRows] = sortedGroups[index];

      const sortedRows = [...groupRows].sort((a, b) => {
        const productDiff = (b._count?.products || 0) - (a._count?.products || 0);
        if (productDiff !== 0) return productDiff;
        return Number(a.sortOrder || 0) - Number(b.sortOrder || 0);
      });

      const keeper = sortedRows[0];
      if (!keeper) continue;

      const finalName = options.capitalizeNames
        ? this.capitalizeVietnameseName(keeper.name)
        : this.normalizeText(keeper.name);

      const nextSortOrder = options.sortAlphabetically ? index + 1 : keeper.sortOrder;
      const nextSlug = key || this.slugify(finalName);
      const nextCode = this.toCode(finalName);

      // Không dùng interactive transaction ở đây để tránh Prisma P2028 timeout.
      // Mỗi bước chạy độc lập, chậm một chút nhưng ổn định hơn khi update nhiều Product.
      for (const duplicate of sortedRows.slice(1)) {
        await this.prisma.product.updateMany({
          where: { categoryId: duplicate.id },
          data: {
            categoryId: keeper.id,
            category: finalName,
          },
        });

        await this.prisma.product.updateMany({
          where: { category: duplicate.name },
          data: {
            categoryId: keeper.id,
            category: finalName,
          },
        });

        await this.prisma.category.delete({
          where: { id: duplicate.id },
        });

        updated += 1;
      }

      const patch: {
        name?: string;
        slug?: string;
        code?: string;
        sortOrder?: number;
      } = {};

      if (finalName && finalName !== keeper.name) patch.name = finalName;
      if (nextSlug && nextSlug !== keeper.slug) patch.slug = nextSlug;
      if (nextCode && nextCode !== keeper.code) patch.code = nextCode;
      if (nextSortOrder !== keeper.sortOrder) patch.sortOrder = nextSortOrder;

      if (Object.keys(patch).length) {
        await this.prisma.category.update({
          where: { id: keeper.id },
          data: patch,
        });

        updated += 1;
      }

      if (finalName) {
        await this.prisma.product.updateMany({
          where: { categoryId: keeper.id },
          data: { category: finalName },
        });
      }
    }

    const rows = await this.findAll();

    return {
      updated,
      rows,
    };
  }

  async create(data: CreateCategoryInput) {
    return this.prisma.category.create({
      data: {
        name: data.name.trim(),
        code: data.code.trim().toUpperCase(),
        slug: data.slug.trim().toLowerCase(),
        description: data.description?.trim() || null,
        sortOrder: data.sortOrder ?? 0,
      },
    });
  }

  async update(id: string, data: UpdateCategoryInput) {
    const existing = await this.prisma.category.findUnique({
      where: { id },
    });

    if (!existing) {
      throw new NotFoundException("Không tìm thấy danh mục.");
    }

    return this.prisma.category.update({
      where: { id },
      data: {
        ...(data.name !== undefined ? { name: data.name.trim() } : {}),
        ...(data.code !== undefined
          ? { code: data.code.trim().toUpperCase() }
          : {}),
        ...(data.slug !== undefined
          ? { slug: data.slug.trim().toLowerCase() }
          : {}),
        ...(data.description !== undefined
          ? { description: data.description?.trim() || null }
          : {}),
        ...(data.sortOrder !== undefined ? { sortOrder: data.sortOrder } : {}),
        ...(data.isActive !== undefined ? { isActive: data.isActive } : {}),
      },
    });
  }

  async toggle(id: string) {
    const existing = await this.prisma.category.findUnique({
      where: { id },
    });

    if (!existing) {
      throw new NotFoundException("Không tìm thấy danh mục.");
    }

    return this.prisma.category.update({
      where: { id },
      data: {
        isActive: !existing.isActive,
      },
    });
  }

  async remove(id: string) {
    const existing = await this.prisma.category.findUnique({
      where: { id },
      include: {
        _count: {
          select: { products: true },
        },
      },
    });

    if (!existing) {
      throw new NotFoundException("Không tìm thấy danh mục.");
    }

    if ((existing._count?.products || 0) > 0) {
      throw new BadRequestException("Danh mục đang có sản phẩm, không thể xoá.");
    }

    return this.prisma.category.delete({
      where: { id },
    });
  }
}
