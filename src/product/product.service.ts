import { BadRequestException, Injectable } from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";
import { Prisma, ProductStatus, VariantStatus } from "@prisma/client";
import * as XLSX from "xlsx";

type BranchStockMap = Record<string, number>;

type CreateProductInput = {
  name: string;
  slug: string;
  category?: string;
  categoryId?: string;
  productType?: string;
  brand?: string;
  weight?: number;
  imageUrl?: string;
  description?: string;
  defaultPrice?: number;
  defaultCostPrice?: number;
  colorOptions?: string[];
  sizeOptions?: string[];
  defaultBranchStocks?: BranchStockMap;
};

type UpdateProductInput = {
  name?: string;
  slug?: string;
  category?: string;
  categoryId?: string;
  brand?: string;
  weight?: number;
  imageUrl?: string;
  description?: string;
  defaultPrice?: number;
  defaultCostPrice?: number;
  colors?: string[];
  sizes?: string[];
  branchStocks?: BranchStockMap;
  applyPriceToAllVariants?: boolean;
};
type AddVariantInput = {
  color: string;
  size: string;
  price?: number;
  costPrice?: number;
  branchStocks?: BranchStockMap;
};

@Injectable()
export class ProductService {
  constructor(private readonly prisma: PrismaService) { }

  private toNumber(value: unknown) {
    if (value === null || value === undefined || value === "") return 0;
    if (typeof value === "number") return Number.isFinite(value) ? Math.round(value) : 0;

    let raw = String(value).trim().replace(/\s/g, "");
    if (!raw) return 0;

    // SAPO / Excel VN:
    // 226.000 -> 226000
    // 451.733,333 -> 451733
    // 459,861 -> 459861
    if (raw.includes(".") && raw.includes(",")) {
      raw = raw.split(",")[0].replace(/\./g, "");
    } else if (raw.includes(",")) {
      const parts = raw.split(",");
      const left = parts[0] || "";
      const right = parts[1] || "";

      if (right.length === 3 && left.length <= 3) {
        raw = raw.replace(/,/g, "");
      } else {
        raw = left;
      }
    } else if (raw.includes(".")) {
      raw = raw.replace(/\./g, "");
    }

    raw = raw.replace(/[^\d-]/g, "");
    const n = Number(raw);
    return Number.isFinite(n) ? Math.round(n) : 0;
  }

  private normalizeText(value: unknown) {
    return String(value ?? "").trim();
  }

  private normalizeHeader(value: unknown) {
    return String(value ?? "")
      .trim()
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/[*:]/g, "")
      .replace(/[_-]+/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  private normalizeSlug(value: string) {
    return String(value || "")
      .trim()
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/đ/g, "d")
      .replace(/[^a-z0-9\s-]/g, "")
      .replace(/\s+/g, "-")
      .replace(/-+/g, "-");
  }

  private buildSku(slug: string, color: string, size: string) {
    const base = this.normalizeSlug(slug).replace(/-/g, "").toUpperCase();
    const colorPart = String(color || "")
      .trim()
      .replace(/\s+/g, "")
      .toUpperCase();
    const sizePart = String(size || "")
      .trim()
      .toUpperCase();
    return `${base}-${colorPart}-${sizePart}`;
  }

  private buildInventoryByBranch(
    inventoryItems: Array<{
      branchId: string;
      availableQty: number;
      reservedQty: number;
      incomingQty: number;
    }>,
  ) {
    return Object.fromEntries(
      inventoryItems.map((item) => [
        item.branchId,
        Number(item.availableQty || 0),
      ]),
    );
  }

  private normalizeBranchStocks(
    input?: Record<string, unknown> | null,
  ): BranchStockMap {
    if (!input || typeof input !== "object" || Array.isArray(input)) {
      return {};
    }

    return Object.fromEntries(
      Object.entries(input)
        .filter(([key]) => String(key).trim().length > 0)
        .map(([key, value]) => [key, this.toNumber(value)]),
    );
  }

  private findValue(row: Record<string, unknown>, keys: string[]) {
    const rowKeys = Object.keys(row);
    for (const key of keys) {
      const normalizedKey = this.normalizeHeader(key);
      const matched = rowKeys.find(
        (rowKey) => this.normalizeHeader(rowKey) === normalizedKey,
      );
      if (matched) {
        const value = row[matched];
        if (
          value !== undefined &&
          value !== null &&
          String(value).trim() !== ""
        ) {
          return String(value).trim();
        }
      }
    }
    return "";
  }


  private findRawValue(row: Record<string, unknown>, keys: string[]) {
    const rowKeys = Object.keys(row);
    for (const key of keys) {
      const normalizedKey = this.normalizeHeader(key);
      const matched = rowKeys.find(
        (rowKey) => this.normalizeHeader(rowKey) === normalizedKey,
      );
      if (matched) {
        const value = row[matched];
        if (value !== undefined && value !== null && String(value).trim() !== "") {
          return value;
        }
      }
    }
    return "";
  }

  private excelColumnToIndex(column: string) {
    let result = 0;
    const clean = String(column || "").trim().toUpperCase();
    for (const char of clean) {
      const code = char.charCodeAt(0);
      if (code < 65 || code > 90) continue;
      result = result * 26 + (code - 64);
    }
    return result > 0 ? result - 1 : -1;
  }

  private findValueByColumn(row: Record<string, unknown>, column: string) {
    const index = this.excelColumnToIndex(column);
    if (index < 0) return "";
    const value = row[`__COL_${index}`];
    return value === undefined || value === null ? "" : value;
  }

  private getImportPrice(row: Record<string, unknown>) {
    return this.toNumber(
      this.findRawValue(row, [
        "Giá nhập",
        "Gia nhap",
        "Giá vốn",
        "Gia von",
        "costPrice",
        "cost price",
      ]) ||
      this.findValueByColumn(row, "BB") ||
      this.findValueByColumn(row, "BD"),
    );
  }

  private getRetailPrice(row: Record<string, unknown>) {
    return this.toNumber(
      this.findRawValue(row, [
        "PL_Giá bán lẻ",
        "PL Gia ban le",
        "Giá bán lẻ",
        "Gia ban le",
        "Giá bán",
        "Gia ban",
        "price",
      ]) || this.findValueByColumn(row, "BA"),
    );
  }

  private getBranchStocksFromImportRow(row: Record<string, unknown>) {
    // Mapping theo file DS sản phẩm SAPO:
    // LC_CN1 = Chùa Láng, LC_CN2 = Xã Đàn, LC_CN3 = Kho Quốc Oai, LC_CN4 = Thái Hà.
    // LC_CN5 tạm bỏ qua nếu hệ thống chưa có branch tương ứng.
    const stocks: BranchStockMap = {
      CL: this.toNumber(this.findValueByColumn(row, "AB")),
      XD: this.toNumber(this.findValueByColumn(row, "AV")),
      QO: this.toNumber(this.findValueByColumn(row, "AQ")),
      TH: this.toNumber(this.findValueByColumn(row, "AL")),
    };

    return Object.fromEntries(
      Object.entries(stocks).filter(([, qty]) => Number.isFinite(qty)),
    );
  }

  private getMainSkuCode(sku: string) {
    return String(sku || "")
      .trim()
      .split("-")[0]
      .trim()
      .toUpperCase();
  }

  private chunkArray<T>(items: T[], size = 1000) {
    const chunks: T[][] = [];
    for (let index = 0; index < items.length; index += size) {
      chunks.push(items.slice(index, index + size));
    }
    return chunks;
  }

  private detectHeaderRowIndex(sheetData: unknown[][]) {
    return sheetData.findIndex((row) => {
      if (!Array.isArray(row)) return false;

      const joined = row.map((cell) => this.normalizeHeader(cell)).join(" | ");

      return (
        joined.includes("ten san pham") ||
        joined.includes("ma sku") ||
        joined.includes("gia tri thuoc tinh 1") ||
        joined.includes("gia tri thuoc tinh 2") ||
        joined.includes("pl gia ban le")
      );
    });
  }

  private buildRowsFromSheetData(
    sheetData: unknown[][],
    headerRowIndex: number,
  ) {
    const headerRow = (sheetData[headerRowIndex] || []).map((cell) =>
      String(cell ?? "").trim(),
    );

    const rows: Record<string, unknown>[] = [];

    for (let index = headerRowIndex + 1; index < sheetData.length; index++) {
      const rowArray = sheetData[index];
      if (!Array.isArray(rowArray)) continue;

      const rowObject: Record<string, unknown> = {};

      for (let col = 0; col < headerRow.length; col++) {
        const header = headerRow[col];
        rowObject[`__COL_${col}`] = rowArray[col] ?? "";
        if (!header) continue;
        rowObject[header] = rowArray[col] ?? "";
      }

      rows.push(rowObject);
    }

    return rows;
  }

  private getLastTokenFromName(value: string) {
    const parts = String(value || "")
      .split("-")
      .map((part) => part.trim())
      .filter(Boolean);

    return parts.length ? parts[parts.length - 1] : "";
  }

  private parseVariantNameFallback(row: Record<string, unknown>) {
    const variantName = this.findValue(row, [
      "Tên phiên bản sản phẩm",
      "ten phien ban san pham",
      "variant name",
      "Tên biến thể",
    ]);

    const lastToken = this.getLastTokenFromName(variantName);

    return {
      color: lastToken || "DEFAULT",
      size: lastToken || "DEFAULT",
    };
  }

  private normalizeVariantOption(value: string, fallback = "DEFAULT") {
    const clean = String(value || "").trim();
    return clean || fallback;
  }

  async getProducts(params?: {
    page?: number;
    limit?: number;
    q?: string;
    category?: string;
    status?: string;
  }) {
    const page = Math.max(1, Number(params?.page || 1));
    const limit = Math.min(1000, Math.max(20, Number(params?.limit || 20)));
    const skip = (page - 1) * limit;

    const q = String(params?.q || "").trim();
    const category = String(params?.category || "ALL").trim();
    const status = String(params?.status || "ALL").trim();

    const where: Prisma.ProductWhereInput = {
      ...(status === "ALL"
        ? { status: { not: ProductStatus.INACTIVE } }
        : { status: status as ProductStatus }),
      ...(q
        ? {
          OR: [
            { name: { contains: q, mode: "insensitive" } },
            { slug: { contains: q, mode: "insensitive" } },
            { category: { contains: q, mode: "insensitive" } },
            {
              variants: {
                some: { sku: { contains: q, mode: "insensitive" } },
              },
            },
          ],
        }
        : {}),
    };

    if (category !== "ALL") {
      const wantedKey = this.normalizeHeader(category);
      const rawCategories = await this.prisma.product.findMany({
        where: {
          category: { not: null },
          ...(status === "ALL"
            ? { status: { not: ProductStatus.INACTIVE } }
            : { status: status as ProductStatus }),
        },
        select: { category: true },
      });

      const matchedCategories = Array.from(
        new Set(
          rawCategories
            .map((item) => String(item.category || "").trim())
            .filter((name) => name && this.normalizeHeader(name) === wantedKey),
        ),
      );

      if (!matchedCategories.length) {
        return { data: [], total: 0, page, limit };
      }

      where.category = { in: matchedCategories };
    }

    const [products, total] = await Promise.all([
      this.prisma.product.findMany({
        where,
        skip,
        take: limit,
        orderBy: { createdAt: "desc" },
        include: {
          categoryRel: true,
          variants: {
            include: { inventoryItems: true },
            orderBy: { createdAt: "asc" },
          },
        },
      }),
      this.prisma.product.count({ where }),
    ]);

    return {
      data: products.map((product) => ({
        ...product,
        category: product.categoryRel?.name || product.category,
        variants: product.variants.map((variant) => {
          const inventoryByBranch = this.buildInventoryByBranch(
            variant.inventoryItems.map((item) => ({
              branchId: item.branchId,
              availableQty: Number(item.availableQty || 0),
              reservedQty: Number(item.reservedQty || 0),
              incomingQty: Number(item.incomingQty || 0),
            })),
          );

          const stock = variant.inventoryItems.reduce(
            (sum, item) => sum + Number(item.availableQty || 0),
            0,
          );

          const price = Number(variant.price || 0);
          const costPrice = Number(variant.costPrice || 0);

          return {
            ...variant,
            price,
            costPrice,
            stock,
            inventoryByBranch,
            inventorySaleValue: price * stock,
            inventoryCostValue: costPrice * stock,
          };
        }),
      })),
      total,
      page,
      limit,
    };
  }

  async getProductById(id: string) {
    return this.prisma.product.findUnique({
      where: { id },
      include: {
        categoryRel: true,
        variants: {
          include: {
            inventoryItems: true,
          },
          orderBy: { createdAt: "asc" },
        },
      },
    });
  }
async getMissingCostProducts() {
  const variants = await this.prisma.productVariant.findMany({
    where: {
      OR: [
        { costPrice: null },
        { costPrice: 0 },
      ],
    },
    include: {
      product: true,
    },
    take: 1000,
  });

  return {
    success: true,
    total: variants.length,
    data: variants,
  };
}

async updateMissingCostBulk(
  items: Array<{ variantId: string; sku?: string; costPrice: number }>,
) {
  if (!Array.isArray(items) || items.length === 0) {
    return { success: true, updated: 0 };
  }

  let updated = 0;

  for (const item of items) {
    const costPrice = this.toNumber(item.costPrice);
    if (!item.variantId || costPrice <= 0) continue;

    await this.prisma.productVariant.update({
      where: { id: item.variantId },
      data: { costPrice: new Prisma.Decimal(costPrice) },
    });

    updated += 1;
  }

  return { success: true, updated };
}

  async createProduct(data: CreateProductInput) {
    if (!data.name?.trim()) {
      throw new BadRequestException("Thiếu tên sản phẩm");
    }

    if (!data.slug?.trim()) {
      throw new BadRequestException("Thiếu mã sản phẩm");
    }

    const slug = this.normalizeSlug(data.slug);

    const existing = await this.prisma.product.findUnique({
      where: { slug },
    });

    if (existing) {
      throw new BadRequestException("Mã sản phẩm đã tồn tại");
    }

    const colors = Array.isArray(data.colorOptions)
      ? data.colorOptions.filter(Boolean)
      : [];
    const sizes = Array.isArray(data.sizeOptions)
      ? data.sizeOptions.filter(Boolean)
      : [];

    if (!colors.length || !sizes.length) {
      throw new BadRequestException("Cần ít nhất 1 màu và 1 size");
    }

    let categoryRecord: { id: string; name: string } | null = null;

    if (data.categoryId?.trim()) {
      categoryRecord = await this.prisma.category.findUnique({
        where: { id: data.categoryId.trim() },
        select: { id: true, name: true },
      });

      if (!categoryRecord) {
        throw new BadRequestException("Danh mục không tồn tại");
      }
    }

    const resolvedCategoryName =
      categoryRecord?.name || data.category?.trim() || null;

    const defaultPrice = new Prisma.Decimal(this.toNumber(data.defaultPrice));
    const defaultCostPrice = new Prisma.Decimal(
      this.toNumber(data.defaultCostPrice),
    );
    const branchStocks = this.normalizeBranchStocks(data.defaultBranchStocks);

    return this.prisma.$transaction(
      async (tx) => {
        const product = await tx.product.create({
          data: {
            name: data.name.trim(),
            slug,
            description: data.description?.trim() || null,
            category: resolvedCategoryName,
            categoryId: categoryRecord?.id || null,
            productType: data.productType?.trim() || null,
            brand: data.brand?.trim() || "The 1970",
            weight: this.toNumber(data.weight) || 0,
            imageUrl: data.imageUrl?.trim() || null,
            status: ProductStatus.ACTIVE,
          },
        });

        for (const color of colors) {
          for (const size of sizes) {
            const cleanColor = String(color).trim();
            const cleanSize = String(size).trim();
            const sku = this.buildSku(slug, cleanColor, cleanSize);

            const variant = await tx.productVariant.create({
              data: {
                productId: product.id,
                sku,
                color: cleanColor,
                size: cleanSize,
                price: defaultPrice,
                costPrice: defaultCostPrice,
                status: VariantStatus.ACTIVE,
              },
            });

            const inventoryRows = Object.entries(branchStocks).map(
              ([branchId, availableQty]) => ({
                variantId: variant.id,
                branchId,
                availableQty: this.toNumber(availableQty),
                reservedQty: 0,
                incomingQty: 0,
              }),
            );

            if (inventoryRows.length > 0) {
              await tx.inventoryItem.createMany({
                data: inventoryRows,
              });
            }
          }
        }

        return tx.product.findUnique({
          where: { id: product.id },
          include: {
            categoryRel: true,
            variants: {
              include: {
                inventoryItems: true,
              },
              orderBy: { createdAt: "asc" },
            },
          },
        });
      },
      {
        maxWait: 10000,
        timeout: 20000,
      },
    );
  }

  async updateProduct(productId: string, data: UpdateProductInput) {
    const existing = await this.prisma.product.findUnique({
      where: { id: productId },
      include: {
        variants: {
          include: {
            inventoryItems: true,
          },
        },
      },
    });

    if (!existing) {
      throw new BadRequestException("Không tìm thấy sản phẩm");
    }

    const nextSlug = data.slug ? this.normalizeSlug(data.slug) : existing.slug;

    if (nextSlug !== existing.slug) {
      const slugTaken = await this.prisma.product.findFirst({
        where: {
          slug: nextSlug,
          NOT: { id: productId },
        },
      });

      if (slugTaken) {
        throw new BadRequestException("Mã sản phẩm đã tồn tại");
      }
    }

    let categoryRecord: { id: string; name: string } | null = null;

    if (data.categoryId?.trim()) {
      categoryRecord = await this.prisma.category.findUnique({
        where: { id: data.categoryId.trim() },
        select: { id: true, name: true },
      });

      if (!categoryRecord) {
        throw new BadRequestException("Danh mục không tồn tại");
      }
    }

    const colors =
      Array.isArray(data.colors) && data.colors.length
        ? data.colors.map((x) => String(x).trim()).filter(Boolean)
        : Array.from(
          new Set(
            existing.variants
              .map((v) => String(v.color || "").trim())
              .filter(Boolean),
          ),
        );

    const sizes =
      Array.isArray(data.sizes) && data.sizes.length
        ? data.sizes.map((x) => String(x).trim()).filter(Boolean)
        : Array.from(
          new Set(
            existing.variants
              .map((v) => String(v.size || "").trim())
              .filter(Boolean),
          ),
        );

    if (!colors.length || !sizes.length) {
      throw new BadRequestException("Cần ít nhất 1 màu và 1 size");
    }

    const branchStocks = this.normalizeBranchStocks(data.branchStocks);

    type WantedCombo = {
      key: string;
      color: string;
      size: string;
      sku: string;
    };

    const wantedCombos: WantedCombo[] = [];
    for (const color of colors) {
      for (const size of sizes) {
        const cleanColor = String(color).trim();
        const cleanSize = String(size).trim();
        wantedCombos.push({
          key: `${cleanColor}__${cleanSize}`,
          color: cleanColor,
          size: cleanSize,
          sku: this.buildSku(nextSlug, cleanColor, cleanSize),
        });
      }
    }

    const existingByCombo = new Map(
      existing.variants.map((variant) => [
        `${String(variant.color || "").trim()}__${String(variant.size || "").trim()}`,
        variant,
      ]),
    );

    const variantsToCreate = wantedCombos.filter(
      (combo) => !existingByCombo.has(combo.key),
    );

    const variantsToUpdate = wantedCombos
      .map((combo) => {
        const found = existingByCombo.get(combo.key);
        if (!found) return null;
        return {
          variantId: found.id,
          color: combo.color,
          size: combo.size,
          sku: combo.sku,
        };
      })
      .filter(Boolean) as Array<{
        variantId: string;
        color: string;
        size: string;
        sku: string;
      }>;

    const wantedKeys = new Set(wantedCombos.map((combo) => combo.key));

    const variantsToDelete = existing.variants.filter(
      (variant) =>
        !wantedKeys.has(
          `${String(variant.color || "").trim()}__${String(variant.size || "").trim()}`,
        ),
    );

    const result = await this.prisma.$transaction(
      async (tx) => {
        const updatedProduct = await tx.product.update({
          where: { id: productId },
          data: {
            name: data.name?.trim() || undefined,
            slug: nextSlug,
            category:
              categoryRecord?.name || data.category?.trim() || undefined,
            categoryId: categoryRecord?.id || null,
            brand: data.brand?.trim() || undefined,
            weight:
              data.weight !== undefined
                ? this.toNumber(data.weight)
                : undefined,
            imageUrl: data.imageUrl?.trim() || undefined,
            description: data.description?.trim() || undefined,
          },
        });

        for (const item of variantsToUpdate) {
          await tx.productVariant.update({
            where: { id: item.variantId },
            data: {
              sku: item.sku,
              color: item.color,
              size: item.size,
              ...(data.applyPriceToAllVariants &&
                data.defaultPrice !== undefined
                ? {
                  price: new Prisma.Decimal(this.toNumber(data.defaultPrice)),
                }
                : {}),
              ...(data.defaultCostPrice !== undefined
                ? {
                  costPrice: new Prisma.Decimal(
                    this.toNumber(data.defaultCostPrice),
                  ),
                }
                : {}),
            },
          });
        }

        const createdVariantIds: string[] = [];

        for (const item of variantsToCreate) {
          const created = await tx.productVariant.create({
            data: {
              productId,
              sku: item.sku,
              color: item.color,
              size: item.size,
              price: new Prisma.Decimal(this.toNumber(data.defaultPrice)),
              costPrice: new Prisma.Decimal(
                this.toNumber(data.defaultCostPrice),
              ),
              status: VariantStatus.ACTIVE,
            },
          });

          createdVariantIds.push(created.id);
        }

        if (variantsToDelete.length > 0) {
          const deleteIds = variantsToDelete.map((item) => item.id);

          await tx.inventoryItem.deleteMany({
            where: {
              variantId: { in: deleteIds },
            },
          });

          await tx.productVariant.deleteMany({
            where: {
              id: { in: deleteIds },
            },
          });
        }

        return {
          updatedProduct,
          inventoryVariantIds: [
            ...variantsToUpdate.map((item) => item.variantId),
            ...createdVariantIds,
          ],
        };
      },
      {
        maxWait: 10000,
        timeout: 20000,
      },
    );

    if (
      Object.keys(branchStocks).length > 0 &&
      result.inventoryVariantIds.length > 0
    ) {
      await Promise.all(
        result.inventoryVariantIds.map((variantId) =>
          Promise.all(
            Object.entries(branchStocks).map(([branchId, qty]) =>
              this.prisma.inventoryItem.upsert({
                where: {
                  variantId_branchId: {
                    variantId,
                    branchId,
                  },
                },
                update: {
                  availableQty: this.toNumber(qty),
                },
                create: {
                  variantId,
                  branchId,
                  availableQty: this.toNumber(qty),
                  reservedQty: 0,
                  incomingQty: 0,
                },
              }),
            ),
          ),
        ),
      );
    }

    return result.updatedProduct;
  }


  private looksLikePriceToken(value: string) {
    const clean = String(value || "").trim().toLowerCase();
    if (!clean) return false;
    return /\d/.test(clean) && (clean.includes("đ") || clean.includes(".") || clean.includes(","));
  }

  private parseVariantOptionsFromImport(row: Record<string, unknown>, sku: string) {
    const explicitColor = this.findValue(row, [
      "Giá trị thuộc tính 1",
      "gia tri thuoc tinh 1",
      "mau",
      "màu",
      "color",
      "Thuộc tính 1",
    ]);

    const explicitSize = this.findValue(row, [
      "Giá trị thuộc tính 2",
      "gia tri thuoc tinh 2",
      "size",
      "Thuộc tính 2",
    ]);

    const variantName = this.findValue(row, [
      "Tên phiên bản sản phẩm",
      "Tên phiên bản",
      "ten phien ban san pham",
      "ten phien ban",
      "variant name",
      "Tên biến thể",
    ]);

    const skuParts = String(sku || "")
      .trim()
      .split("-")
      .map((part) => part.trim())
      .filter(Boolean);

    let color = String(explicitColor || "").trim();
    let size = String(explicitSize || "").trim();

    const nameParts = String(variantName || "")
      .split("-")
      .map((part) => part.trim())
      .filter(Boolean);

    if (!size && nameParts.length >= 2) {
      size = nameParts[nameParts.length - 1];
    }

    if (!color && nameParts.length >= 3) {
      const colorCandidate = nameParts[nameParts.length - 2];
      if (!this.looksLikePriceToken(colorCandidate)) {
        color = colorCandidate;
      }
    }

    if (!size && skuParts.length >= 2) {
      size = skuParts[skuParts.length - 1];
    }

    if (!color && skuParts.length >= 3) {
      color = skuParts[skuParts.length - 2];
    }

    return {
      color: this.normalizeVariantOption(color, "DEFAULT"),
      size: this.normalizeVariantOption(size, "DEFAULT"),
      variantName,
    };
  }

  private buildProductNameFromSkuImport(
    currentProductName: string,
    productName: string,
    variantName: string,
    parentSku: string,
  ) {
    const baseName = String(productName || currentProductName || "").trim();
    if (baseName) return baseName;

    const cleanVariantName = String(variantName || "").trim();
    if (!cleanVariantName) return parentSku;

    const parentIndex = cleanVariantName.toUpperCase().indexOf(parentSku.toUpperCase());
    if (parentIndex >= 0) {
      const end = parentIndex + parentSku.length;
      return cleanVariantName.slice(0, end).trim();
    }

    return cleanVariantName;
  }

  async importProducts(files: Express.Multer.File[], overwrite = true) {
    if (!files?.length) {
      throw new BadRequestException("Thiếu file import");
    }

    let successRows = 0;
    let failedRows = 0;
    const errors: string[] = [];

    type VariantSeed = {
      color: string;
      size: string;
      sku: string;
      imageUrl: string;
      retailPrice: number;
      importPrice: number;
      branchStocks: BranchStockMap;
    };

    type ProductSeed = {
      name: string;
      slug: string;
      category: string;
      description: string;
      brand: string;
      weight: number;
      variants: VariantSeed[];
    };

    // Product = mã SKU chính, ví dụ SM936 / SP151 / AB797.
    // Variant = full SKU, ví dụ SM936-N-M / SP151-L / AB797-XG-X.
    const grouped = new Map<string, ProductSeed>();

    for (const file of files) {
      let rows: Record<string, unknown>[] = [];

      try {
        const workbook = XLSX.read(file.buffer, { type: "buffer" });
        const firstSheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[firstSheetName];

        const sheetData = XLSX.utils.sheet_to_json<unknown[]>(worksheet, {
          header: 1,
          defval: "",
        });

        const headerRowIndex = this.detectHeaderRowIndex(sheetData);

        if (headerRowIndex >= 0) {
          rows = this.buildRowsFromSheetData(sheetData, headerRowIndex).filter(
            (row) =>
              Object.values(row).some(
                (value) => String(value ?? "").trim() !== "",
              ),
          );
        } else {
          rows = XLSX.utils.sheet_to_json<Record<string, unknown>>(worksheet, {
            defval: "",
          });
        }
      } catch {
        failedRows += 1;
        errors.push(`${file.originalname}: không đọc được file Excel`);
        continue;
      }

      let currentProductName = "";
      let currentCategory = "";
      let currentDescription = "";
      let currentBrand = "";
      let currentWeight = 0;

      for (let index = 0; index < rows.length; index++) {
        const row = rows[index];

        const productName = this.findValue(row, [
          "Tên sản phẩm*",
          "Tên sản phẩm",
          "ten san pham",
          "product name",
        ]);

        const category = this.findValue(row, [
          "Loại sản phẩm",
          "Danh mục sản phẩm",
          "loai san pham",
          "danh muc san pham",
          "category",
        ]);

        const description = this.findValue(row, [
          "Mô tả sản phẩm",
          "Mô tả",
          "mo ta san pham",
          "description",
        ]);

        const brand = this.findValue(row, ["Nhãn hiệu", "nhan hieu", "brand"]);
        const weight = this.toNumber(
          this.findValue(row, ["Khối lượng", "khoi luong", "weight"]),
        );

        if (productName) currentProductName = productName;
        if (category) currentCategory = category;
        if (description) currentDescription = description;
        if (brand) currentBrand = brand;
        if (weight) currentWeight = weight;

        const hasAnyUsefulValue = Object.values(row).some(
          (value) => String(value ?? "").trim() !== "",
        );

        if (!hasAnyUsefulValue) continue;

        const sku = this.findValue(row, [
          "Mã SKU*",
          "Mã SKU",
          "ma sku",
          "sku",
          "Mã hàng",
          "SKU sản phẩm",
          "Mã biến thể",
        ]);

        if (!sku) {
          continue;
        }

        const cleanSku = sku.trim();
        const parentSku = this.getMainSkuCode(cleanSku);

        if (!parentSku) {
          failedRows += 1;
          errors.push(
            `${file.originalname} - dòng ${index + 2}: SKU không hợp lệ`,
          );
          continue;
        }

        const productSlug = this.normalizeSlug(parentSku);
        const imageUrl = this.findValue(row, [
          "Ảnh đại diện",
          "anh dai dien",
          "image",
          "image url",
        ]);

        const retailPrice = this.getRetailPrice(row);
        const rawImportPrice = this.getImportPrice(row);
        const importPrice = rawImportPrice > 0 ? rawImportPrice : 0;
        const branchStocks = this.getBranchStocksFromImportRow(row);

        const parsedVariant = this.parseVariantOptionsFromImport(row, cleanSku);
        const productDisplayName = this.buildProductNameFromSkuImport(
          currentProductName,
          productName,
          parsedVariant.variantName,
          parentSku,
        );

        if (!grouped.has(productSlug)) {
          grouped.set(productSlug, {
            name: productDisplayName,
            slug: productSlug,
            category: category || currentCategory || "",
            description: description || currentDescription || "",
            brand: brand || currentBrand || "The 1970",
            weight: weight || currentWeight || 0,
            variants: [],
          });
        } else {
          const seed = grouped.get(productSlug)!;
          if (!seed.name && productDisplayName) seed.name = productDisplayName;
          if (!seed.category && (category || currentCategory)) {
            seed.category = category || currentCategory;
          }
          if (!seed.description && (description || currentDescription)) {
            seed.description = description || currentDescription;
          }
          if (!seed.brand && (brand || currentBrand)) {
            seed.brand = brand || currentBrand;
          }
          if (!seed.weight && (weight || currentWeight)) {
            seed.weight = weight || currentWeight;
          }
        }

        const productSeed = grouped.get(productSlug)!;
        const variantSeed: VariantSeed = {
          color: parsedVariant.color,
          size: parsedVariant.size,
          sku: cleanSku,
          imageUrl,
          retailPrice,
          importPrice,
          branchStocks,
        };

        const existingVariantIndex = productSeed.variants.findIndex(
          (item) => item.sku.trim() === cleanSku,
        );

        if (existingVariantIndex >= 0) {
          productSeed.variants[existingVariantIndex] = variantSeed;
        } else {
          productSeed.variants.push(variantSeed);
        }
      }
    }

    const slugs = Array.from(grouped.keys());
    const allSkus = Array.from(
      new Set(
        Array.from(grouped.values()).flatMap((product) =>
          product.variants.map((variant) => variant.sku.trim()),
        ),
      ),
    );

    const existingProducts = await this.prisma.product.findMany({
      where: { slug: { in: slugs } },
      select: { id: true, slug: true, imageUrl: true },
    });

    const existingVariants = allSkus.length
      ? await this.prisma.productVariant.findMany({
        where: { sku: { in: allSkus } },
        select: { id: true, sku: true, productId: true },
      })
      : [];

    const productBySlug = new Map(
      existingProducts.map((product) => [product.slug, product]),
    );
    const variantBySku = new Map(
      existingVariants.map((variant) => [variant.sku, variant]),
    );

    const variantsToCreate: Prisma.ProductVariantCreateManyInput[] = [];
    const variantsToUpdate: Array<{
      id: string;
      data: Prisma.ProductVariantUpdateInput;
    }> = [];

    const inventoryBySku = new Map<string, BranchStockMap>();

    for (const [, productSeed] of grouped.entries()) {
      try {
        let product = productBySlug.get(productSeed.slug);

        if (!product) {
          product = await this.prisma.product.create({
            data: {
              name: productSeed.name,
              slug: productSeed.slug,
              category: productSeed.category || null,
              productType: null,
              brand: productSeed.brand || "The 1970",
              weight: productSeed.weight || 0,
              imageUrl: productSeed.variants[0]?.imageUrl || null,
              description: productSeed.description || null,
              status: ProductStatus.ACTIVE,
            },
            select: { id: true, slug: true, imageUrl: true },
          });

          productBySlug.set(product.slug, product);
        } else if (overwrite) {
          product = await this.prisma.product.update({
            where: { id: product.id },
            data: {
              name: productSeed.name,
              category: productSeed.category || null,
              brand: productSeed.brand || "The 1970",
              weight: productSeed.weight || 0,
              imageUrl:
                productSeed.variants[0]?.imageUrl || product.imageUrl || null,
              description: productSeed.description || null,
              status: ProductStatus.ACTIVE,
            },
            select: { id: true, slug: true, imageUrl: true },
          });

          productBySlug.set(product.slug, product);
        }

        for (const variantSeed of productSeed.variants) {
          const cleanSku = variantSeed.sku.trim();
          const existingVariant = variantBySku.get(cleanSku);
          inventoryBySku.set(cleanSku, variantSeed.branchStocks || {});

          if (!existingVariant) {
            variantsToCreate.push({
              productId: product.id,
              sku: cleanSku,
              color: variantSeed.color.trim(),
              size: variantSeed.size.trim(),
              price: new Prisma.Decimal(this.toNumber(variantSeed.retailPrice)),
              costPrice: new Prisma.Decimal(this.toNumber(variantSeed.importPrice)),
              status: VariantStatus.ACTIVE,
            });
          } else if (overwrite) {
            variantsToUpdate.push({
              id: existingVariant.id,
              data: {
                product: { connect: { id: product.id } },
                color: variantSeed.color.trim(),
                size: variantSeed.size.trim(),
                price: new Prisma.Decimal(
                  this.toNumber(variantSeed.retailPrice),
                ),
                costPrice: new Prisma.Decimal(
                  this.toNumber(variantSeed.importPrice),
                ),
                status: VariantStatus.ACTIVE,
              },
            });
          }
        }
      } catch (error) {
        failedRows += Math.max(1, productSeed.variants.length);
        errors.push(
          `${productSeed.name}: ${error instanceof Error ? error.message : "Import lỗi"}`,
        );
      }
    }

    for (const chunk of this.chunkArray(variantsToCreate, 1000)) {
      if (!chunk.length) continue;
      await this.prisma.productVariant.createMany({
        data: chunk,
        skipDuplicates: true,
      });
    }

    for (const chunk of this.chunkArray(variantsToUpdate, 200)) {
      await Promise.all(
        chunk.map((item) =>
          this.prisma.productVariant.update({
            where: { id: item.id },
            data: item.data,
          }),
        ),
      );
    }

    const importedVariants = allSkus.length
      ? await this.prisma.productVariant.findMany({
        where: { sku: { in: allSkus } },
        select: { id: true, sku: true },
      })
      : [];

    let inventoryRows = 0;
    const variantIds = importedVariants.map((variant) => variant.id);

    if (variantIds.length) {
      await this.prisma.inventoryItem.deleteMany({
        where: { variantId: { in: variantIds } },
      });

      const branchIds = new Set(
        (await this.prisma.branch.findMany({ select: { id: true } })).map(
          (branch) => branch.id,
        ),
      );

      const inventoryCreateRows: Prisma.InventoryItemCreateManyInput[] = [];

      for (const variant of importedVariants) {
        const branchStocks = inventoryBySku.get(variant.sku) || {};

        for (const [branchId, qty] of Object.entries(branchStocks)) {
          if (!branchIds.has(branchId)) continue;

          inventoryCreateRows.push({
            variantId: variant.id,
            branchId,
            availableQty: this.toNumber(qty),
            reservedQty: 0,
            incomingQty: 0,
          });
        }
      }

      for (const chunk of this.chunkArray(inventoryCreateRows, 3000)) {
        if (!chunk.length) continue;
        await this.prisma.inventoryItem.createMany({
          data: chunk,
          skipDuplicates: true,
        });
        inventoryRows += chunk.length;
      }
    }

    successRows = allSkus.length;

    return {
      successRows,
      failedRows,
      errors,
      importedProducts: grouped.size,
      importedVariants: allSkus.length,
      createdVariants: variantsToCreate.length,
      updatedVariants: variantsToUpdate.length,
      inventoryRows,
      note: "Import 1 bước: tạo/cập nhật sản phẩm + variant, giá nhập BB và tồn kho các chi nhánh từ file Excel.",
    };
  }

  async getProductCategoryOptions() {
    const products = await this.prisma.product.findMany({
      where: {
        status: { not: ProductStatus.INACTIVE },
        category: { not: null },
      },
      select: { category: true },
    });

    const categoryByKey = new Map<string, string>();

    for (const product of products) {
      const raw = String(product.category || "").trim();
      if (!raw) continue;

      const key = this.normalizeHeader(raw);
      if (!categoryByKey.has(key)) {
        categoryByKey.set(key, raw);
      }
    }

    return Array.from(categoryByKey.values()).sort((a, b) =>
      a.localeCompare(b, "vi"),
    );
  }

  async renameCategory(oldName: string, newName: string) {
    const cleanOldName = String(oldName || "").trim();
    const cleanNewName = String(newName || "").trim();

    if (!cleanOldName) {
      throw new BadRequestException("Thiếu danh mục cần gộp");
    }

    if (!cleanNewName) {
      throw new BadRequestException("Thiếu tên danh mục chuẩn");
    }

    const normalizedOld = this.normalizeHeader(cleanOldName);
    const normalizedNew = this.normalizeHeader(cleanNewName);
    const newSlug = this.normalizeSlug(cleanNewName);
    const newCode = newSlug.replace(/-/g, "_").toUpperCase();

    let targetCategory = await this.prisma.category.findFirst({
      where: {
        OR: [{ name: cleanNewName }, { slug: newSlug }],
      },
      select: { id: true, name: true },
    });

    if (!targetCategory) {
      targetCategory = await this.prisma.category.create({
        data: {
          name: cleanNewName,
          slug: newSlug,
          code: newCode,
          description: null,
          isActive: true,
        },
        select: { id: true, name: true },
      });
    } else {
      targetCategory = await this.prisma.category.update({
        where: { id: targetCategory.id },
        data: {
          name: cleanNewName,
          slug: newSlug,
          code: newCode,
          isActive: true,
        },
        select: { id: true, name: true },
      });
    }

    const products = await this.prisma.product.findMany({
      where: { category: { not: null } },
      select: { id: true, category: true, categoryId: true },
    });

    const matchedProductIds = products
      .filter((product) => {
        const categoryKey = this.normalizeHeader(product.category);
        return categoryKey === normalizedOld || categoryKey === normalizedNew;
      })
      .map((product) => product.id);

    let updatedProducts = 0;

    for (const chunk of this.chunkArray(matchedProductIds, 1000)) {
      if (!chunk.length) continue;
      const result = await this.prisma.product.updateMany({
        where: { id: { in: chunk } },
        data: {
          category: cleanNewName,
          categoryId: targetCategory.id,
        },
      });

      updatedProducts += result.count;
    }

    const oldCategories = await this.prisma.category.findMany({
      where: {
        NOT: { id: targetCategory.id },
      },
      select: { id: true, name: true, slug: true },
    });

    const oldCategoryIds = oldCategories
      .filter((category) => {
        const nameKey = this.normalizeHeader(category.name);
        const slugKey = this.normalizeHeader(category.slug);
        return (
          nameKey === normalizedOld ||
          nameKey === normalizedNew ||
          slugKey === normalizedOld ||
          slugKey === normalizedNew
        );
      })
      .map((category) => category.id);

    let relinkedProducts = 0;

    if (oldCategoryIds.length > 0) {
      const relinked = await this.prisma.product.updateMany({
        where: { categoryId: { in: oldCategoryIds } },
        data: {
          category: cleanNewName,
          categoryId: targetCategory.id,
        },
      });
      relinkedProducts = relinked.count;

      await this.prisma.category.deleteMany({
        where: { id: { in: oldCategoryIds } },
      });
    }

    return {
      success: true,
      category: cleanNewName,
      categoryId: targetCategory.id,
      updatedProducts: updatedProducts + relinkedProducts,
      removedOldCategories: oldCategoryIds.length,
    };
  }

  async updateCostBulk(
    items: Array<{ variantId?: string; sku?: string; costPrice: number }>,
  ) {
    if (!Array.isArray(items) || items.length === 0) {
      throw new BadRequestException("Không có dữ liệu giá nhập để cập nhật");
    }

    let updated = 0;

    for (const chunk of this.chunkArray(items, 200)) {
      await Promise.all(
        chunk.map(async (item) => {
          const costPrice = this.toNumber(item.costPrice);
          if (costPrice <= 0) return;

          const where = item.variantId
            ? { id: item.variantId }
            : { sku: String(item.sku || "").trim() };

          const result = await this.prisma.productVariant.updateMany({
            where,
            data: { costPrice: new Prisma.Decimal(costPrice) },
          });

          updated += result.count;
        }),
      );
    }

    return { success: true, updated };
  }

  async addVariant(productId: string, data: AddVariantInput) {
    const product = await this.prisma.product.findUnique({
      where: { id: productId },
    });

    if (!product) {
      throw new BadRequestException("Không tìm thấy sản phẩm");
    }

    if (!data.color?.trim() || !data.size?.trim()) {
      throw new BadRequestException("Thiếu màu hoặc size");
    }

    const sku = this.buildSku(product.slug, data.color, data.size);

    const exists = await this.prisma.productVariant.findUnique({
      where: { sku },
    });

    if (exists) {
      throw new BadRequestException("Variant đã tồn tại");
    }

    const branchStocks = this.normalizeBranchStocks(data.branchStocks);

    return this.prisma.$transaction(
      async (tx) => {
        const variant = await tx.productVariant.create({
          data: {
            productId: product.id,
            sku,
            color: data.color.trim(),
            size: data.size.trim(),
            price: new Prisma.Decimal(this.toNumber(data.price)),
            costPrice: new Prisma.Decimal(this.toNumber(data.costPrice)),
            status: VariantStatus.ACTIVE,
          },
        });

        const inventoryRows = Object.entries(branchStocks).map(
          ([branchId, availableQty]) => ({
            variantId: variant.id,
            branchId,
            availableQty: this.toNumber(availableQty),
            reservedQty: 0,
            incomingQty: 0,
          }),
        );

        if (inventoryRows.length > 0) {
          await tx.inventoryItem.createMany({
            data: inventoryRows,
          });
        }

        return tx.product.findUnique({
          where: { id: product.id },
          include: {
            categoryRel: true,
            variants: {
              include: {
                inventoryItems: true,
              },
              orderBy: { createdAt: "asc" },
            },
          },
        });
      },
      {
        maxWait: 10000,
        timeout: 20000,
      },
    );
  }


async checkMissingCostFromExcel(file: Express.Multer.File) {
  if (!file) {
    return { success: false, message: "Thiếu file" };
  }

  const workbook = XLSX.read(file.buffer, { type: "buffer" });
  const sheet = workbook.Sheets[workbook.SheetNames[0]];

  const rows: any[] = XLSX.utils.sheet_to_json(sheet);

  const result = rows.filter((row) => {
    const cost =
      Number(row["Giá nhập"] || row["Gia nhap"] || row["costPrice"] || 0);

    return cost <= 0;
  });

  return {
    success: true,
    total: result.length,
    data: result,
  };
}
  async toggleProductStatus(productId: string) {
    const product = await this.prisma.product.findUnique({
      where: { id: productId },
    });

    if (!product) {
      throw new BadRequestException("Không tìm thấy sản phẩm");
    }

    const nextStatus =
      product.status === ProductStatus.ACTIVE
        ? ProductStatus.INACTIVE
        : ProductStatus.ACTIVE;

    return this.prisma.product.update({
      where: { id: productId },
      data: {
        status: nextStatus,
      },
      include: {
        categoryRel: true,
        variants: {
          include: {
            inventoryItems: true,
          },
        },
      },
    });
  }
  async deleteProduct(productId: string) {
    const existing = await this.prisma.product.findUnique({
      where: { id: productId },
    });

    if (!existing) {
      throw new BadRequestException("Không tìm thấy sản phẩm");
    }

    await this.prisma.product.update({
      where: { id: productId },
      data: {
        status: ProductStatus.INACTIVE,
      },
    });

    return { success: true };
  }
  async syncCategoriesFromProducts() {
    const products = await this.prisma.product.findMany({
      where: { category: { not: null } },
      select: { id: true, category: true },
    });

    const groups = new Map<string, { name: string; productIds: string[] }>();

    for (const product of products) {
      const name = String(product.category || "").trim();
      if (!name) continue;

      const key = this.normalizeHeader(name);

      if (!groups.has(key)) {
        groups.set(key, { name, productIds: [] });
      }

      groups.get(key)!.productIds.push(product.id);
    }

    const existingCategories = await this.prisma.category.findMany({
      select: { id: true, name: true },
    });

    const categoryByKey = new Map(
      existingCategories.map((category) => [
        this.normalizeHeader(category.name),
        category,
      ]),
    );

    let created = 0;
    let updatedProducts = 0;

    for (const [key, group] of groups.entries()) {
      let category = categoryByKey.get(key);

      if (!category) {
        category = await this.prisma.category.create({
          data: {
            name: group.name,
            code: this.normalizeSlug(group.name)
              .replace(/-/g, "_")
              .toUpperCase(),
            slug: this.normalizeSlug(group.name),
            description: null,
            isActive: true,
          },
          select: { id: true, name: true },
        });

        categoryByKey.set(key, category);
        created += 1;
      }

      const result = await this.prisma.product.updateMany({
        where: { id: { in: group.productIds } },
        data: {
          categoryId: category.id,
          category: category.name,
        },
      });

      updatedProducts += result.count;
    }

    return {
      success: true,
      categories: groups.size,
      created,
      updatedProducts,
    };
  }
  async mergeDuplicateProducts() {
    const products = await this.prisma.product.findMany({
      where: {
        status: { not: ProductStatus.INACTIVE },
      },
      select: {
        id: true,
        name: true,
        slug: true,
        createdAt: true,
      },
      orderBy: { createdAt: "asc" },
    });

    const groups = new Map<string, typeof products>();

    for (const product of products) {
      const key = this.normalizeSlug(product.slug || product.name);
      if (!key) continue;

      if (!groups.has(key)) groups.set(key, []);
      groups.get(key)!.push(product);
    }

    let mergedProducts = 0;
    let movedVariants = 0;

    for (const [, group] of groups.entries()) {
      if (group.length <= 1) continue;

      const keeper = group[0];
      const duplicates = group.slice(1);

      for (const duplicate of duplicates) {
        const duplicateVariants = await this.prisma.productVariant.findMany({
          where: { productId: duplicate.id },
          select: {
            id: true,
            sku: true,
          },
        });

        for (const variant of duplicateVariants) {
          const sameSku = await this.prisma.productVariant.findFirst({
            where: {
              productId: keeper.id,
              sku: variant.sku,
              NOT: { id: variant.id },
            },
            select: { id: true },
          });

          if (sameSku) {
            await this.prisma.inventoryItem.deleteMany({
              where: { variantId: variant.id },
            });

            await this.prisma.productVariant.delete({
              where: { id: variant.id },
            });
          } else {
            await this.prisma.productVariant.update({
              where: { id: variant.id },
              data: { productId: keeper.id },
            });

            movedVariants += 1;
          }
        }

        await this.prisma.product.update({
          where: { id: duplicate.id },
          data: { status: ProductStatus.INACTIVE },
        });

        mergedProducts += 1;
      }
    }

    return {
      success: true,
      mergedProducts,
      movedVariants,
    };
  }


  async clearAllDescriptions() {
    const result = await this.prisma.product.updateMany({
      data: {
        description: null,
      },
    });

    return {
      success: true,
      count: result.count,
    };
  }


}