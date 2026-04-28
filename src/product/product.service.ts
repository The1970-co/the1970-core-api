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
    if (typeof value === "number") return value;
    if (value === null || value === undefined || value === "") return 0;
    const raw = String(value).replace(/[^\d.-]/g, "");
    const n = Number(raw);
    return Number.isFinite(n) ? n : 0;
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
    const sizePart = String(size || "").trim().toUpperCase();
    return `${base}-${colorPart}-${sizePart}`;
  }

  private buildInventoryByBranch(
    inventoryItems: Array<{
      branchId: string;
      availableQty: number;
      reservedQty: number;
      incomingQty: number;
    }>
  ) {
    return Object.fromEntries(
      inventoryItems.map((item) => [item.branchId, Number(item.availableQty || 0)])
    );
  }

  private normalizeBranchStocks(input?: Record<string, unknown> | null): BranchStockMap {
    if (!input || typeof input !== "object" || Array.isArray(input)) {
      return {};
    }

    return Object.fromEntries(
      Object.entries(input)
        .filter(([key]) => String(key).trim().length > 0)
        .map(([key, value]) => [key, this.toNumber(value)])
    );
  }

  private findValue(row: Record<string, unknown>, keys: string[]) {
    const rowKeys = Object.keys(row);
    for (const key of keys) {
      const normalizedKey = this.normalizeHeader(key);
      const matched = rowKeys.find(
        (rowKey) => this.normalizeHeader(rowKey) === normalizedKey
      );
      if (matched) {
        const value = row[matched];
        if (value !== undefined && value !== null && String(value).trim() !== "") {
          return String(value).trim();
        }
      }
    }
    return "";
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

  private buildRowsFromSheetData(sheetData: unknown[][], headerRowIndex: number) {
    const headerRow = (sheetData[headerRowIndex] || []).map((cell) =>
      String(cell ?? "").trim()
    );

    const rows: Record<string, unknown>[] = [];

    for (let index = headerRowIndex + 1; index < sheetData.length; index++) {
      const rowArray = sheetData[index];
      if (!Array.isArray(rowArray)) continue;

      const rowObject: Record<string, unknown> = {};

      for (let col = 0; col < headerRow.length; col++) {
        const header = headerRow[col];
        if (!header) continue;
        rowObject[header] = rowArray[col] ?? "";
      }

      rows.push(rowObject);
    }

    return rows;
  }
  async getProducts(params?: {
    page?: number;
    limit?: number;
    q?: string;
    category?: string;
    status?: string;
  }) {
    const page = Math.max(1, Number(params?.page || 1));
    const limit = Math.min(100, Math.max(20, Number(params?.limit || 20)));
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
              { variants: { some: { sku: { contains: q, mode: "insensitive" } } } },
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
            .filter((name) => name && this.normalizeHeader(name) === wantedKey)
        )
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
            }))
          );

          const stock = variant.inventoryItems.reduce(
            (sum, item) => sum + Number(item.availableQty || 0),
            0
          );

          return {
            ...variant,
            price: Number(variant.price || 0),
            costPrice: Number(variant.costPrice || 0),
            stock,
            inventoryByBranch,
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

    const colors = Array.isArray(data.colorOptions) ? data.colorOptions.filter(Boolean) : [];
    const sizes = Array.isArray(data.sizeOptions) ? data.sizeOptions.filter(Boolean) : [];

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

    const resolvedCategoryName = categoryRecord?.name || data.category?.trim() || null;

    const defaultPrice = new Prisma.Decimal(this.toNumber(data.defaultPrice));
    const defaultCostPrice = new Prisma.Decimal(this.toNumber(data.defaultCostPrice));
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
              })
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
      }
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
              .filter(Boolean)
          )
        );

    const sizes =
      Array.isArray(data.sizes) && data.sizes.length
        ? data.sizes.map((x) => String(x).trim()).filter(Boolean)
        : Array.from(
          new Set(
            existing.variants
              .map((v) => String(v.size || "").trim())
              .filter(Boolean)
          )
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
      ])
    );

    const variantsToCreate = wantedCombos.filter(
      (combo) => !existingByCombo.has(combo.key)
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
          `${String(variant.color || "").trim()}__${String(variant.size || "").trim()}`
        )
    );

    const result = await this.prisma.$transaction(
      async (tx) => {
        const updatedProduct = await tx.product.update({
          where: { id: productId },
          data: {
            name: data.name?.trim() || undefined,
            slug: nextSlug,
            category: categoryRecord?.name || data.category?.trim() || undefined,
            categoryId: categoryRecord?.id || null,
            brand: data.brand?.trim() || undefined,
            weight:
              data.weight !== undefined ? this.toNumber(data.weight) : undefined,
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
                    this.toNumber(data.defaultCostPrice)
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
                this.toNumber(data.defaultCostPrice)
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
      }
    );

    if (Object.keys(branchStocks).length > 0 && result.inventoryVariantIds.length > 0) {
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
              })
            )
          )
        )
      );
    }

    return result.updatedProduct;
  }

  async importProducts(files: Express.Multer.File[], overwrite = true) {
    if (!files?.length) {
      throw new BadRequestException("Thiếu file import");
    }

    const branches = await this.prisma.branch.findMany({
      select: { id: true, name: true },
    });

    const findBranchId = (aliases: string[]) => {
      for (const alias of aliases) {
        const found = branches.find(
          (branch) => this.normalizeHeader(branch.name) === this.normalizeHeader(alias)
        );

        if (found) return found.id;
      }

      return undefined;
    };

    const chuaLangBranchId = findBranchId(["CHÙA LÁNG", "CHUA LANG", "CL"]);
    const xaDanBranchId = findBranchId(["XÃ ĐÀN", "XA DAN", "XD"]);
    const quocOaiBranchId = findBranchId(["QUỐC OAI", "QUOC OAI", "QO"]);
    const thaiHaBranchId = findBranchId(["THÁI HÀ", "THAI HA", "TH"]);

    const sapoBranchColumns: Array<{ keys: string[]; branchId?: string }> = [
      {
        branchId: chuaLangBranchId,
        keys: ["LC_CN1_Tồn kho ban đầu*", "LC_CN1_Tồn kho ban đầu", "lc cn1 ton kho ban dau"],
      },
      {
        branchId: xaDanBranchId,
        keys: ["LC_CN2_Tồn kho ban đầu*", "LC_CN2_Tồn kho ban đầu", "lc cn2 ton kho ban dau"],
      },
      {
        branchId: quocOaiBranchId,
        keys: ["LC_CN3_Tồn kho ban đầu*", "LC_CN3_Tồn kho ban đầu", "lc cn3 ton kho ban dau"],
      },
      {
        branchId: thaiHaBranchId,
        keys: ["LC_CN4_Tồn kho ban đầu*", "LC_CN4_Tồn kho ban đầu", "lc cn4 ton kho ban dau"],
      },
    ];

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
      branchStocks: Record<string, number>;
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
          rows = this.buildRowsFromSheetData(sheetData, headerRowIndex).filter((row) =>
            Object.values(row).some((value) => String(value ?? "").trim() !== "")
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
          this.findValue(row, ["Khối lượng", "khoi luong", "weight"])
        );

        if (productName) {
          currentProductName = productName;
          currentCategory = category || "";
          currentDescription = description || "";
          currentBrand = brand || "";
          currentWeight = weight || 0;
        } else {
          if (category) currentCategory = category;
          if (brand) currentBrand = brand;
          if (weight) currentWeight = weight;
        }

        const color = this.findValue(row, [
          "Giá trị thuộc tính 1",
          "gia tri thuoc tinh 1",
          "mau",
          "màu",
          "color",
          "Thuộc tính 1",
        ]);
        const size = this.findValue(row, [
          "Giá trị thuộc tính 2",
          "gia tri thuoc tinh 2",
          "size",
          "Thuộc tính 2",
        ]);
        const sku = this.findValue(row, [
          "Mã SKU*",
          "Mã SKU",
          "ma sku",
          "sku",
          "Mã hàng",
          "SKU sản phẩm",
          "Mã biến thể",
        ]);
        const imageUrl = this.findValue(row, [
          "Ảnh đại diện",
          "anh dai dien",
          "image",
          "image url",
        ]);
        const retailPrice = this.toNumber(
          this.findValue(row, ["PL_Giá bán lẻ", "pl gia ban le", "gia ban le"])
        );
        const importPrice = this.toNumber(
          this.findValue(row, [
            "PL_Giá nhập",
            "pl gia nhap",
            "gia nhap",
            "PL_Giá vốn",
            "pl gia von",
          ])
        );

        const branchStocks: Record<string, number> = {};
        for (const item of sapoBranchColumns) {
          if (!item.branchId) continue;
          branchStocks[item.branchId] = this.toNumber(this.findValue(row, item.keys));
        }

        const hasAnyUsefulValue =
          currentProductName || currentCategory || currentBrand || color || size || sku;

        if (!hasAnyUsefulValue) continue;

        if (!currentProductName) {
          failedRows += 1;
          errors.push(
            `${file.originalname} - dòng ${index + 2}: không xác định được sản phẩm gốc`
          );
          continue;
        }

        if (!sku || !color || !size) {
          failedRows += 1;
          errors.push(
            `${file.originalname} - dòng ${index + 2}: thiếu SKU hoặc màu hoặc size`
          );
          continue;
        }

        const mainSkuCode = this.getMainSkuCode(sku);
        const productSlug = this.normalizeSlug(mainSkuCode || currentProductName);

        if (!grouped.has(productSlug)) {
          grouped.set(productSlug, {
            name: currentProductName,
            slug: productSlug,
            category: currentCategory,
            description: currentDescription,
            brand: currentBrand || "The 1970",
            weight: currentWeight || 0,
            variants: [],
          });
        }

        grouped.get(productSlug)!.variants.push({
          color,
          size,
          sku: sku.trim(),
          imageUrl,
          retailPrice,
          importPrice,
          branchStocks,
        });
      }
    }

    const slugs = Array.from(grouped.keys());
    const allSkus = Array.from(
      new Set(
        Array.from(grouped.values()).flatMap((product) =>
          product.variants.map((variant) => variant.sku.trim())
        )
      )
    );

    const existingProducts = await this.prisma.product.findMany({
      where: { slug: { in: slugs } },
      select: { id: true, slug: true, imageUrl: true },
    });

    const existingVariants = await this.prisma.productVariant.findMany({
      where: { sku: { in: allSkus } },
      select: { id: true, sku: true, productId: true },
    });

    const productBySlug = new Map(existingProducts.map((product) => [product.slug, product]));
    const variantBySku = new Map(existingVariants.map((variant) => [variant.sku, variant]));

    const variantsToCreate: Prisma.ProductVariantCreateManyInput[] = [];
    const variantsToUpdate: Array<{ id: string; data: Prisma.ProductVariantUpdateInput }> = [];
    const productIdsNeedingInventory = new Set<string>();

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
              imageUrl: productSeed.variants[0]?.imageUrl || product.imageUrl || null,
              description: productSeed.description || null,
              status: ProductStatus.ACTIVE,
            },
            select: { id: true, slug: true, imageUrl: true },
          });

          productBySlug.set(product.slug, product);
        }

        for (const variantSeed of productSeed.variants) {
          const existingVariant = variantBySku.get(variantSeed.sku.trim());

          if (!existingVariant) {
            variantsToCreate.push({
              productId: product.id,
              sku: variantSeed.sku.trim(),
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
                price: new Prisma.Decimal(this.toNumber(variantSeed.retailPrice)),
                costPrice: new Prisma.Decimal(this.toNumber(variantSeed.importPrice)),
              },
            });
          }

          productIdsNeedingInventory.add(product.id);
        }
      } catch (error) {
        failedRows += productSeed.variants.length;
        errors.push(
          `${productSeed.name}: ${error instanceof Error ? error.message : "Import lỗi"}`
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
          })
        )
      );
    }

    const finalVariants = await this.prisma.productVariant.findMany({
      where: { sku: { in: allSkus } },
      select: { id: true, sku: true },
    });

    const finalVariantBySku = new Map(finalVariants.map((variant) => [variant.sku, variant]));
    const finalVariantIds = finalVariants.map((variant) => variant.id);

    if (finalVariantIds.length > 0) {
      for (const chunk of this.chunkArray(finalVariantIds, 1000)) {
        await this.prisma.inventoryItem.deleteMany({
          where: { variantId: { in: chunk } },
        });
      }
    }

    const inventoryRows: Prisma.InventoryItemCreateManyInput[] = [];

    for (const productSeed of grouped.values()) {
      for (const variantSeed of productSeed.variants) {
        const variant = finalVariantBySku.get(variantSeed.sku.trim());
        if (!variant) continue;

        for (const [branchId, qty] of Object.entries(variantSeed.branchStocks || {})) {
          inventoryRows.push({
            variantId: variant.id,
            branchId,
            availableQty: this.toNumber(qty),
            reservedQty: 0,
            incomingQty: 0,
          });
        }
      }
    }

    for (const chunk of this.chunkArray(inventoryRows, 1000)) {
      if (!chunk.length) continue;
      await this.prisma.inventoryItem.createMany({
        data: chunk,
        skipDuplicates: true,
      });
    }

    successRows = finalVariants.length;

    return {
      successRows,
      failedRows,
      errors,
      importedProducts: grouped.size,
      importedVariants: allSkus.length,
      createdVariants: variantsToCreate.length,
      updatedVariants: variantsToUpdate.length,
      inventoryRows: inventoryRows.length,
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
      a.localeCompare(b, "vi")
    );
  }

  async renameCategory(oldName: string, newName: string) {
    const cleanOldName = String(oldName || "").trim();
    const cleanNewName = String(newName || "").trim();

    if (!cleanOldName) {
      throw new BadRequestException("Thiếu danh mục cần đổi");
    }

    if (!cleanNewName) {
      throw new BadRequestException("Thiếu tên danh mục chuẩn");
    }

    const normalizedOld = this.normalizeHeader(cleanOldName);

    const products = await this.prisma.product.findMany({
      where: { category: { not: null } },
      select: { id: true, category: true },
    });

    const matchedIds = products
      .filter((product) => this.normalizeHeader(product.category) === normalizedOld)
      .map((product) => product.id);

    if (!matchedIds.length) {
      return { updated: 0, category: cleanNewName };
    }

    let updated = 0;
    for (const chunk of this.chunkArray(matchedIds, 1000)) {
      const result = await this.prisma.product.updateMany({
        where: { id: { in: chunk } },
        data: { category: cleanNewName },
      });
      updated += result.count;
    }

    try {
      const slug = this.normalizeSlug(cleanNewName);
      const code = slug.replace(/-/g, "_").toUpperCase();

      const existing = await this.prisma.category.findFirst({
        where: { OR: [{ slug }, { name: cleanNewName }] },
        select: { id: true },
      });

      if (existing) {
        await this.prisma.category.update({
          where: { id: existing.id },
          data: { name: cleanNewName, slug, code, isActive: true },
        });
      } else {
        await this.prisma.category.create({
          data: {
            name: cleanNewName,
            slug,
            code,
            description: null,
            isActive: true,
          },
        });
      }
    } catch {
      // Product.category is still updated even if the config category sync fails.
    }

    return { updated, category: cleanNewName };
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
          })
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
      }
    );
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
    ])
  );

  let created = 0;
  let updatedProducts = 0;

  for (const [key, group] of groups.entries()) {
    let category = categoryByKey.get(key);

    if (!category) {
      category = await this.prisma.category.create({
        data: {
          name: group.name,
          code: this.normalizeSlug(group.name).replace(/-/g, "_").toUpperCase(),
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
}
