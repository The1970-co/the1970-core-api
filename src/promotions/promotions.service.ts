import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from "@nestjs/common";
import {
  PromotionStatus,
  PromotionType,
  SalesChannel,
} from "@prisma/client";
import { PrismaService } from "../prisma/prisma.service";
import { CreatePromotionDto } from "./dto/create-promotion.dto";
import { UpdatePromotionDto } from "./dto/update-promotion.dto";

@Injectable()
export class PromotionsService {
  constructor(private readonly prisma: PrismaService) {}

  async findAll() {
    return this.prisma.promotion.findMany({
      orderBy: [{ status: "asc" }, { priority: "desc" }, { createdAt: "desc" }],
      include: this.promotionInclude(),
    });
  }

  async findOne(id: string) {
    const promotion = await this.prisma.promotion.findUnique({
      where: { id },
      include: this.promotionInclude(),
    });

    if (!promotion) {
      throw new NotFoundException("Không tìm thấy khuyến mại");
    }

    return promotion;
  }

  async create(dto: CreatePromotionDto) {
    const targets = await this.normalizeAndValidateTargets(dto);
    this.validatePromotion(dto, targets);

    return this.prisma.promotion.create({
      data: {
        name: dto.name.trim(),
        type: dto.type,
        status: dto.status ?? PromotionStatus.ACTIVE,
        discountType: dto.discountType,
        discountValue: dto.discountValue,
        minOrderAmount:
          dto.type === PromotionType.ORDER_DISCOUNT
            ? dto.minOrderAmount || 0
            : null,
        branchId: dto.branchId || null,
        salesChannel: dto.salesChannel || null,
        startAt: dto.startAt ? new Date(dto.startAt) : null,
        endAt: dto.endAt ? new Date(dto.endAt) : null,
        priority: dto.priority ?? 0,
        note: dto.note || null,
        products: {
          create:
            dto.type === PromotionType.PRODUCT_DISCOUNT
              ? this.buildPromotionProductCreates(targets)
              : [],
        },
      },
      include: this.promotionInclude(),
    });
  }

  async update(id: string, dto: UpdatePromotionDto) {
    const existing = await this.findOne(id);

    const nextType = dto.type ?? existing.type;
    const nextProductIds =
      dto.productIds ??
      existing.products
        .map((row) => row.productId)
        .filter(Boolean) as string[];
    const nextVariantIds =
      dto.variantIds ??
      existing.products
        .map((row) => row.variantId)
        .filter(Boolean) as string[];

    const targets = await this.normalizeAndValidateTargets({
      productIds: nextProductIds,
      variantIds: nextVariantIds,
    });

    this.validatePromotion(
      {
        name: dto.name ?? existing.name,
        type: nextType,
        status: dto.status ?? existing.status,
        discountType: dto.discountType ?? existing.discountType,
        discountValue:
          dto.discountValue !== undefined
            ? dto.discountValue
            : Number(existing.discountValue),
        minOrderAmount:
          dto.minOrderAmount !== undefined
            ? dto.minOrderAmount
            : existing.minOrderAmount
              ? Number(existing.minOrderAmount)
              : undefined,
        branchId: dto.branchId ?? existing.branchId ?? undefined,
        salesChannel: dto.salesChannel ?? existing.salesChannel ?? undefined,
        startAt:
          dto.startAt !== undefined
            ? dto.startAt
            : existing.startAt
              ? existing.startAt.toISOString()
              : undefined,
        endAt:
          dto.endAt !== undefined
            ? dto.endAt
            : existing.endAt
              ? existing.endAt.toISOString()
              : undefined,
        priority: dto.priority ?? existing.priority,
        note: dto.note ?? existing.note ?? undefined,
        productIds: targets.productIds,
        variantIds: targets.variantIds,
      },
      targets,
    );

    return this.prisma.$transaction(async (tx) => {
      const targetsChanged = dto.productIds !== undefined || dto.variantIds !== undefined;
      if (targetsChanged || dto.type === PromotionType.ORDER_DISCOUNT) {
        await tx.promotionProduct.deleteMany({
          where: { promotionId: id },
        });
      }

      const shouldCreateProducts =
        (targetsChanged || dto.type === PromotionType.PRODUCT_DISCOUNT) &&
        nextType === PromotionType.PRODUCT_DISCOUNT;

      return tx.promotion.update({
        where: { id },
        data: {
          name: dto.name !== undefined ? dto.name.trim() : undefined,
          type: dto.type,
          status: dto.status,
          discountType: dto.discountType,
          discountValue: dto.discountValue,
          minOrderAmount:
            nextType === PromotionType.PRODUCT_DISCOUNT
              ? null
              : dto.minOrderAmount,
          branchId: dto.branchId === undefined ? undefined : dto.branchId || null,
          salesChannel:
            dto.salesChannel === undefined ? undefined : dto.salesChannel || null,
          startAt:
            dto.startAt === undefined
              ? undefined
              : dto.startAt
                ? new Date(dto.startAt)
                : null,
          endAt:
            dto.endAt === undefined
              ? undefined
              : dto.endAt
                ? new Date(dto.endAt)
                : null,
          priority: dto.priority,
          note: dto.note,
          products: shouldCreateProducts
            ? {
                create: this.buildPromotionProductCreates(targets),
              }
            : undefined,
        },
        include: this.promotionInclude(),
      });
    });
  }

  async remove(id: string) {
    await this.findOne(id);

    return this.prisma.promotion.delete({
      where: { id },
    });
  }

  async getActivePromotions(input: {
    branchId?: string | null;
    salesChannel?: SalesChannel | null;
  }) {
    const now = new Date();

    return this.prisma.promotion.findMany({
      where: {
        status: PromotionStatus.ACTIVE,
        AND: [
          {
            OR: [{ startAt: null }, { startAt: { lte: now } }],
          },
          {
            OR: [{ endAt: null }, { endAt: { gte: now } }],
          },
          {
            OR: [{ branchId: null }, { branchId: input.branchId ?? undefined }],
          },
          {
            OR: [
              { salesChannel: null },
              { salesChannel: input.salesChannel ?? undefined },
            ],
          },
        ],
      },
      include: {
        products: true,
      },
      orderBy: [{ priority: "desc" }, { createdAt: "desc" }],
    });
  }

  private promotionInclude() {
    return {
      branch: true,
      products: {
        include: {
          product: true,
          variant: {
            include: {
              product: true,
            },
          },
        },
      },
    } as const;
  }

  private buildPromotionProductCreates(targets: {
    productIds: string[];
    variantIds: string[];
  }) {
    return [
      ...targets.productIds.map((productId) => ({
        product: {
          connect: { id: productId },
        },
      })),
      ...targets.variantIds.map((variantId) => ({
        variant: {
          connect: { id: variantId },
        },
      })),
    ];
  }

  private async normalizeAndValidateTargets(dto: {
    productIds?: string[];
    variantIds?: string[];
  }) {
    const rawProductIds = Array.from(new Set(dto.productIds ?? []))
      .map((value) => String(value || "").trim())
      .filter(Boolean);
    const rawVariantIds = Array.from(new Set(dto.variantIds ?? []))
      .map((value) => String(value || "").trim())
      .filter(Boolean);

    // Chống lỗi UI cũ gửi nhầm SKU con vào productIds: backend sẽ tự nhận diện
    // id/sku của ProductVariant rồi chuyển sang variantIds để không còn crash 500.
    const variantLookupKeys = Array.from(new Set([...rawProductIds, ...rawVariantIds]));
    const variants = variantLookupKeys.length
      ? await this.prisma.productVariant.findMany({
          where: {
            OR: [{ id: { in: variantLookupKeys } }, { sku: { in: variantLookupKeys } }],
          },
          select: { id: true, productId: true, sku: true },
        })
      : [];

    const variantByInput = new Map<string, (typeof variants)[number]>();
    for (const variant of variants) {
      variantByInput.set(variant.id, variant);
      variantByInput.set(variant.sku, variant);
    }

    const productIds = rawProductIds.filter((idOrSku) => !variantByInput.has(idOrSku));
    const variantIds = Array.from(
      new Set(
        [...rawVariantIds, ...rawProductIds]
          .map((idOrSku) => variantByInput.get(idOrSku)?.id)
          .filter(Boolean) as string[],
      ),
    );

    const missingVariants = rawVariantIds.filter((idOrSku) => !variantByInput.has(idOrSku));
    if (missingVariants.length) {
      throw new BadRequestException(
        `Không tìm thấy SKU con: ${missingVariants.join(", ")}`,
      );
    }

    if (productIds.length) {
      const products = await this.prisma.product.findMany({
        where: { id: { in: productIds } },
        select: { id: true },
      });
      const foundProductIds = new Set(products.map((product) => product.id));
      const missingProducts = productIds.filter((id) => !foundProductIds.has(id));
      if (missingProducts.length) {
        throw new BadRequestException(
          `Không tìm thấy mã chính: ${missingProducts.join(", ")}`,
        );
      }
    }

    const selectedVariants = variantIds
      .map((id) => variants.find((variant) => variant.id === id))
      .filter(Boolean) as typeof variants;
    const productIdSet = new Set(productIds);
    const conflictSkus = selectedVariants.filter((variant) => productIdSet.has(variant.productId));
    if (conflictSkus.length) {
      throw new BadRequestException(
        "Không được chọn đồng thời mã chính và SKU con của cùng một sản phẩm",
      );
    }

    return { productIds, variantIds };
  }

  private validatePromotion(
    dto: Partial<CreatePromotionDto>,
    targets?: { productIds: string[]; variantIds: string[] },
  ) {
    if (!dto.name || !String(dto.name).trim()) {
      throw new BadRequestException("Thiếu tên khuyến mại");
    }

    const totalTargetCount =
      (targets?.productIds?.length ?? dto.productIds?.length ?? 0) +
      (targets?.variantIds?.length ?? dto.variantIds?.length ?? 0);

    if (dto.type === PromotionType.PRODUCT_DISCOUNT && totalTargetCount === 0) {
      throw new BadRequestException(
        "Khuyến mại sản phẩm phải chọn ít nhất 1 mã chính hoặc SKU con",
      );
    }

    if (dto.discountValue !== undefined && Number(dto.discountValue) <= 0) {
      throw new BadRequestException("Giá trị khuyến mại phải lớn hơn 0");
    }

    if (
      dto.type === PromotionType.ORDER_DISCOUNT &&
      Number(dto.minOrderAmount || 0) < 0
    ) {
      throw new BadRequestException("Giá trị đơn tối thiểu không hợp lệ");
    }

    if (dto.startAt && dto.endAt && new Date(dto.startAt) > new Date(dto.endAt)) {
      throw new BadRequestException(
        "Ngày bắt đầu không được lớn hơn ngày kết thúc",
      );
    }
  }
}
