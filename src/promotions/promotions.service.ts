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
      include: {
        branch: true,
        products: {
          include: {
            product: true,
          },
        },
      },
    });
  }

  async findOne(id: string) {
    const promotion = await this.prisma.promotion.findUnique({
      where: { id },
      include: {
        branch: true,
        products: {
          include: {
            product: true,
          },
        },
      },
    });

    if (!promotion) {
      throw new NotFoundException("Không tìm thấy khuyến mại");
    }

    return promotion;
  }

  async create(dto: CreatePromotionDto) {
    this.validatePromotion(dto);

    const productIds = Array.from(new Set(dto.productIds ?? [])).filter(Boolean);

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
              ? productIds.map((productId) => ({
                  product: {
                    connect: { id: productId },
                  },
                }))
              : [],
        },
      },
      include: {
        branch: true,
        products: {
          include: {
            product: true,
          },
        },
      },
    });
  }

  async update(id: string, dto: UpdatePromotionDto) {
    const existing = await this.findOne(id);

    const nextType = dto.type ?? existing.type;
    const nextProductIds =
      dto.productIds ?? existing.products.map((row) => row.productId);

    this.validatePromotion({
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
      productIds: nextProductIds,
    });

    return this.prisma.$transaction(async (tx) => {
      if (dto.productIds !== undefined || dto.type === PromotionType.ORDER_DISCOUNT) {
        await tx.promotionProduct.deleteMany({
          where: { promotionId: id },
        });
      }

      const shouldCreateProducts =
        (dto.productIds !== undefined || dto.type === PromotionType.PRODUCT_DISCOUNT) &&
        nextType === PromotionType.PRODUCT_DISCOUNT;

      const uniqueProductIds = Array.from(new Set(nextProductIds)).filter(Boolean);

      return tx.promotion.update({
        where: { id },
        data: {
          name: dto.name !== undefined ? dto.name.trim() : undefined,
          type: dto.type,
          status: dto.status,
          discountType: dto.discountType,
          discountValue: dto.discountValue,
          minOrderAmount:
            dto.type === PromotionType.PRODUCT_DISCOUNT
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
                create: uniqueProductIds.map((productId) => ({
                  product: {
                    connect: { id: productId },
                  },
                })),
              }
            : undefined,
        },
        include: {
          branch: true,
          products: {
            include: {
              product: true,
            },
          },
        },
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

  private validatePromotion(dto: Partial<CreatePromotionDto>) {
    if (!dto.name || !String(dto.name).trim()) {
      throw new BadRequestException("Thiếu tên khuyến mại");
    }

    if (
      dto.type === PromotionType.PRODUCT_DISCOUNT &&
      (!dto.productIds || dto.productIds.length === 0)
    ) {
      throw new BadRequestException(
        "Khuyến mại sản phẩm phải chọn ít nhất 1 sản phẩm"
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
        "Ngày bắt đầu không được lớn hơn ngày kết thúc"
      );
    }
  }
}
