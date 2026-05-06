import { Injectable } from "@nestjs/common";
import { PrismaService } from "../../prisma/prisma.service";

@Injectable()
export class MobileProductsService {
  constructor(private prisma: PrismaService) {}

  async getProducts(input: {
    q?: string;
    branchId?: string;
    status?: string;
    take?: number;
  }) {
    const q = input.q?.trim();
    const take = Math.min(Math.max(input.take || 50, 1), 100);

    const where: any = {};

    if (input.status && input.status !== "all") {
      where.status = input.status;
    }

    if (q) {
      where.OR = [
        {
          name: {
            contains: q,
            mode: "insensitive",
          },
        },
        {
          variants: {
            some: {
              sku: {
                contains: q,
                mode: "insensitive",
              },
            },
          },
        },
      ];
    }

    const products = await this.prisma.product.findMany({
      where,
      select: {
        id: true,
        name: true,
        slug: true,
        category: true,
        productType: true,
        brand: true,
        imageUrl: true,
        status: true,
        createdAt: true,
        variants: {
          select: {
            id: true,
            sku: true,
            color: true,
            size: true,
            price: true,
            compareAtPrice: true,
            costPrice: true,
            status: true,
            inventoryItems: {
              select: {
                branchId: true,
                availableQty: true,
                reservedQty: true,
                incomingQty: true,
              },
            },
          },
          orderBy: [{ color: "asc" }, { size: "asc" }],
        },
      },
      orderBy: {
        updatedAt: "desc",
      },
      take,
    });

    return products.map((product) => {
      let totalAvailable = 0;
      let totalReserved = 0;
      let totalIncoming = 0;
      let variantCount = 0;
      const prices: number[] = [];

      const variants = product.variants.map((variant) => {
        const inventoryItems =
          input.branchId && input.branchId !== "all"
            ? variant.inventoryItems.filter((item) => item.branchId === input.branchId)
            : variant.inventoryItems;

        const availableQty = inventoryItems.reduce(
          (sum, item) => sum + (item.availableQty || 0),
          0
        );
        const reservedQty = inventoryItems.reduce(
          (sum, item) => sum + (item.reservedQty || 0),
          0
        );
        const incomingQty = inventoryItems.reduce(
          (sum, item) => sum + (item.incomingQty || 0),
          0
        );

        totalAvailable += availableQty;
        totalReserved += reservedQty;
        totalIncoming += incomingQty;
        variantCount += 1;
        prices.push(Number(variant.price || 0));

        return {
          id: variant.id,
          sku: variant.sku,
          color: variant.color,
          size: variant.size,
          status: variant.status,
          price: Number(variant.price || 0),
          compareAtPrice: Number(variant.compareAtPrice || 0),
          costPrice: Number(variant.costPrice || 0),
          availableQty,
          reservedQty,
          incomingQty,
          branches: inventoryItems.map((item) => ({
            branchId: item.branchId,
            availableQty: item.availableQty,
            reservedQty: item.reservedQty,
            incomingQty: item.incomingQty,
          })),
        };
      });

      return {
        id: product.id,
        name: product.name,
        slug: product.slug,
        category: product.category,
        productType: product.productType,
        brand: product.brand,
        imageUrl: product.imageUrl,
        status: product.status,
        createdAt: product.createdAt,
        variantCount,
        totalAvailable,
        totalReserved,
        totalIncoming,
        minPrice: prices.length ? Math.min(...prices) : 0,
        maxPrice: prices.length ? Math.max(...prices) : 0,
        variants,
      };
    });
  }
}
