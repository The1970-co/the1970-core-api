import { Injectable } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

type CreateVariantInput = {
  sku: string;
  color?: string;
  size?: string;
  priceVnd: number;
  priceUsd?: number;
  cost?: number;
  weightGram?: number;
  status?: 'ACTIVE' | 'INACTIVE';
  stock?: number;
};

type CreateProductInput = {
  name: string;
  slug: string;
  description?: string;
  category?: string;
  brand?: string;
  status?: 'ACTIVE' | 'INACTIVE' | 'DRAFT';
  variants?: CreateVariantInput[];
};

@Injectable()
export class ProductService {
  constructor(private readonly prisma: PrismaService) {}

  async createProduct(data: CreateProductInput) {
    const variants = data.variants ?? [];

    return this.prisma.product.create({
      data: {
        name: data.name,
        slug: data.slug,
        description: data.description,
        category: data.category,
        brand: data.brand ?? 'The 1970',
        status: data.status ?? 'DRAFT',
        variants: {
          create: variants.map((v) => ({
            sku: v.sku,
            color: v.color,
            size: v.size,
            priceVnd: v.priceVnd,
            priceUsd: v.priceUsd,
            cost: v.cost,
            weightGram: v.weightGram,
            status: v.status ?? 'ACTIVE',
            inventoryItem: {
              create: {
                availableQty: v.stock ?? 0,
                reservedQty: 0,
                incomingQty: 0,
              },
            },
          })),
        },
      },
      include: {
        variants: {
          include: {
            inventoryItem: true,
          },
        },
      },
    });
  }

  async getProducts() {
    return this.prisma.product.findMany({
      orderBy: { createdAt: 'desc' },
      include: {
        variants: {
          include: {
            inventoryItem: true,
          },
        },
      },
    });
  }

  async getProductById(id: string) {
    return this.prisma.product.findUnique({
      where: { id },
      include: {
        variants: {
          include: {
            inventoryItem: true,
          },
        },
      },
    });
  }
}