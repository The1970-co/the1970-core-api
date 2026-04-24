import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

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
      orderBy: [{ sortOrder: 'asc' }, { createdAt: 'desc' }],
      include: {
        _count: {
          select: { products: true },
        },
      },
    });
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
      throw new NotFoundException('Không tìm thấy danh mục.');
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
      throw new NotFoundException('Không tìm thấy danh mục.');
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
      throw new NotFoundException('Không tìm thấy danh mục.');
    }

    if ((existing._count?.products || 0) > 0) {
      throw new BadRequestException(
        'Danh mục đang có sản phẩm, không thể xoá.'
      );
    }

    return this.prisma.category.delete({
      where: { id },
    });
  }
}