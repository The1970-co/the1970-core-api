import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

type CreateSupplierInput = {
  name: string;
  code: string;
  phone?: string;
  email?: string;
  address?: string;
  note?: string;
};

type UpdateSupplierInput = {
  name?: string;
  code?: string;
  phone?: string;
  email?: string;
  address?: string;
  note?: string;
  isActive?: boolean;
};

@Injectable()
export class SuppliersService {
  constructor(private readonly prisma: PrismaService) {}

  async findAll() {
    return this.prisma.supplier.findMany({
      orderBy: [{ isActive: 'desc' }, { createdAt: 'desc' }],
      include: {
        _count: {
          select: { receipts: true },
        },
      },
    });
  }

  async create(data: CreateSupplierInput) {
    if (!data.name?.trim()) {
      throw new BadRequestException('Thiếu tên nhà cung cấp');
    }

    if (!data.code?.trim()) {
      throw new BadRequestException('Thiếu mã nhà cung cấp');
    }

    const code = data.code.trim().toUpperCase();

    const exists = await this.prisma.supplier.findUnique({
      where: { code },
    });

    if (exists) {
      throw new BadRequestException('Mã nhà cung cấp đã tồn tại');
    }

    return this.prisma.supplier.create({
      data: {
        name: data.name.trim(),
        code,
        phone: data.phone?.trim() || null,
        email: data.email?.trim() || null,
        address: data.address?.trim() || null,
        note: data.note?.trim() || null,
      },
    });
  }

  async update(id: string, data: UpdateSupplierInput) {
    const existing = await this.prisma.supplier.findUnique({
      where: { id },
    });

    if (!existing) {
      throw new NotFoundException('Không tìm thấy nhà cung cấp');
    }

    return this.prisma.supplier.update({
      where: { id },
      data: {
        ...(data.name !== undefined ? { name: data.name.trim() } : {}),
        ...(data.code !== undefined ? { code: data.code.trim().toUpperCase() } : {}),
        ...(data.phone !== undefined ? { phone: data.phone?.trim() || null } : {}),
        ...(data.email !== undefined ? { email: data.email?.trim() || null } : {}),
        ...(data.address !== undefined ? { address: data.address?.trim() || null } : {}),
        ...(data.note !== undefined ? { note: data.note?.trim() || null } : {}),
        ...(data.isActive !== undefined ? { isActive: data.isActive } : {}),
      },
    });
  }

  async toggle(id: string) {
    const existing = await this.prisma.supplier.findUnique({
      where: { id },
    });

    if (!existing) {
      throw new NotFoundException('Không tìm thấy nhà cung cấp');
    }

    return this.prisma.supplier.update({
      where: { id },
      data: { isActive: !existing.isActive },
    });
  }
}