import {
  BadRequestException,
  ForbiddenException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { InventoryMovementType } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class InventoryService {
  constructor(private readonly prisma: PrismaService) {}

  private isOwner(user?: any) {
    return user?.role === 'owner' || user?.role === 'admin';
  }

  private resolveBranchIdFromUser(user?: any) {
    return user?.branchId || user?.branchName || null;
  }

  private ensureBranchAccess(user: any, branchId?: string | null) {
    if (this.isOwner(user)) return;

    const userBranch = this.resolveBranchIdFromUser(user);

    if (!userBranch) {
      throw new ForbiddenException('Tài khoản chưa được gán chi nhánh.');
    }

    if (branchId && userBranch !== branchId) {
      throw new ForbiddenException('Bạn không có quyền truy cập chi nhánh này.');
    }
  }

  async getInventory(user?: any, branchId?: string) {
    const requestedBranchId = branchId?.trim() || null;

    if (requestedBranchId) {
      this.ensureBranchAccess(user, requestedBranchId);
    }

    const effectiveBranchId = this.isOwner(user)
      ? requestedBranchId
      : this.resolveBranchIdFromUser(user);

    const rows = await this.prisma.inventoryItem.findMany({
      where: effectiveBranchId ? { branchId: effectiveBranchId } : {},
      include: {
        variant: {
          include: {
            product: true,
          },
        },
      },
      orderBy: [{ updatedAt: 'desc' }],
    });

    return rows.map((row) => {
      const variant = (row as any).variant;

      return {
        id: row.id,
        branchId: row.branchId,
        availableQty: Number((row as any).availableQty || 0),
        reservedQty: Number((row as any).reservedQty || 0),
        incomingQty: Number((row as any).incomingQty || 0),
        updatedAt: new Date(row.updatedAt).toLocaleString('vi-VN'),
        variantId: row.variantId,
        sku: variant?.sku || '—',
        color: variant?.color || '',
        size: variant?.size || '',
        productName: variant?.product?.name || '—',
      };
    });
  }

  async adjustInventory(
    body: {
      variantId: string;
      qty: number;
      type: 'IN' | 'OUT' | 'SET';
      note?: string;
      branchId?: string;
    },
    user?: any
  ) {
    const branchId = this.isOwner(user)
      ? body.branchId?.trim() || this.resolveBranchIdFromUser(user)
      : this.resolveBranchIdFromUser(user);

    this.ensureBranchAccess(user, branchId);

    if (!branchId) {
      throw new BadRequestException('Thiếu branchId');
    }

    if (!body.variantId) {
      throw new BadRequestException('Thiếu variantId');
    }

    const qty = Number(body.qty || 0);
    if (!Number.isFinite(qty) || qty <= 0) {
      throw new BadRequestException('Số lượng không hợp lệ');
    }

    const inventory = await this.prisma.inventoryItem.findUnique({
      where: {
        variantId_branchId: {
          variantId: body.variantId,
          branchId,
        },
      },
      include: {
        variant: {
          include: {
            product: true,
          },
        },
      },
    });

    if (!inventory) {
      throw new NotFoundException('Không tìm thấy tồn kho của variant ở chi nhánh này');
    }

    const currentQty = Number((inventory as any).availableQty || 0);
    let nextQty = currentQty;

    if (body.type === 'SET') {
      nextQty = qty;
    } else if (body.type === 'IN') {
      nextQty = currentQty + qty;
    } else if (body.type === 'OUT') {
      nextQty = currentQty - qty;
      if (nextQty < 0) {
        throw new BadRequestException('Tồn kho không đủ');
      }
    } else {
      throw new BadRequestException('Loại điều chỉnh không hợp lệ');
    }

    const movementQty =
      body.type === 'OUT'
        ? -qty
        : body.type === 'SET'
        ? nextQty - currentQty
        : qty;

    const updated = await this.prisma.$transaction(async (tx) => {
      const row = await tx.inventoryItem.update({
        where: {
          variantId_branchId: {
            variantId: body.variantId,
            branchId,
          },
        },
        data: {
          availableQty: nextQty,
        },
        include: {
          variant: {
            include: {
              product: true,
            },
          },
        },
      });

      await tx.inventoryMovement.create({
        data: {
          variantId: body.variantId,
          type: InventoryMovementType.ADJUSTMENT,
          qty: movementQty,
          note: body.note || `Điều chỉnh tồn kho (${body.type})`,
          refType: 'INVENTORY',
          branchId,
        },
      });

      return row;
    });

    const updatedVariant = (updated as any).variant;

    return {
      id: updated.id,
      branchId: updated.branchId,
      availableQty: Number((updated as any).availableQty || 0),
      reservedQty: Number((updated as any).reservedQty || 0),
      incomingQty: Number((updated as any).incomingQty || 0),
      updatedAt: new Date(updated.updatedAt).toLocaleString('vi-VN'),
      variantId: updated.variantId,
      sku: updatedVariant?.sku || '—',
      color: updatedVariant?.color || '',
      size: updatedVariant?.size || '',
      productName: updatedVariant?.product?.name || '—',
    };
  }

  async transferInventory(
    body: {
      variantId: string;
      qty: number;
      fromBranchId: string;
      toBranchId: string;
      note?: string;
    },
    user?: any
  ) {
    if (!body.variantId || !body.fromBranchId || !body.toBranchId) {
      throw new BadRequestException('Thiếu dữ liệu chuyển kho');
    }

    if (body.fromBranchId === body.toBranchId) {
      throw new BadRequestException('Chi nhánh chuyển và nhận không được trùng nhau');
    }

    const qty = Number(body.qty || 0);
    if (!Number.isFinite(qty) || qty <= 0) {
      throw new BadRequestException('Số lượng chuyển không hợp lệ');
    }

    this.ensureBranchAccess(user, body.fromBranchId);

    const fromInventory = await this.prisma.inventoryItem.findUnique({
      where: {
        variantId_branchId: {
          variantId: body.variantId,
          branchId: body.fromBranchId,
        },
      },
      include: {
        variant: {
          include: {
            product: true,
          },
        },
      },
    });

    if (!fromInventory) {
      throw new NotFoundException('Không tìm thấy tồn kho ở chi nhánh chuyển');
    }

    const currentQty = Number((fromInventory as any).availableQty || 0);
    if (currentQty < qty) {
      throw new BadRequestException('Tồn kho không đủ để chuyển');
    }

    const updated = await this.prisma.$transaction(async (tx) => {
      const deductedRow = await tx.inventoryItem.update({
        where: {
          variantId_branchId: {
            variantId: body.variantId,
            branchId: body.fromBranchId,
          },
        },
        data: {
          availableQty: currentQty - qty,
        },
        include: {
          variant: {
            include: {
              product: true,
            },
          },
        },
      });

      await tx.inventoryItem.upsert({
        where: {
          variantId_branchId: {
            variantId: body.variantId,
            branchId: body.toBranchId,
          },
        },
        update: {
          availableQty: {
            increment: qty,
          },
        },
        create: {
          variantId: body.variantId,
          branchId: body.toBranchId,
          availableQty: qty,
          reservedQty: 0,
          incomingQty: 0,
        },
      });

      await tx.inventoryMovement.create({
        data: {
          variantId: body.variantId,
          type: InventoryMovementType.ADJUSTMENT,
          qty: -qty,
          note:
            body.note ||
            `Chuyển kho từ ${body.fromBranchId} sang ${body.toBranchId}`,
          refType: 'INVENTORY_TRANSFER',
          branchId: body.fromBranchId,
        },
      });

      await tx.inventoryMovement.create({
        data: {
          variantId: body.variantId,
          type: InventoryMovementType.ADJUSTMENT,
          qty,
          note:
            body.note ||
            `Nhận kho từ ${body.fromBranchId} sang ${body.toBranchId}`,
          refType: 'INVENTORY_TRANSFER',
          branchId: body.toBranchId,
        },
      });

      return deductedRow;
    });

    const updatedVariant = (updated as any).variant;

    return {
      id: updated.id,
      branchId: updated.branchId,
      availableQty: Number((updated as any).availableQty || 0),
      reservedQty: Number((updated as any).reservedQty || 0),
      incomingQty: Number((updated as any).incomingQty || 0),
      updatedAt: new Date(updated.updatedAt).toLocaleString('vi-VN'),
      variantId: updated.variantId,
      sku: updatedVariant?.sku || '—',
      color: updatedVariant?.color || '',
      size: updatedVariant?.size || '',
      productName: updatedVariant?.product?.name || '—',
    };
  }

  async getInventoryMovements(limit = 100, user?: any) {
    const where = this.isOwner(user)
      ? {}
      : {
          branchId: this.resolveBranchIdFromUser(user) || '__NO_BRANCH__',
        };

    const rows = await this.prisma.inventoryMovement.findMany({
      where,
      orderBy: { createdAt: 'desc' },
      take: limit,
      include: {
        variant: {
          include: {
            product: true,
          },
        },
      },
    });

    return rows.map((row) => {
      const variant = (row as any).variant;

      return {
        id: row.id,
        type: row.type,
        qty: row.qty,
        note: row.note,
        refType: row.refType,
        refId: row.refId,
        branchId: row.branchId,
        createdAt: new Date(row.createdAt).toLocaleString('vi-VN'),
        sku: variant?.sku || '—',
        productName: variant?.product?.name || '—',
        color: variant?.color || '',
        size: variant?.size || '',
      };
    });
  }
}