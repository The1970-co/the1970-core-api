import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import {
  InventoryMovementType,
  Prisma,
  PurchaseReceiptStatus,
} from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';

type CreateReceiptItemInput = {
  variantId: string;
  qty: number;
  unitCost: number;
};

type CreateReceiptInput = {
  supplierId: string;
  branchId: string;
  note?: string;
  createdById?: string;
  items: CreateReceiptItemInput[];
};

@Injectable()
export class PurchaseReceiptsService {
  constructor(private readonly prisma: PrismaService) {}

  private toNumber(value: unknown) {
    if (typeof value === 'number') return value;
    return Number(value || 0);
  }

  private async generateReceiptCode() {
    const count = await this.prisma.purchaseReceipt.count();
    return `PN${String(count + 1).padStart(6, '0')}`;
  }

  async findAll() {
    return this.prisma.purchaseReceipt.findMany({
      orderBy: { createdAt: 'desc' },
      include: {
        supplier: true,
        branch: true,
        items: true,
      },
    });
  }

  async getById(id: string) {
    return this.prisma.purchaseReceipt.findUnique({
      where: { id },
      include: {
        supplier: true,
        branch: true,
        items: {
          include: {
            variant: true,
            product: true,
          },
        },
      },
    });
  }

  async create(data: CreateReceiptInput) {
    if (!data.supplierId) {
      throw new BadRequestException('Thiếu nhà cung cấp');
    }

    if (!data.branchId) {
      throw new BadRequestException('Thiếu kho nhập');
    }

    if (!Array.isArray(data.items) || !data.items.length) {
      throw new BadRequestException('Phiếu nhập phải có ít nhất 1 dòng hàng');
    }

    const supplier = await this.prisma.supplier.findUnique({
      where: { id: data.supplierId },
    });

    if (!supplier) {
      throw new BadRequestException('Nhà cung cấp không tồn tại');
    }

    const branch = await this.prisma.branch.findUnique({
      where: { id: data.branchId },
    });

    if (!branch) {
      throw new BadRequestException('Kho nhập không tồn tại');
    }

    const receiptCode = await this.generateReceiptCode();

    return this.prisma.$transaction(async (tx) => {
      const receipt = await tx.purchaseReceipt.create({
        data: {
          receiptCode,
          supplierId: data.supplierId,
          branchId: data.branchId,
          note: data.note?.trim() || null,
          createdById: data.createdById || null,
          status: PurchaseReceiptStatus.DRAFT,
        },
      });

      for (const item of data.items) {
        const variant = await tx.productVariant.findUnique({
          where: { id: item.variantId },
          include: { product: true },
        });

        if (!variant) {
          throw new BadRequestException('Có variant không tồn tại');
        }

        const qty = this.toNumber(item.qty);
        const unitCost = this.toNumber(item.unitCost);

        if (qty <= 0) {
          throw new BadRequestException('Số lượng nhập phải lớn hơn 0');
        }

        await tx.purchaseReceiptItem.create({
          data: {
            receiptId: receipt.id,
            productId: variant.productId,
            variantId: variant.id,
            sku: variant.sku,
            productName: variant.product.name,
            color: variant.color,
            size: variant.size,
            qty,
            unitCost: new Prisma.Decimal(unitCost),
            lineTotal: new Prisma.Decimal(qty * unitCost),
          },
        });
      }

      return tx.purchaseReceipt.findUnique({
        where: { id: receipt.id },
        include: {
          supplier: true,
          branch: true,
          items: true,
        },
      });
    });
  }

  async confirm(id: string, createdById?: string) {
    const receipt = await this.prisma.purchaseReceipt.findUnique({
      where: { id },
      include: { items: true },
    });

    if (!receipt) {
      throw new NotFoundException('Không tìm thấy phiếu nhập');
    }

    if (receipt.status !== PurchaseReceiptStatus.DRAFT) {
      throw new BadRequestException('Chỉ xác nhận được phiếu đang nháp');
    }

    return this.prisma.$transaction(async (tx) => {
      for (const item of receipt.items) {
        const inventory = await tx.inventoryItem.findUnique({
          where: {
            variantId_branchId: {
              variantId: item.variantId,
              branchId: receipt.branchId,
            },
          },
        });

        if (inventory) {
          await tx.inventoryItem.update({
            where: {
              variantId_branchId: {
                variantId: item.variantId,
                branchId: receipt.branchId,
              },
            },
            data: {
              availableQty: inventory.availableQty + item.qty,
            },
          });
        } else {
          await tx.inventoryItem.create({
            data: {
              variantId: item.variantId,
              branchId: receipt.branchId,
              availableQty: item.qty,
              reservedQty: 0,
              incomingQty: 0,
            },
          });
        }

        await tx.inventoryMovement.create({
          data: {
            variantId: item.variantId,
            type: InventoryMovementType.IMPORT,
            qty: item.qty,
            note: `Nhập hàng từ phiếu ${receipt.receiptCode}`,
            refType: 'PURCHASE_RECEIPT',
            refId: receipt.id,
            createdById: createdById || receipt.createdById || null,
            branchId: receipt.branchId,
          },
        });

        await tx.productVariant.update({
          where: { id: item.variantId },
          data: {
            costPrice: item.unitCost,
          },
        });
      }

      return tx.purchaseReceipt.update({
        where: { id: receipt.id },
        data: {
          status: PurchaseReceiptStatus.CONFIRMED,
          confirmedAt: new Date(),
        },
        include: {
          supplier: true,
          branch: true,
          items: true,
        },
      });
    });
  }

  async cancel(id: string) {
    const receipt = await this.prisma.purchaseReceipt.findUnique({
      where: { id },
    });

    if (!receipt) {
      throw new NotFoundException('Không tìm thấy phiếu nhập');
    }

    if (receipt.status !== PurchaseReceiptStatus.DRAFT) {
      throw new BadRequestException('Chỉ hủy được phiếu đang nháp');
    }

    return this.prisma.purchaseReceipt.update({
      where: { id },
      data: { status: PurchaseReceiptStatus.CANCELLED },
    });
  }
}