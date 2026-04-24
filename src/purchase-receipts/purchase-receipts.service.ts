import { BadRequestException, Injectable, NotFoundException } from "@nestjs/common";
import {
  InventoryMovementType,
  Prisma,
  PurchaseReceiptStatus,
} from "@prisma/client";
import { PrismaService } from "../prisma/prisma.service";

type CreateReceiptItemInput = {
  variantId: string;
  qty: number;
  unitCost?: number;
};

type CreateReceiptInput = {
  supplierId?: string;
  branchId: string;
  note?: string;
  createdById?: string;
  items: CreateReceiptItemInput[];
};

type UpdateReceiptInput = {
  supplierId?: string | null;
  branchId?: string;
  note?: string;
  items?: CreateReceiptItemInput[];
};

@Injectable()
export class PurchaseReceiptsService {
  constructor(private readonly prisma: PrismaService) {}

  private toNumber(value: unknown) {
    if (typeof value === "number") return value;
    return Number(value || 0);
  }

  private async generateReceiptCode() {
    const count = await this.prisma.purchaseReceipt.count();
    return `PN${String(count + 1).padStart(6, "0")}`;
  }

  private async resolveCreatedById(createdById?: string | null) {
    if (!createdById) return null;

    const staff = await this.prisma.staffUser.findUnique({
      where: { id: createdById },
      select: {
        id: true,
        isActive: true,
      },
    });

    if (!staff || !staff.isActive) {
      throw new BadRequestException("Người tạo phiếu không hợp lệ hoặc đã ngừng hoạt động");
    }

    return staff.id;
  }

  async findAll() {
    return this.prisma.purchaseReceipt.findMany({
      orderBy: { createdAt: "desc" },
      include: {
        supplier: true,
        branch: true,
        createdBy: true,
        items: true,
      },
    });
  }

  async getById(id: string) {
    const receipt = await this.prisma.purchaseReceipt.findUnique({
      where: { id },
      include: {
        supplier: true,
        branch: true,
        createdBy: true,
        items: {
          include: {
            variant: true,
            product: true,
          },
        },
      },
    });

    if (!receipt) {
      throw new NotFoundException("Không tìm thấy phiếu nhập");
    }

    return receipt;
  }

  async create(data: CreateReceiptInput) {
    if (!data.branchId) {
      throw new BadRequestException("Thiếu kho nhập");
    }

    if (!data.supplierId) {
      throw new BadRequestException("Thiếu nhà cung cấp");
    }

    if (!Array.isArray(data.items) || data.items.length === 0) {
      throw new BadRequestException("Phiếu nhập phải có ít nhất 1 dòng hàng");
    }

    const [branch, supplier, validCreatedById] = await Promise.all([
      this.prisma.branch.findUnique({
        where: { id: data.branchId },
      }),
      this.prisma.supplier.findUnique({
        where: { id: data.supplierId },
      }),
      this.resolveCreatedById(data.createdById),
    ]);

    if (!branch) {
      throw new BadRequestException("Kho nhập không tồn tại");
    }

    if (!supplier) {
      throw new BadRequestException("Nhà cung cấp không tồn tại");
    }

    if (!branch.isActive) {
      throw new BadRequestException("Kho nhập đã ngừng hoạt động");
    }

    if (!supplier.isActive) {
      throw new BadRequestException("Nhà cung cấp đã ngừng hoạt động");
    }

    const normalizedItems = data.items.map((item) => {
      const qty = this.toNumber(item.qty);
      const unitCost = this.toNumber(item.unitCost);

      if (!item.variantId) {
        throw new BadRequestException("Có dòng hàng thiếu variantId");
      }

      if (!Number.isFinite(qty) || qty <= 0) {
        throw new BadRequestException("Số lượng nhập phải lớn hơn 0");
      }

      if (!Number.isFinite(unitCost) || unitCost < 0) {
        throw new BadRequestException("Giá nhập không hợp lệ");
      }

      return {
        variantId: item.variantId,
        qty,
        unitCost,
      };
    });

    const duplicatedVariant = normalizedItems.find((item, index) =>
      normalizedItems.some(
        (other, otherIndex) =>
          otherIndex !== index && other.variantId === item.variantId,
      ),
    );

    if (duplicatedVariant) {
      throw new BadRequestException("Một variant đang bị thêm trùng trong phiếu nhập");
    }

    const receiptCode = await this.generateReceiptCode();

    return this.prisma.$transaction(async (tx) => {
      const receipt = await tx.purchaseReceipt.create({
        data: {
          receiptCode,
          supplierId: data.supplierId,
          branchId: data.branchId,
          note: data.note?.trim() || null,
          createdById: validCreatedById,
          status: PurchaseReceiptStatus.DRAFT,
        },
      });

      for (const item of normalizedItems) {
        const variant = await tx.productVariant.findUnique({
          where: { id: item.variantId },
          include: { product: true },
        });

        if (!variant) {
          throw new BadRequestException("Có variant không tồn tại");
        }

        if (!variant.product) {
          throw new BadRequestException(`Variant ${variant.sku} chưa có sản phẩm cha`);
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
            qty: item.qty,
            unitCost: new Prisma.Decimal(item.unitCost),
            lineTotal: new Prisma.Decimal(item.qty * item.unitCost),
          },
        });
      }

      return tx.purchaseReceipt.findUnique({
        where: { id: receipt.id },
        include: {
          supplier: true,
          branch: true,
          createdBy: true,
          items: true,
        },
      });
    });
  }

  async updateDraft(id: string, data: UpdateReceiptInput) {
    const receipt = await this.prisma.purchaseReceipt.findUnique({
      where: { id },
      include: { items: true },
    });

    if (!receipt) {
      throw new NotFoundException("Không tìm thấy phiếu nhập");
    }

    if (receipt.status !== PurchaseReceiptStatus.DRAFT) {
      throw new BadRequestException("Chỉ sửa được phiếu đang nháp");
    }

    if (data.branchId) {
      const branch = await this.prisma.branch.findUnique({
        where: { id: data.branchId },
      });

      if (!branch) {
        throw new BadRequestException("Kho nhập không tồn tại");
      }

      if (!branch.isActive) {
        throw new BadRequestException("Kho nhập đã ngừng hoạt động");
      }
    }

    if (data.supplierId) {
      const supplier = await this.prisma.supplier.findUnique({
        where: { id: data.supplierId },
      });

      if (!supplier) {
        throw new BadRequestException("Nhà cung cấp không tồn tại");
      }

      if (!supplier.isActive) {
        throw new BadRequestException("Nhà cung cấp đã ngừng hoạt động");
      }
    }

    let normalizedItems:
      | {
          variantId: string;
          qty: number;
          unitCost: number;
        }[]
      | undefined;

    if (Array.isArray(data.items)) {
      if (data.items.length === 0) {
        throw new BadRequestException("Phiếu nhập phải có ít nhất 1 dòng hàng");
      }

      normalizedItems = data.items.map((item) => {
        const qty = this.toNumber(item.qty);
        const unitCost = this.toNumber(item.unitCost);

        if (!item.variantId) {
          throw new BadRequestException("Có dòng hàng thiếu variantId");
        }

        if (!Number.isFinite(qty) || qty <= 0) {
          throw new BadRequestException("Số lượng nhập phải lớn hơn 0");
        }

        if (!Number.isFinite(unitCost) || unitCost < 0) {
          throw new BadRequestException("Giá nhập không hợp lệ");
        }

        return {
          variantId: item.variantId,
          qty,
          unitCost,
        };
      });

      const duplicatedVariant = normalizedItems.find((item, index) =>
        normalizedItems!.some(
          (other, otherIndex) =>
            otherIndex !== index && other.variantId === item.variantId,
        ),
      );

      if (duplicatedVariant) {
        throw new BadRequestException("Một variant đang bị thêm trùng trong phiếu nhập");
      }
    }

    return this.prisma.$transaction(async (tx) => {
      await tx.purchaseReceipt.update({
        where: { id },
        data: {
          ...(data.branchId !== undefined ? { branchId: data.branchId } : {}),
          ...(data.supplierId !== undefined
            ? { supplierId: data.supplierId || null }
            : {}),
          ...(data.note !== undefined ? { note: data.note?.trim() || null } : {}),
        },
      });

      if (normalizedItems) {
        await tx.purchaseReceiptItem.deleteMany({
          where: { receiptId: id },
        });

        for (const item of normalizedItems) {
          const variant = await tx.productVariant.findUnique({
            where: { id: item.variantId },
            include: { product: true },
          });

          if (!variant) {
            throw new BadRequestException("Có variant không tồn tại");
          }

          if (!variant.product) {
            throw new BadRequestException(`Variant ${variant.sku} chưa có sản phẩm cha`);
          }

          await tx.purchaseReceiptItem.create({
            data: {
              receiptId: id,
              productId: variant.productId,
              variantId: variant.id,
              sku: variant.sku,
              productName: variant.product.name,
              color: variant.color,
              size: variant.size,
              qty: item.qty,
              unitCost: new Prisma.Decimal(item.unitCost),
              lineTotal: new Prisma.Decimal(item.qty * item.unitCost),
            },
          });
        }
      }

      return tx.purchaseReceipt.findUnique({
        where: { id },
        include: {
          supplier: true,
          branch: true,
          createdBy: true,
          items: true,
        },
      });
    });
  }

  async importStock(id: string, createdById?: string) {
    const receipt = await this.prisma.purchaseReceipt.findUnique({
      where: { id },
      include: { items: true },
    });

    if (!receipt) {
      throw new NotFoundException("Không tìm thấy phiếu nhập");
    }

    if (receipt.status !== PurchaseReceiptStatus.DRAFT) {
      throw new BadRequestException("Chỉ nhập kho được từ phiếu nháp");
    }

    if (!receipt.items.length) {
      throw new BadRequestException("Phiếu nhập chưa có dòng hàng");
    }

    const validCreatedById = await this.resolveCreatedById(
      createdById || receipt.createdById,
    );

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
            note: `Nhập kho từ phiếu ${receipt.receiptCode}`,
            refType: "PURCHASE_RECEIPT",
            refId: receipt.id,
            createdById: validCreatedById,
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
          status: PurchaseReceiptStatus.STOCK_IMPORTED,
          confirmedAt: new Date(),
        },
        include: {
          supplier: true,
          branch: true,
          createdBy: true,
          items: true,
        },
      });
    });
  }

  async complete(id: string) {
    const receipt = await this.prisma.purchaseReceipt.findUnique({
      where: { id },
    });

    if (!receipt) {
      throw new NotFoundException("Không tìm thấy phiếu nhập");
    }

    if (receipt.status !== PurchaseReceiptStatus.STOCK_IMPORTED) {
      throw new BadRequestException("Chỉ hoàn tất được phiếu đã nhập kho");
    }

    return this.prisma.purchaseReceipt.update({
      where: { id },
      data: {
        status: PurchaseReceiptStatus.COMPLETED,
      },
      include: {
        supplier: true,
        branch: true,
        createdBy: true,
        items: true,
      },
    });
  }

  async cancel(id: string) {
    const receipt = await this.prisma.purchaseReceipt.findUnique({
      where: { id },
    });

    if (!receipt) {
      throw new NotFoundException("Không tìm thấy phiếu nhập");
    }

    if (receipt.status !== PurchaseReceiptStatus.DRAFT) {
      throw new BadRequestException("Chỉ hủy được phiếu đang nháp");
    }

    return this.prisma.purchaseReceipt.update({
      where: { id },
      data: { status: PurchaseReceiptStatus.CANCELLED },
      include: {
        supplier: true,
        branch: true,
        createdBy: true,
        items: true,
      },
    });
  }
}