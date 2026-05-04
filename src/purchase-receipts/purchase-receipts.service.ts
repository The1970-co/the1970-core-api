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

type PayReceiptInput = {
  paymentSourceId?: string | null;
  amount?: number;
  note?: string;
  paidById?: string;
  paidByName?: string;
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

    // PurchaseReceipt.createdById liên kết StaffUser.
    // Admin/Owner thường không nằm trong StaffUser nên không được làm crash phiếu nhập.
    if (!staff || !staff.isActive) return null;

    return staff.id;
  }

  private async resolveInventoryMovementCreatedById(createdById?: string | null) {
    if (!createdById) return null;

    const admin = await this.prisma.adminUser.findUnique({
      where: { id: createdById },
      select: {
        id: true,
        isActive: true,
      },
    });

    // InventoryMovement.createdById liên kết AdminUser.
    // Nếu user hiện tại là StaffUser thì để null để không vỡ FK.
    if (!admin || !admin.isActive) return null;

    return admin.id;
  }

  private getReceiptInclude() {
    return {
      supplier: true,
      branch: true,
      createdBy: true,
      items: {
        orderBy: { createdAt: "asc" as const },
      },
      purchaseReceiptPayments: {
        include: {
          paymentSource: true,
        },
        orderBy: { paidAt: "desc" as const },
      },
    };
  }

  private normalizeItems(items: CreateReceiptItemInput[]) {
    if (!Array.isArray(items) || items.length === 0) {
      throw new BadRequestException("Phiếu nhập phải có ít nhất 1 dòng hàng");
    }

    const normalizedItems = items.map((item) => {
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

    const seen = new Set<string>();

    for (const item of normalizedItems) {
      if (seen.has(item.variantId)) {
        throw new BadRequestException("Một variant đang bị thêm trùng trong phiếu nhập");
      }
      seen.add(item.variantId);
    }

    return normalizedItems;
  }

  private getReceiptTotal(receipt: { items: { lineTotal: Prisma.Decimal | number | string }[] }) {
    return receipt.items.reduce((sum, item) => sum + this.toNumber(item.lineTotal), 0);
  }

  private getPaidTotal(receipt: { purchaseReceiptPayments?: { amount: Prisma.Decimal | number | string }[] }) {
    return (receipt.purchaseReceiptPayments || []).reduce(
      (sum, payment) => sum + this.toNumber(payment.amount),
      0,
    );
  }

  async findAll() {
    return this.prisma.purchaseReceipt.findMany({
      orderBy: { createdAt: "desc" },
      include: this.getReceiptInclude(),
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
          orderBy: { createdAt: "asc" },
        },
        purchaseReceiptPayments: {
          include: {
            paymentSource: true,
          },
          orderBy: { paidAt: "desc" },
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

    const normalizedItems = this.normalizeItems(data.items);
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
        include: this.getReceiptInclude(),
      });
    });
  }

  async updateDraft(id: string, data: UpdateReceiptInput) {
    const receipt = await this.prisma.purchaseReceipt.findUnique({
      where: { id },
      include: {
        items: true,
        purchaseReceiptPayments: true,
      },
    });

    if (!receipt) {
      throw new NotFoundException("Không tìm thấy phiếu nhập");
    }

    if (receipt.status !== PurchaseReceiptStatus.DRAFT) {
      throw new BadRequestException("Chỉ sửa được phiếu đang nháp");
    }

    if (this.getPaidTotal(receipt) > 0) {
      throw new BadRequestException("Phiếu đã thanh toán, không được sửa trực tiếp");
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

    const normalizedItems = Array.isArray(data.items)
      ? this.normalizeItems(data.items)
      : undefined;

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
        include: this.getReceiptInclude(),
      });
    });
  }


  async requestPayment(id: string) {
    const receipt = await this.prisma.purchaseReceipt.findUnique({
      where: { id },
      include: {
        items: true,
        purchaseReceiptPayments: true,
      },
    });

    if (!receipt) {
      throw new NotFoundException("Không tìm thấy phiếu nhập");
    }

    if (receipt.status !== PurchaseReceiptStatus.DRAFT) {
      throw new BadRequestException("Chỉ xác nhận đủ hàng được từ phiếu nháp");
    }

    if (!receipt.items.length) {
      throw new BadRequestException("Phiếu nhập chưa có dòng hàng");
    }

    return this.prisma.purchaseReceipt.update({
      where: { id },
      data: {
        status: PurchaseReceiptStatus.PAYMENT_REQUESTED,
      },
      include: this.getReceiptInclude(),
    });
  }

  async pay(id: string, data: PayReceiptInput = {}) {
    const receipt = await this.prisma.purchaseReceipt.findUnique({
      where: { id },
      include: {
        supplier: true,
        items: true,
        purchaseReceiptPayments: true,
      },
    });

    if (!receipt) {
      throw new NotFoundException("Không tìm thấy phiếu nhập");
    }

    if (
      receipt.status !== PurchaseReceiptStatus.PAYMENT_REQUESTED &&
      receipt.status !== PurchaseReceiptStatus.PARTIALLY_PAID
    ) {
      throw new BadRequestException("Phiếu chưa ở trạng thái chờ thanh toán");
    }

    if (!receipt.items.length) {
      throw new BadRequestException("Phiếu nhập chưa có dòng hàng");
    }

    for (const item of receipt.items) {
      if (this.toNumber(item.unitCost) <= 0) {
        throw new BadRequestException(`SKU ${item.sku} chưa có giá nhập. Phải nhập giá trước khi thanh toán/nhập kho`);
      }
    }

    const totalAmount = this.getReceiptTotal(receipt);
    const paidBefore = this.getPaidTotal(receipt);
    const remainingAmount = Math.max(totalAmount - paidBefore, 0);
    const amount =
      data.amount === undefined || data.amount === null
        ? remainingAmount
        : this.toNumber(data.amount);

    if (!Number.isFinite(amount) || amount <= 0) {
      throw new BadRequestException("Số tiền thanh toán phải lớn hơn 0");
    }

    if (amount > remainingAmount) {
      throw new BadRequestException("Số tiền thanh toán vượt quá số tiền còn phải trả");
    }

    if (!data.paymentSourceId) {
      throw new BadRequestException("Vui lòng chọn nguồn tiền thanh toán");
    }

    const paymentSource = await this.prisma.paymentSource.findUnique({
      where: { id: data.paymentSourceId },
    });

    if (!paymentSource || !paymentSource.isActive) {
      throw new BadRequestException("Nguồn tiền không tồn tại hoặc đã ngừng hoạt động");
    }

    return this.prisma.$transaction(async (tx) => {
      await tx.purchaseReceiptPayment.create({
        data: {
          receiptId: id,
          paymentSourceId: data.paymentSourceId || null,
          amount: new Prisma.Decimal(amount),
          note:
            data.note?.trim() ||
            `Thanh toán nhà cung cấp ${receipt.supplier?.name || ""} cho phiếu ${receipt.receiptCode}`.trim(),
          paidById: data.paidById || null,
          paidByName: data.paidByName || null,
          paidAt: new Date(),
        },
      });

      const paidAfter = paidBefore + amount;
      const nextStatus =
        paidAfter >= totalAmount
          ? PurchaseReceiptStatus.PAID
          : PurchaseReceiptStatus.PARTIALLY_PAID;

      return tx.purchaseReceipt.update({
        where: { id },
        data: {
          status: nextStatus,
        },
        include: this.getReceiptInclude(),
      });
    });
  }

  async importStock(id: string, createdById?: string) {
    const receipt = await this.prisma.purchaseReceipt.findUnique({
      where: { id },
      include: {
        items: true,
        purchaseReceiptPayments: true,
      },
    });

    if (!receipt) {
      throw new NotFoundException("Không tìm thấy phiếu nhập");
    }

    if (receipt.status !== PurchaseReceiptStatus.PAID) {
      throw new BadRequestException("Phiếu chưa thanh toán đủ, không được nhập kho");
    }

    if (!receipt.items.length) {
      throw new BadRequestException("Phiếu nhập chưa có dòng hàng");
    }


    const totalAmount = this.getReceiptTotal(receipt);
    const paidAmount = this.getPaidTotal(receipt);

    if (totalAmount <= 0) {
      throw new BadRequestException("Tổng tiền phiếu nhập phải lớn hơn 0");
    }

    if (paidAmount < totalAmount) {
      throw new BadRequestException("Phiếu chưa thanh toán đủ cho nhà cung cấp, không được nhập kho");
    }

    const validCreatedById = await this.resolveInventoryMovementCreatedById(
      createdById || receipt.createdById,
    );

    return this.prisma.$transaction(async (tx) => {
      for (const item of receipt.items) {
        await tx.inventoryItem.upsert({
          where: {
            variantId_branchId: {
              variantId: item.variantId,
              branchId: receipt.branchId,
            },
          },
          update: {
            availableQty: {
              increment: item.qty,
            },
          },
          create: {
            variantId: item.variantId,
            branchId: receipt.branchId,
            availableQty: item.qty,
            reservedQty: 0,
            incomingQty: 0,
          },
        });

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
        include: this.getReceiptInclude(),
      });
    });
  }

  async complete(id: string) {
    const receipt = await this.prisma.purchaseReceipt.findUnique({
      where: { id },
      include: {
        items: true,
        purchaseReceiptPayments: true,
      },
    });

    if (!receipt) {
      throw new NotFoundException("Không tìm thấy phiếu nhập");
    }

    if (receipt.status !== PurchaseReceiptStatus.STOCK_IMPORTED) {
      throw new BadRequestException("Chỉ hoàn tất được phiếu đã nhập kho");
    }

    const totalAmount = this.getReceiptTotal(receipt);
    const paidAmount = this.getPaidTotal(receipt);

    if (paidAmount < totalAmount) {
      throw new BadRequestException("Phiếu chưa thanh toán đủ, không thể hoàn tất");
    }

    return this.prisma.purchaseReceipt.update({
      where: { id },
      data: {
        status: PurchaseReceiptStatus.COMPLETED,
      },
      include: this.getReceiptInclude(),
    });
  }

  async cancel(id: string) {
    const receipt = await this.prisma.purchaseReceipt.findUnique({
      where: { id },
      include: {
        purchaseReceiptPayments: true,
      },
    });

    if (!receipt) {
      throw new NotFoundException("Không tìm thấy phiếu nhập");
    }

    if (receipt.status !== PurchaseReceiptStatus.DRAFT) {
      throw new BadRequestException("Chỉ hủy được phiếu đang nháp/chưa nhập kho");
    }

    if (this.getPaidTotal(receipt) > 0) {
      throw new BadRequestException("Phiếu đã thanh toán, cần xử lý hoàn tiền trước khi hủy");
    }

    return this.prisma.purchaseReceipt.update({
      where: { id },
      data: { status: PurchaseReceiptStatus.CANCELLED },
      include: this.getReceiptInclude(),
    });
  }
}
