import { BadRequestException, Injectable, NotFoundException } from "@nestjs/common";
import { Prisma, PurchaseReceiptStatus } from "@prisma/client";
import { PrismaService } from "../prisma/prisma.service";

type PaySupplierInput = {
  receiptId: string;
  amount: number;
  paymentSourceId: string;
  note?: string;
  paidById?: string;
  paidByName?: string;
};

type UpdateItemCostInput = {
  itemId: string;
  unitCost: number;
};

@Injectable()
export class SupplierPaymentsService {
  constructor(private readonly prisma: PrismaService) {}

  private toNumber(value: unknown) {
    if (typeof value === "number") return value;
    return Number(value || 0);
  }

  private getReceiptInclude() {
    return {
      supplier: true,
      branch: true,
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
      where: {
        status: {
          in: [
            PurchaseReceiptStatus.PAYMENT_REQUESTED,
            PurchaseReceiptStatus.PARTIALLY_PAID,
            PurchaseReceiptStatus.PAID,
            PurchaseReceiptStatus.STOCK_IMPORTED,
            PurchaseReceiptStatus.COMPLETED,
          ],
        },
      },
      orderBy: { updatedAt: "desc" },
      include: this.getReceiptInclude(),
    });
  }

  async getByReceiptId(receiptId: string) {
    const receipt = await this.prisma.purchaseReceipt.findUnique({
      where: { id: receiptId },
      include: this.getReceiptInclude(),
    });

    if (!receipt) {
      throw new NotFoundException("Không tìm thấy phiếu nhập");
    }

    return receipt;
  }

  async updateItemCosts(receiptId: string, items: UpdateItemCostInput[]) {
    const receipt = await this.prisma.purchaseReceipt.findUnique({
      where: { id: receiptId },
      include: {
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
      throw new BadRequestException("Chỉ cập nhật giá khi phiếu đang chờ thanh toán");
    }

    if (this.getPaidTotal(receipt) > 0) {
      throw new BadRequestException("Phiếu đã có thanh toán, không được sửa giá nhập");
    }

    if (!Array.isArray(items) || !items.length) {
      throw new BadRequestException("Không có dòng giá cần cập nhật");
    }

    const receiptItemIds = new Set(receipt.items.map((item) => item.id));

    return this.prisma.$transaction(async (tx) => {
      for (const item of items) {
        if (!receiptItemIds.has(item.itemId)) {
          throw new BadRequestException("Có dòng hàng không thuộc phiếu này");
        }

        const unitCost = this.toNumber(item.unitCost);

        if (!Number.isFinite(unitCost) || unitCost < 0) {
          throw new BadRequestException("Giá nhập không hợp lệ");
        }

        const current = receipt.items.find((row) => row.id === item.itemId);
        const qty = this.toNumber(current?.qty || 0);

        await tx.purchaseReceiptItem.update({
          where: { id: item.itemId },
          data: {
            unitCost: new Prisma.Decimal(unitCost),
            lineTotal: new Prisma.Decimal(qty * unitCost),
          },
        });
      }

      return tx.purchaseReceipt.findUnique({
        where: { id: receiptId },
        include: this.getReceiptInclude(),
      });
    });
  }

  async pay(data: PaySupplierInput) {
    if (!data.receiptId) {
      throw new BadRequestException("Thiếu phiếu nhập cần thanh toán");
    }

    if (!data.paymentSourceId) {
      throw new BadRequestException("Thiếu nguồn tiền thanh toán");
    }

    const amount = this.toNumber(data.amount);

    if (!Number.isFinite(amount) || amount <= 0) {
      throw new BadRequestException("Số tiền thanh toán phải lớn hơn 0");
    }

    const [receipt, paymentSource] = await Promise.all([
      this.prisma.purchaseReceipt.findUnique({
        where: { id: data.receiptId },
        include: {
          items: true,
          purchaseReceiptPayments: true,
        },
      }),
      this.prisma.paymentSource.findUnique({
        where: { id: data.paymentSourceId },
      }),
    ]);

    if (!receipt) {
      throw new NotFoundException("Không tìm thấy phiếu nhập");
    }

    if (!paymentSource || !paymentSource.isActive) {
      throw new BadRequestException("Nguồn tiền không tồn tại hoặc đã ngừng hoạt động");
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
        throw new BadRequestException(`SKU ${item.sku} chưa có giá nhập. Cần cập nhật giá trước khi thanh toán`);
      }
    }

    const totalAmount = this.getReceiptTotal(receipt);
    const paidBefore = this.getPaidTotal(receipt);
    const remaining = Math.max(totalAmount - paidBefore, 0);

    if (totalAmount <= 0) {
      throw new BadRequestException("Tổng tiền phiếu nhập phải lớn hơn 0");
    }

    if (remaining <= 0) {
      throw new BadRequestException("Phiếu đã thanh toán đủ");
    }

    if (amount > remaining) {
      throw new BadRequestException("Số tiền thanh toán vượt quá số còn phải trả");
    }

    return this.prisma.$transaction(async (tx) => {
      await tx.purchaseReceiptPayment.create({
        data: {
          receiptId: receipt.id,
          paymentSourceId: data.paymentSourceId,
          amount: new Prisma.Decimal(amount),
          note: data.note?.trim() || `Thanh toán NCC cho phiếu ${receipt.receiptCode}`,
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
        where: { id: receipt.id },
        data: {
          status: nextStatus,
        },
        include: this.getReceiptInclude(),
      });
    });
  }
}
