import {
  BadRequestException,
  ForbiddenException,
  Injectable,
} from "@nestjs/common";
import { InventoryMovementType, Prisma } from "@prisma/client";
import { PrismaService } from "../prisma/prisma.service";

@Injectable()
export class ReturnsService {
  constructor(private readonly prisma: PrismaService) { }

  private n(v: any) {
    return Number(v || 0);
  }

  private isOwner(user?: any) {
    return user?.role === "owner" || user?.role === "admin";
  }

  private userBranch(user?: any) {
    return user?.branchId || null;
  }

  private ensureBranch(user: any, branchId?: string | null) {
    if (this.isOwner(user)) return;

    const ub = this.userBranch(user);

    if (!ub) {
      throw new ForbiddenException("Tài khoản chưa được gán chi nhánh.");
    }

    if (branchId && ub !== branchId) {
      throw new ForbiddenException("Không có quyền xử lý phiếu ở chi nhánh này.");
    }
  }

  private code() {
    return `RTN-${Date.now()}`;
  }

  private voucherCode(direction: "IN" | "OUT") {
    return `${direction === "IN" ? "PT" : "PC"}-${Date.now()}`;
  }

  private async ensurePaymentSourceExists(paymentSourceId?: string | null) {
    if (!paymentSourceId) return null;

    const source = await this.prisma.paymentSource.findUnique({
      where: { id: String(paymentSourceId) },
    });

    if (!source) {
      throw new BadRequestException(`Nguồn tiền không tồn tại: ${paymentSourceId}`);
    }

    return source;
  }

  private isReturnableOrder(order: any) {
    const status = String(order?.status || "").toUpperCase();
    const paymentStatus = String(order?.paymentStatus || "").toUpperCase();
    const fulfillmentStatus = String(order?.fulfillmentStatus || "").toUpperCase();

    return (
      status === "COMPLETED" ||
      fulfillmentStatus === "FULFILLED" ||
      (paymentStatus === "PAID" && fulfillmentStatus !== "UNFULFILLED")
    );
  }

  private map(row: any) {
    if (!row) return null;

    return {
      ...row,
      returnAmount: this.n(row.returnAmount),
      exchangeAmount: this.n(row.exchangeAmount),
      differenceAmount: this.n(row.differenceAmount),
      refundAmount: this.n(row.refundAmount),
      extraChargeAmount: this.n(row.extraChargeAmount),
      createdAt: row.createdAt ? new Date(row.createdAt).toLocaleString("vi-VN") : null,
      updatedAt: row.updatedAt ? new Date(row.updatedAt).toLocaleString("vi-VN") : null,
      items: (row.items || []).map((i: any) => ({
        ...i,
        unitPrice: this.n(i.unitPrice),
        refundPrice: this.n(i.refundPrice),
        lineTotal: this.n(i.lineTotal),
      })),
      cashVouchers: (row.cashVouchers || []).map((v: any) => ({
        ...v,
        amount: this.n(v.amount),
        createdAt: v.createdAt ? new Date(v.createdAt).toLocaleString("vi-VN") : null,
        updatedAt: v.updatedAt ? new Date(v.updatedAt).toLocaleString("vi-VN") : null,
      })),
    };
  }

  private async validateReturnQuantity(originalOrderId: string, returnItems: any[]) {
    const orderItemIds = returnItems
      .map((item) => item.orderItemId)
      .filter(Boolean)
      .map(String);

    if (!orderItemIds.length) return;

    const orderItems = await this.prisma.orderItem.findMany({
      where: {
        id: { in: orderItemIds },
        orderId: originalOrderId,
      },
      select: {
        id: true,
        qty: true,
        sku: true,
        productName: true,
      },
    });

    const orderItemMap = new Map(orderItems.map((item) => [item.id, item]));

    for (const item of returnItems) {
      const orderItemId = item.orderItemId ? String(item.orderItemId) : "";
      if (!orderItemId) continue;

      const orderItem = orderItemMap.get(orderItemId);

      if (!orderItem) {
        throw new BadRequestException("Sản phẩm trả không thuộc đơn gốc.");
      }

      const requestedQty = this.n(item.qty);

      if (requestedQty <= 0) {
        throw new BadRequestException("Số lượng trả phải lớn hơn 0.");
      }

      if (requestedQty > Number(orderItem.qty || 0)) {
        throw new BadRequestException(
          `Số lượng trả ${orderItem.sku || orderItem.productName || ""} vượt số lượng đã mua.`
        );
      }
    }

    const previousRows = await this.prisma.returnExchangeItem.findMany({
      where: {
        itemType: "RETURN",
        orderItemId: { in: orderItemIds },
        returnExchange: {
          originalOrderId,
          status: { not: "CANCELLED" },
        },
      },
      select: {
        orderItemId: true,
        qty: true,
      },
    });

    const previousQtyByOrderItemId = previousRows.reduce((acc, row) => {
      if (!row.orderItemId) return acc;
      acc[row.orderItemId] = (acc[row.orderItemId] || 0) + Number(row.qty || 0);
      return acc;
    }, {} as Record<string, number>);

    const requestedQtyByOrderItemId = returnItems.reduce((acc, item) => {
      if (!item.orderItemId) return acc;
      const key = String(item.orderItemId);
      acc[key] = (acc[key] || 0) + this.n(item.qty);
      return acc;
    }, {} as Record<string, number>);

    for (const [orderItemId, requestedQtyRaw] of Object.entries(requestedQtyByOrderItemId)) {
      const requestedQty = Number(requestedQtyRaw || 0);

      const orderItem = orderItemMap.get(orderItemId);
      const purchasedQty = Number(orderItem?.qty || 0);
      const previousQty = Number(previousQtyByOrderItemId[orderItemId] || 0);
      const remainQty = purchasedQty - previousQty;

      if (requestedQty > remainQty) {
        throw new BadRequestException(
          `Sản phẩm ${orderItem?.sku || orderItem?.productName || ""} chỉ còn được trả ${remainQty}.`
        );
      }
    }
  }

  async createReturn(body: any, user?: any) {
    const originalOrderId = String(body?.originalOrderId || "");

    if (!originalOrderId) {
      throw new BadRequestException("Thiếu originalOrderId.");
    }

    const originalOrder = await this.prisma.order.findUnique({
      where: { id: originalOrderId },
      include: {
        payments: {
          include: {
            paymentSource: true,
          },
        },
      },
    });

    if (!originalOrder) {
      throw new BadRequestException("Không tìm thấy đơn hàng gốc.");
    }

    if (!this.isReturnableOrder(originalOrder)) {
      throw new BadRequestException(
        "Chỉ được đổi/trả đơn đã hoàn thành, đã giao hoặc đã thanh toán hợp lệ."
      );
    }

    const receiveBranchId =
      body.returnReceiveBranchId ||
      body.handledAtBranchId ||
      originalOrder.branchId;

    const exchangeIssueBranchId =
      body.exchangeIssueBranchId || receiveBranchId;

    this.ensureBranch(user, receiveBranchId);

    const items = Array.isArray(body.items) ? body.items : [];

    const returnItems = items.filter(
      (x: any) => String(x.itemType || "RETURN") === "RETURN"
    );

    const exchangeItems = items.filter(
      (x: any) => String(x.itemType || "") === "EXCHANGE"
    );

    if (!returnItems.length && !exchangeItems.length) {
      throw new BadRequestException("Chưa có sản phẩm trả/đổi.");
    }

    await this.validateReturnQuantity(originalOrderId, returnItems);

    const returnAmount = returnItems.reduce((sum: number, item: any) => {
      return sum + this.n(item.refundPrice ?? item.unitPrice) * this.n(item.qty);
    }, 0);

    const exchangeAmount = exchangeItems.reduce((sum: number, item: any) => {
      return sum + this.n(item.refundPrice ?? item.unitPrice) * this.n(item.qty);
    }, 0);

    const differenceAmount = returnAmount - exchangeAmount;
    const refundAmount = differenceAmount > 0 ? differenceAmount : 0;
    const extraChargeAmount =
      differenceAmount < 0 ? Math.abs(differenceAmount) : 0;

    if (refundAmount > 0 && !body.refundPaymentSourceId) {
      throw new BadRequestException("Thiếu nguồn tiền hoàn khách.");
    }

    if (extraChargeAmount > 0 && !body.extraChargePaymentSourceId) {
      throw new BadRequestException("Thiếu nguồn tiền khách bù thêm.");
    }

    if (refundAmount > 0) {
      await this.ensurePaymentSourceExists(body.refundPaymentSourceId);
    }

    if (extraChargeAmount > 0) {
      await this.ensurePaymentSourceExists(body.extraChargePaymentSourceId);
    }

    return this.prisma.$transaction(async (tx) => {
      const record = await tx.returnExchange.create({
        data: {
          code: this.code(),
          originalOrderId,
          originalBranchId: originalOrder.branchId || null,
          originalStaffId: originalOrder.createdByStaffId || null,
          originalStaffName: originalOrder.createdByStaffName || null,

          handledByStaffId: user?.id || null,
          handledByStaffName:
            user?.name || user?.code || user?.username || null,

          handledAtBranchId: receiveBranchId,
          returnReceiveBranchId: receiveBranchId,
          exchangeIssueBranchId,

          type: exchangeItems.length ? "RETURN_EXCHANGE" : "RETURN",
          status: body.status || "COMPLETED",

          returnAmount: new Prisma.Decimal(returnAmount),
          exchangeAmount: new Prisma.Decimal(exchangeAmount),
          differenceAmount: new Prisma.Decimal(differenceAmount),

          refundAmount: new Prisma.Decimal(refundAmount),
          extraChargeAmount: new Prisma.Decimal(extraChargeAmount),

          refundPaymentSourceId: body.refundPaymentSourceId || null,
          extraChargePaymentSourceId: body.extraChargePaymentSourceId || null,

          note: body.note || null,

          items: {
            create: items.map((item: any) => ({
              itemType: String(item.itemType || "RETURN"),
              orderItemId: item.orderItemId || null,
              variantId: item.variantId || null,
              sku: item.sku || null,
              productName: item.productName || null,
              qty: this.n(item.qty),
              unitPrice: new Prisma.Decimal(this.n(item.unitPrice)),
              refundPrice: new Prisma.Decimal(
                this.n(item.refundPrice ?? item.unitPrice)
              ),
              lineTotal: new Prisma.Decimal(
                this.n(item.refundPrice ?? item.unitPrice) * this.n(item.qty)
              ),
              reason: item.reason || null,
            })),
          },
        },
      });

      if (record.status === "COMPLETED") {
        for (const item of returnItems) {
          if (!item.variantId || !receiveBranchId || this.n(item.qty) <= 0) {
            continue;
          }

          await tx.inventoryItem.upsert({
            where: {
              variantId_branchId: {
                variantId: item.variantId,
                branchId: receiveBranchId,
              },
            },
            update: {
              availableQty: {
                increment: this.n(item.qty),
              },
            },
            create: {
              variantId: item.variantId,
              branchId: receiveBranchId,
              availableQty: this.n(item.qty),
            },
          });

          await tx.inventoryMovement.create({
            data: {
              variantId: item.variantId,
              branchId: receiveBranchId,
              type: InventoryMovementType.RETURN,
              qty: this.n(item.qty),
              refType: "RETURN_EXCHANGE",
              refId: record.id,
              note: `Nhập hàng trả ${record.code}`,
            },
          });
        }

        for (const item of exchangeItems) {
          if (
            !item.variantId ||
            !exchangeIssueBranchId ||
            this.n(item.qty) <= 0
          ) {
            continue;
          }

          const inv = await tx.inventoryItem.findUnique({
            where: {
              variantId_branchId: {
                variantId: item.variantId,
                branchId: exchangeIssueBranchId,
              },
            },
          });

          if (!inv || Number(inv.availableQty || 0) < this.n(item.qty)) {
            throw new BadRequestException(
              `Không đủ tồn kho sản phẩm đổi ${item.sku || item.productName || ""}.`
            );
          }

          await tx.inventoryItem.update({
            where: {
              variantId_branchId: {
                variantId: item.variantId,
                branchId: exchangeIssueBranchId,
              },
            },
            data: {
              availableQty: {
                decrement: this.n(item.qty),
              },
            },
          });

          await tx.inventoryMovement.create({
            data: {
              variantId: item.variantId,
              branchId: exchangeIssueBranchId,
              type: InventoryMovementType.SALE,
              qty: -this.n(item.qty),
              refType: "RETURN_EXCHANGE",
              refId: record.id,
              note: `Xuất hàng đổi ${record.code}`,
            },
          });
        }
      }

      if (refundAmount > 0) {
        await tx.cashVoucher.create({
          data: {
            code: this.voucherCode("OUT"),
            direction: "OUT",
            voucherType: "RETURN_REFUND",
            amount: new Prisma.Decimal(refundAmount),
            paymentSourceId: body.refundPaymentSourceId,
            branchId: receiveBranchId,
            staffId: user?.id || null,
            staffName: user?.name || user?.code || user?.username || null,
            customerName: originalOrder.customerName || null,
            customerPhone: originalOrder.customerPhone || null,
            refType: "RETURN_EXCHANGE",
            refId: record.id,
            note: `Hoàn tiền phiếu ${record.code} / đơn ${originalOrder.orderCode}`,
          },
        });
      }

      if (extraChargeAmount > 0) {
        await tx.cashVoucher.create({
          data: {
            code: this.voucherCode("IN"),
            direction: "IN",
            voucherType: "RETURN_EXTRA_CHARGE",
            amount: new Prisma.Decimal(extraChargeAmount),
            paymentSourceId: body.extraChargePaymentSourceId,
            branchId: receiveBranchId,
            staffId: user?.id || null,
            staffName: user?.name || user?.code || user?.username || null,
            customerName: originalOrder.customerName || null,
            customerPhone: originalOrder.customerPhone || null,
            refType: "RETURN_EXCHANGE",
            refId: record.id,
            note: `Thu thêm phiếu ${record.code} / đơn ${originalOrder.orderCode}`,
          },
        });
      }

      const full = await tx.returnExchange.findUnique({
        where: { id: record.id },
        include: {
          items: true,
          cashVouchers: true,
        },
      });

      return this.map(full);
    });
  }

  async getReturns(params: any, user?: any) {
    const filters: any[] = [];

    if (params.status && params.status !== "ALL") {
      filters.push({ status: params.status });
    }

    if (params.branchId && params.branchId !== "ALL") {
      filters.push({ returnReceiveBranchId: params.branchId });
    }

    if (params.q) {
      filters.push({
        OR: [
          { code: { contains: params.q, mode: "insensitive" } },
          { note: { contains: params.q, mode: "insensitive" } },
          { originalOrderId: { contains: params.q, mode: "insensitive" } },
        ],
      });
    }

    if (!this.isOwner(user)) {
      const ub = this.userBranch(user) || "__NO_BRANCH__";

      filters.push({
        OR: [
          { handledAtBranchId: ub },
          { returnReceiveBranchId: ub },
          { originalBranchId: ub },
        ],
      });
    }

    const where = filters.length ? { AND: filters } : {};

    const rows = await this.prisma.returnExchange.findMany({
      where,
      orderBy: { createdAt: "desc" },
      take: 100,
      include: {
        items: true,
        cashVouchers: true,
      },
    });

    return {
      data: rows.map((row) => this.map(row)),
    };
  }

  async getReturnById(idOrCode: string, user?: any) {
    const row = await this.prisma.returnExchange.findFirst({
      where: {
        OR: [{ id: idOrCode }, { code: idOrCode }],
      },
      include: {
        items: true,
        cashVouchers: true,
      },
    });

    if (!row) {
      throw new BadRequestException("Không tìm thấy phiếu đổi/trả.");
    }

    if (!this.isOwner(user)) {
      const ub = this.userBranch(user);

      if (
        ub &&
        row.handledAtBranchId !== ub &&
        row.returnReceiveBranchId !== ub &&
        row.originalBranchId !== ub
      ) {
        throw new ForbiddenException("Không có quyền xem phiếu này.");
      }
    }

    return this.map(row);
  }

  async getReturnDetail(idOrCode: string, user?: any) {
    const row = await this.prisma.returnExchange.findFirst({
      where: {
        OR: [{ id: idOrCode }, { code: idOrCode }],
      },
      include: {
        items: true,
        cashVouchers: true,
      },
    });

    if (!row) {
      throw new BadRequestException("Không tìm thấy phiếu đổi/trả.");
    }

    if (!this.isOwner(user)) {
      const ub = this.userBranch(user);

      if (
        ub &&
        row.handledAtBranchId !== ub &&
        row.returnReceiveBranchId !== ub &&
        row.originalBranchId !== ub
      ) {
        throw new ForbiddenException("Không có quyền xem phiếu này.");
      }
    }

    const order = await this.prisma.order.findUnique({
      where: {
        id: row.originalOrderId,
      },
      select: {
        id: true,
        orderCode: true,
        branchId: true,
        customerName: true,
        customerPhone: true,
        createdByStaffId: true,
        createdByStaffName: true,
        soldAt: true,
        createdAt: true,
        finalAmount: true,
        paymentStatus: true,
        fulfillmentStatus: true,
        payments: {
          include: {
            paymentSource: true,
          },
        },
      },
    });

    const mapped = this.map(row);

    return {
      ...mapped,
      originalOrder: order
        ? {
          ...order,
          finalAmount: this.n(order.finalAmount),
          soldAt: order.soldAt
            ? new Date(order.soldAt).toLocaleString("vi-VN")
            : null,
          createdAt: order.createdAt
            ? new Date(order.createdAt).toLocaleString("vi-VN")
            : null,
          payments: (order.payments || []).map((payment: any) => ({
            ...payment,
            amount: this.n(payment.amount),
            sourceName: payment.paymentSource?.name || payment.method || null,
          })),
        }
        : null,
    };
  }

  async searchOrdersForReturn(q: string, user?: any) {
    const keyword = String(q || "").trim();

    if (keyword.length < 2) {
      return [];
    }

    const rows = await this.prisma.order.findMany({
      where: {
        OR: [
          { orderCode: { contains: keyword, mode: "insensitive" } },
          { customerName: { contains: keyword, mode: "insensitive" } },
          { customerPhone: { contains: keyword, mode: "insensitive" } },
          {
            items: {
              some: {
                sku: { contains: keyword, mode: "insensitive" },
              },
            },
          },
        ],
      },
      orderBy: { createdAt: "desc" },
      take: 20,
      include: {
        items: true,
        payments: {
          include: {
            paymentSource: true,
          },
        },
      },
    });

    return rows.map((order: any) => ({
      id: order.id,
      orderCode: order.orderCode,
      customerName: order.customerName,
      customerPhone: order.customerPhone,

      branchId: order.branchId,
      createdByStaffId: order.createdByStaffId,
      createdByStaffName: order.createdByStaffName,
      soldAt: order.soldAt
        ? new Date(order.soldAt).toLocaleString("vi-VN")
        : null,

      finalAmount: this.n(order.finalAmount),
      paymentStatus: order.paymentStatus,
      fulfillmentStatus: order.fulfillmentStatus,
      isReturnable: this.isReturnableOrder(order),

      items: order.items.map((item: any) => ({
        id: item.id,
        variantId: item.variantId,
        sku: item.sku,
        productName: item.productName,
        color: item.color,
        size: item.size,
        qty: item.qty,
        unitPrice: this.n(item.unitPrice),
        lineTotal: this.n(item.lineTotal),
      })),

      payments: order.payments.map((payment: any) => ({
        id: payment.id,
        amount: this.n(payment.amount),
        method: payment.method,
        sourceName: payment.paymentSource?.name || payment.method,
        paymentSourceId: payment.paymentSourceId,
      })),
    }));
  }
}
