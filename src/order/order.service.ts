import {
  BadRequestException,
  ForbiddenException,
  Injectable,
} from "@nestjs/common";
import {
  FulfillmentStatus,
  InventoryMovementType,
  OrderStatus,
  PaymentStatus,
  Prisma,
  PrismaClient,
  SalesChannel,
} from "@prisma/client";
import { PrismaService } from "../prisma/prisma.service";
import { ShipmentService } from "../shipment/shipment.service";

type TxClient = Omit<
  PrismaClient,
  "$connect" | "$disconnect" | "$on" | "$transaction" | "$use" | "$extends"
>;

type CreateOrderMode = "draft" | "approve" | "ship";

type GetOrdersParams = {
  page?: number;
  pageSize?: number;
  q?: string;
  branchId?: string;
  orderStatus?: string;
  paymentStatus?: string;
  dateFrom?: string;
  dateTo?: string;
};

@Injectable()
export class OrderService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly shipmentService: ShipmentService
  ) { }

  private toNumber(value: unknown) {
    if (typeof value === "number") return value;
    return Number(value || 0);
  }

  private normalizePhone(phone?: string | null) {
    if (!phone) return null;
    const cleaned = String(phone).replace(/\D/g, "").trim();
    return cleaned || null;
  }

  private isOwner(user?: any) {
    return user?.role === "owner" || user?.role === "admin";
  }

  private resolveBranchIdFromUser(user?: any) {
    return user?.branchId || null;
  }

  private ensureBranchAccess(user: any, branchId?: string | null) {
    if (this.isOwner(user)) return;

    const userBranch = this.resolveBranchIdFromUser(user);

    if (!userBranch) {
      throw new ForbiddenException("Tài khoản chưa được gán chi nhánh.");
    }

    if (branchId && userBranch !== branchId) {
      throw new ForbiddenException("Bạn không có quyền truy cập chi nhánh này.");
    }
  }

  private buildOrderWhereByUser(user: any, extraWhere?: Prisma.OrderWhereInput) {
    if (this.isOwner(user)) {
      return extraWhere || {};
    }

    const userBranch = this.resolveBranchIdFromUser(user);

    if (!userBranch) {
      return {
        ...extraWhere,
        id: "__NO_ACCESS__",
      } satisfies Prisma.OrderWhereInput;
    }

    return {
      ...extraWhere,
      branchId: userBranch,
    } satisfies Prisma.OrderWhereInput;
  }

  private resolveMode(body: any): CreateOrderMode {
    const mode = String(body?.mode || "draft");
    if (mode === "approve" || mode === "ship") return mode;
    return "draft";
  }

  private getModeConfig(mode: CreateOrderMode) {
    if (mode === "draft") {
      return {
        status: OrderStatus.NEW,
        fulfillmentStatus: FulfillmentStatus.UNFULFILLED,
        deductStockNow: false,
      };
    }

    if (mode === "approve") {
      return {
        status: OrderStatus.APPROVED,
        fulfillmentStatus: FulfillmentStatus.UNFULFILLED,
        deductStockNow: false,
      };
    }

    return {
      status: OrderStatus.APPROVED,
      fulfillmentStatus: FulfillmentStatus.PROCESSING,
      deductStockNow: true,
    };
  }

  private ensureShipModePayload(body: any, branchId?: string | null) {
    const snapshot = body?.shippingSnapshot;

    if (!branchId) {
      throw new BadRequestException("Thiếu chi nhánh xuất kho.");
    }

    if (!snapshot) {
      throw new BadRequestException("Thiếu shippingSnapshot để xuất kho.");
    }

    if (!snapshot.shippingAddressLine1) {
      throw new BadRequestException("Thiếu địa chỉ giao hàng.");
    }

    if (!snapshot.shippingRecipientName) {
      throw new BadRequestException("Thiếu tên người nhận.");
    }

    if (!snapshot.shippingPhone) {
      throw new BadRequestException("Thiếu số điện thoại người nhận.");
    }

    if (!snapshot.ghnDistrictId || !snapshot.ghnWardCode) {
      throw new BadRequestException(
        "Địa chỉ chưa có mã GHN (ghnDistrictId / ghnWardCode)."
      );
    }
  }

  private async logInventoryMovement(
    tx: TxClient,
    input: {
      variantId: string;
      type: InventoryMovementType;
      qty: number;
      note?: string;
      refType?: string;
      refId?: string;
      branchId?: string;
    }
  ) {
    await tx.inventoryMovement.create({
      data: {
        variantId: input.variantId,
        type: input.type,
        qty: input.qty,
        note: input.note || null,
        refType: input.refType || null,
        refId: input.refId || null,
        branchId: input.branchId || null,
      },
    });
  }

  private async deductStockForItems(
    tx: TxClient,
    items: Array<{ variantId: string; quantity: number }>,
    orderRefId: string,
    branchId?: string | null
  ) {
    if (!branchId) {
      throw new BadRequestException("Thiếu branchId để trừ kho");
    }

    for (const item of items) {
      const variant = await tx.productVariant.findUnique({
        where: { id: item.variantId },
        include: {
          product: true,
          inventoryItems: {
            where: { branchId },
          },
        },
      });

      if (!variant) {
        throw new BadRequestException(`Variant không tồn tại: ${item.variantId}`);
      }

      const inventory = variant.inventoryItems[0];

      if (!inventory) {
        throw new BadRequestException(
          `Variant ${variant.sku} chưa có tồn kho ở chi nhánh ${branchId}`
        );
      }

      const availableQty = Number(inventory.availableQty || 0);
      const neededQty = Number(item.quantity || 0);

      if (neededQty <= 0) {
        throw new BadRequestException(`Số lượng không hợp lệ cho ${variant.sku}`);
      }

      if (availableQty < neededQty) {
        throw new BadRequestException(
          `Không đủ tồn kho cho ${variant.sku}. Còn ${availableQty}, cần ${neededQty}`
        );
      }

      await tx.inventoryItem.update({
        where: {
          variantId_branchId: {
            variantId: item.variantId,
            branchId,
          },
        },
        data: {
          availableQty: {
            decrement: neededQty,
          },
        },
      });

      await this.logInventoryMovement(tx, {
        variantId: item.variantId,
        type: InventoryMovementType.SALE,
        qty: -neededQty,
        note: "Trừ kho khi xuất đơn",
        refType: "ORDER",
        refId: orderRefId,
        branchId,
      });
    }
  }

  private async restoreStockForOrder(
    tx: TxClient,
    orderId: string,
    branchId?: string | null
  ) {
    if (!branchId) return;

    const items = await tx.orderItem.findMany({
      where: { orderId },
      select: {
        variantId: true,
        qty: true,
      },
    });

    for (const item of items) {
      if (!item.variantId) continue;

      const inventory = await tx.inventoryItem.findUnique({
        where: {
          variantId_branchId: {
            variantId: item.variantId,
            branchId,
          },
        },
      });

      if (!inventory) continue;

      const qty = Number(item.qty || 0);

      await tx.inventoryItem.update({
        where: {
          variantId_branchId: {
            variantId: item.variantId,
            branchId,
          },
        },
        data: {
          availableQty: {
            increment: qty,
          },
        },
      });

      await this.logInventoryMovement(tx, {
        variantId: item.variantId,
        type: InventoryMovementType.CANCEL,
        qty,
        note: "Hoàn kho khi hủy đơn",
        refType: "ORDER",
        refId: orderId,
        branchId,
      });
    }
  }

  private mapOrderResponse(order: any) {
    return {
      ...order,
      totalAmount: this.toNumber(order.totalAmount),
      discountAmount: this.toNumber(order.discountAmount),
      shippingFee: this.toNumber(order.shippingFee),
      finalAmount: this.toNumber(order.finalAmount),
      createdAt: new Date(order.createdAt).toLocaleString("vi-VN"),
      updatedAt: new Date(order.updatedAt).toLocaleString("vi-VN"),
      customerName: order.customerName || order.customer?.fullName || "Khách lẻ",
      customerPhone: order.customerPhone || order.customer?.phone || "—",
      items: Array.isArray(order.items)
        ? order.items.map((item: any) => ({
          ...item,
          unitPrice: this.toNumber(item.unitPrice),
          lineTotal: this.toNumber(item.lineTotal),
        }))
        : [],
      shipment: order.shipment
        ? {
          ...order.shipment,
          shippingFee: this.toNumber(order.shipment.shippingFee),
          codAmount: this.toNumber(order.shipment.codAmount),
        }
        : null,
    };
  }

  private async createShipmentIfNeeded(order: any, body: any) {
    const mode = this.resolveMode(body);
    if (mode !== "ship") return null;

    const snapshot = body?.shippingSnapshot;
    if (!snapshot) return null;

    const items = Array.isArray(order?.items) ? order.items : [];

    const ghnItems = items.map((item: any) => ({
      name: item.productName || item.sku || "Sản phẩm",
      quantity: Number(item.qty || 0),
      price: this.toNumber(item.unitPrice),
      length: 20,
      width: 20,
      height: 5,
      weight: 200,
      category: "Thời trang",
    }));

    return this.shipmentService.createGhnShipment(order.id, {
      clientOrderCode: order.orderCode,
      toName: snapshot.shippingRecipientName,
      toPhone: snapshot.shippingPhone,
      toAddress: snapshot.shippingAddressLine1,
      toWardCode: snapshot.ghnWardCode,
      toDistrictId: Number(snapshot.ghnDistrictId),
      codAmount: this.toNumber(order.finalAmount),
      content: `Đơn hàng ${order.orderCode}`,
      weight: 500,
      length: 20,
      width: 20,
      height: 5,
      insuranceValue: this.toNumber(order.finalAmount),
      items: ghnItems,
    });
  }

  async createOrder(body: any, user?: any) {
    const mode = this.resolveMode(body);
    const modeConfig = this.getModeConfig(mode);

    const items = Array.isArray(body.items) ? body.items : [];

    if (!items.length) {
      throw new BadRequestException("Đơn hàng phải có ít nhất 1 sản phẩm");
    }

    const normalizedItems = items.map((item) => ({
      variantId: String(item.variantId),
      quantity: Number(item.quantity ?? item.qty ?? 0),
    }));

    const createdOrder = await this.prisma.$transaction(
      async (tx) => {
        const customerPhone = this.normalizePhone(body.customerPhone);
        const customerName = String(body.customerName || "").trim();
        const salesChannel = String(
          body.salesChannel || "SHOWROOM"
        ) as SalesChannel;

        let branchId = body.branchId ? String(body.branchId).trim() : null;

        if (!this.isOwner(user)) {
          branchId = this.resolveBranchIdFromUser(user);
        }

        this.ensureBranchAccess(user, branchId);

        if (mode === "ship") {
          this.ensureShipModePayload(body, branchId);
        }

        let customerId: string | null =
          body.customerId ? String(body.customerId) : null;

        if (!customerId && customerPhone && customerName) {
          let customer = await tx.customer.findFirst({
            where: { phone: customerPhone },
          });

          if (!customer) {
            customer = await tx.customer.create({
              data: {
                fullName: customerName,
                phone: customerPhone,
                source: salesChannel,
              },
            });
          }

          customerId = customer.id;
        }

        let totalAmount = 0;
        const preparedItems: Array<{
          variantId: string;
          sku: string;
          productName: string;
          color: string | null;
          size: string | null;
          qty: number;
          unitPrice: Prisma.Decimal;
          lineTotal: Prisma.Decimal;
        }> = [];

        for (const item of normalizedItems) {
          if (!branchId) {
            throw new BadRequestException("Thiếu branchId khi chuẩn bị đơn hàng");
          }

          const variant = await tx.productVariant.findUnique({
            where: { id: item.variantId },
            include: {
              product: true,
              inventoryItems: {
                where: { branchId },
              },
            },
          });

          if (!variant) {
            throw new BadRequestException(`Variant không tồn tại: ${item.variantId}`);
          }

          const qty = Number(item.quantity || 0);

          if (qty <= 0) {
            throw new BadRequestException(`Số lượng không hợp lệ cho ${variant.sku}`);
          }

          const unitPriceNumber = this.toNumber(variant.price);
          const lineTotalNumber = qty * unitPriceNumber;

          totalAmount += lineTotalNumber;

          preparedItems.push({
            variantId: variant.id,
            sku: variant.sku,
            productName: variant.product?.name || "Unknown Product",
            color: variant.color || null,
            size: variant.size || null,
            qty,
            unitPrice: new Prisma.Decimal(unitPriceNumber),
            lineTotal: new Prisma.Decimal(lineTotalNumber),
          });
        }

        const discountAmount = new Prisma.Decimal(0);
        const shippingFee = new Prisma.Decimal(0);
        const finalAmount = new Prisma.Decimal(totalAmount);

        const order = await tx.order.create({
          data: {
            orderCode: `ORD-${Date.now()}`,
            salesChannel,
            customerId,
            customerName: customerName || null,
            customerPhone: customerPhone || null,
            branchId,
            currency: "VND",

            totalAmount: new Prisma.Decimal(totalAmount),
            discountAmount,
            shippingFee,
            finalAmount,

            paymentStatus: PaymentStatus.UNPAID,
            fulfillmentStatus: modeConfig.fulfillmentStatus,
            status: modeConfig.status,
            note: body.note || null,

            customerAddressId: body?.shippingSnapshot?.shippingAddressId || null,
            shippingRecipientName:
              body?.shippingSnapshot?.shippingRecipientName || null,
            shippingPhone: body?.shippingSnapshot?.shippingPhone || null,
            shippingAddressLine1:
              body?.shippingSnapshot?.shippingAddressLine1 || null,
            shippingAddressLine2:
              body?.shippingSnapshot?.shippingAddressLine2 || null,
            shippingWard: body?.shippingSnapshot?.shippingWard || null,
            shippingDistrict: body?.shippingSnapshot?.shippingDistrict || null,
            shippingCity: body?.shippingSnapshot?.shippingCity || null,
            shippingProvince: body?.shippingSnapshot?.shippingProvince || null,
            shippingCountry: body?.shippingSnapshot?.shippingCountry || null,
            shippingPostalCode:
              body?.shippingSnapshot?.shippingPostalCode || null,
            shippingGhnDistrictId:
              body?.shippingSnapshot?.ghnDistrictId || null,
            shippingGhnWardCode:
              body?.shippingSnapshot?.ghnWardCode || null,

            items: {
              create: preparedItems,
            },
          },
          include: {
            items: true,
            shipment: true,
            customer: {
              select: {
                id: true,
                fullName: true,
                phone: true,
              },
            },
          },
        });

        if (modeConfig.deductStockNow) {
          await this.deductStockForItems(tx, normalizedItems, order.id, branchId);
        }
        // =========================
        // CREATE PAYMENT (NEW)
        // =========================

        const paymentSourceId = body.paymentSourceId
          ? String(body.paymentSourceId)
          : null;

        const paidAmount = Number(body.paidAmount || 0);
        const finalAmountNumber = Number(totalAmount);

        let paymentStatus: PaymentStatus = PaymentStatus.UNPAID;

        // PARTIAL
        if (paidAmount > 0 && paidAmount < finalAmountNumber) {
          paymentStatus = PaymentStatus.PARTIAL;
        }

        // FULL PAID
        if (paidAmount >= finalAmountNumber && finalAmountNumber > 0) {
          paymentStatus = PaymentStatus.PAID;
        }

        // check source
        const paymentSource = paymentSourceId
          ? await tx.paymentSource.findUnique({
            where: { id: paymentSourceId },
          })
          : null;

        // COD override
        if (paymentSource?.type === "COD") {
          paymentStatus = PaymentStatus.PENDING_COD;
        }

        // tạo payment
        if (paymentSourceId || paidAmount > 0) {
          await tx.payment.create({
            data: {
              orderId: order.id,
              amount: new Prisma.Decimal(paidAmount || finalAmountNumber),
              status: paymentStatus,
              method: paymentSource?.name || "Manual",
              paymentSourceId,
              note: body.paymentNote || null,
              paidAt:
                paymentStatus === PaymentStatus.PAID ? new Date() : null,
            },
          });
        }

        // update order paymentStatus
        await tx.order.update({
          where: { id: order.id },
          data: {
            paymentStatus,
          },
        });
        if (customerId) {
          await tx.customer.update({
            where: { id: customerId },
            data: {
              totalOrders: {
                increment: 1,
              },
              totalSpent: {
                increment: new Prisma.Decimal(totalAmount),
              },
              lastOrderAt: new Date(),
            },
          });
        }

        return order;
      },
      {
        maxWait: 10000,
        timeout: 20000,
      }
    );

    if (mode === "ship") {
      await this.createShipmentIfNeeded(createdOrder, body);
    }

    return this.mapOrderResponse(createdOrder);
  }

  async getOrders(params: GetOrdersParams = {}, user?: any) {
    const {
      page = 1,
      pageSize = 50,
      q = "",
      branchId = "",
      orderStatus = "",
      paymentStatus = "",
      dateFrom = "",
      dateTo = "",
    } = params;

    const safePage = Math.max(Number(page || 1), 1);
    const safePageSize = Math.min(Math.max(Number(pageSize || 50), 1), 100);
    const skip = (safePage - 1) * safePageSize;

    const extraWhere: Prisma.OrderWhereInput = {};

    if (branchId && branchId !== "ALL") {
      extraWhere.branchId = branchId;
    }

    if (orderStatus && orderStatus !== "ALL") {
      extraWhere.status = orderStatus as OrderStatus;
    }

    if (paymentStatus && paymentStatus !== "ALL") {
      extraWhere.paymentStatus = paymentStatus as PaymentStatus;
    }

    if (dateFrom || dateTo) {
      extraWhere.createdAt = {};
      if (dateFrom) {
        (extraWhere.createdAt as Prisma.DateTimeFilter).gte = new Date(
          `${dateFrom}T00:00:00.000Z`
        );
      }
      if (dateTo) {
        (extraWhere.createdAt as Prisma.DateTimeFilter).lte = new Date(
          `${dateTo}T23:59:59.999Z`
        );
      }
    }

    if (q.trim()) {
      const keyword = q.trim();
      extraWhere.OR = [
        { orderCode: { contains: keyword, mode: "insensitive" } },
        { customerName: { contains: keyword, mode: "insensitive" } },
        { customerPhone: { contains: keyword, mode: "insensitive" } },
        { salesChannel: { equals: keyword as SalesChannel } },
        { note: { contains: keyword, mode: "insensitive" } },
      ];
    }

    const where = this.buildOrderWhereByUser(user, extraWhere);

    const [orders, total] = await Promise.all([
      this.prisma.order.findMany({
        where,
        orderBy: { createdAt: "desc" },
        skip,
        take: safePageSize,
        select: {
          id: true,
          orderCode: true,
          salesChannel: true,
          customerName: true,
          customerPhone: true,
          branchId: true,
          totalAmount: true,
          discountAmount: true,
          shippingFee: true,
          finalAmount: true,
          paymentStatus: true,
          fulfillmentStatus: true,
          status: true,
          note: true,
          createdAt: true,
          updatedAt: true,

          items: {
            select: {
              id: true,
            },
          },

          // ✅ THÊM ĐOẠN NÀY
          shipment: {
            select: {
              id: true,
              carrier: true,
              trackingCode: true,
              shippingStatus: true,
              codAmount: true,
              shippingFee: true,
            },
          },
        },
      }),
      this.prisma.order.count({ where }),
    ]);

const data = orders.map((order) => ({
  ...order,
  totalAmount: this.toNumber(order.totalAmount),
  discountAmount: this.toNumber(order.discountAmount),
  shippingFee: this.toNumber(order.shippingFee),
  finalAmount: this.toNumber(order.finalAmount),
  createdAt: new Date(order.createdAt).toLocaleString("vi-VN"),
  updatedAt: new Date(order.updatedAt).toLocaleString("vi-VN"),
  items: [],

  shipment: order.shipment
    ? {
        ...order.shipment,
        shippingFee: this.toNumber(order.shipment.shippingFee),
        codAmount: this.toNumber(order.shipment.codAmount),
      }
    : null,
}));

    return {
      data,
      pagination: {
        page: safePage,
        pageSize: safePageSize,
        total,
        totalPages: Math.ceil(total / safePageSize),
      },
    };
  }

  async getOrderById(idOrCode: string, user?: any) {
    const order = await this.prisma.order.findFirst({
      where: {
        OR: [{ id: idOrCode }, { orderCode: idOrCode }],
      },
      include: {
        items: true,
        shipment: true,
        customer: {
          select: {
            id: true,
            fullName: true,
            phone: true,
          },
        },
      },
    });

    if (!order) {
      throw new BadRequestException("Không tìm thấy đơn hàng");
    }

    this.ensureBranchAccess(user, order.branchId);

    return this.mapOrderResponse(order);
  }

  async updateOrder(orderId: string, body: any, user?: any) {
    const existing = await this.prisma.order.findFirst({
      where: this.buildOrderWhereByUser(user, { id: orderId }),
      include: {
        items: true,
        shipment: true,
        customer: {
          select: {
            id: true,
            fullName: true,
            phone: true,
          },
        },
      },
    });

    if (!existing) {
      throw new BadRequestException("Không tìm thấy đơn hàng");
    }

    if (existing.status === OrderStatus.CANCELLED) {
      throw new BadRequestException("Đơn đã hủy, không thể sửa.");
    }

    const items = Array.isArray(body.items) ? body.items : null;

    const updated = await this.prisma.$transaction(async (tx) => {
      if (items) {
        await tx.orderItem.deleteMany({
          where: { orderId },
        });

        if (items.length) {
          await tx.orderItem.createMany({
            data: items.map((item: any) => ({
              orderId,
              variantId: item.variantId ? String(item.variantId) : null,
              productName: String(item.productName || ""),
              sku: String(item.sku || ""),
              color: item.color ? String(item.color) : null,
              size: item.size ? String(item.size) : null,
              qty: Number(item.qty || 0),
              unitPrice: new Prisma.Decimal(this.toNumber(item.unitPrice)),
              lineTotal: new Prisma.Decimal(this.toNumber(item.lineTotal)),
            })),
          });
        }
      }

      await tx.order.update({
        where: { id: orderId },
        data: {
          customerName:
            typeof body.customerName === "string"
              ? body.customerName.trim()
              : undefined,
          customerPhone:
            typeof body.customerPhone === "string"
              ? this.normalizePhone(body.customerPhone)
              : undefined,

          salesChannel:
            typeof body.salesChannel === "string"
              ? (body.salesChannel as SalesChannel)
              : undefined,

          note: typeof body.note === "string" ? body.note : undefined,

          shippingRecipientName:
            typeof body.shippingRecipientName === "string"
              ? body.shippingRecipientName
              : undefined,
          shippingPhone:
            typeof body.shippingPhone === "string"
              ? body.shippingPhone
              : undefined,
          shippingEmail:
            typeof body.shippingEmail === "string"
              ? body.shippingEmail
              : undefined,

          shippingAddressLine1:
            typeof body.shippingAddressLine1 === "string"
              ? body.shippingAddressLine1
              : undefined,
          shippingAddressLine2:
            typeof body.shippingAddressLine2 === "string"
              ? body.shippingAddressLine2
              : undefined,
          shippingWard:
            typeof body.shippingWard === "string" ? body.shippingWard : undefined,
          shippingDistrict:
            typeof body.shippingDistrict === "string"
              ? body.shippingDistrict
              : undefined,
          shippingProvince:
            typeof body.shippingProvince === "string"
              ? body.shippingProvince
              : undefined,
          shippingPostalCode:
            typeof body.shippingPostalCode === "string"
              ? body.shippingPostalCode
              : undefined,

          discountAmount:
            body.discountAmount !== undefined
              ? new Prisma.Decimal(this.toNumber(body.discountAmount))
              : undefined,
          shippingFee:
            body.shippingFee !== undefined
              ? new Prisma.Decimal(this.toNumber(body.shippingFee))
              : undefined,
          totalAmount:
            body.totalAmount !== undefined
              ? new Prisma.Decimal(this.toNumber(body.totalAmount))
              : undefined,
          finalAmount:
            body.finalAmount !== undefined
              ? new Prisma.Decimal(this.toNumber(body.finalAmount))
              : undefined,
        },
      });

      return tx.order.findUnique({
        where: { id: orderId },
        include: {
          items: true,
          shipment: true,
          customer: {
            select: {
              id: true,
              fullName: true,
              phone: true,
            },
          },
        },
      });
    });

    if (!updated) {
      throw new BadRequestException("Không cập nhật được đơn hàng");
    }

    return this.mapOrderResponse(updated);
  }
  async updateOrderStatus(orderId: string, status: OrderStatus, user?: any) {
    return this.prisma.$transaction(
      async (tx) => {
        const order = await tx.order.findFirst({
          where: this.buildOrderWhereByUser(user, { id: orderId }),
          include: {
            items: true,
            shipment: true,
            customer: {
              select: {
                id: true,
                fullName: true,
                phone: true,
              },
            },
          },
        });

        if (!order) {
          throw new BadRequestException("Không tìm thấy đơn hàng");
        }

        const currentStatus = order.status;
        const nextStatus = status;

        if (
          currentStatus !== OrderStatus.CANCELLED &&
          nextStatus === OrderStatus.CANCELLED
        ) {
          await this.restoreStockForOrder(tx, order.id, order.branchId || null);
        }

        const updated = await tx.order.update({
          where: { id: orderId },
          data: {
            status: nextStatus,
            fulfillmentStatus:
              nextStatus === OrderStatus.APPROVED
                ? FulfillmentStatus.UNFULFILLED
                : nextStatus === OrderStatus.PACKING
                  ? FulfillmentStatus.PROCESSING
                  : nextStatus === OrderStatus.SHIPPED
                    ? FulfillmentStatus.FULFILLED
                    : nextStatus === OrderStatus.COMPLETED
                      ? FulfillmentStatus.FULFILLED
                      : undefined,
          },
          include: {
            items: true,
            shipment: true,
            customer: {
              select: {
                id: true,
                fullName: true,
                phone: true,
              },
            },
          },
        });

        return this.mapOrderResponse(updated);
      },
      {
        maxWait: 10000,
        timeout: 20000,
      }
    );
  }

  async updatePaymentStatus(
    orderId: string,
    paymentStatus: PaymentStatus,
    user?: any
  ) {
    const existing = await this.prisma.order.findFirst({
      where: this.buildOrderWhereByUser(user, { id: orderId }),
      select: { id: true },
    });

    if (!existing) {
      throw new BadRequestException("Không tìm thấy đơn hàng");
    }

    const updated = await this.prisma.order.update({
      where: { id: orderId },
      data: {
        paymentStatus,
      },
      include: {
        items: true,
        shipment: true,
        customer: {
          select: {
            id: true,
            fullName: true,
            phone: true,
          },
        },
      },
    });

    return this.mapOrderResponse(updated);
  }
  async shipOrder(
    id: string,
    body: { weight: number; shippingFee?: number; note?: string },
    user?: any
  ) {
    const order = await this.prisma.order.findFirst({
      where: this.buildOrderWhereByUser(user, { id }),
      include: {
        items: true,
        shipment: true,
      },
    });

    if (!order) {
      throw new BadRequestException("Không tìm thấy đơn hàng.");
    }

    if (order.status !== OrderStatus.PACKING) {
      throw new BadRequestException(
        "Chỉ đơn ở trạng thái Đang xử lý mới được gửi hàng."
      );
    }

    const shippingFee = Number(body?.shippingFee || 30000);
    const trackingCode = `SHIP-${Date.now()}`;

    await this.prisma.shipment.upsert({
      where: { orderId: id },
      update: {
        carrier: "GHN",
        trackingCode,
        shippingStatus: "SHIPPED",
        shippingFee: new Prisma.Decimal(shippingFee),
        codAmount: new Prisma.Decimal(this.toNumber(order.finalAmount)),
      },
      create: {
        orderId: id,
        carrier: "GHN",
        trackingCode,
        shippingStatus: "SHIPPED",
        shippingFee: new Prisma.Decimal(shippingFee),
        codAmount: new Prisma.Decimal(this.toNumber(order.finalAmount)),
      },
    });

    await this.prisma.order.update({
      where: { id },
      data: {
        status: OrderStatus.SHIPPED,
        fulfillmentStatus: FulfillmentStatus.FULFILLED,
        shippingFee: new Prisma.Decimal(shippingFee),
        note: body?.note
          ? `${order.note ? order.note + " | " : ""}Ghi chú giao hàng: ${body.note}`
          : order.note,
      },
    });

    return {
      success: true,
      trackingCode,
      shippingFee,
      weight: Number(body?.weight || 0),
    };
  }

  async getInventoryMovements(limit = 100, user?: any) {
    const where = this.isOwner(user)
      ? {}
      : {
        branchId: this.resolveBranchIdFromUser(user) || "__NO_BRANCH__",
      };

    const rows = await this.prisma.inventoryMovement.findMany({
      where,
      orderBy: { createdAt: "desc" },
      take: limit,
      include: {
        variant: {
          include: {
            product: true,
          },
        },
      },
    });

    return rows.map((row) => ({
      id: row.id,
      type: row.type,
      qty: row.qty,
      note: row.note,
      refType: row.refType,
      refId: row.refId,
      branchId: row.branchId,
      createdAt: new Date(row.createdAt).toLocaleString("vi-VN"),
      sku: row.variant?.sku || "—",
      productName: row.variant?.product?.name || "—",
      color: row.variant?.color || "",
      size: row.variant?.size || "",
    }));
  }
}