import {
  BadRequestException,
  ForbiddenException,
  Injectable,
  NotFoundException,
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
import { PromotionsService } from "../promotions/promotions.service";
import { PromotionEngineService } from "../promotions/promotion-engine.service";

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
    private readonly shipmentService: ShipmentService,
    private readonly promotionsService: PromotionsService,
    private readonly promotionEngine: PromotionEngineService
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

  private getOrderPermissionKeys(user?: any, branchId?: string | null) {
    const keys = new Set<string>();

    const addKeys = (items?: any[]) => {
      if (!Array.isArray(items)) return;
      items.forEach((permission) => {
        if (permission) keys.add(String(permission));
      });
    };

    addKeys(user?.permissions);
    addKeys(user?.permissionKeys);

    const branchRows = Array.isArray(user?.branchPermissions)
      ? user.branchPermissions
      : [];

    const matchedRows = branchId
      ? branchRows.filter((row: any) => String(row?.branchId) === String(branchId))
      : branchRows;

    matchedRows.forEach((row: any) => addKeys(row?.permissionKeys));

    return keys;
  }

  private hasOrderPermission(
    user: any,
    permission: "orders.view" | "orders.view_own",
    branchId?: string | null
  ) {
    if (this.isOwner(user)) return true;

    const keys = this.getOrderPermissionKeys(user, branchId);

    // ✅ RBAC mới: nếu có permissionKeys thì chỉ tin key rõ ràng.
    // Action như orders.approve / orders.edit / orders.pay không được mở rộng phạm vi xem.
    if (keys.size > 0) {
      return keys.has(permission);
    }

    // ✅ Fallback cho dữ liệu legacy chưa migrate permissionKeys.
    const branchPermission = Array.isArray(user?.branchPermissions)
      ? user.branchPermissions.find(
        (p: any) => !branchId || String(p?.branchId) === String(branchId)
      )
      : null;

    if (permission === "orders.view") {
      return Boolean(branchPermission?.canViewBranchOrders);
    }

    return Boolean(branchPermission?.canViewOwnOrders);
  }

  private noOrderAccessWhere(extraWhere?: Prisma.OrderWhereInput) {
    return {
      ...(extraWhere || {}),
      id: "__NO_ACCESS__",
    } as Prisma.OrderWhereInput;
  }

  private buildOwnOrderScope(user: any): Prisma.OrderWhereInput {
    const ownConditions: Prisma.OrderWhereInput[] = [];

    if (user?.id) ownConditions.push({ createdByStaffId: String(user.id) });

    const nameValues = [user?.name, user?.code, user?.username, user?.fullName]
      .map((value) => String(value || "").trim())
      .filter(Boolean);

    nameValues.forEach((value) => {
      ownConditions.push({ createdByStaffName: { equals: value, mode: "insensitive" } });
      ownConditions.push({ createdByStaffName: { contains: value, mode: "insensitive" } });
    });

    if (!ownConditions.length) {
      return { id: "__NO_ACCESS__" };
    }

    return { OR: ownConditions };
  }

  private buildOrderWhereByUser(user: any, extraWhere?: Prisma.OrderWhereInput) {
    if (this.isOwner(user)) {
      return extraWhere || {};
    }

    const userBranch = this.resolveBranchIdFromUser(user);

    if (!userBranch) {
      return this.noOrderAccessWhere(extraWhere);
    }

    const requestedBranch = String((extraWhere as any)?.branchId || "").trim();

    if (requestedBranch && requestedBranch !== String(userBranch)) {
      return this.noOrderAccessWhere(extraWhere);
    }

    const baseWhere: Prisma.OrderWhereInput = {
      ...(extraWhere || {}),
      branchId: userBranch,
    };

    // ✅ Chỉ quyền orders.view mới được xem toàn bộ đơn trong chi nhánh.
    // Tuyệt đối không dùng orders.approve / orders.edit / orders.pay để mở data scope.
    const canViewAll = this.hasOrderPermission(user, "orders.view", userBranch);
    const canViewOwn = this.hasOrderPermission(user, "orders.view_own", userBranch);

    if (canViewAll) {
      return baseWhere;
    }

    if (canViewOwn) {
      return {
        AND: [baseWhere, this.buildOwnOrderScope(user)],
      } as Prisma.OrderWhereInput;
    }

    return this.noOrderAccessWhere(extraWhere);
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

  private isPickupLikeOrder(body: any, salesChannel?: SalesChannel | string | null) {
    const snapshot = body?.shippingSnapshot || {};
    const channel = String(salesChannel || body?.salesChannel || "").toUpperCase();

    return (
      body?.isPosSale === true ||
      channel === "POS" ||
      String(body?.deliveryMethod || "").toUpperCase() === "PICKUP" ||
      String(body?.shippingMethod || "").toUpperCase() === "PICKUP" ||
      String(body?.fulfillmentType || "").toUpperCase() === "PICKUP" ||
      String(snapshot?.shippingPartner || "").toLowerCase() === "pickup" ||
      String(snapshot?.shippingMethod || "").toUpperCase() === "PICKUP" ||
      String(snapshot?.fulfillmentType || "").toUpperCase() === "PICKUP"
    );
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

    const shippingPartner = String(
      snapshot.shippingPartner || "GHN"
    ).toUpperCase();

    // ✅ Chỉ GHN mới bắt district + ward code
    if (shippingPartner === "GHN") {
      if (!snapshot.ghnDistrictId || !snapshot.ghnWardCode) {
        throw new BadRequestException(
          "Địa chỉ chưa có mã GHN (ghnDistrictId / ghnWardCode)."
        );
      }
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
      branchId: string;
      beforeQty?: number;
      afterQty?: number;
    }
  ) {
    const branchId = String(input.branchId || "").trim();

    if (!branchId) {
      throw new BadRequestException("Thiếu branchId khi ghi lịch sử kho.");
    }

    let beforeQty =
      typeof input.beforeQty === "number" ? input.beforeQty : undefined;
    let afterQty = typeof input.afterQty === "number" ? input.afterQty : undefined;

    if (beforeQty === undefined || afterQty === undefined) {
      const inventory = await tx.inventoryItem.findUnique({
        where: {
          variantId_branchId: {
            variantId: input.variantId,
            branchId,
          },
        },
        select: {
          availableQty: true,
        },
      });

      beforeQty = Number(inventory?.availableQty || 0);
      afterQty = beforeQty + Number(input.qty || 0);
    }

    await tx.inventoryMovement.create({
      data: {
        variantId: input.variantId,
        type: input.type,
        qty: input.qty,
        beforeQty,
        afterQty,
        note: input.note || null,
        refType: input.refType || null,
        refId: input.refId || null,
        branchId,
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

      const inventory = variant.inventoryItems[0] || null;

      const availableQty = Number(inventory?.availableQty || 0);
      const neededQty = Number(item.quantity || 0);

      if (neededQty <= 0) {
        throw new BadRequestException(`Số lượng không hợp lệ cho ${variant.sku}`);
      }

      // ✅ CHO PHÉP XUẤT ÂM
      // chỉ cảnh báo log, không block

      if (availableQty < neededQty) {
        console.warn(
          `[ALLOW_NEGATIVE_STOCK] ${variant.sku} | available=${availableQty} | needed=${neededQty}`
        );
      }

      const beforeQty = availableQty;
      const afterQty = beforeQty - neededQty;

      await tx.inventoryItem.upsert({
        where: {
          variantId_branchId: {
            variantId: item.variantId,
            branchId,
          },
        },
        update: {
          availableQty: afterQty,
        },
        create: {
          variantId: item.variantId,
          branchId,
          availableQty: afterQty,
        },
      });

      await this.logInventoryMovement(tx, {
        variantId: item.variantId,
        type: InventoryMovementType.SALE,
        qty: -neededQty,
        beforeQty,
        afterQty,
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

      const beforeQty = Number(inventory.availableQty || 0);
      const afterQty = beforeQty + qty;

      await tx.inventoryItem.update({
        where: {
          variantId_branchId: {
            variantId: item.variantId,
            branchId,
          },
        },
        data: {
          availableQty: afterQty,
        },
      });

      await this.logInventoryMovement(tx, {
        variantId: item.variantId,
        type: InventoryMovementType.CANCEL,
        qty,
        beforeQty,
        afterQty,
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
      soldAt: order.soldAt ? new Date(order.soldAt).toLocaleString("vi-VN") : null,
      customerName: order.customerName || order.customer?.fullName || "Khách lẻ",
      customerPhone: order.customerPhone || order.customer?.phone || "—",
      itemCount: Array.isArray(order.items)
        ? order.items.reduce(
          (sum: number, item: any) => sum + Number(item.qty || item.quantity || 1),
          0
        )
        : Number(order.itemCount || 0),
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
      payments: Array.isArray(order.payments)
        ? order.payments.map((payment: any) => ({
          ...payment,
          amount: this.toNumber(payment.amount),
          sourceName: payment.paymentSource?.name || payment.method || null,
          sourceCode: payment.paymentSource?.code || null,
          sourceType: payment.paymentSource?.type || null,
          paidAt: payment.paidAt
            ? new Date(payment.paidAt).toLocaleString("vi-VN")
            : null,
        }))
        : [],
    };
  }

  private async attachAssignedStaffFields<T extends any>(orders: T[]): Promise<T[]> {
    if (!orders.length) return orders;

    const ids = orders
      .map((order: any) => String(order?.id || "").replace(/'/g, "''"))
      .filter(Boolean);

    if (!ids.length) return orders;

    try {
      const rows = await this.prisma.$queryRawUnsafe<
        Array<{
          id: string;
          assignedStaffId: string | null;
          assignedStaffName: string | null;
        }>
      >(
        `SELECT id, "assignedStaffId", "assignedStaffName" FROM "Order" WHERE id IN (${ids
          .map((id) => `'${id}'`)
          .join(",")})`
      );

      const map = new Map(rows.map((row) => [String(row.id), row]));

      return orders.map((order: any) => ({
        ...order,
        assignedStaffId: map.get(String(order.id))?.assignedStaffId || null,
        assignedStaffName: map.get(String(order.id))?.assignedStaffName || null,
      }));
    } catch {
      return orders;
    }
  }

  private async createShipmentIfNeeded(order: any, body: any) {
    const mode = this.resolveMode(body);
    if (mode !== "ship") return null;

    const snapshot = body?.shippingSnapshot;
    if (!snapshot) return null;

    const items = Array.isArray(order?.items) ? order.items : [];
    const partner = String(snapshot?.shippingPartner || "GHN").toUpperCase();

    // =======================
    // 👉 AHAMOVE
    // =======================
    if (partner === "AHAMOVE") {
      const ahamoveItems = items.map((item: any) => ({
        name: item.productName || item.sku || "Sản phẩm",
        num: Number(item.qty || 1),
        price: this.toNumber(item.unitPrice),
      }));

      const paidAmount = Array.isArray(order?.payments)
        ? order.payments.reduce(
          (sum: number, payment: any) => {
            const sourceType = String(
              payment?.paymentSource?.type || payment?.sourceType || ""
            ).toUpperCase();

            if (sourceType === "COD" || payment?.status === PaymentStatus.PENDING_COD) {
              return sum;
            }

            return sum + this.toNumber(payment?.amount);
          },
          0
        )
        : 0;

      const remainingCodAmount = Math.max(
        0,
        Math.round(this.toNumber(order.finalAmount) - paidAmount)
      );

      return this.shipmentService.createAhamoveShipment(order.id, {
        fromName: process.env.AHAMOVE_FROM_NAME || "The 1970",
        fromPhone: process.env.AHAMOVE_FROM_PHONE || "",
        fromAddress: process.env.AHAMOVE_FROM_ADDRESS || "",

        toName: snapshot.shippingRecipientName,
        toPhone: snapshot.shippingPhone,
        toAddress: snapshot.shippingAddressLine1,

        codAmount: remainingCodAmount,
        items: ahamoveItems,
      });
    }

    // =======================
    // 👉 GHN (GIỮ NGUYÊN)
    // =======================
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
    const negativeStockWarnings: string[] = [];
    const negativeStockWarningKeys = new Set<string>();

    const addNegativeStockWarning = (input: {
      variantId: string;
      sku?: string | null;
      availableQty: number;
      neededQty: number;
    }) => {
      const key = String(input.variantId || input.sku || "");
      if (!key || negativeStockWarningKeys.has(key)) return;

      negativeStockWarningKeys.add(key);
      negativeStockWarnings.push(
        `${input.sku || input.variantId} đang xuất âm kho. Tồn hiện tại ${input.availableQty}, bán ${input.neededQty}.`
      );
    };

    const items = Array.isArray(body.items) ? body.items : [];

    if (!items.length) {
      throw new BadRequestException("Đơn hàng phải có ít nhất 1 sản phẩm");
    }

    const normalizedItems = items.map((item) => ({
      variantId: String(item.variantId),
      quantity: Number(item.quantity ?? item.qty ?? 0),
    }));

    const invalidItem = normalizedItems.find(
      (item) => !item.variantId || item.quantity <= 0
    );

    if (invalidItem) {
      throw new BadRequestException("Sản phẩm hoặc số lượng không hợp lệ.");
    }

    const createdOrder = await this.prisma.$transaction(
      async (tx) => {
        const customerPhone = this.normalizePhone(body.customerPhone);
        const customerName = String(body.customerName || "").trim();
        const salesChannel = String(
          body.salesChannel || "SHOWROOM"
        ) as SalesChannel;
        const isPosSale =
          salesChannel === SalesChannel.POS || body?.isPosSale === true;
        const isPickupOrder = this.isPickupLikeOrder(body, salesChannel);
        const isInstantCounterSale = isPosSale || isPickupOrder;

        let branchId = body.branchId ? String(body.branchId).trim() : null;

        if (!this.isOwner(user)) {
          branchId = this.resolveBranchIdFromUser(user);
        }

        this.ensureBranchAccess(user, branchId);

        if (!branchId) {
          throw new BadRequestException("Thiếu chi nhánh bán hàng.");
        }

        if (mode === "ship" && !isInstantCounterSale) {
          this.ensureShipModePayload(body, branchId);
        }

        let customerId: string | null =
          body.customerId ? String(body.customerId) : null;

        if (!customerId && customerPhone && customerName) {
          let customer = await tx.customer.findFirst({
            where: { phone: customerPhone },
          });

          if (customer) {
            customer = await tx.customer.update({
              where: { id: customer.id },
              data: {
                fullName: customerName || customer.fullName,
                source: salesChannel,
              },
            });
          } else {
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

        const variantIds: string[] = Array.from(
          new Set(normalizedItems.map((item) => String(item.variantId)))
        );

        const variants = await tx.productVariant.findMany({
          where: {
            id: { in: variantIds },
          },
          include: {
            product: {
              select: {
                id: true,
                name: true,
              },
            },
            inventoryItems: {
              where: { branchId },
              select: {
                variantId: true,
                branchId: true,
                availableQty: true,
              },
            },
          },
        }) as any[];

        const variantMap = new Map<string, any>(
          variants.map((variant) => [String(variant.id), variant])
        );

        const qtyByVariantId = normalizedItems.reduce((acc, item) => {
          const key = String(item.variantId);
          acc[key] = (acc[key] || 0) + Number(item.quantity || 0);
          return acc;
        }, {} as Record<string, number>);

        for (const item of normalizedItems) {
          const variant = variantMap.get(String(item.variantId));

          if (!variant) {
            throw new BadRequestException(`Variant không tồn tại: ${item.variantId}`);
          }

          const inventory = Array.isArray(variant.inventoryItems)
            ? variant.inventoryItems[0]
            : null;

          const availableQty = Number(inventory?.availableQty || 0);
          const neededQty = Number(qtyByVariantId[String(item.variantId)] || 0);

          // ✅ Cho phép tạo đơn / bán âm kho.
          // Nếu chi nhánh chưa có dòng tồn kho thì coi như tồn = 0,
          // đến bước trừ kho sẽ tự tạo inventoryItem và ghi âm.
          if (availableQty < neededQty) {
            addNegativeStockWarning({
              variantId: String(item.variantId),
              sku: variant.sku,
              availableQty,
              neededQty,
            });

            console.warn(
              `[ALLOW_NEGATIVE_ORDER] ${variant.sku} | branch=${branchId} | available=${availableQty} | needed=${neededQty}`
            );
          }
        }

        const rawPreparedItems = normalizedItems.map((item) => {
          const variant = variantMap.get(String(item.variantId));
          const qty = Number(item.quantity || 0);
          const unitPriceNumber = this.toNumber(variant.price);
          const lineTotalNumber = qty * unitPriceNumber;

          return {
            variantId: String(variant.id),
            productId: variant.product?.id ? String(variant.product.id) : null,
            sku: String(variant.sku || ""),
            productName: variant.product?.name || "Unknown Product",
            color: variant.color || null,
            size: variant.size || null,
            qty,
            unitPriceNumber,
            lineTotalNumber,
          };
        });

        const activePromotions = await this.promotionsService.getActivePromotions({
          branchId,
          salesChannel,
        });

        const promotionResult = this.promotionEngine.apply({
          items: rawPreparedItems.map((item) => ({
            productId: item.productId,
            variantId: item.variantId,
            quantity: item.qty,
            unitPrice: item.unitPriceNumber,
          })),
          promotions: activePromotions,
        });

        const totalAmount = promotionResult.subtotal;

        const preparedItems = rawPreparedItems.map((item) => ({
          variantId: item.variantId,
          sku: item.sku,
          productName: item.productName,
          color: item.color,
          size: item.size,
          qty: item.qty,
          unitPrice: new Prisma.Decimal(item.unitPriceNumber),
          lineTotal: new Prisma.Decimal(item.lineTotalNumber),
        }));

        const manualDiscountAmountNumber = this.toNumber(body.discountAmount || 0);
        const autoDiscountAmountNumber = this.toNumber(
          promotionResult.totalDiscountAmount || 0
        );
        const autoPromotionNote = Array.isArray(promotionResult.appliedPromotions)
          ? promotionResult.appliedPromotions
            .map((promotion: any) => `${promotion.name}: ${Number(promotion.discountAmount || 0).toLocaleString("vi-VN")}đ`)
            .join(", ")
          : "";
        const discountAmountNumber = manualDiscountAmountNumber + autoDiscountAmountNumber;
        const requestedShippingFeeNumber = this.toNumber(
          body.shippingFee ??
          body.shipFee ??
          body.deliveryFee ??
          body.shippingSnapshot?.shippingFee ??
          body.shippingSnapshot?.shipFee ??
          body.shippingSnapshot?.fee ??
          body.shippingSnapshot?.serviceFee ??
          0
        );

        const shippingFeeNumber = requestedShippingFeeNumber;

        const finalAmountNumber = Math.max(
          0,
          totalAmount - discountAmountNumber + shippingFeeNumber
        );

        const discountAmount = new Prisma.Decimal(discountAmountNumber);
        const shippingFee = new Prisma.Decimal(shippingFeeNumber);
        const finalAmount = new Prisma.Decimal(finalAmountNumber);

        const requestedPaidAmountNumber = this.toNumber(body.paidAmount || 0);

        const rawPayments = Array.isArray(body.payments)
          ? body.payments
          : body.paymentSourceId || requestedPaidAmountNumber > 0
            ? [
              {
                paymentSourceId: body.paymentSourceId
                  ? String(body.paymentSourceId)
                  : null,
                amount: requestedPaidAmountNumber,
                note: body.paymentNote || null,
              },
            ]
            : [];

        const cleanedPayments = rawPayments
          .map((paymentInput: any) => ({
            paymentSourceId:
              paymentInput?.paymentSourceId ||
                paymentInput?.sourceId ||
                paymentInput?.id
                ? String(
                  paymentInput?.paymentSourceId ||
                  paymentInput?.sourceId ||
                  paymentInput?.id
                )
                : null,
            amount: Number(
              paymentInput?.amount ??
              paymentInput?.value ??
              paymentInput?.paidAmount ??
              0
            ),
            note: paymentInput?.note || body.paymentNote || null,
          }))
          .filter((payment: any) => payment.amount > 0);

        const missingPaymentSource = cleanedPayments.find(
          (payment: any) => !payment.paymentSourceId
        );

        if (missingPaymentSource) {
          throw new BadRequestException(
            "Đã nhập tiền khách đã trả nhưng chưa chọn nguồn tiền."
          );
        }

        const paymentSourceIds: string[] = Array.from(
          new Set(cleanedPayments.map((payment: any) => String(payment.paymentSourceId)))
        );

        const paymentSources = paymentSourceIds.length
          ? await tx.paymentSource.findMany({
            where: { id: { in: paymentSourceIds } },
          })
          : [];

        const paymentSourceMap = new Map<string, any>(
          paymentSources.map((source: any) => [String(source.id), source])
        );

        for (const payment of cleanedPayments) {
          if (!paymentSourceMap.has(String(payment.paymentSourceId!))) {
            throw new BadRequestException(
              `Nguồn tiền không tồn tại: ${payment.paymentSourceId}`
            );
          }
        }

        let totalPaid = 0;
        let hasCodPayment = false;

        for (const payment of cleanedPayments) {
          const source = paymentSourceMap.get(String(payment.paymentSourceId!))!;

          if (source.type === "COD") {
            hasCodPayment = true;
          } else {
            totalPaid += Number(payment.amount || 0);
          }
        }

        let paymentStatus: PaymentStatus = PaymentStatus.UNPAID;

        if (hasCodPayment && totalPaid <= 0) {
          paymentStatus = PaymentStatus.PENDING_COD;
        } else if (totalPaid > 0 && totalPaid < finalAmountNumber) {
          paymentStatus = PaymentStatus.PARTIAL;
        } else if (totalPaid >= finalAmountNumber && finalAmountNumber > 0) {
          paymentStatus = PaymentStatus.PAID;
        }

        const shouldCompleteCounterSale = isInstantCounterSale;

        const initialPaymentStatus = paymentStatus;

        const initialFulfillmentStatus = shouldCompleteCounterSale
          ? FulfillmentStatus.FULFILLED
          : modeConfig.fulfillmentStatus;

        const initialOrderStatus = shouldCompleteCounterSale
          ? OrderStatus.COMPLETED
          : modeConfig.status;

        const order = await tx.order.create({
          data: {
            orderCode: `ORD-${Date.now()}`,
            salesChannel,
            customerId,
            customerName: customerName || null,
            customerPhone: customerPhone || null,
            branchId,
            currency: "VND",
            createdByStaffId: user?.id || null,
            createdByStaffName: user?.name || user?.code || user?.username || null,
            soldAt: new Date(),

            totalAmount: new Prisma.Decimal(totalAmount),
            discountAmount,
            shippingFee,
            finalAmount,

            paymentStatus: initialPaymentStatus,
            fulfillmentStatus: initialFulfillmentStatus,
            status: initialOrderStatus,
            note: body.note || null,

            customerAddressId: isInstantCounterSale
              ? null
              : body?.shippingSnapshot?.shippingAddressId || null,
            shippingRecipientName: isInstantCounterSale
              ? null
              : body?.shippingSnapshot?.shippingRecipientName || null,
            shippingPhone: isInstantCounterSale
              ? null
              : body?.shippingSnapshot?.shippingPhone || null,
            shippingAddressLine1: isInstantCounterSale
              ? null
              : body?.shippingSnapshot?.shippingAddressLine1 || null,
            shippingAddressLine2: isInstantCounterSale
              ? null
              : body?.shippingSnapshot?.shippingAddressLine2 || null,
            shippingWard: isInstantCounterSale
              ? null
              : body?.shippingSnapshot?.shippingWard || null,
            shippingDistrict: isInstantCounterSale
              ? null
              : body?.shippingSnapshot?.shippingDistrict || null,
            shippingCity: isInstantCounterSale
              ? null
              : body?.shippingSnapshot?.shippingCity || null,
            shippingProvince: isInstantCounterSale
              ? null
              : body?.shippingSnapshot?.shippingProvince || null,
            shippingCountry: isInstantCounterSale
              ? null
              : body?.shippingSnapshot?.shippingCountry || null,
            shippingPostalCode: isInstantCounterSale
              ? null
              : body?.shippingSnapshot?.shippingPostalCode || null,
            shippingGhnDistrictId: isInstantCounterSale
              ? null
              : body?.shippingSnapshot?.ghnDistrictId || null,
            shippingGhnWardCode: isInstantCounterSale
              ? null
              : body?.shippingSnapshot?.ghnWardCode || null,
          },
        });

        await tx.orderItem.createMany({
          data: preparedItems.map((item) => ({
            orderId: order.id,
            variantId: item.variantId,
            sku: item.sku,
            productName: item.productName,
            color: item.color,
            size: item.size,
            qty: item.qty,
            unitPrice: item.unitPrice,
            lineTotal: item.lineTotal,
          })),
        });

        if (cleanedPayments.length) {
          await tx.payment.createMany({
            data: cleanedPayments.map((payment: any) => {
              const source = paymentSourceMap.get(String(payment.paymentSourceId!))!;

              return {
                orderId: order.id,
                amount: new Prisma.Decimal(payment.amount),
                status:
                  source.type === "COD"
                    ? PaymentStatus.PENDING_COD
                    : PaymentStatus.PAID,
                method: source.name,
                paymentSourceId: payment.paymentSourceId,
                note: payment.note || null,
                paidAt: source.type === "COD" ? null : new Date(),
              };
            }),
          });
        }

        if (modeConfig.deductStockNow || shouldCompleteCounterSale) {
          for (const [variantId, qty] of Object.entries(qtyByVariantId)) {
            const deductQty = Number(qty || 0);

            if (deductQty <= 0) continue;

            const inventory = await tx.inventoryItem.findUnique({
              where: {
                variantId_branchId: {
                  variantId,
                  branchId,
                },
              },
              select: {
                availableQty: true,
              },
            });

            const beforeQty = Number(inventory?.availableQty || 0);
            const afterQty = beforeQty - deductQty;

            if (beforeQty < deductQty) {
              const variant = variantMap.get(String(variantId));
              addNegativeStockWarning({
                variantId: String(variantId),
                sku: variant?.sku,
                availableQty: beforeQty,
                neededQty: deductQty,
              });
            }

            await tx.inventoryItem.upsert({
              where: {
                variantId_branchId: {
                  variantId,
                  branchId,
                },
              },
              update: {
                availableQty: afterQty,
              },
              create: {
                variantId,
                branchId,
                availableQty: afterQty,
              },
            });

            await this.logInventoryMovement(tx, {
              variantId,
              type: InventoryMovementType.SALE,
              qty: -deductQty,
              beforeQty,
              afterQty,
              note: shouldCompleteCounterSale
                ? "Trừ kho bán tại quầy / khách nhận tại cửa hàng"
                : "Trừ kho khi xuất đơn",
              refType: "ORDER",
              refId: order.id,
              branchId,
            });
          }
        }

        if (customerId) {
          await tx.customer.update({
            where: { id: customerId },
            data: {
              totalOrders: {
                increment: 1,
              },
              totalSpent: {
                increment: new Prisma.Decimal(finalAmountNumber),
              },
              lastOrderAt: new Date(),
            },
          });
        }

        if (isInstantCounterSale) {
          return {
            ...order,
            items: preparedItems.map((item) => ({
              ...item,
              id: item.variantId,
              orderId: order.id,
              unitPrice: item.unitPrice,
              lineTotal: item.lineTotal,
            })),
            shipment: null,
            payments: cleanedPayments.map((payment: any) => {
              const source = paymentSourceMap.get(String(payment.paymentSourceId!))!;

              return {
                id: payment.paymentSourceId,
                orderId: order.id,
                amount: new Prisma.Decimal(payment.amount),
                status:
                  source.type === "COD"
                    ? PaymentStatus.PENDING_COD
                    : PaymentStatus.PAID,
                method: source.name,
                paymentSourceId: payment.paymentSourceId,
                paymentSource: source,
                note: payment.note || null,
                paidAt: source.type === "COD" ? null : new Date(),
              };
            }),
            customer: customerId
              ? {
                id: customerId,
                fullName: customerName || null,
                phone: customerPhone || null,
              }
              : null,
          };
        }

        const completedOrder = await tx.order.findUnique({
          where: { id: order.id },
          include: {
            items: true,
            shipment: true,
            payments: {
              include: {
                paymentSource: true,
              },
            },
            customer: {
              select: {
                id: true,
                fullName: true,
                phone: true,
              },
            },
          },
        });

        return completedOrder || order;
      },
      {
        maxWait: 10000,
        timeout: 20000,
      }
    );

    let finalOrder = createdOrder;

    if (mode === "ship" && !this.isPickupLikeOrder(body, body?.salesChannel)) {
      await this.createShipmentIfNeeded(createdOrder, body);

      // createShipmentIfNeeded gọi ShipmentService để tạo vận đơn và service đã set:
      // order.status = SHIPPED, order.fulfillmentStatus = PROCESSING,
      // shipment.shippingStatus = READY_TO_PICK nếu GHN chưa trả trạng thái rõ ràng.
      const reloadedOrder = await this.prisma.order.findUnique({
        where: { id: createdOrder.id },
        include: {
          items: true,
          shipment: true,
          payments: {
            include: {
              paymentSource: true,
            },
          },
          customer: {
            select: {
              id: true,
              fullName: true,
              phone: true,
            },
          },
        },
      });

      if (reloadedOrder) {
        finalOrder = reloadedOrder;
      }
    }

    return {
      ...this.mapOrderResponse(finalOrder),
      negativeStockWarnings,
      hasNegativeStockWarning: negativeStockWarnings.length > 0,
    };
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
          createdByStaffId: true,
          createdByStaffName: true,
          soldAt: true,
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
              qty: true,
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
          payments: {
            select: {
              id: true,
              method: true,
              amount: true,
              status: true,
              paidAt: true,
              paymentSourceId: true,
              paymentSource: {
                select: {
                  id: true,
                  code: true,
                  name: true,
                  type: true,
                  branchId: true,
                },
              },
            },
          },
        },
      }),
      this.prisma.order.count({ where }),
    ]);

    const ordersWithAssignedStaff = await this.attachAssignedStaffFields(orders);

    const data = ordersWithAssignedStaff.map((order) => ({
      ...order,
      totalAmount: this.toNumber(order.totalAmount),
      discountAmount: this.toNumber(order.discountAmount),
      shippingFee: this.toNumber(order.shippingFee),
      finalAmount: this.toNumber(order.finalAmount),
      createdAt: new Date(order.createdAt).toLocaleString("vi-VN"),
      updatedAt: new Date(order.updatedAt).toLocaleString("vi-VN"),
      soldAt: order.soldAt ? new Date(order.soldAt).toLocaleString("vi-VN") : null,
      items: Array.isArray(order.items)
        ? order.items.map((item: any) => ({
          ...item,
          qty: Number(item.qty || 0),
        }))
        : [],
      itemCount: Array.isArray(order.items)
        ? order.items.reduce(
          (sum: number, item: any) => sum + Number(item.qty || 0),
          0
        )
        : 0,
      payments: Array.isArray(order.payments)
        ? order.payments.map((payment: any) => ({
          ...payment,
          amount: this.toNumber(payment.amount),
          sourceName: payment.paymentSource?.name || payment.method || null,
          sourceCode: payment.paymentSource?.code || null,
          sourceType: payment.paymentSource?.type || null,
          paidAt: payment.paidAt
            ? new Date(payment.paidAt).toLocaleString("vi-VN")
            : null,
        }))
        : [],

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
        payments: {
          include: {
            paymentSource: true,
          },
        },
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

    if (!this.isOwner(user)) {
      const canViewAll = this.hasOrderPermission(
        user,
        "orders.view",
        order.branchId || null
      );
      const canViewOwn = this.hasOrderPermission(
        user,
        "orders.view_own",
        order.branchId || null
      );

      if (!canViewAll) {
        const ownWhere = this.buildOwnOrderScope(user);
        const ownOrder = await this.prisma.order.findFirst({
          where: {
            AND: [
              { id: order.id, branchId: order.branchId },
              ownWhere,
            ],
          },
          select: { id: true },
        });

        if (!canViewOwn || !ownOrder) {
          throw new ForbiddenException("Bạn không có quyền xem đơn này.");
        }
      }
    }

    const [orderWithAssignedStaff] = await this.attachAssignedStaffFields([order]);
    return this.mapOrderResponse(orderWithAssignedStaff);
  }

  async updateOrder(orderId: string, body: any, user?: any) {
    const existing = await this.prisma.order.findFirst({
      where: this.buildOrderWhereByUser(user, { id: orderId }),
      include: {
        items: true,
        shipment: true,
        payments: {
          include: {
            paymentSource: true,
          },
        },
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
          payments: {
            include: {
              paymentSource: true,
            },
          },
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
  async assignStaffToOrder(
    orderId: string,
    assignedStaffId: string | null,
    user?: any
  ) {
    const existing = await this.prisma.order.findFirst({
      where: this.buildOrderWhereByUser(user, { id: orderId }),
      select: {
        id: true,
        branchId: true,
      },
    });

    if (!existing) {
      throw new BadRequestException("Không tìm thấy đơn hàng");
    }

    if (!this.isOwner(user) && !this.hasOrderPermission(user, "orders.view", existing.branchId || null)) {
      throw new ForbiddenException("Bạn không có quyền gán đơn cho nhân viên khác.");
    }

    let assignedStaffName: string | null = null;

    if (assignedStaffId) {
      const staff = await this.prisma.staffUser.findUnique({
        where: { id: assignedStaffId },
        select: {
          id: true,
          code: true,
          name: true,
          branchId: true,
          isActive: true,
        },
      });

      if (!staff || staff.isActive === false) {
        throw new BadRequestException("Nhân viên nhận đơn không tồn tại hoặc đã ngừng hoạt động.");
      }

      if (staff.branchId && existing.branchId && staff.branchId !== existing.branchId && !this.isOwner(user)) {
        throw new BadRequestException("Nhân viên nhận đơn không cùng chi nhánh với đơn hàng.");
      }

      assignedStaffName = staff.name || staff.code || staff.id;
    }

    await this.prisma.$executeRawUnsafe(
      `UPDATE "Order" SET "assignedStaffId" = $1, "assignedStaffName" = $2, "updatedAt" = NOW() WHERE id = $3`,
      assignedStaffId,
      assignedStaffName,
      orderId
    );

    return this.getOrderById(orderId, user);
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
        payments: {
          include: {
            paymentSource: true,
          },
        },
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



  async deleteOrder(orderId: string, user?: any) {
    const order = await this.prisma.order.findFirst({
      where: this.buildOrderWhereByUser(user, {
        id: orderId,
      }),
      include: {
        items: true,
      },
    });

    if (!order) {
      throw new NotFoundException("Không tìm thấy đơn hàng");
    }

    await this.prisma.$transaction(async (tx) => {
      if (
        order.status === OrderStatus.APPROVED ||
        order.status === OrderStatus.PACKING
      ) {
        await this.restoreStockForOrder(
          tx,
          order.id,
          order.branchId || null
        );
      }

      await tx.payment.deleteMany({
        where: {
          orderId: order.id,
        },
      });

      await tx.shipment.deleteMany({
        where: {
          orderId: order.id,
        },
      });

      await tx.orderItem.deleteMany({
        where: {
          orderId: order.id,
        },
      });

      await tx.order.delete({
        where: {
          id: order.id,
        },
      });
    });

    return {
      success: true,
      message: "Đã xoá đơn hàng",
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