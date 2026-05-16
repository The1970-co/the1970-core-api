import {
  BadRequestException,
  ForbiddenException,
  Injectable,
} from "@nestjs/common";
import { InventoryMovementType, PaymentStatus, Prisma } from "@prisma/client";
import { PrismaService } from "../prisma/prisma.service";
import { OrderService } from "../order/order.service";
import { ShipmentService } from "../shipment/shipment.service";

@Injectable()
export class ReturnsService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly orderService: OrderService,
    private readonly shipmentService: ShipmentService,
  ) {}

  private n(v: any) {
    return Number(v || 0);
  }

  private isOwner(user?: any) {
    const role = String(user?.role || "")
      .trim()
      .toUpperCase();
    return role === "OWNER" || role === "ADMIN";
  }

  private userBranch(user?: any) {
    return this.userBranchCandidates(user)[0] || null;
  }

  private normalizeBranchKey(value: any) {
    return String(value || "")
      .trim()
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/đ/g, "d")
      .replace(/[^a-z0-9]/g, "");
  }

  private userBranchCandidates(user?: any) {
    return [
      user?.branchId,
      user?.workingBranchId,
      user?.currentBranchId,
      user?.branchCode,
      user?.branchName,
      user?.branch,
      user?.activeBranchId,
    ]
      .map((value) => String(value || "").trim())
      .filter(Boolean);
  }

  private ensureBranch(user: any, branchId?: string | null) {
    if (this.isOwner(user)) return;

    const candidates = this.userBranchCandidates(user);

    if (!candidates.length) {
      throw new ForbiddenException("Tài khoản chưa được gán chi nhánh.");
    }

    if (!branchId) return;

    const target = this.normalizeBranchKey(branchId);
    const hasAccess = candidates.some((value) => {
      const normalized = this.normalizeBranchKey(value);
      return normalized && normalized === target;
    });

    if (!hasAccess) {
      throw new ForbiddenException(
        "Không có quyền xử lý phiếu ở chi nhánh này.",
      );
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
      throw new BadRequestException(
        `Nguồn tiền không tồn tại: ${paymentSourceId}`,
      );
    }

    return source;
  }

  private isReturnableOrder(order: any) {
    const status = String(order?.status || "").toUpperCase();
    const paymentStatus = String(order?.paymentStatus || "").toUpperCase();
    const fulfillmentStatus = String(
      order?.fulfillmentStatus || "",
    ).toUpperCase();

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
      shippingFee: this.n(row.shippingFee),
      customerPayableAmount: this.n(row.customerPayableAmount),
      exchangeOrderId: row.exchangeOrderId || null,
      exchangeOrderCode: row.exchangeOrderCode || null,
      exchangeShipmentId: row.exchangeShipmentId || null,
      exchangeTrackingCode: row.exchangeTrackingCode || null,
      exchangeCarrier: row.exchangeCarrier || null,
      createdAt: row.createdAt
        ? new Date(row.createdAt).toLocaleString("vi-VN")
        : null,
      updatedAt: row.updatedAt
        ? new Date(row.updatedAt).toLocaleString("vi-VN")
        : null,
      items: (row.items || []).map((i: any) => ({
        ...i,
        unitPrice: this.n(i.unitPrice),
        refundPrice: this.n(i.refundPrice),
        lineTotal: this.n(i.lineTotal),
      })),
      cashVouchers: (row.cashVouchers || []).map((v: any) => ({
        ...v,
        amount: this.n(v.amount),
        createdAt: v.createdAt
          ? new Date(v.createdAt).toLocaleString("vi-VN")
          : null,
        updatedAt: v.updatedAt
          ? new Date(v.updatedAt).toLocaleString("vi-VN")
          : null,
      })),
    };
  }

  private async validateReturnQuantity(
    originalOrderId: string,
    returnItems: any[],
  ) {
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
          `Số lượng trả ${orderItem.sku || orderItem.productName || ""} vượt số lượng đã mua.`,
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

    const previousQtyByOrderItemId = previousRows.reduce(
      (acc, row) => {
        if (!row.orderItemId) return acc;
        acc[row.orderItemId] =
          (acc[row.orderItemId] || 0) + Number(row.qty || 0);
        return acc;
      },
      {} as Record<string, number>,
    );

    const requestedQtyByOrderItemId = returnItems.reduce(
      (acc, item) => {
        if (!item.orderItemId) return acc;
        const key = String(item.orderItemId);
        acc[key] = (acc[key] || 0) + this.n(item.qty);
        return acc;
      },
      {} as Record<string, number>,
    );

    for (const [orderItemId, requestedQtyRaw] of Object.entries(
      requestedQtyByOrderItemId,
    )) {
      const requestedQty = Number(requestedQtyRaw || 0);

      const orderItem = orderItemMap.get(orderItemId);
      const purchasedQty = Number(orderItem?.qty || 0);
      const previousQty = Number(previousQtyByOrderItemId[orderItemId] || 0);
      const remainQty = purchasedQty - previousQty;

      if (requestedQty > remainQty) {
        throw new BadRequestException(
          `Sản phẩm ${orderItem?.sku || orderItem?.productName || ""} chỉ còn được trả ${remainQty}.`,
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
        "Chỉ được đổi/trả đơn đã hoàn thành, đã giao hoặc đã thanh toán hợp lệ.",
      );
    }

    const receiveBranchId =
      body.returnReceiveBranchId ||
      body.handledAtBranchId ||
      originalOrder.branchId;

    const exchangeIssueBranchId = body.exchangeIssueBranchId || receiveBranchId;

    this.ensureBranch(user, receiveBranchId);

    const items = Array.isArray(body.items) ? body.items : [];

    const returnItems = items.filter(
      (x: any) => String(x.itemType || "RETURN") === "RETURN",
    );

    const exchangeItems = items.filter(
      (x: any) => String(x.itemType || "") === "EXCHANGE",
    );

    if (!returnItems.length && !exchangeItems.length) {
      throw new BadRequestException("Chưa có sản phẩm trả/đổi.");
    }

    await this.validateReturnQuantity(originalOrderId, returnItems);

    const returnAmount = returnItems.reduce((sum: number, item: any) => {
      return (
        sum + this.n(item.refundPrice ?? item.unitPrice) * this.n(item.qty)
      );
    }, 0);

    const exchangeAmount = exchangeItems.reduce((sum: number, item: any) => {
      return (
        sum + this.n(item.refundPrice ?? item.unitPrice) * this.n(item.qty)
      );
    }, 0);

    const rawShippingFee = this.n(
      body.shippingFee ??
        body.shipFee ??
        body.deliveryFee ??
        body.customerShippingFee ??
        0,
    );
    const shippingFee = exchangeItems.length ? Math.max(0, rawShippingFee) : 0;

    // Chênh lệch thực tế phải tính cả phí ship thu khách:
    // khách phải trả = tiền hàng đổi + phí ship - tiền hàng trả.
    const customerPayableAmount = Math.max(
      0,
      exchangeAmount + shippingFee - returnAmount,
    );
    const differenceAmount = returnAmount - exchangeAmount - shippingFee;
    const refundAmount = differenceAmount > 0 ? differenceAmount : 0;
    const extraChargeAmount = customerPayableAmount;

    if (refundAmount > 0 && !body.refundPaymentSourceId) {
      throw new BadRequestException("Thiếu nguồn tiền hoàn khách.");
    }

    const deferExtraChargeToShipment = body.deferExtraChargeToShipment === true;

    if (
      extraChargeAmount > 0 &&
      !deferExtraChargeToShipment &&
      !body.extraChargePaymentSourceId
    ) {
      throw new BadRequestException("Thiếu nguồn tiền khách bù thêm.");
    }

    if (refundAmount > 0) {
      await this.ensurePaymentSourceExists(body.refundPaymentSourceId);
    }

    if (extraChargeAmount > 0 && !deferExtraChargeToShipment) {
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
          shippingFee: new Prisma.Decimal(shippingFee),
          customerPayableAmount: new Prisma.Decimal(customerPayableAmount),

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
                this.n(item.refundPrice ?? item.unitPrice),
              ),
              lineTotal: new Prisma.Decimal(
                this.n(item.refundPrice ?? item.unitPrice) * this.n(item.qty),
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
              `Không đủ tồn kho sản phẩm đổi ${item.sku || item.productName || ""}.`,
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

      if (extraChargeAmount > 0 && !deferExtraChargeToShipment) {
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

  private buildExchangeOrderUser(user?: any) {
    const ensure = (items?: any[]) => {
      const set = new Set<string>();
      if (Array.isArray(items))
        items.forEach((item) => item && set.add(String(item)));
      set.add("orders.create");
      return Array.from(set);
    };

    return {
      ...(user || {}),
      permissions: ensure(user?.permissions),
      permissionKeys: ensure(user?.permissionKeys),
    };
  }

  private parseStructuredNoteValue(note?: string | null, prefix?: string) {
    if (!note || !prefix) return "";
    const parts = String(note)
      .split(" | ")
      .map((item) => item.trim())
      .filter(Boolean);

    const found = parts.find((item) => item.startsWith(prefix));
    return found ? found.replace(prefix, "").trim() : "";
  }

  private buildFullAddress(order: any) {
    return [
      order.shippingAddressLine1,
      order.shippingAddressLine2,
      order.shippingWard,
      order.shippingDistrict,
      order.shippingProvince,
      order.shippingPostalCode,
    ]
      .filter(Boolean)
      .join(", ");
  }

  private normalizeGhnRequiredNote(input?: string | null) {
    const raw = String(input || "")
      .trim()
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "");

    if (raw.includes("cho xem") && raw.includes("khong cho thu")) {
      return "CHOXEMHANGKHONGTHU";
    }

    if (raw.includes("cho xem")) {
      return "CHOXEMHANG";
    }

    return "KHONGCHOXEMHANG";
  }

  private normalizeShippingPartner(input?: string | null) {
    const raw = String(input || "")
      .trim()
      .toUpperCase()
      .replace(/[\s_-]+/g, "");

    if (raw.includes("VIETTEL") || raw === "VTP") return "VIETTELPOST";
    if (raw.includes("AHA")) return "AHAMOVE";
    return "GHN";
  }

  private quoteFee(input: any) {
    return this.n(
      input?._fee ||
        input?.fee?.total ||
        input?.fee?.total_fee ||
        input?.fee?.service_fee ||
        input?.data?.user_price_details?.total_fee ||
        input?.data?.user_price_details?.total_price ||
        input?.data?.total_price ||
        input?.data?.total_fee ||
        input?.data?.service_fee ||
        input?.totalFee ||
        input?.total_fee ||
        input?.totalPrice ||
        input?.total_price ||
        input?.fee ||
        0,
    );
  }

  private buildShippingSnapshotFromOriginalOrder(
    originalOrder: any,
    shippingFee: number,
    shippingPartner = "GHN",
    options: any = {},
  ) {
    const partner = this.normalizeShippingPartner(shippingPartner);
    const selectedQuote =
      options?.selectedShippingQuote || options?.shippingQuote || {};
    const oldNote = String(originalOrder.note || "");
    const shippingNote =
      this.parseStructuredNoteValue(oldNote, "Ghi chú giao hàng:") ||
      this.parseStructuredNoteValue(oldNote, "Ghi chú:") ||
      "Cho xem hàng, không cho thử";

    return {
      shippingPartner: partner,
      carrier: partner,
      shippingFee,
      shippingRecipientName:
        originalOrder.shippingRecipientName ||
        originalOrder.customerName ||
        "Khách hàng",
      shippingPhone:
        originalOrder.shippingPhone || originalOrder.customerPhone || "",
      shippingAddressLine1: originalOrder.shippingAddressLine1 || "",
      shippingAddressLine2: originalOrder.shippingAddressLine2 || "",
      shippingWard: originalOrder.shippingWard || "",
      shippingDistrict: originalOrder.shippingDistrict || "",
      shippingCity: originalOrder.shippingCity || "",
      shippingProvince: originalOrder.shippingProvince || "",
      shippingCountry: originalOrder.shippingCountry || "VN",
      shippingPostalCode: originalOrder.shippingPostalCode || "",
      ghnDistrictId: originalOrder.shippingGhnDistrictId || null,
      ghnWardCode: originalOrder.shippingGhnWardCode || null,
      shippingGhnDistrictId: originalOrder.shippingGhnDistrictId || null,
      shippingGhnWardCode: originalOrder.shippingGhnWardCode || null,
      requiredNote: this.normalizeGhnRequiredNote(shippingNote),
      shippingNote,
      deliveryRequirement: shippingNote,
      note: shippingNote,
      selectedServiceId:
        options?.selectedServiceId || selectedQuote?.serviceId || null,
      selectedServiceTypeId:
        options?.selectedServiceTypeId || selectedQuote?.serviceTypeId || null,
      selectedQuoteKey:
        options?.selectedQuoteKey || selectedQuote?._quoteKey || null,
      serviceCode:
        options?.serviceCode ||
        options?.viettelServiceCode ||
        selectedQuote?._viettelServiceCode ||
        selectedQuote?.serviceCode ||
        selectedQuote?.orderService ||
        null,
      viettelServiceCode:
        options?.viettelServiceCode ||
        options?.serviceCode ||
        selectedQuote?._viettelServiceCode ||
        selectedQuote?.serviceCode ||
        null,
      orderService:
        options?.serviceCode ||
        options?.viettelServiceCode ||
        selectedQuote?._viettelServiceCode ||
        selectedQuote?.serviceCode ||
        null,
      ahamoveServiceId:
        options?.ahamoveServiceId ||
        selectedQuote?._ahamoveServiceId ||
        selectedQuote?.service_id ||
        selectedQuote?.serviceId ||
        null,
      serviceId:
        options?.ahamoveServiceId ||
        selectedQuote?._ahamoveServiceId ||
        selectedQuote?.service_id ||
        selectedQuote?.serviceId ||
        options?.selectedServiceId ||
        null,
      weight: Number(options?.weight || selectedQuote?.weight || 500),
      length: 20,
      width: 20,
      height: 5,
    };
  }

  private getExchangeShipmentDims(body: any, selectedQuote: any = {}) {
    return {
      weight: Math.max(
        1,
        Number(
          body?.weight || body?.shippingWeight || selectedQuote?.weight || 500,
        ),
      ),
      length: Math.max(
        1,
        Number(
          body?.length || body?.shippingLength || selectedQuote?.length || 20,
        ),
      ),
      width: Math.max(
        1,
        Number(
          body?.width || body?.shippingWidth || selectedQuote?.width || 20,
        ),
      ),
      height: Math.max(
        1,
        Number(
          body?.height || body?.shippingHeight || selectedQuote?.height || 5,
        ),
      ),
    };
  }

  private buildOrderFullAddress(order: any) {
    return [
      order?.shippingAddressLine1,
      order?.shippingAddressLine2,
      order?.shippingWard,
      order?.shippingDistrict,
      order?.shippingProvince,
      order?.shippingPostalCode,
    ]
      .filter(Boolean)
      .join(", ");
  }

  private calculateOrderRemainingCod(order: any) {
    const paidAmount = Array.isArray(order?.payments)
      ? order.payments.reduce((sum: number, payment: any) => {
          const sourceType = String(
            payment?.paymentSource?.type || payment?.sourceType || "",
          ).toUpperCase();
          if (
            sourceType === "COD" ||
            payment?.status === PaymentStatus.PENDING_COD
          )
            return sum;
          return sum + this.n(payment?.amount);
        }, 0)
      : 0;

    if (String(order?.paymentStatus || "").toUpperCase() === "PAID") return 0;
    return Math.max(0, Math.round(this.n(order?.finalAmount) - paidAmount));
  }

  private buildShipmentItemsFromOrder(
    order: any,
    dims: { weight: number; length: number; width: number; height: number },
  ) {
    const items = Array.isArray(order?.items) ? order.items : [];
    const itemWeight = Math.max(
      1,
      Math.floor(Number(dims.weight || 1) / Math.max(items.length, 1)),
    );

    return items.map((item: any) => ({
      name: item.productName || item.sku || "Sản phẩm",
      quantity: Number(item.qty || 1),
      num: Number(item.qty || 1),
      price: this.n(item.unitPrice),
      length: dims.length,
      width: dims.width,
      height: dims.height,
      weight: itemWeight,
      category: "Hàng hóa",
    }));
  }

  private getSelectedCarrierService(body: any, selectedQuote: any = {}) {
    return {
      selectedServiceId:
        Number(body?.selectedServiceId || selectedQuote?.serviceId || 0) ||
        undefined,
      selectedServiceTypeId:
        Number(
          body?.selectedServiceTypeId || selectedQuote?.serviceTypeId || 0,
        ) || undefined,
      serviceCode: String(
        body?.serviceCode ||
          body?.viettelServiceCode ||
          selectedQuote?._viettelServiceCode ||
          selectedQuote?.serviceCode ||
          selectedQuote?.orderService ||
          "",
      ).trim(),
      ahamoveServiceId: String(
        body?.ahamoveServiceId ||
          selectedQuote?._ahamoveServiceId ||
          selectedQuote?.service_id ||
          selectedQuote?.serviceId ||
          "",
      ).trim(),
      viettelReceiverProvinceId:
        Number(
          selectedQuote?._viettelReceiverProvinceId ||
            body?.viettelReceiverProvinceId ||
            0,
        ) || undefined,
      viettelReceiverDistrictId:
        Number(
          selectedQuote?._viettelReceiverDistrictId ||
            body?.viettelReceiverDistrictId ||
            0,
        ) || undefined,
      viettelReceiverWardId:
        Number(
          selectedQuote?._viettelReceiverWardId ||
            body?.viettelReceiverWardId ||
            0,
        ) || undefined,
      viettelSenderGroupAddressId:
        Number(
          selectedQuote?._viettelSenderGroupAddressId ||
            body?.viettelSenderGroupAddressId ||
            0,
        ) || undefined,
    };
  }

  private async createShipmentForExchangeOrder(input: {
    exchangeOrder: any;
    shippingPartner: string;
    shippingFee: number;
    body: any;
    selectedQuote?: any;
    shippingNote?: string;
    user?: any;
  }) {
    const order = input.exchangeOrder;
    const selectedQuote = input.selectedQuote || {};
    const partner = this.normalizeShippingPartner(
      input.shippingPartner ||
        selectedQuote?._carrier ||
        selectedQuote?.carrier ||
        "GHN",
    );
    const dims = this.getExchangeShipmentDims(input.body, selectedQuote);
    const items = this.buildShipmentItemsFromOrder(order, dims);
    const service = this.getSelectedCarrierService(input.body, selectedQuote);
    const codAmount = this.calculateOrderRemainingCod(order);
    const toAddress =
      this.buildOrderFullAddress(order) || order?.shippingAddressLine1 || "";
    const note =
      input.shippingNote ||
      this.parseStructuredNoteValue(
        String(order?.note || ""),
        "Ghi chú giao hàng:",
      ) ||
      "Đơn đổi/trả";

    if (partner === "AHAMOVE") {
      return this.shipmentService.createAhamoveShipment(order.id, {
        toName:
          order.shippingRecipientName || order.customerName || "Khách hàng",
        toPhone: order.shippingPhone || order.customerPhone || "",
        toAddress,
        codAmount,
        serviceId: service.ahamoveServiceId || undefined,
        note,
        items: items.map((item: any, index: number) => ({
          _id: String(item?.name || index + 1),
          name: item.name,
          num: item.quantity,
          quantity: item.quantity,
          price: item.price,
          weight: item.weight,
        })),
      });
    }

    if (partner === "VIETTELPOST") {
      return this.shipmentService.createViettelPostShipment(order.id, {
        toName:
          order.shippingRecipientName || order.customerName || "Khách hàng",
        toPhone: order.shippingPhone || order.customerPhone || "",
        toAddress: order.shippingAddressLine1 || toAddress,
        toProvince: order.shippingProvince || "",
        toDistrict: order.shippingDistrict || "",
        toWard: order.shippingWard || "",
        province: order.shippingProvince || "",
        district: order.shippingDistrict || "",
        ward: order.shippingWard || "",
        receiverProvinceId: service.viettelReceiverProvinceId,
        receiverDistrictId: service.viettelReceiverDistrictId,
        receiverWardId: service.viettelReceiverWardId,
        senderGroupAddressId: service.viettelSenderGroupAddressId,
        codAmount,
        insuranceValue: this.n(order.finalAmount),
        productPrice: this.n(order.finalAmount),
        serviceCode: service.serviceCode || "VCN",
        clientOrderCode: order.orderCode,
        orderCode: order.orderCode,
        content: `Đơn đổi/trả ${order.orderCode}`,
        note,
        weight: dims.weight,
        length: dims.length,
        width: dims.width,
        height: dims.height,
        items: items.map((item: any) => ({
          name: item.name,
          quantity: item.quantity,
          price: item.price,
          weight: item.weight,
        })),
      });
    }

    const toDistrictId = Number(
      order.shippingGhnDistrictId ||
        input.body?.ghnDistrictId ||
        input.body?.shippingGhnDistrictId ||
        0,
    );
    const toWardCode = String(
      order.shippingGhnWardCode ||
        input.body?.ghnWardCode ||
        input.body?.shippingGhnWardCode ||
        "",
    );
    if (!toDistrictId || !toWardCode) {
      throw new BadRequestException(
        "Đơn đổi chưa có mã GHN quận/huyện hoặc phường/xã.",
      );
    }

    return this.shipmentService.createGhnShipment(order.id, {
      toName: order.shippingRecipientName || order.customerName || "Khách hàng",
      toPhone: order.shippingPhone || order.customerPhone || "",
      toAddress,
      toDistrictId,
      toWardCode,
      codAmount,
      clientOrderCode: order.orderCode,
      note,
      content: `Đơn đổi/trả ${order.orderCode}`,
      requiredNote: this.normalizeGhnRequiredNote(note),
      weight: dims.weight,
      length: dims.length,
      width: dims.width,
      height: dims.height,
      insuranceValue: this.n(order.finalAmount),
      items: items.map((item: any) => ({
        name: item.name,
        quantity: item.quantity,
        price: item.price,
        length: item.length,
        width: item.width,
        height: item.height,
        weight: item.weight,
        category: "Hàng hóa",
      })),
    });
  }

  async createReturnAndShipExchange(body: any, user?: any) {
    const items = Array.isArray(body?.items) ? body.items : [];
    const exchangeItems = items.filter(
      (item: any) => String(item?.itemType || "").toUpperCase() === "EXCHANGE",
    );

    if (!exchangeItems.length) {
      throw new BadRequestException("Chưa có sản phẩm đổi để gửi vận chuyển.");
    }

    const originalOrderId = String(body?.originalOrderId || "").trim();
    if (!originalOrderId) {
      throw new BadRequestException("Thiếu originalOrderId.");
    }

    const originalOrder = await this.prisma.order.findUnique({
      where: { id: originalOrderId },
      include: { items: true, payments: true },
    });

    if (!originalOrder) {
      throw new BadRequestException("Không tìm thấy đơn hàng gốc.");
    }

    const selectedQuote =
      body?.selectedShippingQuote || body?.shippingQuote || {};
    const shippingPartner = this.normalizeShippingPartner(
      body?.shippingPartner ||
        body?.carrier ||
        selectedQuote?._carrier ||
        selectedQuote?.carrier ||
        "GHN",
    );

    const shippingFee = Math.max(
      0,
      this.n(
        body?.shippingFee ??
          body?.shipFee ??
          this.quoteFee(selectedQuote) ??
          30000,
      ),
    );

    const record = await this.createReturn(
      {
        ...body,
        status: body?.status || "COMPLETED",
        shippingFee,
        shippingPartner,
        deferExtraChargeToShipment: true,
      },
      user,
    );

    const freshRecord = await this.prisma.returnExchange.findUnique({
      where: { id: record.id },
      include: { items: true, cashVouchers: true },
    });

    if (!freshRecord) {
      throw new BadRequestException("Không tìm thấy phiếu đổi/trả vừa tạo.");
    }

    const exchangeAmount = this.n(freshRecord.exchangeAmount);
    const returnAmount = this.n(freshRecord.returnAmount);
    const customerPayableAmount = Math.max(
      0,
      exchangeAmount + shippingFee - returnAmount,
    );

    const originalAddress = this.buildFullAddress(originalOrder);
    const originalNote = String(originalOrder.note || "");
    const originalShippingMode =
      this.parseStructuredNoteValue(originalNote, "Cách giao:") || "Giao hàng";
    const originalShippingPartner =
      shippingPartner ||
      this.parseStructuredNoteValue(originalNote, "Đơn vị giao:") ||
      "GHN";
    const originalShippingNote =
      this.parseStructuredNoteValue(originalNote, "Ghi chú giao hàng:") ||
      "Cho xem hàng, không cho thử";
    const exchangeOrderNote = [
      `Ghi chú: Đơn đổi/trả từ phiếu ${freshRecord.code}`,
      originalAddress ? `Địa chỉ: ${originalAddress}` : "",
      `Cách giao: ${originalShippingMode}`,
      `Đơn vị giao: ${originalShippingPartner}`,
      `Ghi chú giao hàng: ${originalShippingNote}`,
      `Phí ship: ${shippingFee.toLocaleString("vi-VN")}đ`,
      `Còn phải trả: ${customerPayableAmount.toLocaleString("vi-VN")}đ`,
      `Đơn gốc: ${originalOrder.orderCode}`,
      body?.note ? `Ghi chú nội bộ: ${body.note}` : "",
    ]
      .filter(Boolean)
      .join(" | ");

    let exchangeOrder = await this.orderService.createOrder(
      {
        mode: "approve",
        salesChannel: originalOrder.salesChannel || "OTHER",
        customerId: originalOrder.customerId || undefined,
        customerName:
          originalOrder.customerName ||
          originalOrder.shippingRecipientName ||
          "Khách hàng",
        customerPhone:
          originalOrder.customerPhone || originalOrder.shippingPhone || "",
        branchId:
          body.exchangeIssueBranchId ||
          body.handledAtBranchId ||
          originalOrder.branchId,
        discountAmount: returnAmount,
        shippingFee,
        note: exchangeOrderNote,
        shippingSnapshot: this.buildShippingSnapshotFromOriginalOrder(
          originalOrder,
          shippingFee,
          shippingPartner,
          body,
        ),
        items: exchangeItems.map((item: any) => ({
          variantId: String(item.variantId || item.id || ""),
          quantity: this.n(item.qty),
        })),
      },
      this.buildExchangeOrderUser(user),
    );

    const exchangeOrderId = String((exchangeOrder as any)?.id || "");
    if (!exchangeOrderId) {
      throw new BadRequestException("Không tạo được đơn đổi mới.");
    }

    // Khóa số tiền của đơn đổi mới theo đúng công thức đổi/trả, tránh promotion hoặc giá DB làm lệch COD.
    const lockedExchangeOrder = await this.prisma.order.update({
      where: { id: exchangeOrderId },
      data: {
        totalAmount: new Prisma.Decimal(exchangeAmount),
        discountAmount: new Prisma.Decimal(returnAmount),
        shippingFee: new Prisma.Decimal(shippingFee),
        finalAmount: new Prisma.Decimal(customerPayableAmount),
        paymentStatus:
          customerPayableAmount > 0
            ? PaymentStatus.PENDING_COD
            : PaymentStatus.PAID,
        shippingRecipientName:
          originalOrder.shippingRecipientName ||
          originalOrder.customerName ||
          null,
        shippingPhone:
          originalOrder.shippingPhone || originalOrder.customerPhone || null,
        shippingAddressLine1: originalOrder.shippingAddressLine1 || null,
        shippingAddressLine2: originalOrder.shippingAddressLine2 || null,
        shippingWard: originalOrder.shippingWard || null,
        shippingDistrict: originalOrder.shippingDistrict || null,
        shippingCity: originalOrder.shippingCity || null,
        shippingProvince: originalOrder.shippingProvince || null,
        shippingCountry: originalOrder.shippingCountry || "VN",
        shippingPostalCode: originalOrder.shippingPostalCode || null,
        shippingGhnDistrictId: originalOrder.shippingGhnDistrictId || null,
        shippingGhnWardCode: originalOrder.shippingGhnWardCode || null,
        note: [
          (exchangeOrder as any)?.note,
          `Bù trừ đổi/trả: hàng đổi ${exchangeAmount.toLocaleString("vi-VN")}đ - hàng trả ${returnAmount.toLocaleString("vi-VN")}đ + ship ${shippingFee.toLocaleString("vi-VN")}đ = COD ${customerPayableAmount.toLocaleString("vi-VN")}đ`,
        ]
          .filter(Boolean)
          .join(" | "),
      },
      include: { items: true, shipment: true, payments: true },
    });

    exchangeOrder = lockedExchangeOrder;

    let shipmentResult: any = null;
    try {
      shipmentResult = await this.createShipmentForExchangeOrder({
        exchangeOrder,
        shippingPartner,
        shippingFee,
        body,
        selectedQuote,
        shippingNote: originalShippingNote,
        user,
      });
    } catch (error) {
      await this.prisma.returnExchange.update({
        where: { id: freshRecord.id },
        data: {
          exchangeOrderId,
          exchangeOrderCode: (exchangeOrder as any)?.orderCode || null,
          exchangeCarrier: shippingPartner || "GHN",
          note: [
            freshRecord.note,
            `Đã tạo đơn đổi ${(exchangeOrder as any)?.orderCode || exchangeOrderId} nhưng chưa gửi được HVC: ${error instanceof Error ? error.message : String(error)}`,
          ]
            .filter(Boolean)
            .join(" | "),
        },
      });
      throw error;
    }

    const shipment = shipmentResult?.shipment || null;

    const updated = await this.prisma.returnExchange.update({
      where: { id: freshRecord.id },
      data: {
        shippingFee: new Prisma.Decimal(shippingFee),
        customerPayableAmount: new Prisma.Decimal(customerPayableAmount),
        exchangeOrderId,
        exchangeOrderCode: (exchangeOrder as any)?.orderCode || null,
        exchangeShipmentId: shipment?.id || null,
        exchangeTrackingCode:
          shipment?.trackingCode || shipmentResult?.ghn?.order_code || null,
        exchangeCarrier: shipment?.carrier || shippingPartner || "GHN",
      },
      include: {
        items: true,
        cashVouchers: true,
      },
    });

    return {
      ...this.map(updated),
      exchangeOrder,
      shipment: shipmentResult,
    };
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

  async getReturnsByOrder(orderIdOrCode: string, user?: any) {
    const key = String(orderIdOrCode || "").trim();

    if (!key) {
      throw new BadRequestException("Thiếu mã đơn gốc.");
    }

    const order = await this.prisma.order.findFirst({
      where: {
        OR: [{ id: key }, { orderCode: key }],
      },
      select: {
        id: true,
        orderCode: true,
        branchId: true,
      },
    });

    const candidateOriginalOrderIds = Array.from(
      new Set([order?.id, order?.orderCode, key].filter(Boolean).map(String)),
    );

    if (order?.branchId) {
      this.ensureBranch(user, order.branchId);
    }

    const userBranchId = this.userBranch(user);

    const rows = await this.prisma.returnExchange.findMany({
      where: {
        originalOrderId: { in: candidateOriginalOrderIds },
        ...(this.isOwner(user) || !userBranchId
          ? {}
          : {
              OR: [
                { originalBranchId: userBranchId },
                { handledAtBranchId: userBranchId },
                { returnReceiveBranchId: userBranchId },
                { exchangeIssueBranchId: userBranchId },
              ],
            }),
      },
      orderBy: { createdAt: "desc" },
      take: 50,
      include: {
        items: true,
        cashVouchers: true,
      },
    });

    return {
      data: rows.map((row) => ({
        ...this.map(row),
        originalOrderCode:
          (row as any).originalOrder?.orderCode || order?.orderCode || null,
        originalOrderCustomerName:
          (row as any).originalOrder?.customerName || null,
        originalOrderCustomerPhone:
          (row as any).originalOrder?.customerPhone || null,
      })),
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

  private mapOrderForReturn(order: any) {
    return {
      id: order.id,
      orderCode: order.orderCode,
      customerName: order.customerName,
      customerPhone: order.customerPhone,

      note: order.note || null,
      shippingRecipientName:
        order.shippingRecipientName || order.customerName || null,
      shippingPhone: order.shippingPhone || order.customerPhone || null,
      shippingEmail: order.shippingEmail || null,
      shippingAddressLine1: order.shippingAddressLine1 || null,
      shippingAddressLine2: order.shippingAddressLine2 || null,
      shippingWard: order.shippingWard || null,
      shippingDistrict: order.shippingDistrict || null,
      shippingCity: order.shippingCity || null,
      shippingProvince: order.shippingProvince || null,
      shippingPostalCode: order.shippingPostalCode || null,
      shippingGhnDistrictId: order.shippingGhnDistrictId || null,
      shippingGhnWardCode: order.shippingGhnWardCode || null,

      branchId: order.branchId,
      createdByStaffId: order.createdByStaffId,
      createdByStaffName: order.createdByStaffName,
      salesChannel: order.salesChannel,
      status: order.status,
      soldAt: order.soldAt ? new Date(order.soldAt).toISOString() : null,
      createdAt: order.createdAt
        ? new Date(order.createdAt).toISOString()
        : null,

      totalAmount: this.n(order.totalAmount),
      finalAmount: this.n(order.finalAmount),
      paymentStatus: order.paymentStatus,
      fulfillmentStatus: order.fulfillmentStatus,
      isReturnable: this.isReturnableOrder(order),

      items: (order.items || []).map((item: any) => ({
        id: item.id,
        variantId: item.variantId,
        sku: item.sku,
        productName: item.productName,
        color: item.color,
        size: item.size,
        qty: this.n(item.qty),
        unitPrice: this.n(item.unitPrice),
        lineTotal: this.n(item.lineTotal),
      })),

      payments: (order.payments || []).map((payment: any) => ({
        id: payment.id,
        amount: this.n(payment.amount),
        method: payment.method,
        sourceName: payment.paymentSource?.name || payment.method,
        paymentSourceId: payment.paymentSourceId,
        paymentSource: payment.paymentSource
          ? {
              id: payment.paymentSource.id,
              name: payment.paymentSource.name,
              code: payment.paymentSource.code,
              type: payment.paymentSource.type,
            }
          : null,
      })),
    };
  }

  async getSourceOrderForReturn(orderId: string, user?: any) {
    const id = String(orderId || "").trim();

    if (!id) {
      throw new BadRequestException("Thiếu orderId.");
    }

    const order = await this.prisma.order.findFirst({
      where: {
        OR: [{ id }, { orderCode: id }],
      },
      include: {
        items: true,
        payments: {
          include: {
            paymentSource: true,
          },
        },
      },
    });

    if (!order) {
      throw new BadRequestException("Không tìm thấy đơn hàng gốc.");
    }

    this.ensureBranch(user, order.branchId);

    return this.mapOrderForReturn(order);
  }

  async searchOrdersForReturn(q: string, user?: any) {
    const keyword = String(q || "").trim();

    if (keyword.length < 2) {
      return [];
    }

    const filters: any[] = [
      {
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
    ];

    if (!this.isOwner(user)) {
      filters.push({ branchId: this.userBranch(user) || "__NO_BRANCH__" });
    }

    const rows = await this.prisma.order.findMany({
      where: { AND: filters },
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

    return rows.map((order: any) => this.mapOrderForReturn(order));
  }
}
