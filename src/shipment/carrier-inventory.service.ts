import { BadRequestException, Injectable } from "@nestjs/common";

@Injectable()
export class CarrierInventoryService {
  getActorName(user?: any) {
    return String(
      user?.name ||
        user?.fullName ||
        user?.username ||
        user?.email ||
        user?.code ||
        user?.id ||
        user?.sub ||
        "system",
    ).trim() || "system";
  }

  private normalizeCarrier(input?: string | null) {
    return String(input || "CARRIER")
      .trim()
      .toUpperCase()
      .replace(/[^A-Z0-9]+/g, "_")
      .replace(/^_+|_+$/g, "") || "CARRIER";
  }

  private carrierLabel(input?: string | null) {
    const carrier = this.normalizeCarrier(input);
    if (carrier === "GHN") return "GHN";
    if (carrier === "AHAMOVE") return "AhaMove";
    if (carrier === "VIETTELPOST") return "ViettelPost";
    if (carrier === "SPX") return "SPX";
    return carrier;
  }

  async ensureOrderStockOutForShipment(
    tx: any,
    order: any,
    input?: {
      carrier?: string | null;
      trackingCode?: string | null;
      actorName?: string | null;
      notePrefix?: string | null;
    },
  ) {
    const orderId = String(order?.id || "").trim();
    const branchId = String(order?.branchId || "").trim();

    if (!orderId) {
      throw new BadRequestException("Không tìm thấy order để xuất kho.");
    }

    if (!branchId) {
      throw new BadRequestException("Đơn chưa có chi nhánh, không thể xuất kho khi gửi vận chuyển.");
    }

    const existingSaleMovement = await tx.inventoryMovement.findFirst({
      where: {
        refType: "ORDER",
        refId: orderId,
        type: "SALE",
      },
      select: { id: true },
    });

    if (existingSaleMovement) {
      return {
        stockOutApplied: false,
        stockOutAlreadyApplied: true,
        stockOutItems: [] as Array<{ sku: string; qty: number; beforeQty: number; afterQty: number }>,
      };
    }

    const items = Array.isArray(order?.items) ? order.items : [];
    if (!items.length) {
      throw new BadRequestException("Đơn không có sản phẩm để xuất kho khi gửi vận chuyển.");
    }

    const actorName = String(input?.actorName || "system").trim() || "system";
    const trackingCode = String(input?.trackingCode || "").trim();
    const carrier = this.normalizeCarrier(input?.carrier);
    const carrierLabel = this.carrierLabel(carrier);
    const notePrefix = String(input?.notePrefix || `Trừ kho khi gửi HVC ${carrierLabel}`).trim();
    const createdAt = new Date();
    const stockOutItems: Array<{ sku: string; qty: number; beforeQty: number; afterQty: number }> = [];

    for (const item of items as any[]) {
      const qty = Math.max(0, Math.trunc(Number(item?.qty || item?.quantity || 0)));
      if (!qty) continue;

      let variantId = String(item?.variantId || "").trim();
      if (!variantId && item?.sku) {
        const variant = await tx.productVariant.findFirst({
          where: { sku: String(item.sku).trim() },
          select: { id: true },
        });
        variantId = String(variant?.id || "").trim();
      }

      if (!variantId) {
        throw new BadRequestException(`Không tìm thấy variant để xuất kho cho SKU ${item?.sku || item?.productName || item?.id}.`);
      }

      const inventoryItem = await tx.inventoryItem.findUnique({
        where: {
          variantId_branchId: {
            variantId,
            branchId,
          },
        },
      });

      const beforeQty = Number(inventoryItem?.availableQty || 0);
      const afterQty = beforeQty - qty;

      if (inventoryItem) {
        await tx.inventoryItem.update({
          where: { id: inventoryItem.id },
          data: { availableQty: afterQty },
        });
      } else {
        await tx.inventoryItem.create({
          data: {
            variantId,
            branchId,
            availableQty: afterQty,
            reservedQty: 0,
            incomingQty: 0,
          },
        });
      }

      await tx.inventoryMovement.create({
        data: {
          variantId,
          branchId,
          type: "SALE",
          qty: -qty,
          beforeQty,
          afterQty,
          note: `${notePrefix} từ đơn ${order.orderCode || orderId}${trackingCode ? ` - MVD ${trackingCode}` : ""} | Người gửi HVC: ${actorName}`,
          refType: "ORDER",
          refId: orderId,
          createdById: null,
          createdAt,
        },
      });

      stockOutItems.push({
        sku: String(item?.sku || ""),
        qty,
        beforeQty,
        afterQty,
      });
    }

    return {
      stockOutApplied: stockOutItems.length > 0,
      stockOutAlreadyApplied: false,
      stockOutItems,
    };
  }

  async restoreOrderStockForShipmentCancel(
    tx: any,
    order: any,
    shipment: any,
    input?: {
      carrier?: string | null;
      actorName?: string | null;
      restoreRefType?: string | null;
      notePrefix?: string | null;
    },
  ) {
    const orderId = String(order?.id || shipment?.orderId || "").trim();
    if (!orderId) {
      throw new BadRequestException("Không tìm thấy order để hoàn tồn kho khi huỷ vận chuyển.");
    }

    const carrier = this.normalizeCarrier(input?.carrier || shipment?.carrier);
    const carrierLabel = this.carrierLabel(carrier);
    const restoreRefType = String(input?.restoreRefType || `${carrier}_CANCEL_RESTORE`).trim();
    const actorName = String(input?.actorName || "system").trim() || "system";
    const trackingCode = String(shipment?.trackingCode || shipment?.ahamoveOrderId || "").trim();
    const notePrefix = String(input?.notePrefix || `Hoàn tồn kho do huỷ ${carrierLabel}`).trim();

    const existingRestore = await tx.inventoryMovement.findFirst({
      where: {
        refType: restoreRefType,
        refId: orderId,
      },
      select: { id: true },
    });

    if (existingRestore) {
      return {
        inventoryRestored: false,
        inventoryRestoreAlreadyApplied: true,
        restoredItems: [] as Array<{ variantId: string; qty: number; beforeQty: number; afterQty: number }>,
      };
    }

    const saleMovements = await tx.inventoryMovement.findMany({
      where: {
        refType: "ORDER",
        refId: orderId,
        type: "SALE",
      },
      select: {
        variantId: true,
        branchId: true,
        qty: true,
      },
    });

    const restoredItems: Array<{ variantId: string; qty: number; beforeQty: number; afterQty: number }> = [];

    for (const movement of saleMovements as any[]) {
      const restoreQty = Math.abs(Math.trunc(Number(movement?.qty || 0)));
      const variantId = String(movement?.variantId || "").trim();
      const branchId = String(movement?.branchId || order?.branchId || "").trim();

      if (!restoreQty || !variantId || !branchId) continue;

      const inventoryItem = await tx.inventoryItem.findUnique({
        where: {
          variantId_branchId: {
            variantId,
            branchId,
          },
        },
      });

      const beforeQty = Number(inventoryItem?.availableQty || 0);
      const afterQty = beforeQty + restoreQty;

      if (inventoryItem) {
        await tx.inventoryItem.update({
          where: { id: inventoryItem.id },
          data: { availableQty: afterQty },
        });
      } else {
        await tx.inventoryItem.create({
          data: {
            variantId,
            branchId,
            availableQty: afterQty,
            reservedQty: 0,
            incomingQty: 0,
          },
        });
      }

      await tx.inventoryMovement.create({
        data: {
          variantId,
          branchId,
          type: "CANCEL",
          qty: restoreQty,
          beforeQty,
          afterQty,
          note: `${notePrefix} từ đơn ${order?.orderCode || orderId}${trackingCode ? ` - MVD ${trackingCode}` : ""} | Người huỷ: ${actorName}`,
          refType: restoreRefType,
          refId: orderId,
          createdById: null,
          createdAt: new Date(),
        },
      });

      restoredItems.push({ variantId, qty: restoreQty, beforeQty, afterQty });
    }

    return {
      inventoryRestored: restoredItems.length > 0,
      inventoryRestoreAlreadyApplied: false,
      restoredItems,
    };
  }
}
