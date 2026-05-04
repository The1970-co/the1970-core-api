import { Injectable } from "@nestjs/common";
import {
  Promotion,
  PromotionDiscountType,
  PromotionProduct,
  PromotionType,
} from "@prisma/client";

type PromotionWithProducts = Promotion & {
  products: PromotionProduct[];
};

export type PromotionCartItem = {
  productId?: string | null;
  variantId: string;
  quantity: number;
  unitPrice: number;
  discountAmount?: number;
  finalUnitPrice?: number;
  appliedPromotionIds?: string[];
};

export type PromotionCartInput = {
  items: PromotionCartItem[];
  promotions: PromotionWithProducts[];
};

@Injectable()
export class PromotionEngineService {
  apply(input: PromotionCartInput) {
    const items = input.items.map((item) => ({
      ...item,
      discountAmount: item.discountAmount ?? 0,
      finalUnitPrice: item.finalUnitPrice ?? item.unitPrice,
      appliedPromotionIds: item.appliedPromotionIds ?? [],
    }));

    let orderDiscountAmount = 0;
    const appliedPromotions: {
      id: string;
      name: string;
      type: PromotionType;
      discountAmount: number;
    }[] = [];

    const subtotal = items.reduce(
      (sum, item) => sum + item.unitPrice * item.quantity,
      0,
    );

    const promotions = [...input.promotions].sort(
      (a, b) => b.priority - a.priority,
    );

    for (const promotion of promotions) {
      if (promotion.type === PromotionType.PRODUCT_DISCOUNT) {
        const targetProductIds = new Set(
          promotion.products.map((p) => String(p.productId)),
        );

        let totalDiscountForPromotion = 0;

        for (const item of items) {
          if (!item.productId) continue;
          if (!targetProductIds.has(String(item.productId))) continue;

          const alreadyDiscountedPerUnit =
            Number(item.discountAmount || 0) / Math.max(1, item.quantity);
          const baseForThisPromotion = Math.max(
            0,
            item.unitPrice - alreadyDiscountedPerUnit,
          );

          const discountPerUnit = this.calculateDiscount({
            discountType: promotion.discountType,
            discountValue: Number(promotion.discountValue),
            baseAmount: baseForThisPromotion,
          });

          const safeDiscountPerUnit = Math.min(
            Math.max(0, discountPerUnit),
            baseForThisPromotion,
          );
          const discountAmount = safeDiscountPerUnit * item.quantity;

          item.discountAmount = Number(item.discountAmount || 0) + discountAmount;
          item.finalUnitPrice = Math.max(
            0,
            item.unitPrice - Number(item.discountAmount || 0) / item.quantity,
          );
          item.appliedPromotionIds?.push(promotion.id);

          totalDiscountForPromotion += discountAmount;
        }

        if (totalDiscountForPromotion > 0) {
          appliedPromotions.push({
            id: promotion.id,
            name: promotion.name,
            type: promotion.type,
            discountAmount: totalDiscountForPromotion,
          });
        }
      }

      if (promotion.type === PromotionType.ORDER_DISCOUNT) {
        const minAmount = promotion.minOrderAmount
          ? Number(promotion.minOrderAmount)
          : 0;

        if (subtotal < minAmount) continue;

        const productDiscountSoFar = items.reduce(
          (sum, item) => sum + Number(item.discountAmount || 0),
          0,
        );
        const orderBase = Math.max(0, subtotal - productDiscountSoFar - orderDiscountAmount);

        const discountAmount = this.calculateDiscount({
          discountType: promotion.discountType,
          discountValue: Number(promotion.discountValue),
          baseAmount: orderBase,
        });

        const safeDiscount = Math.min(Math.max(0, discountAmount), orderBase);

        if (safeDiscount > 0) {
          orderDiscountAmount += safeDiscount;

          appliedPromotions.push({
            id: promotion.id,
            name: promotion.name,
            type: promotion.type,
            discountAmount: safeDiscount,
          });
        }
      }
    }

    const productDiscountAmount = items.reduce(
      (sum, item) => sum + Number(item.discountAmount || 0),
      0,
    );

    const totalDiscountAmount = productDiscountAmount + orderDiscountAmount;

    return {
      items,
      subtotal,
      productDiscountAmount,
      orderDiscountAmount,
      totalDiscountAmount,
      totalAfterDiscount: Math.max(0, subtotal - totalDiscountAmount),
      appliedPromotions,
    };
  }

  private calculateDiscount(input: {
    discountType: PromotionDiscountType;
    discountValue: number;
    baseAmount: number;
  }) {
    if (input.discountType === PromotionDiscountType.PERCENT) {
      return (input.baseAmount * input.discountValue) / 100;
    }

    return input.discountValue;
  }
}
