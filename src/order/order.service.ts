import { BadRequestException, Injectable } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { Prisma } from '@prisma/client';

type CreateOrderItemInput = {
  variantId: string;
  quantity: number;
};

type CreateOrderInput = {
  salesChannel?: 'VN_WEB' | 'INTL_WEB' | 'FACEBOOK_MANUAL' | 'SHOWROOM' | 'OTHER';
  currency?: string;
  note?: string;
  customerId?: string;
  items: CreateOrderItemInput[];
};

@Injectable()
export class OrderService {
  constructor(private readonly prisma: PrismaService) {}

  async createOrder(data: CreateOrderInput) {
    if (!data.items || data.items.length === 0) {
      throw new BadRequestException('Order must have at least 1 item');
    }

    return this.prisma.$transaction(async (tx) => {
      let subtotal = new Prisma.Decimal(0);

      const orderItemsData: Array<{
        variantId: string;
        sku: string;
        productName: string;
        color?: string;
        size?: string;
        qty: number;
        unitPrice: Prisma.Decimal;
        lineTotal: Prisma.Decimal;
      }> = [];

      for (const item of data.items) {
        const variant = await tx.productVariant.findUnique({
          where: { id: item.variantId },
          include: {
            product: true,
            inventoryItem: true,
          },
        });

        if (!variant) {
          throw new BadRequestException(`Variant not found: ${item.variantId}`);
        }

        if (!variant.inventoryItem) {
          throw new BadRequestException(`Inventory item missing for variant: ${variant.sku}`);
        }

        if (variant.inventoryItem.availableQty < item.quantity) {
          throw new BadRequestException(
            `Not enough stock for ${variant.sku}. Available: ${variant.inventoryItem.availableQty}`,
          );
        }

        const unitPrice = new Prisma.Decimal(variant.priceVnd);
        const lineTotal = unitPrice.mul(item.quantity);
        subtotal = subtotal.add(lineTotal);

        orderItemsData.push({
          variantId: variant.id,
          sku: variant.sku,
          productName: variant.product.name,
          color: variant.color ?? undefined,
          size: variant.size ?? undefined,
          qty: item.quantity,
          unitPrice,
          lineTotal,
        });
      }

      const discountTotal = new Prisma.Decimal(0);
      const shippingFee = new Prisma.Decimal(0);
      const grandTotal = subtotal.add(shippingFee).sub(discountTotal);

      const order = await tx.order.create({
        data: {
          orderCode: `ORD-${Date.now()}`,
          salesChannel: data.salesChannel ?? 'VN_WEB',
          customerId: data.customerId,
          currency: data.currency ?? 'VND',
          subtotal,
          discountTotal,
          shippingFee,
          grandTotal,
          paymentStatus: 'AWAITING_PAYMENT',
          fulfillmentStatus: 'UNFULFILLED',
          orderStatus: 'NEW',
          note: data.note,
          items: {
            create: orderItemsData,
          },
        },
        include: {
          items: true,
        },
      });

      for (const item of data.items) {
        await tx.inventoryItem.update({
          where: { variantId: item.variantId },
          data: {
            availableQty: {
              decrement: item.quantity,
            },
          },
        });

        await tx.inventoryMovement.create({
          data: {
            variantId: item.variantId,
            type: 'SALE',
            qty: -item.quantity,
            refType: 'ORDER',
            refId: order.id,
            note: `Sold via order ${order.orderCode}`,
          },
        });
      }

      return order;
    });
  }

  async getOrders() {
    return this.prisma.order.findMany({
      orderBy: { createdAt: 'desc' },
      include: {
        items: true,
        customer: true,
      },
    });
  }

  async getOrderById(id: string) {
    return this.prisma.order.findUnique({
      where: { id },
      include: {
        items: true,
        customer: true,
        payments: true,
        shipments: true,
      },
    });
  }

  async updateOrderStatus(id: string, orderStatus: string) {
    const allowed = ['NEW', 'CONFIRMED', 'PACKING', 'SHIPPED', 'COMPLETED', 'CANCELLED'];

    if (!allowed.includes(orderStatus)) {
      throw new BadRequestException('Invalid order status');
    }

    return this.prisma.$transaction(async (tx) => {
      const existingOrder = await tx.order.findUnique({
        where: { id },
        include: { items: true },
      });

      if (!existingOrder) {
        throw new BadRequestException('Order not found');
      }

      // Nếu đã cancelled rồi thì không làm lại
      if (existingOrder.orderStatus === 'CANCELLED' && orderStatus === 'CANCELLED') {
        return existingOrder;
      }

      // Nếu chuyển sang CANCELLED thì cộng kho lại
      if (orderStatus === 'CANCELLED' && existingOrder.orderStatus !== 'CANCELLED') {
        for (const item of existingOrder.items) {
          if (!item.variantId) continue;

          await tx.inventoryItem.update({
            where: { variantId: item.variantId },
            data: {
              availableQty: {
                increment: item.qty,
              },
            },
          });

          await tx.inventoryMovement.create({
            data: {
              variantId: item.variantId,
              type: 'CANCEL',
              qty: item.qty,
              refType: 'ORDER',
              refId: existingOrder.id,
              note: `Restock from cancelled order ${existingOrder.orderCode}`,
            },
          });
        }
      }

      const updated = await tx.order.update({
        where: { id },
        data: { orderStatus: orderStatus as any },
        include: {
          items: true,
          customer: true,
          payments: true,
          shipments: true,
        },
      });

      return updated;
    });
  }

  async updatePaymentStatus(id: string, paymentStatus: string) {
    const allowed = ['AWAITING_PAYMENT', 'PAID', 'PENDING_COD', 'REFUNDED', 'FAILED'];

    if (!allowed.includes(paymentStatus)) {
      throw new BadRequestException('Invalid payment status');
    }

    return this.prisma.order.update({
      where: { id },
      data: { paymentStatus: paymentStatus as any },
      include: {
        items: true,
        customer: true,
        payments: true,
        shipments: true,
      },
    });
  } 
 }  