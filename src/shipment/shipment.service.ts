import { BadRequestException, Injectable } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

type CreateShipmentInput = {
  orderId: string;
  carrier: string;
  trackingCode?: string;
  shippingFee?: number;
  codAmount?: number;
  note?: string;
};

@Injectable()
export class ShipmentService {
  constructor(private readonly prisma: PrismaService) {}

  async createShipment(data: CreateShipmentInput) {
    const order = await this.prisma.order.findUnique({
      where: { id: data.orderId },
      include: {
        shipments: true,
      },
    });

    if (!order) {
      throw new BadRequestException('Order not found');
    }

    const shipment = await this.prisma.shipment.create({
      data: {
        orderId: data.orderId,
        carrier: data.carrier,
        trackingCode: data.trackingCode,
        shippingStatus: 'READY_TO_SHIP',
        shippingFee: data.shippingFee,
        codAmount: data.codAmount,
        note: data.note,
      },
    });

    await this.prisma.order.update({
      where: { id: data.orderId },
      data: {
        fulfillmentStatus: 'PROCESSING',
      },
    });

    return this.prisma.order.findUnique({
      where: { id: data.orderId },
      include: {
        items: true,
        customer: true,
        payments: true,
        shipments: true,
      },
    });
  }

  async updateShipmentStatus(id: string, shippingStatus: string) {
    const allowed = [
      'NOT_CREATED',
      'READY_TO_SHIP',
      'PICKED',
      'IN_TRANSIT',
      'DELIVERED',
      'RETURNED',
    ];

    if (!allowed.includes(shippingStatus)) {
      throw new BadRequestException('Invalid shipping status');
    }

    const shipment = await this.prisma.shipment.findUnique({
      where: { id },
    });

    if (!shipment) {
      throw new BadRequestException('Shipment not found');
    }

    const updatedShipment = await this.prisma.shipment.update({
      where: { id },
      data: {
        shippingStatus,
      },
    });

    if (shippingStatus === 'DELIVERED') {
      await this.prisma.order.update({
        where: { id: shipment.orderId },
        data: {
          fulfillmentStatus: 'FULFILLED',
          orderStatus: 'COMPLETED',
        },
      });
    }

    if (shippingStatus === 'IN_TRANSIT') {
      await this.prisma.order.update({
        where: { id: shipment.orderId },
        data: {
          fulfillmentStatus: 'PROCESSING',
          orderStatus: 'SHIPPED',
        },
      });
    }

    if (shippingStatus === 'RETURNED') {
      await this.prisma.order.update({
        where: { id: shipment.orderId },
        data: {
          fulfillmentStatus: 'RETURNED',
        },
      });
    }

    return updatedShipment;
  }

  async getShipments() {
    return this.prisma.shipment.findMany({
      orderBy: { createdAt: 'desc' },
    });
  }

  async getShipmentById(id: string) {
    return this.prisma.shipment.findUnique({
      where: { id },
    });
  }
}