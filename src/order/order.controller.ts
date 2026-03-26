import { Body, Controller, Get, Param, Patch, Post } from '@nestjs/common';
import { OrderService } from './order.service';

@Controller('orders')
export class OrderController {
  constructor(private readonly orderService: OrderService) {}

  @Post()
  create(@Body() body: any) {
    return this.orderService.createOrder(body);
  }

  @Get()
  findAll() {
    return this.orderService.getOrders();
  }

  @Get(':id')
  findOne(@Param('id') id: string) {
    return this.orderService.getOrderById(id);
  }

  @Patch(':id/status')
  updateStatus(
    @Param('id') id: string,
    @Body() body: { orderStatus: string },
  ) {
    return this.orderService.updateOrderStatus(id, body.orderStatus);
  }

  @Patch(':id/payment-status')
  updatePaymentStatus(
    @Param('id') id: string,
    @Body() body: { paymentStatus: string },
  ) {
    return this.orderService.updatePaymentStatus(id, body.paymentStatus);
  }
}