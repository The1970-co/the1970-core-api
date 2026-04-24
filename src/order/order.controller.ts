import {
  Body,
  Controller,
  Get,
  Param,
  Patch,
  Post,
  Query,
  Req,
  UseGuards,
} from "@nestjs/common";
import { Request } from "express";
import { PaymentStatus } from "@prisma/client";
import { JwtGuard } from "../auth/jwt.guard";
import { OrderService } from "./order.service";
import { UpdateOrderStatusDto } from "./dto/update-order-status.dto";
import { CreateOrderDto } from "./dto/create-order.dto";

@UseGuards(JwtGuard)
@Controller("orders")
export class OrderController {
  constructor(private readonly orderService: OrderService) {}

  @Post()
  async createOrder(
    @Body() body: CreateOrderDto,
    @Req() req: Request & { user?: any }
  ) {
    return this.orderService.createOrder(body, req.user);
  }

  @Get()
  async getOrders(
    @Req() req: Request & { user?: any },
    @Query("page") page?: string,
    @Query("pageSize") pageSize?: string,
    @Query("q") q?: string,
    @Query("branchId") branchId?: string,
    @Query("orderStatus") orderStatus?: string,
    @Query("paymentStatus") paymentStatus?: string,
    @Query("dateFrom") dateFrom?: string,
    @Query("dateTo") dateTo?: string
  ) {
    return this.orderService.getOrders(
      {
        page: Number(page || 1),
        pageSize: Number(pageSize || 50),
        q: q || "",
        branchId: branchId || "",
        orderStatus: orderStatus || "",
        paymentStatus: paymentStatus || "",
        dateFrom: dateFrom || "",
        dateTo: dateTo || "",
      },
      req.user
    );
  }

  @Get(":id")
  async getOrderById(
    @Param("id") id: string,
    @Req() req: Request & { user?: any }
  ) {
    return this.orderService.getOrderById(id, req.user);
  }

  @Patch(":id")
  async updateOrder(
    @Param("id") id: string,
    @Body() body: any,
    @Req() req: Request & { user?: any }
  ) {
    return this.orderService.updateOrder(id, body, req.user);
  }

  @Patch(":id/status")
  async updateOrderStatus(
    @Param("id") id: string,
    @Body() body: UpdateOrderStatusDto,
    @Req() req: Request & { user?: any }
  ) {
    return this.orderService.updateOrderStatus(id, body.status, req.user);
  }

  @Patch(":id/payment-status")
  async updatePaymentStatus(
    @Param("id") id: string,
    @Body() body: { paymentStatus: PaymentStatus },
    @Req() req: Request & { user?: any }
  ) {
    return this.orderService.updatePaymentStatus(
      id,
      body.paymentStatus,
      req.user
    );
  }

  @Post(":id/ship")
  async shipOrder(
    @Param("id") id: string,
    @Body() body: { weight: number; shippingFee?: number; note?: string },
    @Req() req: Request & { user?: any }
  ) {
    return this.orderService.shipOrder(id, body, req.user);
  }

  @Get("inventory-movements/history")
  async getInventoryMovements(
    @Query("limit") limit?: string,
    @Req() req?: Request & { user?: any }
  ) {
    const parsedLimit = Number(limit || 100);
    return this.orderService.getInventoryMovements(parsedLimit, req?.user);
  }
}