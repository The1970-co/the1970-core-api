import {
  Body,
  Controller,
  Delete,
  ForbiddenException,
  Get,
  Param,
  Patch,
  Post,
  Query,
  Req,
  UseGuards,
} from "@nestjs/common";
import { Request } from "express";
import { OrderStatus, PaymentStatus } from "@prisma/client";
import { JwtGuard } from "../auth/jwt.guard";
import { PermissionGuard } from "../auth/guards/permission.guard";
import { RequirePermissions } from "../auth/decorators/require-permissions.decorator";
import { OrderService } from "./order.service";
import { UpdateOrderStatusDto } from "./dto/update-order-status.dto";
import { CreateOrderDto } from "./dto/create-order.dto";

@UseGuards(JwtGuard, PermissionGuard)
@Controller("orders")
export class OrderController {
  constructor(private readonly orderService: OrderService) {}

  private getPermissionKeys(user?: any) {
    const keys = new Set<string>();

    const addKeys = (items?: any[]) => {
      if (!Array.isArray(items)) return;
      items.forEach((item) => {
        const key = String(item || "").trim();
        if (key) keys.add(key);
      });
    };

    addKeys(user?.permissions);
    addKeys(user?.permissionKeys);

    if (Array.isArray(user?.branchPermissions)) {
      user.branchPermissions.forEach((row: any) => addKeys(row?.permissionKeys));
    }

    return keys;
  }

  private hasPermission(user: any, permission: string) {
    const role = String(user?.role || "").toLowerCase();
    const roles = Array.isArray(user?.roles)
      ? user.roles.map((item: any) => String(item || "").toLowerCase())
      : [];

    if (role === "owner" || role === "admin" || roles.includes("owner") || roles.includes("admin")) {
      return true;
    }

    const keys = this.getPermissionKeys(user);
    return keys.has("*") || keys.has(permission);
  }

  private assertPermission(user: any, permission: string) {
    if (!this.hasPermission(user, permission)) {
      throw new ForbiddenException("Bạn không có quyền thực hiện thao tác này");
    }
  }

  private permissionForOrderStatus(status?: OrderStatus | string | null) {
    const next = String(status || "").trim().toUpperCase();

    if (next === "CANCELLED") return "orders.cancel";
    if (next === "APPROVED") return "orders.approve";
    if (next === "PACKING" || next === "SHIPPED" || next === "COMPLETED") {
      return "orders.pack_ship";
    }

    return "orders.edit";
  }

  @Post()
  @RequirePermissions("orders.create")
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


  @Get("assignable-staff")
  async getAssignableStaffForOrders(@Req() req: Request & { user?: any }) {
    return this.orderService.getAssignableStaffForOrders(req.user);
  }

  @Get(":id")
  async getOrderById(
    @Param("id") id: string,
    @Req() req: Request & { user?: any }
  ) {
    return this.orderService.getOrderById(id, req.user);
  }


  @Patch(":id/assign-staff")
  @RequirePermissions("orders.edit")
  async assignStaffToOrder(
    @Param("id") id: string,
    @Body() body: { assignedStaffId?: string | null },
    @Req() req: Request & { user?: any }
  ) {
    return this.orderService.assignStaffToOrder(
      id,
      body.assignedStaffId || null,
      req.user
    );
  }

  @Patch(":id")
  @RequirePermissions("orders.edit")
  async updateOrder(
    @Param("id") id: string,
    @Body() body: any,
    @Req() req: Request & { user?: any }
  ) {
    return this.orderService.updateOrder(id, body, req.user);
  }



  @Delete(":id")
  @RequirePermissions("orders.delete")
  async deleteOrder(
    @Param("id") id: string,
    @Req() req: Request & { user?: any }
  ) {
    return this.orderService.deleteOrder(id, req.user);
  }

  @Patch(":id/status")
  async updateOrderStatus(
    @Param("id") id: string,
    @Body() body: UpdateOrderStatusDto,
    @Req() req: Request & { user?: any }
  ) {
    this.assertPermission(req.user, this.permissionForOrderStatus(body.status));
    return this.orderService.updateOrderStatus(id, body.status, req.user);
  }

  @Patch(":id/payment-status")
  @RequirePermissions("orders.pay")
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
  @RequirePermissions("orders.pack_ship")
  async shipOrder(
    @Param("id") id: string,
    @Body() body: { weight: number; shippingFee?: number; note?: string },
    @Req() req: Request & { user?: any }
  ) {
    return this.orderService.shipOrder(id, body, req.user);
  }

  @Get("inventory-movements/history")
  @RequirePermissions("inventory.logs.view")
  async getInventoryMovements(
    @Query("limit") limit?: string,
    @Req() req?: Request & { user?: any }
  ) {
    const parsedLimit = Number(limit || 100);
    return this.orderService.getInventoryMovements(parsedLimit, req?.user);
  }
}