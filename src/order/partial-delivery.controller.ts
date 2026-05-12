import { Body, Controller, Get, Param, Post, Req, UseGuards } from "@nestjs/common";
import { Request } from "express";
import { JwtGuard } from "../auth/jwt.guard";
import { PermissionGuard } from "../auth/guards/permission.guard";
import { RequirePermissions } from "../auth/decorators/require-permissions.decorator";
import { OrderService } from "./order.service";

@UseGuards(JwtGuard, PermissionGuard)
@Controller("partial-delivery")
export class PartialDeliveryController {
  constructor(private readonly orderService: OrderService) {}

  @Post()
  @RequirePermissions("orders.edit")
  create(@Body() body: any, @Req() req: Request & { user?: any }) {
    return this.orderService.createPartialDelivery(body, req.user);
  }

  @Get(":id")
  @RequirePermissions("orders.view")
  getOne(@Param("id") id: string, @Req() req: Request & { user?: any }) {
    return this.orderService.getPartialDelivery(id, req.user);
  }
}
