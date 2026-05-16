import {
  Body,
  Controller,
  Get,
  Param,
  Post,
  Query,
  Req,
  UseGuards,
} from "@nestjs/common";
import { Request } from "express";

import { JwtGuard } from "../auth/jwt.guard";
import { PermissionGuard } from "../auth/guards/permission.guard";
import { RequirePermissions } from "../auth/decorators/require-permissions.decorator";

import { ReturnsService } from "./returns.service";

@UseGuards(JwtGuard, PermissionGuard)
@Controller("returns")
export class ReturnsController {
  constructor(private readonly returnsService: ReturnsService) {}

  @Post()
  @RequirePermissions("returns.create")
  createReturn(
    @Body() body: any,
    @Req() req: Request & { user?: any },
  ) {
    return this.returnsService.createReturn(body, req.user);
  }

  @Get()
  @RequirePermissions("returns.view")
  getReturns(
    @Query("q") q: string,
    @Query("status") status: string,
    @Query("branchId") branchId: string,
    @Req() req: Request & { user?: any },
  ) {
    return this.returnsService.getReturns(
      { q, status, branchId },
      req.user,
    );
  }

  @Get("search-orders")
  @RequirePermissions("returns.create")
  searchOrdersForReturn(
    @Query("q") q: string,
    @Req() req: Request & { user?: any },
  ) {
    return this.returnsService.searchOrdersForReturn(q, req.user);
  }

  @Get("source-order/:orderId")
  @RequirePermissions("returns.create")
  getSourceOrderForReturn(
    @Param("orderId") orderId: string,
    @Req() req: Request & { user?: any },
  ) {
    return this.returnsService.getSourceOrderForReturn(orderId, req.user);
  }

  @Get("by-order/:orderId")
  @RequirePermissions("returns.create")
  getReturnsByOrder(
    @Param("orderId") orderId: string,
    @Req() req: Request & { user?: any },
  ) {
    return this.returnsService.getReturnsByOrder(orderId, req.user);
  }

  @Get(":id")
  @RequirePermissions("returns.view")
  getReturnDetail(
    @Param("id") id: string,
    @Req() req: Request & { user?: any },
  ) {
    return this.returnsService.getReturnDetail(id, req.user);
  }
}