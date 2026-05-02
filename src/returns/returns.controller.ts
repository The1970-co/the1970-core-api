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
import { ReturnsService } from "./returns.service";

@UseGuards(JwtGuard)
@Controller("returns")
export class ReturnsController {
  constructor(private readonly returnsService: ReturnsService) {}

  // CREATE
  @Post()
  createReturn(
    @Body() body: any,
    @Req() req: Request & { user?: any }
  ) {
    return this.returnsService.createReturn(body, req.user);
  }

  // LIST
  @Get()
  getReturns(
    @Query("q") q: string,
    @Query("status") status: string,
    @Query("branchId") branchId: string,
    @Req() req: Request & { user?: any }
  ) {
    return this.returnsService.getReturns(
      { q, status, branchId },
      req.user
    );
  }

  // 🔥 SEARCH ALL BRANCH (quan trọng nhất)
  @Get("search-orders")
  searchOrdersForReturn(
    @Query("q") q: string,
    @Req() req: Request & { user?: any }
  ) {
    return this.returnsService.searchOrdersForReturn(q, req.user);
  }

  // DETAIL
  @Get(":id")
  getReturnDetail(
    @Param("id") id: string,
    @Req() req: Request & { user?: any }
  ) {
    return this.returnsService.getReturnDetail(id, req.user);
  }
}