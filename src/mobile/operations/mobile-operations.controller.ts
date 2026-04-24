import {
  Controller,
  Get,
  Post,
  Patch,
  Delete,
  Body,
  Param,
  Query,
  UseGuards,
} from "@nestjs/common";

import { JwtGuard } from "../../auth/jwt.guard";
import { MobileOperationsService } from "./mobile-operations.service";

@Controller("mobile/operations")
@UseGuards(JwtGuard)
export class MobileOperationsController {
  constructor(private readonly service: MobileOperationsService) {}

  @Get("orders-summary")
  getOrderSummary(@Query("branchId") branchId?: string) {
    return this.service.getOrderSummary(branchId);
  }

  @Get("inventory-search")
  searchInventory(
    @Query("q") q?: string,
    @Query("branchId") branchId?: string
  ) {
    return this.service.searchInventory(q, branchId);
  }
  @Get("inventory-grouped")
getGroupedInventory(
  @Query("q") q?: string,
  @Query("branchId") branchId?: string
) {
  return this.service.getGroupedInventory(q, branchId);
}
@Get("transfer-suggestions")
getTransferSuggestions() {
  return this.service.getTransferSuggestions();
}
@Get("orders")
getOrders(
  @Query("status") status?: string,
  @Query("branchId") branchId?: string
) {
  return this.service.getOrders(status, branchId);
}
@Get("orders/:id")
getOrderDetail(@Param("id") id: string) {
  return this.service.getOrderDetail(id);
}
}