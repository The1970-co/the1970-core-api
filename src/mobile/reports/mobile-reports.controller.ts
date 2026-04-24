import { Controller, Get, Query, UseGuards } from "@nestjs/common";
import { JwtGuard } from "../../auth/jwt.guard";
import { MobileReportsService } from "./mobile-reports.service";

@Controller("mobile/reports")
@UseGuards(JwtGuard)
export class MobileReportsController {
  constructor(private readonly service: MobileReportsService) {}

  @Get("sales")
  getSales(
    @Query("days") days?: string,
    @Query("branchId") branchId?: string
  ) {
    return this.service.getSales(Number(days || 7), branchId);
  }

  @Get("inventory")
  getInventory(@Query("branchId") branchId?: string) {
    return this.service.getInventory(branchId);
  }

  @Get("top-products")
  getTopProducts(@Query("branchId") branchId?: string) {
    return this.service.getTopProducts(branchId);
  }
  @Get("low-stock-detail")
getLowStockDetail(@Query("branchId") branchId?: string) {
  return this.service.getLowStockDetail(branchId);
}

@Get("out-of-stock-detail")
getOutOfStockDetail(@Query("branchId") branchId?: string) {
  return this.service.getOutOfStockDetail(branchId);
}
}