import { Controller, Get, Query, UseGuards } from "@nestjs/common";
import { JwtGuard } from "../../auth/jwt.guard";
import { MobileProductsService } from "./mobile-products.service";

@Controller("mobile/products")
@UseGuards(JwtGuard)
export class MobileProductsController {
  constructor(private readonly service: MobileProductsService) {}

  @Get()
  getProducts(
    @Query("q") q?: string,
    @Query("branchId") branchId?: string,
    @Query("status") status?: string,
    @Query("category") category?: string,
    @Query("take") take?: string
  ) {
    return this.service.getProducts({
      q,
      branchId,
      status,
      category,
      take: Number(take || 50),
    });
  }
}
