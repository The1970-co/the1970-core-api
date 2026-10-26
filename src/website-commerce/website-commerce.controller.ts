import { Body, Controller, Get, Post, Query } from "@nestjs/common";
import { WebsiteCommerceService } from "./website-commerce.service";

@Controller("storefront/commerce")
export class WebsiteCommerceController {
  constructor(private readonly service: WebsiteCommerceService) {}

  @Get("customer-status")
  customerStatus(
    @Query("phone") phone: string,
    @Query("market") market?: string,
  ) {
    return this.service.customerStatus(phone, market || "VN");
  }

  @Post("quote")
  quote(@Body() body: any) {
    return this.service.quote(body);
  }

  @Post("orders")
  createOrder(@Body() body: any) {
    return this.service.createOrder(body);
  }
}
