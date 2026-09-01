import { Body, Controller, Get, Param, Post, Req, UseGuards } from "@nestjs/common";
import type { Request } from "express";
import { CustomerJwtGuard } from "./customer-jwt.guard";
import { StorefrontService } from "./storefront.service";

@Controller("storefront")
export class StorefrontController {
  constructor(private readonly service: StorefrontService) {}
  private meta(req: Request) {
    return { userAgent: String(req.headers["user-agent"] || ""), ipAddress: String(req.headers["x-forwarded-for"] || req.socket.remoteAddress || "").split(",")[0].trim() };
  }

  @Get("products") listProducts() { return this.service.listProducts(); }
  @Get("products/:slug") getProduct(@Param("slug") slug: string) { return this.service.getProduct(slug); }

  @Post("auth/register") register(@Body() body: any, @Req() req: Request) { return this.service.register(body, this.meta(req)); }
  @Post("auth/login") login(@Body() body: any, @Req() req: Request) { return this.service.login(body, this.meta(req)); }
  @Post("auth/refresh") refresh(@Body() body: any) { return this.service.refresh(String(body?.refreshToken || "")); }
  @Post("auth/logout") logout(@Body() body: any) { return this.service.logout(String(body?.refreshToken || "")); }

  @UseGuards(CustomerJwtGuard)
  @Get("auth/me") me(@Req() req: any) { return this.service.me(req.customer.customerId); }

  @Post("checkout") checkoutGuest(@Body() body: any) { return this.service.createOrder(body, null); }
  @UseGuards(CustomerJwtGuard)
  @Post("checkout/account") checkoutAccount(@Body() body: any, @Req() req: any) { return this.service.createOrder(body, req.customer.customerId); }

  @UseGuards(CustomerJwtGuard)
  @Get("orders") orders(@Req() req: any) { return this.service.listMyOrders(req.customer.customerId); }
  @UseGuards(CustomerJwtGuard)
  @Get("orders/:id") order(@Req() req: any, @Param("id") id: string) { return this.service.getMyOrder(req.customer.customerId, id); }
}
