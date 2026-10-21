import { Body, Controller, Get, Patch, Post, Put, Param, Query, Req, UseGuards } from "@nestjs/common";
import { JwtGuard } from "../auth/jwt.guard";
import { PaymentSourcesService } from "./payment-sources.service";

@UseGuards(JwtGuard)
@Controller("payment-sources")
export class PaymentSourcesController {
  constructor(private readonly service: PaymentSourcesService) {}

  @Get("pos-rules")
  getPosRules(@Req() req: any) { return this.service.getPosRules(req.user); }

  @Put("pos-rules")
  putPosRules(@Req() req: any, @Body() body: any) { return this.service.savePosRules(req.user, body); }

  @Get("pos-customer-history")
  getPosCustomerHistory(@Req() req: any, @Query("phone") phone: string) {
    return this.service.getPosCustomerHistory(req.user, phone);
  }

  @Get()
  findAll() { return this.service.findAll(); }

  @Post()
  create(@Body() body: any) { return this.service.create(body); }

  @Patch(":id")
  update(@Param("id") id: string, @Body() body: any) { return this.service.update(id, body); }
}
