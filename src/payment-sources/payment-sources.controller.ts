import { Body, Controller, Get, Post, UseGuards } from "@nestjs/common";
import { JwtGuard } from "../auth/jwt.guard";
import { PaymentSourcesService } from "./payment-sources.service";

@UseGuards(JwtGuard)
@Controller("payment-sources")
export class PaymentSourcesController {
  constructor(private readonly service: PaymentSourcesService) {}

  @Get()
  findAll() {
    return this.service.findAll();
  }

  @Post()
  create(@Body() body: any) {
    return this.service.create(body);
  }
}