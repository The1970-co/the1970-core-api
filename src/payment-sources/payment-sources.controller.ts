import { Body, Controller, Get, Param, Patch, Post, UseGuards } from "@nestjs/common";
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

  @Patch(":id")
  update(@Param("id") id: string, @Body() body: any) {
    return this.service.update(id, body);
  }
}
