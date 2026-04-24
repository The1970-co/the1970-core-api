import { Body, Controller, Post, UseGuards, Req } from "@nestjs/common";
import { JwtGuard } from "../auth/jwt.guard";
import { PartialDeliveryService } from "./partial-delivery.service";

@UseGuards(JwtGuard)
@Controller("partial-delivery")
export class PartialDeliveryController {
  constructor(private readonly service: PartialDeliveryService) {}

  @Post()
  create(@Body() body: any, @Req() req: any) {
    return this.service.createPartialDelivery(body, req.user);
  }
}