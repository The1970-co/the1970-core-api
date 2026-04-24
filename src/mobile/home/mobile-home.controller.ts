import { Controller, Get, Query, UseGuards } from "@nestjs/common";
import { JwtGuard } from "../../auth/jwt.guard";
import { MobileHomeService } from "./mobile-home.service";

@Controller("mobile/home")
@UseGuards(JwtGuard)
export class MobileHomeController {
  constructor(private readonly service: MobileHomeService) {}

  @Get()
  getHome(@Query("branchId") branchId?: string) {
    return this.service.getHome(branchId);
  }
}