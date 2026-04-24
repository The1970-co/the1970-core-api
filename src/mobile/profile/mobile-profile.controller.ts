import { Controller, Get, Req, UseGuards } from "@nestjs/common";
import { JwtGuard } from "../../auth/jwt.guard";
import { MobileProfileService } from "./mobile-profile.service";

@Controller("mobile/profile")
@UseGuards(JwtGuard)
export class MobileProfileController {
  constructor(private readonly service: MobileProfileService) {}

  @Get()
  getProfile(@Req() req: any) {
    return this.service.getProfile(req.user);
  }
}