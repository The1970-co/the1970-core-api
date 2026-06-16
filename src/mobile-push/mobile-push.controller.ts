import { Body, Controller, Post, Req, UseGuards } from "@nestjs/common";
import { Request } from "express";
import { JwtGuard } from "../auth/jwt.guard";
import { MobilePushService } from "./mobile-push.service";

@UseGuards(JwtGuard)
@Controller("mobile/push")
export class MobilePushController {
  constructor(private readonly mobilePushService: MobilePushService) {}

  @Post("register")
  register(@Req() req: Request & { user?: any }, @Body() body: any) {
    const user = req.user || {};

    return this.mobilePushService.registerToken({
      userId: user.id || user.userId || null,
      staffId: user.staffId || user.staffUserId || user.id || null,
      branchId: user.branchId || user.workingBranchId || null,
      platform: body.platform || "ios",
      provider: body.provider || "apns",
      token: body.token,
      deviceId: body.deviceId || null,
      appVersion: body.appVersion || null,
    });
  }
}
