import {
  Body,
  Controller,
  Get,
  Post,
  Req,
  UseGuards,
} from "@nestjs/common";
import { JwtGuard } from "../auth/jwt.guard";
import { AuthTotpService } from "./auth-totp.service";

@Controller("auth/totp")
@UseGuards(JwtGuard)
export class AuthTotpController {
  constructor(private readonly authTotpService: AuthTotpService) {}

  @Get("me")
  async me(@Req() req: any) {
    return {
      ok: true,
      user: req.user,
    };
  }

  @Post("setup")
  async setup(@Req() req: any) {
    return this.authTotpService.setup(req.user);
  }

  @Post("verify-setup")
  async verifySetup(@Req() req: any, @Body() body: { code: string }) {
    return this.authTotpService.verifySetup(req.user, body.code);
  }
}