import {
  Body,
  Controller,
  Get,
  Headers,
  Patch,
  Post,
  Req,
  UnauthorizedException,
  UseGuards,
} from "@nestjs/common";
import * as jwt from "jsonwebtoken";
import { AuthService } from "./auth.service";
import { JwtGuard } from "./jwt.guard";

@Controller("auth")
export class AuthController {
  constructor(private authService: AuthService) {}

  @Post("login")
  login(@Body() body: any) {
    const code = body.code || body.username || body.email;
    return this.authService.login(code, body.password);
  }

  @Post("second-password/verify")
  verifySecondPassword(
    @Body() body: { tempToken: string; secondPassword: string }
  ) {
    return this.authService.verifySecondPassword(
      body.tempToken,
      body.secondPassword
    );
  }

  @Get("me")
  async me(@Headers("authorization") authorization?: string) {
    if (!authorization?.startsWith("Bearer ")) {
      throw new UnauthorizedException("Missing token");
    }

    const token = authorization.replace("Bearer ", "");
    const payload = jwt.verify(
      token,
      process.env.JWT_SECRET || "dev-secret"
    ) as { sub: string };

    return this.authService.me(payload.sub);
  }

  @UseGuards(JwtGuard)
  @Patch("me/password")
  changeMyPassword(
    @Req() req: any,
    @Body() body: { oldPassword: string; newPassword: string }
  ) {
    return this.authService.changeMyPassword(
      req.user.id,
      body.oldPassword,
      body.newPassword
    );
  }
}