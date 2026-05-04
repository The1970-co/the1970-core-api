import {
  Body,
  Controller,
  Get,
  Headers,
  Patch,
  Post,
  Req,
  Res,
  UnauthorizedException,
  UseGuards,
} from "@nestjs/common";
import type { Request, Response } from "express";
import * as jwt from "jsonwebtoken";
import { AuthService } from "./auth.service";
import { JwtGuard } from "./jwt.guard";

@Controller("auth")
export class AuthController {
  constructor(private authService: AuthService) { }

  private getRefreshCookieOptions() {
    return {
      httpOnly: true,
      secure: process.env.NODE_ENV === "production",
      sameSite: "none" as const,
      path: "/auth",
      maxAge: 30 * 24 * 60 * 60 * 1000,
    };
  }

  private getCookieFromHeader(req: Request, key: string) {
    const raw = req.headers.cookie || "";
    const found = raw
      .split(";")
      .map((item) => item.trim())
      .find((item) => item.startsWith(`${key}=`));

    if (!found) return "";
    return decodeURIComponent(found.slice(key.length + 1));
  }

  @Post("login")
  async login(@Body() body: any, @Res({ passthrough: true }) res: Response) {
    const code = body.code || body.username || body.email;
    const data: any = await this.authService.login(code, body.password);

    if (data?.refreshToken) {
      res.cookie(
        "refreshToken",
        data.refreshToken,
        this.getRefreshCookieOptions()
      );

      const { refreshToken, ...safeData } = data;
      return safeData;
    }

    return data;
  }

  @Post("second-password/verify")
  async verifySecondPassword(
    @Body() body: { tempToken: string; secondPassword: string },
    @Res({ passthrough: true }) res: Response
  ) {
    const data: any = await this.authService.verifySecondPassword(
      body.tempToken,
      body.secondPassword
    );

    if (data.refreshToken) {
      res.cookie(
        "refreshToken",
        data.refreshToken,
        this.getRefreshCookieOptions()
      );
    }

    const { refreshToken, ...safeData } = data;
    return safeData;
  }

  @Post("refresh")
  async refresh(@Req() req: Request) {
    const refreshToken = this.getCookieFromHeader(req, "refreshToken");
    return this.authService.refresh(refreshToken);
  }

  @Post("logout")
  logout(@Res({ passthrough: true }) res: Response) {
    res.clearCookie("refreshToken", {
      httpOnly: true,
      secure: process.env.NODE_ENV === "production",
      sameSite: "none",
      path: "/auth",
    });

    return { message: "Đăng xuất thành công" };
  }

  @Get("me")
  async me(@Headers("authorization") authorization?: string) {
    if (!authorization?.startsWith("Bearer ")) {
      throw new UnauthorizedException("Missing token");
    }

    const token = authorization.replace("Bearer ", "");
    const payload = jwt.verify(token, process.env.JWT_SECRET || "dev-secret") as {
      sub: string;
    };

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