import {
  Body,
  Controller,
  Get,
  Patch,
  Post,
  Req,
  Res,
  UseGuards,
} from "@nestjs/common";
import type { Request, Response } from "express";
import { AuthService } from "./auth.service";
import { JwtGuard } from "./jwt.guard";

@Controller("auth")
export class AuthController {
  constructor(private authService: AuthService) {}

  private getRefreshCookieOptions() {
    const isProduction = process.env.NODE_ENV === "production";

    return {
      httpOnly: true,
      secure: isProduction,
      sameSite: isProduction ? ("none" as const) : ("lax" as const),
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

  @Post("refresh")
  async refresh(@Req() req: Request) {
    const refreshToken = this.getCookieFromHeader(req, "refreshToken");
    return this.authService.refresh(refreshToken);
  }

  @Post("logout")
  logout(@Res({ passthrough: true }) res: Response) {
    const isProduction = process.env.NODE_ENV === "production";

    res.clearCookie("refreshToken", {
      httpOnly: true,
      secure: isProduction,
      sameSite: isProduction ? "none" : "lax",
      path: "/auth",
    });

    return { message: "Đăng xuất thành công" };
  }

  // ✅ FIX QUAN TRỌNG
  @UseGuards(JwtGuard)
  @Get("me")
  async me(@Req() req: any) {
    return this.authService.me(req.user.sub || req.user.id);
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