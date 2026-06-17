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

  private getClearRefreshCookieOptions() {
    const isProduction = process.env.NODE_ENV === "production";

    return {
      httpOnly: true,
      secure: isProduction,
      sameSite: isProduction ? ("none" as const) : ("lax" as const),
      path: "/auth",
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

  private getRefreshTokenFromRequest(req: Request, body?: any) {
    return String(
      body?.refreshToken ||
        body?.refresh_token ||
        this.getCookieFromHeader(req, "refreshToken") ||
        this.getCookieFromHeader(req, "the1970_mobile_refresh_token") ||
        this.getCookieFromHeader(req, "the1970_refresh_token") ||
        "",
    ).trim();
  }

  private getRequestMeta(req: Request) {
    return {
      userAgent: req.headers["user-agent"] || "",
      ipAddress:
        String(req.headers["x-forwarded-for"] || "")
          .split(",")[0]
          .trim() || req.socket.remoteAddress || "",
    };
  }

  @Post("login")
  async login(
    @Body() body: any,
    @Req() req: Request,
    @Res({ passthrough: true }) res: Response,
  ) {
    const code = body.code || body.username || body.email;
    const data: any = await this.authService.login(
      code,
      body.password,
      this.getRequestMeta(req),
    );

    if (data?.refreshToken) {
      res.cookie("refreshToken", data.refreshToken, this.getRefreshCookieOptions());
    }

    // Mobile app không nên phụ thuộc cookie WebView. Trả refreshToken trong JSON để
    // app lưu vào Capacitor Preferences và refresh ngầm sau khi access token 15 phút hết hạn.
    return data;
  }

  @Post("second-password/verify")
  async verifySecondPassword(
    @Body() body: any,
    @Req() req: Request,
    @Res({ passthrough: true }) res: Response,
  ) {
    const data: any = await this.authService.verifySecondPassword(
      body.tempToken,
      body.secondPassword,
      this.getRequestMeta(req),
    );

    if (data?.refreshToken) {
      res.cookie("refreshToken", data.refreshToken, this.getRefreshCookieOptions());
    }

    return data;
  }

  @Post("refresh")
  async refresh(
    @Body() body: any,
    @Req() req: Request,
    @Res({ passthrough: true }) res: Response,
  ) {
    // Web lấy từ cookie, mobile gửi trong body { refreshToken }.
    const refreshToken = this.getRefreshTokenFromRequest(req, body);
    const data: any = await this.authService.refresh(refreshToken);

    if (data?.refreshToken) {
      res.cookie("refreshToken", data.refreshToken, this.getRefreshCookieOptions());
    }

    // Trả refreshToken mới để mobile cập nhật Preferences sau mỗi lần rotate token.
    return data;
  }

  @Post("logout")
  async logout(
    @Body() body: any,
    @Req() req: Request,
    @Res({ passthrough: true }) res: Response,
  ) {
    const refreshToken = this.getRefreshTokenFromRequest(req, body);
    await this.authService.logout(refreshToken);
    res.clearCookie("refreshToken", this.getClearRefreshCookieOptions());
    return { message: "Đăng xuất thành công" };
  }

  @UseGuards(JwtGuard)
  @Get("me")
  async me(@Req() req: any) {
    // JwtGuard đã verify access token, session, revokedAt, sessionVersion,
    // trạng thái active và đã build đủ role/permission/branch vào req.user.
    // Trả thẳng req.user để tránh query DB lần 2 ở AuthService.me().
    return {
      ...req.user,
      status: "active",
    };
  }

  @UseGuards(JwtGuard)
  @Patch("me/password")
  changeMyPassword(
    @Req() req: any,
    @Body() body: { oldPassword: string; newPassword: string },
  ) {
    return this.authService.changeMyPassword(
      req.user.id,
      body.oldPassword,
      body.newPassword,
    );
  }
}
