import {
  Controller,
  Get,
  Headers,
  Post,
  Body,
  UnauthorizedException,
} from "@nestjs/common";
import * as jwt from "jsonwebtoken";
import { AuthService } from "./auth.service";

@Controller("auth")
export class AuthController {
  constructor(private authService: AuthService) {}

 @Post("login")
login(@Body() body: any) {
  const code = body.code || body.username || body.email;
  return this.authService.login(code, body.password);
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
}