import { Module } from "@nestjs/common";
import { APP_GUARD } from "@nestjs/core";
import { AuthService } from "./auth.service";
import { AuthController } from "./auth.controller";
import { JwtGuard } from "./jwt.guard";
import { RolesGuard } from "./roles.guard";
import { PermissionGuard } from "./guards/permission.guard";

@Module({
  controllers: [AuthController],
  providers: [AuthService, JwtGuard, RolesGuard, PermissionGuard],
  exports: [AuthService, JwtGuard, RolesGuard, PermissionGuard],
})
export class AuthModule {}
