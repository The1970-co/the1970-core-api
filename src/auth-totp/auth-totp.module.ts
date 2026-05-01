import { Module } from "@nestjs/common";
import { AuthTotpController } from "./auth-totp.controller";
import { AuthTotpService } from "./auth-totp.service";


@Module({
  controllers: [AuthTotpController],
  providers: [AuthTotpService],
  exports: [AuthTotpService],
})
export class AuthTotpModule {}