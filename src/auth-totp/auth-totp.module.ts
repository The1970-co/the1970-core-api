import { Module } from "@nestjs/common";
import { AuthTotpController } from "./auth-totp.controller";
import { AuthTotpService } from "./auth-totp.service";
import { PrismaModule } from "../prisma/prisma.module";

@Module({
  imports: [PrismaModule],
  controllers: [AuthTotpController],
  providers: [AuthTotpService],
  exports: [AuthTotpService],
})
export class AuthTotpModule {}