import { Module } from "@nestjs/common";
import { PrismaModule } from "../prisma/prisma.module";
import { MobilePushController } from "./mobile-push.controller";
import { MobilePushService } from "./mobile-push.service";

@Module({
  imports: [PrismaModule],
  controllers: [MobilePushController],
  providers: [MobilePushService],
  exports: [MobilePushService],
})
export class MobilePushModule {}
