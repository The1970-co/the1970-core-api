import { Module } from "@nestjs/common";
import { PrismaModule } from "../prisma/prisma.module";
import { WebsiteSettingsController } from "./website-settings.controller";
import { WebsiteSettingsService } from "./website-settings.service";

@Module({
  imports: [PrismaModule],
  controllers: [WebsiteSettingsController],
  providers: [WebsiteSettingsService],
  exports: [WebsiteSettingsService],
})
export class WebsiteSettingsModule {}
