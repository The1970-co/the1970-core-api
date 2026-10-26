import { Module } from "@nestjs/common";
import { PrismaModule } from "../prisma/prisma.module";
import { WebsiteSettingsModule } from "../website-settings/website-settings.module";
import { WebsiteCommerceController } from "./website-commerce.controller";
import { WebsiteCommerceService } from "./website-commerce.service";

@Module({
  imports: [PrismaModule, WebsiteSettingsModule],
  controllers: [WebsiteCommerceController],
  providers: [WebsiteCommerceService],
})
export class WebsiteCommerceModule {}
