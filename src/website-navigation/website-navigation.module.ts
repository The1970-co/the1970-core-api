import { Module } from "@nestjs/common";
import { PrismaModule } from "../prisma/prisma.module";
import { WebsiteNavigationService } from "./website-navigation.service";
import { WebsiteNavigationAdminController } from "./website-navigation-admin.controller";
import { WebsiteNavigationPublicController } from "./website-navigation-public.controller";

@Module({
  imports: [PrismaModule],
  controllers: [WebsiteNavigationAdminController, WebsiteNavigationPublicController],
  providers: [WebsiteNavigationService],
  exports: [WebsiteNavigationService],
})
export class WebsiteNavigationModule {}
