import { Module } from "@nestjs/common";
import { PrismaModule } from "../prisma/prisma.module";
import { WebsiteHomepageService } from "./website-homepage.service";
import { WebsiteHomepageAdminController } from "./website-homepage-admin.controller";
import { WebsiteHomepagePublicController } from "./website-homepage-public.controller";

@Module({
  imports: [PrismaModule],
  controllers: [WebsiteHomepageAdminController, WebsiteHomepagePublicController],
  providers: [WebsiteHomepageService],
  exports: [WebsiteHomepageService],
})
export class WebsiteHomepageModule {}
