import { Module } from "@nestjs/common";
import { PrismaModule } from "../prisma/prisma.module";
import { WebsiteCatalogController } from "./website-catalog.controller";
import { WebsiteCatalogService } from "./website-catalog.service";

@Module({
  imports: [PrismaModule],
  controllers: [WebsiteCatalogController],
  providers: [WebsiteCatalogService],
  exports: [WebsiteCatalogService],
})
export class WebsiteCatalogModule {}
