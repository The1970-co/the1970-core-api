import { Module } from "@nestjs/common";
import { PrismaModule } from "../prisma/prisma.module";
import { CustomerJwtGuard } from "./customer-jwt.guard";
import { StorefrontController } from "./storefront.controller";
import { StorefrontService } from "./storefront.service";

@Module({
  imports: [PrismaModule],
  controllers: [StorefrontController],
  providers: [StorefrontService, CustomerJwtGuard],
  exports: [StorefrontService],
})
export class StorefrontModule {}
