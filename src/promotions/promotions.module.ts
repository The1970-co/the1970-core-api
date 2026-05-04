import { Module } from "@nestjs/common";
import { PrismaModule } from "../prisma/prisma.module";
import { PromotionsController } from "./promotions.controller";
import { PromotionsService } from "./promotions.service";
import { PromotionEngineService } from "./promotion-engine.service";

@Module({
  imports: [PrismaModule],
  controllers: [PromotionsController],
  providers: [PromotionsService, PromotionEngineService],
  exports: [PromotionsService, PromotionEngineService],
})
export class PromotionsModule {}
