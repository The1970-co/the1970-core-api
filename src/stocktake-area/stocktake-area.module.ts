import { Module } from "@nestjs/common";
import { PrismaService } from "src/prisma/prisma.service";
import { StocktakeAreaController } from "./stocktake-area.controller";
import { StocktakeAreaService } from "./stocktake-area.service";

@Module({
  controllers: [StocktakeAreaController],
  providers: [StocktakeAreaService, PrismaService],
})
export class StocktakeAreaModule {}