import { Module } from "@nestjs/common";
import { PrismaService } from "src/prisma/prisma.service";
import { StocktakeSessionController } from "./stocktake-session.controller";
import { StocktakeSessionService } from "./stocktake-session.service";

@Module({
  controllers: [StocktakeSessionController],
  providers: [StocktakeSessionService, PrismaService],
})
export class StocktakeSessionModule {}