import { Module } from "@nestjs/common";
import { StocktakeSessionController } from "./stocktake-session.controller";
import { StocktakeSessionService } from "./stocktake-session.service";

@Module({
  controllers: [StocktakeSessionController],
  providers: [StocktakeSessionService],
})
export class StocktakeSessionModule {}