import { Module } from "@nestjs/common";
import { StocktakeAreaController } from "./stocktake-area.controller";
import { StocktakeAreaService } from "./stocktake-area.service";

@Module({
  controllers: [StocktakeAreaController],
  providers: [StocktakeAreaService],
})
export class StocktakeAreaModule {}