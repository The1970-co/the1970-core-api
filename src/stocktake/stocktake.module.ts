import { Module } from '@nestjs/common';
import { StocktakeController } from './stocktake.controller';
import { StocktakeService } from './stocktake.service';


@Module({
  controllers: [StocktakeController],
  providers: [StocktakeService],
})
export class StocktakeModule {}