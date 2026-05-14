import { Module } from '@nestjs/common';
import { StocktakeController, StocktakeSessionMaintenanceController } from './stocktake.controller';
import { StocktakeService } from './stocktake.service';

@Module({
  controllers: [StocktakeController, StocktakeSessionMaintenanceController],
  providers: [StocktakeService],
})
export class StocktakeModule {}
