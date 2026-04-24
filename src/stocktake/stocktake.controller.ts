import { Body, Controller, Post } from '@nestjs/common';
import { StocktakeService } from './stocktake.service';

@Controller('stocktake')
export class StocktakeController {
  constructor(private readonly stocktakeService: StocktakeService) {}

  @Post('apply')
  applyStocktake(@Body() body: any) {
    return this.stocktakeService.applyStocktake(body);
  }
}