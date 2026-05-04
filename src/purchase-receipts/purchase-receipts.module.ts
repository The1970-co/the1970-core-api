import { Module } from '@nestjs/common';
import { PurchaseReceiptsController } from './purchase-receipts.controller';
import { PurchaseReceiptsService } from './purchase-receipts.service';

@Module({
  controllers: [PurchaseReceiptsController],
  providers: [PurchaseReceiptsService],
})
export class PurchaseReceiptsModule {}
