import { Body, Controller, Get, Param, Patch, Post } from '@nestjs/common';
import { PurchaseReceiptsService } from './purchase-receipts.service';

@Controller('purchase-receipts')
export class PurchaseReceiptsController {
  constructor(private readonly purchaseReceiptsService: PurchaseReceiptsService) {}

  @Get()
  findAll() {
    return this.purchaseReceiptsService.findAll();
  }

  @Get(':id')
  getById(@Param('id') id: string) {
    return this.purchaseReceiptsService.getById(id);
  }

  @Post()
  create(
    @Body()
    body: {
      supplierId?: string;
      branchId: string;
      note?: string;
      createdById?: string;
      items: {
        variantId: string;
        qty: number;
        unitCost?: number;
      }[];
    },
  ) {
    return this.purchaseReceiptsService.create(body);
  }

  @Patch(':id')
  updateDraft(
    @Param('id') id: string,
    @Body()
    body: {
      supplierId?: string | null;
      branchId?: string;
      note?: string;
      items?: {
        variantId: string;
        qty: number;
        unitCost?: number;
      }[];
    },
  ) {
    return this.purchaseReceiptsService.updateDraft(id, body);
  }

  @Patch(':id/import-stock')
  importStock(
    @Param('id') id: string,
    @Body() body?: { createdById?: string },
  ) {
    return this.purchaseReceiptsService.importStock(id, body?.createdById);
  }

  @Patch(':id/complete')
  complete(@Param('id') id: string) {
    return this.purchaseReceiptsService.complete(id);
  }

  @Patch(':id/cancel')
  cancel(@Param('id') id: string) {
    return this.purchaseReceiptsService.cancel(id);
  }
}