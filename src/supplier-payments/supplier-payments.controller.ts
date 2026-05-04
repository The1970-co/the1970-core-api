import { Body, Controller, Get, Param, Patch, Post, UseGuards } from '@nestjs/common';
import { JwtGuard } from '../auth/jwt.guard';
import { SupplierPaymentsService } from './supplier-payments.service';

@UseGuards(JwtGuard)
@Controller('supplier-payments')
export class SupplierPaymentsController {
  constructor(private readonly supplierPaymentsService: SupplierPaymentsService) {}

  @Get()
  findAll() {
    return this.supplierPaymentsService.findAll();
  }

  @Get('receipt/:receiptId')
  getByReceiptId(@Param('receiptId') receiptId: string) {
    return this.supplierPaymentsService.getByReceiptId(receiptId);
  }

  @Patch('receipt/:receiptId/item-costs')
  updateItemCosts(
    @Param('receiptId') receiptId: string,
    @Body()
    body: {
      items: {
        itemId: string;
        unitCost: number;
      }[];
    },
  ) {
    return this.supplierPaymentsService.updateItemCosts(receiptId, body.items || []);
  }

  @Post()
  pay(
    @Body()
    body: {
      receiptId: string;
      amount: number;
      paymentSourceId: string;
      note?: string;
      paidById?: string;
      paidByName?: string;
    },
  ) {
    return this.supplierPaymentsService.pay(body);
  }
}
