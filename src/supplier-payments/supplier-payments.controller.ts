import {
  Body,
  Controller,
  Get,
  Param,
  Patch,
  Post,
  UseGuards,
} from '@nestjs/common';
import { JwtGuard } from '../auth/jwt.guard';
import { PermissionGuard } from '../auth/guards/permission.guard';
import { RequirePermissions } from '../auth/decorators/require-permissions.decorator';
import { SupplierPaymentsService } from './supplier-payments.service';

@UseGuards(JwtGuard, PermissionGuard)
@Controller('supplier-payments')
export class SupplierPaymentsController {
  constructor(
    private readonly supplierPaymentsService: SupplierPaymentsService,
  ) {}

  @Get()
  @RequirePermissions('supplier_payments.view')
  findAll() {
    return this.supplierPaymentsService.findAll();
  }

  @Get('receipt/:receiptId')
  @RequirePermissions('supplier_payments.view')
  getByReceiptId(@Param('receiptId') receiptId: string) {
    return this.supplierPaymentsService.getByReceiptId(receiptId);
  }

  @Patch('receipt/:receiptId/item-costs')
  @RequirePermissions('supplier_payments.cost.edit')
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
    return this.supplierPaymentsService.updateItemCosts(
      receiptId,
      body.items || [],
    );
  }

  @Post()
  @RequirePermissions('supplier_payments.pay')
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
