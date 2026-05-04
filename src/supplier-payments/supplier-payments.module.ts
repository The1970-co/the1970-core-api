import { Module } from '@nestjs/common';
import { SupplierPaymentsController } from './supplier-payments.controller';
import { SupplierPaymentsService } from './supplier-payments.service';

@Module({
  controllers: [SupplierPaymentsController],
  providers: [SupplierPaymentsService],
})
export class SupplierPaymentsModule {}
