import { Module } from "@nestjs/common";
import { PayrollController } from "./payroll.controller";
import { PayrollService } from "./payroll.service";
import { FinanceModule } from "../finance/finance.module";

@Module({
  imports: [FinanceModule],
  controllers: [PayrollController],
  providers: [PayrollService],
  exports: [PayrollService],
})
export class PayrollModule {}
