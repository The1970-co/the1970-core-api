import { Module } from "@nestjs/common";
import { FinanceController } from "./finance.controller";
import { FinanceService } from "./finance.service";
import { GhnCodReconciliationController } from "./ghn-cod-reconciliation.controller";
import { GhnCodReconciliationService } from "./ghn-cod-reconciliation.service";

@Module({
  controllers: [
    FinanceController,
    GhnCodReconciliationController,
  ],
  providers: [
    FinanceService,
    GhnCodReconciliationService,
  ],
})
export class FinanceModule {}