import { Body, Controller, Get, Param, Patch, Query, UseGuards } from "@nestjs/common";
import { JwtGuard } from "../auth/jwt.guard";
import { FinanceService } from "./finance.service";

@UseGuards(JwtGuard)
@Controller("finance")
export class FinanceController {
  constructor(private readonly financeService: FinanceService) {}

  @Get("daily")
  getDaily(
    @Query("dateFrom") dateFrom?: string,
    @Query("dateTo") dateTo?: string,
    @Query("branchId") branchId?: string,
    @Query("paymentSourceId") paymentSourceId?: string,
    @Query("status") status?: string,
    @Query("q") q?: string
  ) {
    return this.financeService.getDailyReconciliation({
      dateFrom,
      dateTo,
      branchId,
      paymentSourceId,
      status,
      q,
    });
  }

  @Get("local-delivery-reconciliation")
  getLocalDeliveryReconciliation(
    @Query("dateFrom") dateFrom?: string,
    @Query("dateTo") dateTo?: string,
    @Query("branchId") branchId?: string,
    @Query("carrier") carrier?: string,
    @Query("status") status?: string,
    @Query("q") q?: string
  ) {
    return this.financeService.getLocalDeliveryReconciliation({
      dateFrom,
      dateTo,
      branchId,
      carrier,
      status,
      q,
    });
  }

  @Patch("local-delivery-reconciliation/:orderId/delivered")
  markLocalDeliveryDelivered(
    @Param("orderId") orderId: string,
    @Body()
    body: {
      collectCod?: boolean;
      paymentSourceId?: string;
      amount?: number;
      note?: string;
    }
  ) {
    return this.financeService.markLocalDeliveryDelivered(orderId, body);
  }

  @Patch("local-delivery-reconciliation/:orderId/cod-received")
  markLocalDeliveryCodReceived(
    @Param("orderId") orderId: string,
    @Body()
    body: {
      paymentSourceId?: string;
      amount?: number;
      note?: string;
    }
  ) {
    return this.financeService.markLocalDeliveryDelivered(orderId, {
      ...body,
      collectCod: true,
    });
  }
}
