import {
  Body,
  Controller,
  Get,
  Param,
  Patch,
  Post,
  Query,
  Req,
  UseGuards,
} from "@nestjs/common";
import { JwtGuard } from "../auth/jwt.guard";
import { PermissionGuard } from "../auth/guards/permission.guard";
import { RequirePermissions } from "../auth/decorators/require-permissions.decorator";
import { FinanceService } from "./finance.service";

@UseGuards(JwtGuard, PermissionGuard)
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

  @Get("cash-vouchers")
  @RequirePermissions("cash_voucher.view")
  getCashVouchers(
    @Req() req: any,
    @Query("type") type?: "RECEIPT" | "PAYMENT" | "ALL",
    @Query("dateFrom") dateFrom?: string,
    @Query("dateTo") dateTo?: string,
    @Query("branchId") branchId?: string,
    @Query("paymentSourceId") paymentSourceId?: string,
    @Query("status") status?: string,
    @Query("q") q?: string
  ) {
    return this.financeService.getCashVouchers({
      type,
      dateFrom,
      dateTo,
      branchId,
      paymentSourceId,
      status,
      q,
    }, req.user);
  }

  @Post("cash-vouchers")
  createCashVoucher(
    @Req() req: any,
    @Body()
    body: {
      type: "RECEIPT" | "PAYMENT";
      branchId?: string;
      paymentSourceId?: string;
      amount: number;
      category?: string;
      title: string;
      partnerName?: string;
      partnerPhone?: string;
      note?: string;
      createdById?: string;
      createdByName?: string;
    }
  ) {
    return this.financeService.createCashVoucher(body, req.user);
  }

  @Patch("cash-vouchers/:id")
  updateCashVoucher(
    @Req() req: any,
    @Param("id") id: string,
    @Body()
    body: {
      branchId?: string;
      paymentSourceId?: string;
      amount?: number;
      category?: string;
      title?: string;
      partnerName?: string;
      partnerPhone?: string;
      note?: string;
    }
  ) {
    return this.financeService.updateCashVoucher(id, body, req.user);
  }

  @Patch("cash-vouchers/:id/confirm")
  confirmCashVoucher(
    @Req() req: any,
    @Param("id") id: string,
    @Body()
    body: {
      confirmedById?: string;
      confirmedByName?: string;
      note?: string;
    }
  ) {
    return this.financeService.confirmCashVoucher(id, body, req.user);
  }

  @Patch("cash-vouchers/:id/cancel")
  cancelCashVoucher(
    @Req() req: any,
    @Param("id") id: string,
    @Body()
    body: {
      cancelledById?: string;
      cancelledByName?: string;
      note?: string;
    }
  ) {
    return this.financeService.cancelCashVoucher(id, body, req.user);
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
  @RequirePermissions("finance.local_delivery.confirm")
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
  @RequirePermissions("finance.local_delivery.confirm")
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
