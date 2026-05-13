import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  Patch,
  Post,
  Query,
  Req,
  UseGuards,
  ForbiddenException,
} from "@nestjs/common";
import { JwtGuard } from "../auth/jwt.guard";
import { PermissionGuard } from "../auth/guards/permission.guard";
import { RequirePermissions } from "../auth/decorators/require-permissions.decorator";
import { FinanceService } from "./finance.service";

@UseGuards(JwtGuard, PermissionGuard)
@Controller("finance")
export class FinanceController {
  constructor(private readonly financeService: FinanceService) {}

  private userPermissions(user?: any) {
    const keys = new Set<string>();
    const add = (items?: any[]) => {
      if (!Array.isArray(items)) return;
      items.forEach((item) => {
        const value = String(item || "").trim();
        if (value) keys.add(value);
      });
    };

    add(user?.permissions);
    add(user?.permissionKeys);

    if (Array.isArray(user?.branchPermissions)) {
      user.branchPermissions.forEach((row: any) => {
        add(row?.permissionKeys);
        add(row?.extraPermissionKeys);
      });
      user.branchPermissions.forEach((row: any) => {
        if (Array.isArray(row?.deniedPermissionKeys)) {
          row.deniedPermissionKeys.forEach((key: any) => keys.delete(String(key || "").trim()));
        }
      });
    }

    return keys;
  }

  private ensureAnyPermission(user: any, permissions: string[]) {
    const roles = [
      ...(Array.isArray(user?.roles) ? user.roles : []),
      user?.role,
    ].map((role) => String(role || "").toLowerCase());

    const keys = this.userPermissions(user);
    const allowed =
      roles.includes("owner") ||
      roles.includes("admin") ||
      keys.has("*") ||
      permissions.some((permission) => keys.has(permission));

    if (!allowed) {
      throw new ForbiddenException("Bạn không có quyền truy cập chức năng này.");
    }
  }

  private cashVoucherViewPermissions(type?: "RECEIPT" | "PAYMENT" | "ALL") {
    if (type === "RECEIPT") return ["cash_voucher.view_receipt", "cash_voucher.view"];
    if (type === "PAYMENT") return ["cash_voucher.view_payment", "cash_voucher.view"];
    return ["cash_voucher.view_receipt", "cash_voucher.view_payment", "cash_voucher.view"];
  }

  private cashVoucherCreatePermissions(type?: "RECEIPT" | "PAYMENT") {
    return type === "PAYMENT"
      ? ["cash_voucher.create_payment"]
      : ["cash_voucher.create_receipt"];
  }

  private cashVoucherEditPermissions(type?: "RECEIPT" | "PAYMENT") {
    return type === "PAYMENT"
      ? ["cash_voucher.edit_payment"]
      : ["cash_voucher.edit_receipt"];
  }

  private cashVoucherConfirmPermissions(type?: "RECEIPT" | "PAYMENT") {
    return type === "PAYMENT"
      ? ["cash_voucher.confirm_payment"]
      : ["cash_voucher.confirm_receipt"];
  }

  private cashVoucherCancelPermissions(type?: "RECEIPT" | "PAYMENT") {
    return type === "PAYMENT"
      ? ["cash_voucher.cancel_payment"]
      : ["cash_voucher.cancel_receipt"];
  }

  private cashVoucherDeletePermissions(type?: "RECEIPT" | "PAYMENT") {
    return type === "PAYMENT"
      ? ["cash_voucher.delete_payment", "cash_voucher.cancel_payment"]
      : ["cash_voucher.delete_receipt", "cash_voucher.cancel_receipt"];
  }

  @Get("daily")
  @RequirePermissions("finance.view")
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


  @Get("daily-ledger")
  @RequirePermissions("finance.view")
  getDailyLedger(
    @Req() req: any,
    @Query("dateFrom") dateFrom?: string,
    @Query("dateTo") dateTo?: string,
    @Query("branchId") branchId?: string,
    @Query("paymentSourceId") paymentSourceId?: string
  ) {
    return this.financeService.getDailyLedger({
      dateFrom,
      dateTo,
      branchId,
      paymentSourceId,
    }, req.user);
  }

  @Post("daily-ledger/close")
  closeDailyLedger(
    @Req() req: any,
    @Body()
    body: {
      date: string;
      branchId: string;
      paymentSourceId: string;
      countedAmount?: number;
      note?: string;
      lockedById?: string;
      lockedByName?: string;
    }
  ) {
    this.ensureAnyPermission(req.user, ["finance.manage", "system.manage", "cash_voucher.confirm_receipt", "cash_voucher.confirm_payment"]);
    return this.financeService.closeDailyLedger(body, req.user);
  }

  @Post("daily-ledger/reopen")
  reopenDailyLedger(
    @Req() req: any,
    @Body()
    body: {
      date: string;
      branchId: string;
      paymentSourceId: string;
      note?: string;
    }
  ) {
    this.ensureAnyPermission(req.user, ["finance.manage", "system.manage"]);
    return this.financeService.reopenDailyLedger(body, req.user);
  }

  @Get("cash-vouchers")
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
    this.ensureAnyPermission(req.user, this.cashVoucherViewPermissions(type));
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
    this.ensureAnyPermission(req.user, this.cashVoucherCreatePermissions(body.type));
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
    this.ensureAnyPermission(req.user, ["cash_voucher.edit_receipt", "cash_voucher.edit_payment"]);
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
    this.ensureAnyPermission(req.user, ["cash_voucher.confirm_receipt", "cash_voucher.confirm_payment"]);
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
    this.ensureAnyPermission(req.user, ["cash_voucher.cancel_receipt", "cash_voucher.cancel_payment"]);
    return this.financeService.cancelCashVoucher(id, body, req.user);
  }

  @Delete("cash-vouchers/:id")
  deleteCashVoucher(
    @Req() req: any,
    @Param("id") id: string,
    @Query("type") type?: "RECEIPT" | "PAYMENT"
  ) {
    this.ensureAnyPermission(req.user, this.cashVoucherDeletePermissions(type));
    return this.financeService.deleteCashVoucher(id, req.user);
  }

  @Get("local-delivery-reconciliation")
  @RequirePermissions("finance.local_delivery.view")
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
    @Req() req: any,
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
    @Req() req: any,
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
