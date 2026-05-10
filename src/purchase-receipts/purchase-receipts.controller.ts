import { Body, Controller, Get, Param, Patch, Post, UseGuards } from "@nestjs/common";
import { JwtGuard } from "../auth/jwt.guard";
import { PermissionGuard } from "../auth/guards/permission.guard";
import { RequirePermissions } from "../auth/decorators/require-permissions.decorator";
import { PurchaseReceiptsService } from "./purchase-receipts.service";

@UseGuards(JwtGuard, PermissionGuard)
@Controller("purchase-receipts")
export class PurchaseReceiptsController {
  constructor(private readonly purchaseReceiptsService: PurchaseReceiptsService) {}

  @Get()
  @RequirePermissions("purchase_receipt.view")
  findAll() {
    return this.purchaseReceiptsService.findAll();
  }

  @Get(":id")
  @RequirePermissions("purchase_receipt.view")
  getById(@Param("id") id: string) {
    return this.purchaseReceiptsService.getById(id);
  }

  @Post()
  @RequirePermissions("purchase_receipt.create")
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

  @Patch(":id")
  @RequirePermissions("purchase_receipt.edit")
  updateDraft(
    @Param("id") id: string,
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

  @Patch(":id/request-payment")
  @RequirePermissions("purchase_receipt.request_payment")
  requestPayment(@Param("id") id: string) {
    return this.purchaseReceiptsService.requestPayment(id);
  }

  @Patch(":id/pay")
  @RequirePermissions("purchase_receipt.pay")
  pay(
    @Param("id") id: string,
    @Body()
    body?: {
      paymentSourceId?: string | null;
      amount?: number;
      note?: string;
      paidById?: string;
      paidByName?: string;
    },
  ) {
    return this.purchaseReceiptsService.pay(id, body || {});
  }

  @Patch(":id/import-stock")
  @RequirePermissions("purchase_receipt.import_stock")
  importStock(
    @Param("id") id: string,
    @Body() body?: { createdById?: string },
  ) {
    return this.purchaseReceiptsService.importStock(id, body?.createdById);
  }

  @Patch(":id/complete")
  @RequirePermissions("purchase_receipt.complete")
  complete(@Param("id") id: string) {
    return this.purchaseReceiptsService.complete(id);
  }

  @Patch(":id/cancel")
  @RequirePermissions("purchase_receipt.cancel")
  cancel(@Param("id") id: string) {
    return this.purchaseReceiptsService.cancel(id);
  }
}
