import {
  Body,
  Controller,
  Delete,
  ForbiddenException,
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
import { StockTransferService } from "./stock-transfer.service";
import { CreateStockTransferDto } from "./dto/create-stock-transfer.dto";
import { GenerateOutboundSuggestionsDto } from "./dto/generate-outbound-suggestions.dto";
import { ListStockTransfersDto } from "./dto/list-stock-transfers.dto";
import { UpdateStockTransferStatusDto } from "./dto/update-stock-transfer-status.dto";
import { CreateSelectedOutboundSuggestionsDto } from "./dto/create-selected-outbound-suggestions.dto";
import { UpdateAutoRebalanceConfigDto } from "./dto/update-auto-rebalance-config.dto";

@UseGuards(JwtGuard, PermissionGuard)
@Controller("stock-transfers")
export class StockTransferController {
  constructor(private readonly stockTransferService: StockTransferService) {}

  private normalizeId(value: any) {
    return String(value || "").trim();
  }

  private getScopedPermissionRows(user: any) {
    const rows = Array.isArray(user?.branchPermissions)
      ? user.branchPermissions
      : [];

    const branchId = this.normalizeId(
      user?.branchId ||
        user?.workingBranchId ||
        user?.currentBranchId ||
        user?.branch?.id ||
        user?.branchCode,
    );

    if (!branchId) return rows;

    const scoped = rows.filter(
      (row: any) => this.normalizeId(row?.branchId) === branchId,
    );

    return scoped.length ? scoped : rows;
  }

  private getEffectivePermissionKeys(user: any) {
    const keys = new Set<string>();
    const denied = new Set<string>();

    const add = (items: any) => {
      if (!Array.isArray(items)) return;
      items.forEach((item: any) => {
        const value = String(item || "").trim();
        if (value) keys.add(value);
      });
    };

    add(user?.permissions);
    add(user?.permissionKeys);
    add(user?.extraPermissionKeys);

    this.getScopedPermissionRows(user).forEach((row: any) => {
      add(row?.permissionKeys);
      add(row?.extraPermissionKeys);

      if (Array.isArray(row?.deniedPermissionKeys)) {
        row.deniedPermissionKeys.forEach((item: any) => {
          const value = String(item || "").trim();
          if (value) denied.add(value);
        });
      }
    });

    denied.forEach((key) => keys.delete(key));

    return keys;
  }

  private hasPermission(user: any, permission: string) {
    const permissions = this.getEffectivePermissionKeys(user);
    return permissions.has("*") || permissions.has(permission);
  }

  private assertPermission(user: any, permission: string) {
    if (!this.hasPermission(user, permission)) {
      throw new ForbiddenException("Bạn không có quyền thực hiện thao tác này");
    }
  }

  private permissionForStatus(body: UpdateStockTransferStatusDto) {
    const status = String((body as any)?.status || "").trim().toUpperCase();

    if (["CANCELLED", "CANCELED"].includes(status)) {
      return "stock_transfer.cancel";
    }

    if (["COMPLETED", "RECEIVED", "DONE"].includes(status)) {
      return "stock_transfer.receive";
    }

    if (["CONFIRMED", "IN_TRANSIT", "APPROVED"].includes(status)) {
      return "stock_transfer.confirm";
    }

    return "stock_transfer.edit";
  }

  @Get()
  @RequirePermissions("stock_transfer.view")
  async list(@Query() query: ListStockTransfersDto) {
    return this.stockTransferService.list(query);
  }

  @Post()
  @RequirePermissions("stock_transfer.create")
  async create(@Body() body: CreateStockTransferDto, @Req() req: any) {
    return this.stockTransferService.create(body, req.user);
  }

  @Get("auto-rebalance/config")
  @RequirePermissions("stock_transfer.view")
  async getAutoRebalanceConfig() {
    return this.stockTransferService.getAutoRebalanceConfig();
  }

  @Patch("auto-rebalance/config")
  @RequirePermissions("stock_transfer.create")
  async updateAutoRebalanceConfig(@Body() body: UpdateAutoRebalanceConfigDto) {
    return this.stockTransferService.updateAutoRebalanceConfig(body);
  }

  @Post("auto-rebalance/run-now")
  @RequirePermissions("stock_transfer.create")
  async runAutoRebalanceNow() {
    return this.stockTransferService.runAutoRebalanceNow();
  }

  @Post("suggestions/outbound")
  @RequirePermissions("stock_transfer.view")
  async previewOutboundSuggestionsLegacy(@Body() body: GenerateOutboundSuggestionsDto) {
    return this.stockTransferService.generateOutboundSuggestions(body);
  }

  @Post("suggestions/outbound/preview")
  @RequirePermissions("stock_transfer.view")
  async previewOutboundSuggestions(@Body() body: GenerateOutboundSuggestionsDto) {
    return this.stockTransferService.generateOutboundSuggestions(body);
  }

  @Post("suggestions/outbound/create")
  @RequirePermissions("stock_transfer.create")
  async createOutboundTransfersFromSuggestions(
    @Body() body: GenerateOutboundSuggestionsDto,
  ) {
    return this.stockTransferService.createOutboundTransfersFromSuggestions(body);
  }

  @Post("suggestions/outbound/create-selected")
  @RequirePermissions("stock_transfer.create")
  async createSelectedOutboundTransfers(
    @Body() body: CreateSelectedOutboundSuggestionsDto,
  ) {
    return this.stockTransferService.createSelectedOutboundTransfers(body);
  }

  @Get("scan-variant")
  @RequirePermissions("stock_transfer.create")
  async scanVariant(@Query("code") code: string) {
    return this.stockTransferService.scanVariant(code);
  }

  @Get(":id")
  @RequirePermissions("stock_transfer.view")
  async detail(@Param("id") id: string) {
    return this.stockTransferService.detail(id);
  }

  @Patch(":id")
  @RequirePermissions("stock_transfer.edit")
  async updateDraft(
    @Param("id") id: string,
    @Body() body: CreateStockTransferDto,
    @Req() req: any,
  ) {
    return this.stockTransferService.updateDraft(id, body, req.user);
  }

  @Patch(":id/status")
  async updateStatus(
    @Param("id") id: string,
    @Body() body: UpdateStockTransferStatusDto,
    @Req() req: any,
  ) {
    this.assertPermission(req.user, this.permissionForStatus(body));
    return this.stockTransferService.updateStatus(id, body, req.user);
  }

  @Delete("bulk-delete")
  async bulkDelete(@Body() body: { ids?: string[] }, @Req() req: any) {
    this.assertPermission(req.user, "stock_transfer.cancel");
    return this.stockTransferService.bulkDelete(body.ids || [], req.user);
  }

  @Delete(":id")
  async delete(@Param("id") id: string, @Req() req: any) {
    this.assertPermission(req.user, "stock_transfer.cancel");
    return this.stockTransferService.delete(id, req.user);
  }
}
