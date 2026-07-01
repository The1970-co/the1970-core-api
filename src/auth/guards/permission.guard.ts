import {
  CanActivate,
  ExecutionContext,
  ForbiddenException,
  Injectable,
} from "@nestjs/common";
import { Reflector } from "@nestjs/core";
import { PrismaService } from "../../prisma/prisma.service";
import {
  REQUIRED_PERMISSION_MODE_KEY,
  REQUIRED_PERMISSIONS_KEY,
  RequiredPermissionMode,
} from "../decorators/require-permissions.decorator";

const LEGACY_BOOLEAN_TO_PERMISSION_KEYS: Record<string, string[]> = {
  canView: ["products.view", "menu.products"],
  canSell: ["orders.create", "menu.pos", "pos.access"],
  canViewOwnOrders: ["orders.view_own", "menu.orders"],
  canViewBranchOrders: ["orders.view", "orders.view_branch", "menu.orders"],
  canCreateOrder: ["orders.create", "menu.create_order"],
  canApproveOrder: ["orders.approve", "orders.update_status"],
  canCancelOrder: ["orders.cancel"],
  canHandleReturn: ["returns.view", "returns.create", "menu.returns", "orders.return"],
  canViewStock: ["inventory.view", "inventory.logs.view", "menu.inventory"],
  canManageStock: ["inventory.manage", "inventory.adjust", "inventory.transfer"],
  canStocktake: ["stocktake.view", "stocktake.create", "stocktake.scan", "menu.stocktake"],
  canTransferStock: ["stock_transfer.view", "stock_transfer.create", "menu.stock_transfer"],
  canReceiveStock: ["stock_transfer.receive", "purchase_receipt.receive", "purchase_receipt.import_stock"],
  canViewCustomer: ["customers.view", "customers.view_own", "menu.customers"],
  canEditCustomer: ["customers.edit", "customers.create"],
  canExportProductExcel: ["products.excel.export"],
  canImportProductExcel: ["products.excel.import"],
  canExportOrderExcel: ["orders.excel.export"],
  canExportInventoryExcel: ["inventory.excel.export", "inventory.excel.audit"],
  canExportCustomerExcel: ["customers.excel.export"],
  canViewReport: ["reports.view", "menu.reports"],
  canViewMoney: ["inventory.value.view", "finance.view"],
};

const PERMISSION_KEY_ALIASES: Record<string, string[]> = {
  "omni_messages.view": ["omni_messages.view", "omni_inbox.view"],
  "omni_inbox.view": ["omni_inbox.view", "omni_messages.view"],
  "omni_messages.reply": ["omni_messages.reply", "omni_inbox.reply"],
  "omni_inbox.reply": ["omni_inbox.reply", "omni_messages.reply"],
  "omni_messages.assign": ["omni_messages.assign", "omni_inbox.assign"],
  "omni_inbox.assign": ["omni_inbox.assign", "omni_messages.assign"],
  "omni_messages.manage": ["omni_messages.manage", "omni_inbox.settings"],
  "omni_inbox.settings": ["omni_inbox.settings", "omni_messages.manage"],
  "omni_messages.tags": ["omni_messages.tags", "omni_inbox.tags.manage"],
  "omni_inbox.tags.manage": ["omni_inbox.tags.manage", "omni_messages.tags"],
  "omni_messages.notes": ["omni_messages.notes", "omni_inbox.notes.manage"],
  "omni_inbox.notes.manage": ["omni_inbox.notes.manage", "omni_messages.notes"],
  "omni_messages.create_order": ["omni_messages.create_order", "omni_inbox.create_order"],
  "omni_inbox.create_order": ["omni_inbox.create_order", "omni_messages.create_order"],
};

function expandPermissionKeys(values: string[]) {
  return values.flatMap((value) => {
    const key = normalizeKey(value);
    if (!key) return [];
    return PERMISSION_KEY_ALIASES[key] || [key];
  });
}

function normalizeRole(value: any) {
  return String(value || "").trim().toLowerCase();
}

function normalizeKey(value: any) {
  return String(value || "").trim();
}

function unique(values: string[]) {
  return Array.from(new Set(values.map(normalizeKey).filter(Boolean)));
}

@Injectable()
export class PermissionGuard implements CanActivate {
  constructor(
    private readonly reflector: Reflector,
    private readonly prisma: PrismaService,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const required =
      this.reflector.getAllAndOverride<string[]>(REQUIRED_PERMISSIONS_KEY, [
        context.getHandler(),
        context.getClass(),
      ]) || [];

    if (!required.length) return true;

    const mode =
      this.reflector.getAllAndOverride<RequiredPermissionMode>(
        REQUIRED_PERMISSION_MODE_KEY,
        [context.getHandler(), context.getClass()],
      ) || "all";

    const req = context.switchToHttp().getRequest();
    const user = req.user || {};
    const permissions = await this.getEffectivePermissions(user);

    if (permissions.includes("*")) return true;

    const requiredExpanded = expandPermissionKeys(required);
    const ok =
      mode === "any"
        ? requiredExpanded.some((permission) => permissions.includes(permission))
        : requiredExpanded.every((permission) => permissions.includes(permission));

    if (!ok) {
      throw new ForbiddenException("Bạn không có quyền thực hiện thao tác này");
    }

    return true;
  }

  private async getEffectivePermissions(user: any) {
    const fromRequest = this.collectPermissionsFromUserShape(user);
    if (fromRequest.includes("*")) return ["*"];

    const staffId = normalizeKey(user?.id || user?.sub || user?.staffId);
    if (!staffId) return fromRequest;

    const staff = await this.prisma.staffUser.findUnique({
      where: { id: staffId },
      include: {
        roles: true,
        branchPermissions: true,
      },
    });

    if (!staff || !(staff as any).isActive) return [];

    const roles = unique([
      (staff as any).role,
      ...(((staff as any).roles || []).map((row: any) => row?.roleCode || row)),
    ]).map(normalizeRole);

    if (roles.includes("owner") || roles.includes("admin")) return ["*"];

    return this.collectPermissionsFromUserShape(staff);
  }

  private collectPermissionsFromUserShape(user: any) {
    const keys: string[] = [];
    const denied: string[] = [];

    const roles = unique([
      user?.role,
      ...((Array.isArray(user?.roles) ? user.roles : []).map((row: any) => row?.roleCode || row)),
    ]).map(normalizeRole);

    if (roles.includes("owner") || roles.includes("admin")) return ["*"];

    const addArray = (values: any) => {
      if (!Array.isArray(values)) return;
      values.forEach((value) => {
        const key = normalizeKey(value);
        if (key) keys.push(key);
      });
    };

    addArray(user?.permissions);
    addArray(user?.permissionKeys);
    addArray(user?.extraPermissionKeys);

    const branchPermissions = Array.isArray(user?.branchPermissions)
      ? user.branchPermissions
      : [];

    for (const row of branchPermissions) {
      addArray(row?.permissionKeys);
      addArray(row?.extraPermissionKeys);

      for (const [field, mappedKeys] of Object.entries(LEGACY_BOOLEAN_TO_PERMISSION_KEYS)) {
        if (row?.[field]) keys.push(...mappedKeys);
      }

      if (Array.isArray(row?.deniedPermissionKeys)) {
        row.deniedPermissionKeys.forEach((value: any) => {
          const key = normalizeKey(value);
          if (key) denied.push(key);
        });
      }
    }

    const deniedSet = new Set(expandPermissionKeys(denied));
    return unique(expandPermissionKeys(keys)).filter((key) => !deniedSet.has(key));
  }
}
