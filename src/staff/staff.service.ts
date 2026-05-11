import { Injectable, BadRequestException } from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";
import { CreateStaffDto } from "./dto/create-staff.dto";
import { UpdateStaffStatusDto } from "./dto/update-staff-status.dto";
import * as bcrypt from "bcrypt";

type BranchPermissionTemplate = {
  canView?: boolean;
  canSell?: boolean;
  canViewOwnOrders?: boolean;
  canViewBranchOrders?: boolean;
  canCreateOrder?: boolean;
  canApproveOrder?: boolean;
  canCancelOrder?: boolean;
  canHandleReturn?: boolean;
  canViewStock?: boolean;
  canManageStock?: boolean;
  canStocktake?: boolean;
  canTransferStock?: boolean;
  canReceiveStock?: boolean;
  canViewCustomer?: boolean;
  canEditCustomer?: boolean;
  canExportProductExcel?: boolean;
  canImportProductExcel?: boolean;
  canExportOrderExcel?: boolean;
  canExportInventoryExcel?: boolean;
  canExportCustomerExcel?: boolean;
  canViewReport?: boolean;
  canViewMoney?: boolean;
  permissionKeys?: string[];
  extraPermissionKeys?: string[];
  deniedPermissionKeys?: string[];
};

type BranchRoleInput = {
  branchId: string;
  roleCode: string;
};

function normalizeRoleCode(value: unknown) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/đ/g, "d")
    .replace(/Đ/g, "D")
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9_]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .replace(/_+/g, "_");
}

const UNIQUE = (values: Array<string | undefined | null>) =>
  Array.from(
    new Set(values.map((v) => String(v || "").trim()).filter(Boolean)),
  );

const LEGACY_BOOLEAN_TO_PERMISSION_KEYS: Record<string, string[]> = {
  canView: ["products.view"],
  canSell: ["orders.create", "pos.access"],
  canViewOwnOrders: ["orders.view_own"],
  canViewBranchOrders: ["orders.view_branch", "orders.view"],
  canCreateOrder: ["orders.create"],
  canApproveOrder: ["orders.approve", "orders.update_status"],
  canCancelOrder: ["orders.cancel"],
  canHandleReturn: ["returns.view", "returns.create", "orders.return"],
  canViewStock: ["inventory.view"],
  canManageStock: ["inventory.manage"],
  canStocktake: ["stocktake.view", "stocktake.create"],
  canTransferStock: ["stock_transfer.view", "stock_transfer.create"],
  canReceiveStock: ["purchase_receipt.view", "purchase_receipt.receive"],
  canViewCustomer: ["customers.view"],
  canEditCustomer: ["customers.edit"],
  canExportProductExcel: ["products.excel.export"],
  canImportProductExcel: ["products.excel.import"],
  canExportOrderExcel: ["orders.excel.export"],
  canExportInventoryExcel: ["inventory.excel.export"],
  canExportCustomerExcel: ["customers.excel.export"],
  canViewReport: ["reports.view"],
  canViewMoney: ["inventory.value.view", "finance.view"],
};

const PERMISSION_LABEL_TO_KEY: Record<string, string> = {
  "Tổng quan": "menu.dashboard",
  "Đơn hàng": "menu.orders",
  "Tạo đơn": "menu.create_order",
  "POS bán tại quầy": "menu.pos",
  "Đơn trả hàng": "menu.returns",
  "Sản phẩm": "menu.products",
  "Khuyến mại": "menu.promotions",
  "Danh mục sản phẩm": "menu.product_categories",
  "Nhà cung cấp": "menu.suppliers",
  "Khách hàng": "menu.customers",
  "Kho hàng": "menu.inventory",
  "Lịch sử kho": "menu.inventory_logs",
  "Phiếu nhập": "menu.purchase_receipt",
  "Phiếu chuyển kho": "menu.stock_transfer",
  "Kiểm kho": "menu.stocktake",
  "Sơ đồ kho 3D": "menu.warehouse_map",
  "Tài chính": "menu.finance",
  "Đối soát vận chuyển": "menu.shipping_reconcile",
  "Thanh toán nhà cung cấp": "menu.supplier_payments",
  "Báo cáo": "menu.reports",
  Autopilot: "menu.autopilot",
  "AI Content": "menu.ai_content",
  "Phân quyền": "menu.permissions",
  "Cấu hình": "menu.settings",

  "Xem sản phẩm": "products.view",
  "Tạo sản phẩm": "products.create",
  "Sửa sản phẩm": "products.edit",
  "Xóa sản phẩm": "products.delete",
  "Sửa giá bán": "products.price.edit",
  "Xuất file sản phẩm": "products.excel.export",
  "Nhập file sản phẩm": "products.excel.import",

  "Xem tồn kho": "inventory.view",
  "Quản kho": "inventory.manage",
  "Xem lịch sử kho": "inventory.logs.view",
  "Xem giá trị tồn kho": "inventory.value.view",

  "Xem đơn nhập": "purchase_receipt.view",
  "Tạo đơn nhập": "purchase_receipt.create",
  "Sửa đơn nhập": "purchase_receipt.edit",
  "Thanh toán đơn nhập": "purchase_receipt.pay",
  "Hoàn trả đơn nhập": "purchase_receipt.return",
  "Nhận hàng vào kho": "purchase_receipt.receive",
  "Kết thúc đơn nhập": "purchase_receipt.close",
  "Hủy đơn nhập": "purchase_receipt.cancel",
  "Xuất file đơn nhập": "purchase_receipt.excel.export",
  "Nhập file đơn nhập": "purchase_receipt.excel.import",

  "Xem phiếu chuyển": "stock_transfer.view",
  "Tạo phiếu chuyển": "stock_transfer.create",
  "Sửa phiếu chuyển": "stock_transfer.edit",
  "Xác nhận chuyển": "stock_transfer.confirm",
  "Hủy phiếu chuyển": "stock_transfer.cancel",
  "Xuất file phiếu chuyển": "stock_transfer.excel.export",
  "Nhập file phiếu chuyển": "stock_transfer.excel.import",

  "Xem phiếu kiểm hàng": "stocktake.view",
  "Tạo phiếu kiểm hàng": "stocktake.create",
  "Sửa phiếu kiểm hàng": "stocktake.edit",
  "Xác nhận phiếu kiểm hàng": "stocktake.confirm",
  "Hủy phiếu kiểm hàng": "stocktake.cancel",
  "Xóa phiếu kiểm hàng": "stocktake.delete",
  "Cân bằng kho": "stocktake.apply",
  "Xuất file phiếu kiểm hàng": "stocktake.excel.export",
  "Nhập file phiếu kiểm hàng": "stocktake.excel.import",

  "Bán hàng / POS": "pos.access",
  "Xem đơn hàng được phụ trách": "orders.view_own",
  "Xem tất cả đơn hàng": "orders.view",
  "Tạo đơn hàng": "orders.create",
  "Sửa đơn hàng": "orders.edit",
  "Duyệt đơn hàng": "orders.approve",
  "Hủy đơn hàng": "orders.cancel",
  "Đóng gói và giao hàng": "orders.pack_ship",
  "Thanh toán đơn hàng": "orders.pay",
  "Xuất file đơn hàng": "orders.excel.export",
  "Nhập file đơn hàng": "orders.excel.import",

  "Xem đơn trả hàng": "returns.view",
  "Tạo đơn trả hàng": "returns.create",
  "Hủy đơn trả hàng": "returns.cancel",
  "Thanh toán đơn trả": "returns.pay",
  "Xuất file đơn trả hàng": "returns.excel.export",

  "Xem khách hàng được phụ trách": "customers.view_own",
  "Xem tất cả khách hàng": "customers.view",
  "Tạo khách hàng": "customers.create",
  "Sửa khách hàng": "customers.edit",
  "Xóa khách hàng": "customers.delete",
  "Xuất file khách hàng": "customers.excel.export",
  "Nhập file khách hàng": "customers.excel.import",

  "Xem phiếu thu": "payment_receipt.view",
  "Tạo phiếu thu": "payment_receipt.create",
  "Sửa phiếu thu": "payment_receipt.edit",
  "Hủy phiếu thu": "payment_receipt.cancel",
  "Xem phiếu chi": "payment_expense.view",
  "Tạo phiếu chi": "payment_expense.create",
  "Sửa phiếu chi": "payment_expense.edit",
  "Hủy phiếu chi": "payment_expense.cancel",

  "Báo cáo bán hàng": "reports.sales.view",
  "Báo cáo nhập hàng": "reports.purchase.view",
  "Báo cáo kho": "reports.inventory.view",
  "Báo cáo lãi lỗ": "reports.profit.view",
  "Báo cáo khách hàng": "reports.customers.view",
  "Báo cáo công nợ khách hàng/nhà cung cấp": "reports.debt.view",
  "Sổ quỹ": "reports.cashbook.view",

  "Xem khuyến mãi": "promotions.view",
  "Tạo khuyến mãi": "promotions.create",
  "Sửa khuyến mãi": "promotions.edit",
  "Xác nhận khuyến mãi": "promotions.confirm",
  "Hủy khuyến mãi": "promotions.cancel",
  "Kích hoạt khuyến mãi": "promotions.activate",
  "Tạm dừng khuyến mãi": "promotions.pause",
  "Xóa khuyến mãi": "promotions.delete",

  "Tải Excel sản phẩm": "products.excel.export",
  "Nhập Excel sản phẩm": "products.excel.import",
  "Tải Excel đơn hàng": "orders.excel.export",
  "Tải Excel tồn kho": "inventory.excel.export",
  "Tải Excel khách hàng": "customers.excel.export",
  "Quản lý phân quyền": "permissions.view",
  "Cấu hình hệ thống": "system.manage",
};

const PERMISSION_LABEL_TO_LEGACY_FIELD: Record<
  string,
  keyof BranchPermissionTemplate
> = {
  "Xem sản phẩm": "canView",
  "Bán hàng / POS": "canSell",
  "Xem đơn hàng được phụ trách": "canViewOwnOrders",
  "Xem tất cả đơn hàng": "canViewBranchOrders",
  "Tạo đơn hàng": "canCreateOrder",
  "Duyệt đơn hàng": "canApproveOrder",
  "Hủy đơn hàng": "canCancelOrder",
  "Xem đơn trả hàng": "canHandleReturn",
  "Tạo đơn trả hàng": "canHandleReturn",
  "Xem tồn kho": "canViewStock",
  "Quản kho": "canManageStock",
  "Xem phiếu kiểm hàng": "canStocktake",
  "Tạo phiếu kiểm hàng": "canStocktake",
  "Sửa phiếu kiểm hàng": "canStocktake",
  "Xác nhận phiếu kiểm hàng": "canStocktake",
  "Xem phiếu chuyển": "canTransferStock",
  "Tạo phiếu chuyển": "canTransferStock",
  "Sửa phiếu chuyển": "canTransferStock",
  "Xác nhận chuyển": "canTransferStock",
  "Nhận hàng vào kho": "canReceiveStock",
  "Xem khách hàng được phụ trách": "canViewCustomer",
  "Xem tất cả khách hàng": "canViewCustomer",
  "Tạo khách hàng": "canEditCustomer",
  "Sửa khách hàng": "canEditCustomer",
  "Xuất file sản phẩm": "canExportProductExcel",
  "Nhập file sản phẩm": "canImportProductExcel",
  "Xuất file đơn hàng": "canExportOrderExcel",
  "Tải Excel đơn hàng": "canExportOrderExcel",
  "Tải Excel tồn kho": "canExportInventoryExcel",
  "Xuất file khách hàng": "canExportCustomerExcel",
  "Tải Excel khách hàng": "canExportCustomerExcel",
  "Báo cáo bán hàng": "canViewReport",
  "Báo cáo kho": "canViewReport",
  "Xem giá trị tồn kho": "canViewMoney",
};

function permissionKeyForTemplateLabel(
  groupKey: string,
  permissionName: string,
) {
  if (groupKey === "transfers" && permissionName === "Nhận hàng vào kho")
    return "stock_transfer.receive";
  if (groupKey === "returns" && permissionName === "Nhận hàng vào kho")
    return "returns.receive";
  if (groupKey === "purchaseReceipts" && permissionName === "Nhận hàng vào kho")
    return "purchase_receipt.receive";
  return (
    PERMISSION_LABEL_TO_KEY[permissionName] ||
    `${groupKey}.${String(permissionName || "")
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/đ/g, "d")
      .replace(/Đ/g, "D")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .replace(/^_+|_+$/g, "")}`
  );
}

function buildBranchPermissionRowFromPermissionKeys(keysInput: string[]) {
  const keys = UNIQUE(keysInput);
  const has = (key: string) => keys.includes(key) || keys.includes("*");

  const row: BranchPermissionTemplate = {
    canView: has("products.view") || has("menu.products"),
    canSell: has("menu.pos") || has("pos.access"),
    canViewOwnOrders: has("orders.view_own"),
    canViewBranchOrders: has("orders.view") || has("orders.view_branch"),
    canCreateOrder: has("orders.create"),
    canApproveOrder: has("orders.approve") || has("orders.update_status"),
    canCancelOrder: has("orders.cancel"),
    canHandleReturn: has("returns.view") || has("returns.create") || has("orders.return"),
    canViewStock: has("inventory.view"),
    canManageStock: has("inventory.manage") || has("inventory.adjust") || has("inventory.transfer"),
    canStocktake: has("stocktake.view") || has("stocktake.create") || has("stocktake.scan"),
    canTransferStock: has("stock_transfer.view") || has("stock_transfer.create"),
    canReceiveStock: has("stock_transfer.receive") || has("purchase_receipt.receive") || has("purchase_receipt.import_stock"),
    canViewCustomer: has("customers.view") || has("customers.view_own"),
    canEditCustomer: has("customers.edit") || has("customers.create"),
    canExportProductExcel: has("products.excel.export"),
    canImportProductExcel: has("products.excel.import"),
    canExportOrderExcel: has("orders.excel.export"),
    canExportInventoryExcel: has("inventory.excel.export") || has("inventory.excel.audit"),
    canExportCustomerExcel: has("customers.excel.export") || has("customers.view"),
    canViewReport: has("reports.view") || keys.some((key) => key.startsWith("reports.")),
    canViewMoney: has("inventory.value.view") || has("finance.view"),
    permissionKeys: keys,
    extraPermissionKeys: [],
    deniedPermissionKeys: [],
  };

  return row;
}

function branchPermissionFromTemplatePermissions(permissions: any) {
  // FE mới lưu role template dạng { permissionKeys: [...] }.
  // Nếu không đọc thẳng mảng này mà convert như group label cũ, quyền vừa tắt sẽ bị sync bật lại.
  if (permissions && typeof permissions === "object" && Array.isArray(permissions.permissionKeys)) {
    return buildBranchPermissionRowFromPermissionKeys(permissions.permissionKeys);
  }

  const row: BranchPermissionTemplate = {
    canView: false,
    canSell: false,
    canViewOwnOrders: false,
    canViewBranchOrders: false,
    canCreateOrder: false,
    canApproveOrder: false,
    canCancelOrder: false,
    canHandleReturn: false,
    canViewStock: false,
    canManageStock: false,
    canStocktake: false,
    canTransferStock: false,
    canReceiveStock: false,
    canViewCustomer: false,
    canEditCustomer: false,
    canExportProductExcel: false,
    canImportProductExcel: false,
    canExportOrderExcel: false,
    canExportInventoryExcel: false,
    canExportCustomerExcel: false,
    canViewReport: false,
    canViewMoney: false,
    permissionKeys: [],
    extraPermissionKeys: [],
    deniedPermissionKeys: [],
  };

  const keys: string[] = [];
  const groups =
    permissions && typeof permissions === "object" ? permissions : {};

  for (const [groupKey, permissionNames] of Object.entries(groups)) {
    if (!Array.isArray(permissionNames)) continue;
    for (const permissionName of permissionNames) {
      const label = String(permissionName || "");
      keys.push(permissionKeyForTemplateLabel(groupKey, label));
      const legacyField = PERMISSION_LABEL_TO_LEGACY_FIELD[label];
      if (legacyField) (row as any)[legacyField] = true;
    }
  }

  row.permissionKeys = UNIQUE(keys);
  return row;
}

function permissionKeysFromLegacyBooleans(row: Record<string, any>) {
  const keys: string[] = [];
  for (const [field, permissionKeys] of Object.entries(
    LEGACY_BOOLEAN_TO_PERMISSION_KEYS,
  )) {
    if (row[field]) keys.push(...permissionKeys);
  }
  if (Array.isArray(row.permissionKeys)) keys.push(...row.permissionKeys);
  return UNIQUE(keys);
}

const ROLE_TEMPLATES: Record<string, BranchPermissionTemplate> = {
  owner: {
    canView: true,
    canSell: true,
    canViewOwnOrders: true,
    canViewBranchOrders: true,
    canCreateOrder: true,
    canApproveOrder: true,
    canCancelOrder: true,
    canHandleReturn: true,
    canViewStock: true,
    canManageStock: true,
    canStocktake: true,
    canTransferStock: true,
    canReceiveStock: true,
    canViewCustomer: true,
    canEditCustomer: true,
  },
  admin: {
    canView: true,
    canSell: true,
    canViewOwnOrders: true,
    canViewBranchOrders: true,
    canCreateOrder: true,
    canApproveOrder: true,
    canCancelOrder: true,
    canHandleReturn: true,
    canViewStock: true,
    canManageStock: true,
    canStocktake: true,
    canTransferStock: true,
    canReceiveStock: true,
    canViewCustomer: true,
    canEditCustomer: true,
  },
  "branch-manager": {
    canView: true,
    canSell: true,
    canViewOwnOrders: true,
    canViewBranchOrders: true,
    canCreateOrder: true,
    canApproveOrder: true,
    canCancelOrder: true,
    canHandleReturn: true,
    canViewStock: true,
    canManageStock: true,
    canStocktake: true,
    canTransferStock: true,
    canReceiveStock: true,
    canViewCustomer: true,
    canEditCustomer: true,
  },
  fulltime: {
    canView: true,
    canSell: true,
    canViewOwnOrders: true,
    canViewBranchOrders: true,
    canCreateOrder: true,
    canHandleReturn: true,
    canViewStock: true,
    canStocktake: true,
    canTransferStock: true,
    canReceiveStock: true,
    canViewCustomer: true,
  },
  "retail-staff": {
    canView: true,
    canSell: true,
    canViewOwnOrders: true,
    canCreateOrder: true,
    canHandleReturn: true,
    canViewStock: true,
    canViewCustomer: true,
  },
  "stock-auditor": {
    canView: true,
    canViewStock: true,
    canStocktake: true,
  },
  "stock-staff": {
    canView: true,
    canViewStock: true,
    canManageStock: true,
    canStocktake: true,
    canTransferStock: true,
    canReceiveStock: true,
  },
};

@Injectable()
export class StaffService {
  constructor(private readonly prisma: PrismaService) {}

  private normalizeRole(role: any) {
    return String(role || "")
      .trim()
      .toLowerCase();
  }

  private normalizeRoles(input: any): string[] {
    const raw = Array.isArray(input) ? input : input ? [input] : [];

    return Array.from(
      new Set(raw.map((role) => this.normalizeRole(role)).filter(Boolean)),
    );
  }

  private validateRole(roleCode: string) {
    const normalized = this.normalizeRole(roleCode);

    if (!normalized) {
      throw new BadRequestException(`Role không hợp lệ: ${roleCode || "trống"}`);
    }

    if (!/^[a-z0-9][a-z0-9_-]{1,63}$/.test(normalized)) {
      throw new BadRequestException(
        `Mã role không hợp lệ: ${roleCode}. Chỉ dùng chữ thường, số, dấu - hoặc _.`,
      );
    }

    return normalized;
  }

  private permissionsForRoleLegacy(roleCode: string) {
    const normalized = this.validateRole(roleCode);
    const row = {
      canView: false,
      canSell: false,
      canViewOwnOrders: false,
      canViewBranchOrders: false,
      canCreateOrder: false,
      canApproveOrder: false,
      canCancelOrder: false,
      canHandleReturn: false,
      canViewStock: false,
      canManageStock: false,
      canStocktake: false,
      canTransferStock: false,
      canReceiveStock: false,
      canViewCustomer: false,
      canEditCustomer: false,
      canExportProductExcel: false,
      canImportProductExcel: false,
      canExportOrderExcel: false,
      canExportInventoryExcel: false,
      canExportCustomerExcel: false,
      canViewReport: false,
      canViewMoney: false,
      ...ROLE_TEMPLATES[normalized],
    };

    return {
      ...row,
      permissionKeys: permissionKeysFromLegacyBooleans(row),
      extraPermissionKeys: [],
      deniedPermissionKeys: [],
    };
  }

  private async permissionsForRole(roleCode: string, tx?: any) {
    const normalized = this.validateRole(roleCode);
    const client = tx || this.prisma;

    const template = await client.staffRoleTemplate.findUnique({
      where: { roleCode: normalized },
    });

    if (!template) return this.permissionsForRoleLegacy(normalized);

    return branchPermissionFromTemplatePermissions(template.permissions);
  }

  async getRoleTemplates() {
    const rows = await this.prisma.staffRoleTemplate.findMany({
      orderBy: [{ roleCode: "asc" }],
    });

    return rows.map((row) => ({
      id: row.roleCode,
      name: row.name,
      scope: row.scope,
      description: row.description,
      note: row.note,
      permissions: row.permissions,
      createdAt: row.createdAt,
      updatedAt: row.updatedAt,
    }));
  }

  async saveRoleTemplates(dto: any) {
    const roles = Array.isArray(dto?.roles) ? dto.roles : [];
    if (!roles.length) {
      throw new BadRequestException("Thiếu danh sách mẫu quyền role.");
    }

    await this.prisma.$transaction(async (tx) => {
      for (const role of roles) {
        const roleCode = this.validateRole(role?.id || role?.roleCode);
        await tx.staffRoleTemplate.upsert({
          where: { roleCode },
          create: {
            roleCode,
            name: String(role?.name || roleCode),
            scope: String(role?.scope || "ONE_BRANCH"),
            description: String(role?.description || ""),
            note: String(role?.note || ""),
            permissions: role?.permissions || {},
          },
          update: {
            name: String(role?.name || roleCode),
            scope: String(role?.scope || "ONE_BRANCH"),
            description: String(role?.description || ""),
            note: String(role?.note || ""),
            permissions: role?.permissions || {},
          },
        });
      }
    });

    // Sau khi lưu mẫu role vào DB, đồng bộ lại nhân viên đang dùng role đó.
    // Việc này tránh lỗi deploy/update xong nhân viên cũ vẫn giữ permissionKeys cũ và bung menu.
    await this.syncAllPermissionsFromRoleTemplates({ force: true });

    return this.getRoleTemplates();
  }

  private async assertBranchesExist(branchIds: string[]) {
    const uniqueIds = Array.from(new Set(branchIds.filter(Boolean)));
    if (!uniqueIds.length) return;

    const branches = await this.prisma.branch.findMany({
      where: { id: { in: uniqueIds } },
      select: { id: true },
    });

    const existingIds = new Set(branches.map((branch) => branch.id));
    const missingBranch = uniqueIds.find((id) => !existingIds.has(id));

    if (missingBranch) {
      throw new BadRequestException(
        `Chi nhánh không tồn tại: ${missingBranch}`,
      );
    }
  }

  private normalizeBranchRoles(input: any[]): BranchRoleInput[] {
    if (!Array.isArray(input)) return [];

    const map = new Map<string, BranchRoleInput>();

    input.forEach((item) => {
      const branchId = String(item?.branchId || "").trim();
      const roleCode = this.validateRole(
        item?.roleCode || item?.role || item?.roleId,
      );

      if (!branchId) return;
      map.set(branchId, { branchId, roleCode });
    });

    return Array.from(map.values());
  }

  private deriveLegacyRolesFromBranchRoles(
    branchRoles: BranchRoleInput[],
    fallbackRole?: string,
  ) {
    const roles = branchRoles.map((row) => row.roleCode);
    if (!roles.length && fallbackRole)
      roles.push(this.normalizeRole(fallbackRole));
    return Array.from(new Set(roles.filter(Boolean)));
  }

  private async replaceBranchRolesAndPermissions(
    tx: any,
    staffId: string,
    branchRoles: BranchRoleInput[],
  ) {
    await tx.staffBranchRole.deleteMany({ where: { staffId } });
    await tx.staffBranchPermission.deleteMany({ where: { staffId } });

    if (!branchRoles.length) return;

    await tx.staffBranchRole.createMany({
      data: branchRoles.map((row) => ({
        staffId,
        branchId: row.branchId,
        roleCode: row.roleCode,
      })),
      skipDuplicates: true,
    });

    const permissionRows = await Promise.all(
      branchRoles.map(async (row) => ({
        staffId,
        branchId: row.branchId,
        ...(await this.permissionsForRole(row.roleCode, tx)),
        note: `Auto generated from role ${row.roleCode}`,
      })),
    );

    await tx.staffBranchPermission.createMany({
      data: permissionRows,
      skipDuplicates: true,
    });
  }

  async create(dto: CreateStaffDto) {
    if (!dto.password || dto.password.length < 4) {
      throw new BadRequestException("Mật khẩu tối thiểu 4 ký tự");
    }

    if (!dto.code?.trim()) {
      throw new BadRequestException("Thiếu mã nhân viên");
    }

    if (!dto.name?.trim()) {
      throw new BadRequestException("Thiếu tên nhân viên");
    }

    const branchRoles = this.normalizeBranchRoles(
      (dto as any).branchRoles || [],
    );
    const rolesFromDto = this.normalizeRoles((dto as any).roles);
    const legacyRole = this.validateRole(
      (dto as any).role || rolesFromDto[0] || branchRoles[0]?.roleCode,
    );

    const existingByCode = await this.prisma.staffUser.findUnique({
      where: { code: dto.code.trim() },
    });

    if (existingByCode) {
      throw new BadRequestException("Mã nhân viên đã tồn tại");
    }

    const email = String((dto as any).email || "").trim() || null;
    const usernameInput = String((dto as any).username || "").trim();
    const username = (usernameInput || dto.code.trim()).toLowerCase();

    if (email) {
      const existingEmail = await this.prisma.staffUser.findUnique({
        where: { email },
      });
      if (existingEmail)
        throw new BadRequestException("Email nhân viên đã tồn tại");
    }

    if (username) {
      const existingUsername = await this.prisma.staffUser.findUnique({
        where: { username },
      });
      if (existingUsername)
        throw new BadRequestException("Tên đăng nhập đã tồn tại");
    }

    let branchId: string | null = null;
    let branchName: string | null = null;

    if (dto.branchId) {
      const branch = await this.prisma.branch.findUnique({
        where: { id: String(dto.branchId).trim() },
        select: { id: true, name: true },
      });

      if (!branch) throw new BadRequestException("Chi nhánh không tồn tại");
      branchId = branch.id;
      branchName = branch.name;
    }

    const initialBranchRoles = branchRoles.length
      ? branchRoles
      : branchId
        ? [{ branchId, roleCode: legacyRole }]
        : [];

    await this.assertBranchesExist(
      initialBranchRoles.map((row) => row.branchId),
    );

    const hash = await bcrypt.hash(dto.password, 10);

    const created = await this.prisma.$transaction(async (tx) => {
      const staff = await tx.staffUser.create({
        data: {
          code: dto.code.trim(),
          name: dto.name.trim(),
          username,
          email,
          phone: String((dto as any).phone || "").trim() || null,
          address: String((dto as any).address || "").trim() || null,
          note: String((dto as any).note || "").trim() || null,
          role: legacyRole,
          branchId,
          branchName,
          passwordHash: hash,
          isActive: true,
        },
      });

      const finalRoles = this.deriveLegacyRolesFromBranchRoles(
        initialBranchRoles,
        legacyRole,
      );

      if (finalRoles.length) {
        await tx.staffUserRole.createMany({
          data: finalRoles.map((roleCode) => ({ staffId: staff.id, roleCode })),
          skipDuplicates: true,
        });
      }

      await this.replaceBranchRolesAndPermissions(
        tx,
        staff.id,
        initialBranchRoles,
      );

      return staff;
    });

    return this.findOne(created.id);
  }

  async findAll() {
    const staffList = await this.prisma.staffUser.findMany({
      orderBy: [{ createdAt: "desc" }],
      include: {
        roles: true,
        branchRoles: { include: { branch: true } },
        branchPermissions: { include: { branch: true } },
      },
    });

    const deptMap = await this.getDepartmentsForStaffIds(staffList.map((staff) => staff.id));
    return staffList.map((staff) => ({
      ...staff,
      departments: deptMap.get(staff.id) || [],
    }));
  }

  async findOne(id: string) {
    const staff = await this.prisma.staffUser.findUnique({
      where: { id },
      include: {
        roles: true,
        branchRoles: { include: { branch: true } },
        branchPermissions: { include: { branch: true } },
      },
    });

    if (!staff) return null;
    const deptMap = await this.getDepartmentsForStaffIds([staff.id]);
    return {
      ...staff,
      departments: deptMap.get(staff.id) || [],
    };
  }

  async update(id: string, dto: any) {
    const current = await this.prisma.staffUser.findUnique({ where: { id } });

    if (!current) throw new BadRequestException("Nhân viên không tồn tại");

    const roleInput = this.validateRole(
      dto.role || current.role || "retail-staff",
    );

    let branchId: string | null = current.branchId || null;
    let branchName: string | null = current.branchName || null;

    if (dto.branchId !== undefined) {
      if (dto.branchId) {
        const branch = await this.prisma.branch.findUnique({
          where: { id: String(dto.branchId).trim() },
          select: { id: true, name: true },
        });

        if (!branch) throw new BadRequestException("Chi nhánh không tồn tại");
        branchId = branch.id;
        branchName = branch.name;
      } else {
        branchId = null;
        branchName = null;
      }
    }

    const email =
      dto.email !== undefined
        ? String(dto.email || "").trim() || null
        : current.email;
    const username =
      dto.username !== undefined
        ? String(dto.username || "")
            .trim()
            .toLowerCase() || current.username
        : current.username;

    if (email && email !== current.email) {
      const existingEmail = await this.prisma.staffUser.findUnique({
        where: { email },
      });
      if (existingEmail && existingEmail.id !== id)
        throw new BadRequestException("Email nhân viên đã tồn tại");
    }

    if (username && username !== current.username) {
      const existingUsername = await this.prisma.staffUser.findUnique({
        where: { username },
      });
      if (existingUsername && existingUsername.id !== id)
        throw new BadRequestException("Tên đăng nhập đã tồn tại");
    }

    await this.prisma.staffUser.update({
      where: { id },
      data: {
        code: dto.code !== undefined ? String(dto.code).trim() : current.code,
        name: dto.name !== undefined ? String(dto.name).trim() : current.name,
        username,
        email,
        phone:
          dto.phone !== undefined
            ? String(dto.phone || "").trim() || null
            : current.phone,
        address:
          dto.address !== undefined
            ? String(dto.address || "").trim() || null
            : current.address,
        note:
          dto.note !== undefined
            ? String(dto.note || "").trim() || null
            : current.note,
        role: roleInput,
        branchId,
        branchName,
      },
    });

    return this.findOne(id);
  }

  async updateBranchRoles(staffId: string, dto: any) {
    const staff = await this.prisma.staffUser.findUnique({
      where: { id: staffId },
    });
    if (!staff) throw new BadRequestException("Nhân viên không tồn tại");

    const branchRoles = this.normalizeBranchRoles(dto.branchRoles || []);
    await this.assertBranchesExist(branchRoles.map((row) => row.branchId));

    const legacyRoles = this.deriveLegacyRolesFromBranchRoles(
      branchRoles,
      staff.role || undefined,
    );
    const primaryRole = legacyRoles[0] || staff.role || null;
    const primaryBranchId = branchRoles[0]?.branchId || staff.branchId || null;
    let primaryBranchName: string | null = staff.branchName || null;

    if (primaryBranchId) {
      const branch = await this.prisma.branch.findUnique({
        where: { id: primaryBranchId },
        select: { name: true },
      });
      primaryBranchName = branch?.name || primaryBranchName;
    }

    await this.prisma.$transaction(async (tx) => {
      await tx.staffUserRole.deleteMany({ where: { staffId } });

      if (legacyRoles.length) {
        await tx.staffUserRole.createMany({
          data: legacyRoles.map((roleCode) => ({ staffId, roleCode })),
          skipDuplicates: true,
        });
      }

      await this.replaceBranchRolesAndPermissions(tx, staffId, branchRoles);

      await tx.staffUser.update({
        where: { id: staffId },
        data: {
          role: primaryRole,
          branchId: primaryBranchId,
          branchName: primaryBranchName,
          sessionVersion: { increment: 1 },
        },
      });

      await tx.staffSession.updateMany({
        where: { staffId, revokedAt: null },
        data: { revokedAt: new Date() },
      });
    });

    return this.findOne(staffId);
  }

  private sanitizeBranchPermissionInput(row: any) {
    const clean: any = this.permissionsForRoleLegacy(
      row.roleCode || row.role || "retail-staff",
    );

    const booleanFields = [
      "canView",
      "canSell",
      "canViewOwnOrders",
      "canViewBranchOrders",
      "canCreateOrder",
      "canApproveOrder",
      "canCancelOrder",
      "canHandleReturn",
      "canViewStock",
      "canManageStock",
      "canStocktake",
      "canTransferStock",
      "canReceiveStock",
      "canViewCustomer",
      "canEditCustomer",
      "canExportProductExcel",
      "canImportProductExcel",
      "canExportOrderExcel",
      "canExportInventoryExcel",
      "canExportCustomerExcel",
      "canViewReport",
      "canViewMoney",
    ];

    for (const field of booleanFields) {
      if (row[field] !== undefined) clean[field] = Boolean(row[field]);
    }

    clean.permissionKeys = permissionKeysFromLegacyBooleans({
      ...clean,
      permissionKeys: Array.isArray(row.permissionKeys)
        ? row.permissionKeys
        : [],
    });

    clean.extraPermissionKeys = UNIQUE(
      Array.isArray(row.extraPermissionKeys) ? row.extraPermissionKeys : [],
    );
    clean.deniedPermissionKeys = UNIQUE(
      Array.isArray(row.deniedPermissionKeys) ? row.deniedPermissionKeys : [],
    );

    return clean;
  }


  private async ensureStaffBranchPermissionOverrideColumns() {
    await this.prisma.$executeRawUnsafe(`
      ALTER TABLE "StaffBranchPermission"
      ADD COLUMN IF NOT EXISTS "extraPermissionKeys" TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[];
    `);

    await this.prisma.$executeRawUnsafe(`
      ALTER TABLE "StaffBranchPermission"
      ADD COLUMN IF NOT EXISTS "deniedPermissionKeys" TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[];
    `);
  }

  async updatePermissions(staffId: string, dto: any) {
    await this.ensureStaffBranchPermissionOverrideColumns();
    if (
      Array.isArray(dto.branchRoles) &&
      !Array.isArray(dto.branchPermissions)
    ) {
      return this.updateBranchRoles(staffId, dto);
    }

    const staff = await this.prisma.staffUser.findUnique({
      where: { id: staffId },
    });
    if (!staff) throw new BadRequestException("Nhân viên không tồn tại");

    const branchPermissions = Array.isArray(dto.branchPermissions)
      ? dto.branchPermissions
      : [];

    if (!branchPermissions.length) {
      return this.findOne(staffId);
    }

    const branchIds = branchPermissions
      .map((row: any) => String(row.branchId || "").trim())
      .filter(Boolean);

    await this.assertBranchesExist(branchIds);

    await this.prisma.$transaction(async (tx) => {
      await tx.staffBranchPermission.deleteMany({ where: { staffId } });

      await tx.staffBranchPermission.createMany({
        data: branchPermissions
          .map((row: any) => {
            const branchId = String(row.branchId || "").trim();
            if (!branchId) return null;
            const clean = this.sanitizeBranchPermissionInput(row);
            return {
              staffId,
              branchId,
              ...clean,
              note: row.note || "Saved from permission UI",
            };
          })
          .filter(Boolean),
        skipDuplicates: true,
      });

      await tx.staffUser.update({
        where: { id: staffId },
        data: { sessionVersion: { increment: 1 } },
      });

      await tx.staffSession.updateMany({
        where: { staffId, revokedAt: null },
        data: { revokedAt: new Date() },
      });
    });

    return this.findOne(staffId);
  }

  private shouldAutoRefreshPermissionRow(row: any, force = false) {
    if (force) return true;

    const keys = Array.isArray(row?.permissionKeys)
      ? row.permissionKeys
          .map((key: any) => String(key || "").trim())
          .filter(Boolean)
      : [];

    const note = String(row?.note || "").toLowerCase();

    // Chỉ tự sửa các dòng quyền sinh tự động hoặc dòng cũ chưa có permissionKeys.
    // Dòng user chỉnh tay từ UI vẫn được giữ nguyên, trừ khi gọi force=true từ lưu mẫu role.
    return (
      keys.length === 0 ||
      note.includes("auto generated") ||
      note.includes("auto synced")
    );
  }

  async syncPermissionsForStaff(
    staffId: string,
    options: { force?: boolean } = {},
  ) {
    await this.ensureStaffBranchPermissionOverrideColumns();
    const staff = await this.prisma.staffUser.findUnique({
      where: { id: staffId },
      include: {
        branchRoles: true,
        branchPermissions: true,
      },
    });

    if (!staff) {
      // Staff có thể đã bị xoá/nghỉ nhưng frontend/backend vẫn đang sync lại mẫu quyền.
      // Không được để một staff stale làm fail toàn bộ PATCH /staff/role-templates.
      return null;
    }

    const branchRoles = Array.isArray(staff.branchRoles)
      ? staff.branchRoles
      : [];

    // Không có branchRoles thì không tự mở quyền gì. Đây là nguyên tắc an toàn:
    // permission rỗng = không hiện menu, không fallback theo role legacy.
    if (!branchRoles.length) return staff;

    await this.assertBranchesExist(branchRoles.map((row: any) => row.branchId));

    await this.prisma.$transaction(async (tx) => {
      for (const branchRole of branchRoles) {
        const branchId = String(branchRole.branchId || "").trim();
        const roleCode = this.validateRole(
          branchRole.roleCode || staff.role || "retail-staff",
        );
        if (!branchId) continue;

        const existing = staff.branchPermissions.find(
          (row: any) => String(row.branchId) === branchId,
        );

        if (
          existing &&
          !this.shouldAutoRefreshPermissionRow(existing, Boolean(options.force))
        ) {
          continue;
        }

        const permissionRow = await this.permissionsForRole(roleCode, tx);
        const existingExtraPermissionKeys = Array.isArray((existing as any)?.extraPermissionKeys)
          ? (existing as any).extraPermissionKeys
          : [];
        const existingDeniedPermissionKeys = Array.isArray((existing as any)?.deniedPermissionKeys)
          ? (existing as any).deniedPermissionKeys
          : [];
        (permissionRow as any).extraPermissionKeys = existingExtraPermissionKeys;
        (permissionRow as any).deniedPermissionKeys = existingDeniedPermissionKeys;

        await tx.staffBranchPermission.upsert({
          where: {
            staffId_branchId: {
              staffId,
              branchId,
            },
          },
          create: {
            staffId,
            branchId,
            ...permissionRow,
            note: `Auto synced from role ${roleCode}`,
          },
          update: {
            ...permissionRow,
            note: `Auto synced from role ${roleCode}`,
          },
        });
      }

      await tx.staffUser.update({
        where: { id: staffId },
        data: { sessionVersion: { increment: 1 } },
      });

      await tx.staffSession.updateMany({
        where: { staffId, revokedAt: null },
        data: { revokedAt: new Date() },
      });
    });

    return this.findOne(staffId).catch(() => null);
  }

  async syncAllPermissionsFromRoleTemplates(options: { force?: boolean } = {}) {
    const staffList = await this.prisma.staffUser.findMany({
      where: { isActive: true },
      select: { id: true },
    });

    let synced = 0;
    let skipped = 0;

    for (const staff of staffList) {
      try {
        const result = await this.syncPermissionsForStaff(staff.id, options);
        if (result) synced += 1;
        else skipped += 1;
      } catch (err: any) {
        // Không để một nhân viên stale / branch role lỗi làm fail thao tác lưu mẫu quyền.
        // Trang phân quyền đang lưu role template, không phải update riêng nhân viên.
        skipped += 1;
        console.warn("[RBAC_SYNC_SKIP_STAFF]", staff.id, err?.message || err);
      }
    }

    return {
      success: true,
      synced,
      skipped,
      force: Boolean(options.force),
    };
  }



  // ===== Department Studio =====
  // Lưu ý: dùng raw SQL an toàn để không bị runtime 500 nếu DB chưa kịp có Prisma model Department.
  // Sau khi ổn định nên đưa 2 model Department/StaffDepartment vào prisma/schema.prisma.
  private async ensureDepartmentTables() {
    await this.prisma.$executeRawUnsafe(`
      CREATE TABLE IF NOT EXISTS "Department" (
        "id" TEXT PRIMARY KEY,
        "name" TEXT NOT NULL,
        "code" TEXT NOT NULL UNIQUE,
        "description" TEXT,
        "color" TEXT NOT NULL DEFAULT '#6366f1',
        "isActive" BOOLEAN NOT NULL DEFAULT TRUE,
        "createdAt" TIMESTAMP NOT NULL DEFAULT NOW(),
        "updatedAt" TIMESTAMP NOT NULL DEFAULT NOW()
      );
    `);

    await this.prisma.$executeRawUnsafe(`
      CREATE TABLE IF NOT EXISTS "StaffDepartment" (
        "id" TEXT PRIMARY KEY,
        "staffId" TEXT NOT NULL,
        "departmentId" TEXT NOT NULL,
        "isHead" BOOLEAN NOT NULL DEFAULT FALSE,
        "createdAt" TIMESTAMP NOT NULL DEFAULT NOW(),
        CONSTRAINT "StaffDepartment_staff_department_unique" UNIQUE ("staffId", "departmentId")
      );
    `);

    await this.prisma.$executeRawUnsafe(`
      CREATE INDEX IF NOT EXISTS "StaffDepartment_staffId_idx"
      ON "StaffDepartment" ("staffId");
    `);

    await this.prisma.$executeRawUnsafe(`
      CREATE INDEX IF NOT EXISTS "StaffDepartment_departmentId_idx"
      ON "StaffDepartment" ("departmentId");
    `);
  }

  private normalizeDepartmentCode(value: string) {
    return normalizeRoleCode(value).replace(/_/g, "-");
  }

  private async getDepartmentsForStaffIds(staffIds: string[]) {
    await this.ensureDepartmentTables();
    const ids = UNIQUE(staffIds);
    if (!ids.length) return new Map<string, any[]>();

    const rows = await this.prisma.$queryRawUnsafe<any[]>(
      `
        SELECT
          sd."staffId",
          sd."departmentId",
          sd."isHead",
          d."id",
          d."name",
          d."code",
          d."description",
          d."color",
          d."isActive"
        FROM "StaffDepartment" sd
        INNER JOIN "Department" d ON d."id" = sd."departmentId"
        WHERE sd."staffId" = ANY($1)
        ORDER BY d."name" ASC
      `,
      ids,
    );

    const map = new Map<string, any[]>();
    for (const row of rows) {
      const list = map.get(row.staffId) || [];
      list.push({
        staffId: row.staffId,
        departmentId: row.departmentId,
        isHead: Boolean(row.isHead),
        department: {
          id: row.id,
          name: row.name,
          code: row.code,
          description: row.description,
          color: row.color,
          isActive: row.isActive,
        },
      });
      map.set(row.staffId, list);
    }
    return map;
  }

  async getDepartments() {
    await this.ensureDepartmentTables();

    const rows = await this.prisma.$queryRawUnsafe<any[]>(`
      SELECT
        d."id",
        d."name",
        d."code",
        d."description",
        d."color",
        d."isActive",
        d."createdAt",
        d."updatedAt",
        COALESCE(
          json_agg(
            json_build_object(
              'staffId', sd."staffId",
              'isHead', sd."isHead",
              'staff', json_build_object(
                'id', s."id",
                'name', s."name",
                'code', s."code",
                'isActive', s."isActive"
              )
            )
          ) FILTER (WHERE sd."staffId" IS NOT NULL),
          '[]'
        ) AS "members"
      FROM "Department" d
      LEFT JOIN "StaffDepartment" sd ON sd."departmentId" = d."id"
      LEFT JOIN "StaffUser" s ON s."id" = sd."staffId"
      GROUP BY d."id"
      ORDER BY d."createdAt" DESC
    `);

    return rows.map((row) => ({
      ...row,
      isActive: row.isActive !== false,
      members: Array.isArray(row.members) ? row.members : [],
    }));
  }

  async createDepartment(dto: any) {
    await this.ensureDepartmentTables();

    const name = String(dto?.name || "").trim();
    if (!name) throw new BadRequestException("Thiếu tên phòng ban.");

    const code = this.normalizeDepartmentCode(dto?.code || name);
    const id = `dept_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;

    const rows = await this.prisma.$queryRawUnsafe<any[]>(
      `
        INSERT INTO "Department" (
          "id", "name", "code", "description", "color", "isActive", "createdAt", "updatedAt"
        ) VALUES ($1, $2, $3, $4, $5, TRUE, NOW(), NOW())
        ON CONFLICT ("code") DO UPDATE SET
          "name" = EXCLUDED."name",
          "description" = EXCLUDED."description",
          "color" = EXCLUDED."color",
          "isActive" = TRUE,
          "updatedAt" = NOW()
        RETURNING *
      `,
      id,
      name,
      code,
      String(dto?.description || "").trim() || null,
      String(dto?.color || "#6366f1"),
    );

    return rows[0];
  }

  async updateDepartment(id: string, dto: any) {
    await this.ensureDepartmentTables();

    const rows = await this.prisma.$queryRawUnsafe<any[]>(
      `
        UPDATE "Department"
        SET
          "name" = COALESCE($2, "name"),
          "description" = $3,
          "color" = COALESCE($4, "color"),
          "isActive" = COALESCE($5, "isActive"),
          "updatedAt" = NOW()
        WHERE "id" = $1
        RETURNING *
      `,
      id,
      dto?.name !== undefined ? String(dto.name || "").trim() : null,
      dto?.description !== undefined ? String(dto.description || "").trim() || null : undefined,
      dto?.color !== undefined ? String(dto.color || "#6366f1") : null,
      dto?.isActive !== undefined ? Boolean(dto.isActive) : null,
    );

    if (!rows.length) throw new BadRequestException("Phòng ban không tồn tại.");
    return rows[0];
  }

  async deleteDepartment(id: string) {
    await this.ensureDepartmentTables();

    await this.prisma.$transaction(async (tx) => {
      await tx.$executeRawUnsafe(`DELETE FROM "StaffDepartment" WHERE "departmentId" = $1`, id);
      await tx.$executeRawUnsafe(`DELETE FROM "Department" WHERE "id" = $1`, id);
    });

    return { success: true, message: "Đã xoá phòng ban." };
  }

  async updateStaffDepartments(staffId: string, dto: any) {
    await this.ensureDepartmentTables();

    const staff = await this.prisma.staffUser.findUnique({
      where: { id: staffId },
      select: { id: true },
    });

    if (!staff) {
      // Bulk save phòng ban không được chết cả lô vì 1 nhân viên stale/đã xoá.
      return {
        success: false,
        skipped: true,
        staffId,
        message: "Nhân viên không tồn tại, đã bỏ qua dòng này.",
      };
    }

    const departmentIds = UNIQUE(Array.isArray(dto?.departmentIds) ? dto.departmentIds : []);

    if (departmentIds.length) {
      const rows = await this.prisma.$queryRawUnsafe<any[]>(
        `SELECT "id" FROM "Department" WHERE "id" = ANY($1)`,
        departmentIds,
      );
      const existing = new Set(rows.map((row) => row.id));
      const missing = departmentIds.find((id) => !existing.has(id));
      if (missing) throw new BadRequestException(`Phòng ban không tồn tại: ${missing}`);
    }

    await this.prisma.$transaction(async (tx) => {
      await tx.$executeRawUnsafe(`DELETE FROM "StaffDepartment" WHERE "staffId" = $1`, staffId);

      for (const departmentId of departmentIds) {
        await tx.$executeRawUnsafe(
          `
            INSERT INTO "StaffDepartment" ("id", "staffId", "departmentId", "isHead", "createdAt")
            VALUES ($1, $2, $3, FALSE, NOW())
            ON CONFLICT ("staffId", "departmentId") DO NOTHING
          `,
          `sdept_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
          staffId,
          departmentId,
        );
      }
    });

    return this.findOne(staffId);
  }


  private async bumpStaffSessionVersion(staffId: string, tx?: any) {
    const client = tx || this.prisma;
    await client.staffUser.update({
      where: { id: staffId },
      data: { sessionVersion: { increment: 1 } },
    });
    await client.staffSession.updateMany({
      where: { staffId, revokedAt: null },
      data: { revokedAt: new Date() },
    });
  }

  async updateStatus(id: string, dto: UpdateStaffStatusDto) {
    await this.prisma.$transaction(async (tx) => {
      await tx.staffUser.update({
        where: { id },
        data: {
          isActive: dto.status === "ACTIVE",
          sessionVersion: { increment: 1 },
        },
      });

      await tx.staffSession.updateMany({
        where: { staffId: id, revokedAt: null },
        data: { revokedAt: new Date() },
      });
    });

    return this.findOne(id);
  }

  async updatePassword(id: string, newPassword: string) {
    if (!newPassword || newPassword.length < 4) {
      throw new BadRequestException("Mật khẩu tối thiểu 4 ký tự");
    }

    const hash = await bcrypt.hash(newPassword, 10);

    await this.prisma.$transaction(async (tx) => {
      await tx.staffUser.update({
        where: { id },
        data: {
          passwordHash: hash,
          sessionVersion: { increment: 1 },
        },
      });

      await tx.staffSession.updateMany({
        where: { staffId: id, revokedAt: null },
        data: { revokedAt: new Date() },
      });
    });

    return this.findOne(id);
  }

  async updateSecondPassword(id: string, secondPassword: string) {
    if (!secondPassword || secondPassword.trim().length < 6) {
      throw new BadRequestException("Mật khẩu lớp 2 tối thiểu 6 ký tự.");
    }

    const hash = await bcrypt.hash(secondPassword.trim(), 10);

    await this.prisma.$transaction(async (tx) => {
      await tx.staffUser.update({
        where: { id },
        data: {
          secondPasswordHash: hash,
          secondPasswordEnabled: true,
          sessionVersion: { increment: 1 },
        },
      });

      await tx.staffSession.updateMany({
        where: { staffId: id, revokedAt: null },
        data: { revokedAt: new Date() },
      });
    });

    return { message: "Đã cập nhật mật khẩu lớp 2." };
  }
  async deleteStaff(id: string) {
    const staff = await this.prisma.staffUser.findUnique({
      where: { id },
    });

    if (!staff) {
      throw new BadRequestException("Nhân viên không tồn tại");
    }

    if (String(staff.role || "").toLowerCase() === "owner") {
      throw new BadRequestException("Không thể xoá owner");
    }

    await this.prisma.$transaction(async (tx) => {
      await tx.staffSession.deleteMany({ where: { staffId: id } });
      await tx.staffDepartment.deleteMany({ where: { staffId: id } });
      await tx.staffBranchPermission.deleteMany({ where: { staffId: id } });
      await tx.staffBranchRole.deleteMany({ where: { staffId: id } });
      await tx.staffUserRole.deleteMany({ where: { staffId: id } });
      await tx.staffUser.delete({ where: { id } });
    });

    return { success: true, message: "Đã xoá nhân viên" };
  }

  async changeOwnPassword(
    staffId: string,
    currentPassword: string,
    newPassword: string,
  ) {
    if (!staffId) throw new BadRequestException("Không xác định được nhân viên.");

    const staff = await this.prisma.staffUser.findUnique({
      where: { id: staffId },
    });

    if (!staff) throw new BadRequestException("Nhân viên không tồn tại.");
    if (!newPassword || newPassword.length < 6) {
      throw new BadRequestException("Mật khẩu mới tối thiểu 6 ký tự.");
    }

    const ok = await bcrypt.compare(currentPassword || "", staff.passwordHash || "");

    if (!ok) throw new BadRequestException("Mật khẩu hiện tại không đúng.");

    const passwordHash = await bcrypt.hash(newPassword, 10);

    await this.prisma.$transaction(async (tx) => {
      await tx.staffUser.update({
        where: { id: staffId },
        data: {
          passwordHash,
          sessionVersion: { increment: 1 },
        },
      });

      await tx.staffSession.updateMany({
        where: { staffId, revokedAt: null },
        data: { revokedAt: new Date() },
      });
    });

    return { success: true, message: "Đã đổi mật khẩu." };
  }

  async changeOwnSecurityPin(
    staffId: string,
    currentPassword: string,
    newPin: string,
  ) {
    if (!staffId) throw new BadRequestException("Không xác định được nhân viên.");

    const staff = await this.prisma.staffUser.findUnique({
      where: { id: staffId },
    });

    if (!staff) throw new BadRequestException("Nhân viên không tồn tại.");
    if (!/^\d{4,6}$/.test(String(newPin || ""))) {
      throw new BadRequestException("PIN bảo mật phải từ 4-6 số.");
    }

    const ok = await bcrypt.compare(currentPassword || "", staff.passwordHash || "");

    if (!ok) throw new BadRequestException("Mật khẩu hiện tại không đúng.");

    const secondPasswordHash = await bcrypt.hash(newPin, 10);

    await this.prisma.$transaction(async (tx) => {
      await tx.staffUser.update({
        where: { id: staffId },
        data: {
          secondPasswordHash,
          secondPasswordEnabled: true,
          sessionVersion: { increment: 1 },
        },
      });

      await tx.staffSession.updateMany({
        where: { staffId, revokedAt: null },
        data: { revokedAt: new Date() },
      });
    });

    return { success: true, message: "Đã đổi PIN bảo mật." };
  }

}
