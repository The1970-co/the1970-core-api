import {
  BadRequestException,
  Injectable,
  UnauthorizedException,
} from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";
import * as bcrypt from "bcrypt";
import * as jwt from "jsonwebtoken";

@Injectable()
export class AuthService {
  constructor(private prisma: PrismaService) {}

  private readonly jwtSecret = process.env.JWT_SECRET || "dev-secret";
  private readonly refreshSecret =
    process.env.JWT_REFRESH_SECRET || process.env.JWT_SECRET || "dev-secret";

  private async getStaffAuthData(userId: string) {
    const user = await this.prisma.staffUser.findUnique({
      where: { id: userId },
      include: {
        roles: true,
        branchRoles: { include: { branch: true } },
        branchPermissions: true,
      },
    });

    if (!user || !user.isActive) {
      throw new UnauthorizedException("Tài khoản không hợp lệ.");
    }

    return this.syncPermissionsForAuthUser(user);
  }

  private normalizeRole(role: any) {
    return String(role || "").toLowerCase();
  }

  private getRoleCodes(user: any) {
    const rolesFromRelation = Array.isArray(user.roles)
      ? user.roles
          .map((r: any) => this.normalizeRole(r.roleCode))
          .filter(Boolean)
      : [];

    const legacyRole = this.normalizeRole(user.role);

    return Array.from(
      new Set([...rolesFromRelation, legacyRole].filter(Boolean))
    );
  }


  private shouldAutoRefreshPermissionRow(row: any) {
    const keys = Array.isArray(row?.permissionKeys)
      ? row.permissionKeys.map((key: any) => String(key || "").trim()).filter(Boolean)
      : [];

    const note = String(row?.note || "").toLowerCase();

    // Không tự ghi đè dòng quyền đã lưu tay từ UI.
    // Chỉ backfill dòng cũ/trống hoặc dòng sinh tự động để tránh bung menu sau deploy.
    return keys.length === 0 || note.includes("auto generated") || note.includes("auto synced");
  }

  private legacyPermissionRowForRole(roleCode: string) {
    const role = this.normalizeRole(roleCode || "retail-staff");

    const base: Record<string, any> = {
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
    };

    if (role === "owner" || role === "admin" || role === "branch-manager") {
      Object.keys(base).forEach((key) => (base[key] = true));
    } else if (role === "fulltime") {
      Object.assign(base, {
        canView: true,
        canSell: true,
        canViewOwnOrders: true,
        canCreateOrder: true,
        canHandleReturn: true,
        canViewStock: true,
        canStocktake: true,
        canTransferStock: true,
        canReceiveStock: true,
        canViewCustomer: true,
      });
    } else if (role === "retail-staff") {
      Object.assign(base, {
        canView: true,
        canSell: true,
        canViewOwnOrders: true,
        canCreateOrder: true,
        canHandleReturn: true,
        canViewStock: true,
        canViewCustomer: true,
      });
    } else if (role === "stock-auditor") {
      Object.assign(base, {
        canView: true,
        canViewStock: true,
        canStocktake: true,
      });
    } else if (role === "stock-staff") {
      Object.assign(base, {
        canView: true,
        canViewStock: true,
        canManageStock: true,
        canStocktake: true,
        canTransferStock: true,
        canReceiveStock: true,
      });
    }

    return {
      ...base,
      permissionKeys: this.permissionKeysFromPermissionRow(base),
    };
  }

  private permissionKeyForTemplateLabel(groupKey: string, permissionName: string) {
    const map: Record<string, string> = {
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
      "Autopilot": "menu.autopilot",
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
      "Xem khuyến mãi": "promotions.view",
      "Tạo khuyến mãi": "promotions.create",
      "Sửa khuyến mãi": "promotions.edit",
      "Kích hoạt khuyến mãi": "promotions.activate",
      "Tạm dừng khuyến mãi": "promotions.pause",
      "Xóa khuyến mãi": "promotions.delete",
    };

    if (groupKey === "transfers" && permissionName === "Nhận hàng vào kho") return "stock_transfer.receive";
    if (groupKey === "returns" && permissionName === "Nhận hàng vào kho") return "returns.receive";
    if (groupKey === "purchaseReceipts" && permissionName === "Nhận hàng vào kho") return "purchase_receipt.receive";

    return map[permissionName] || `${groupKey}.${String(permissionName || "")
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/đ/g, "d")
      .replace(/Đ/g, "D")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .replace(/^_+|_+$/g, "")}`;
  }

  private permissionRowFromTemplatePermissions(permissions: any) {
    const row: any = this.legacyPermissionRowForRole("");
    Object.keys(row).forEach((key) => {
      if (key !== "permissionKeys") row[key] = false;
    });

    const keys: string[] = [];
    const groups = permissions && typeof permissions === "object" ? permissions : {};

    for (const [groupKey, permissionNames] of Object.entries(groups)) {
      if (!Array.isArray(permissionNames)) continue;
      for (const permissionName of permissionNames) {
        const label = String(permissionName || "");
        keys.push(this.permissionKeyForTemplateLabel(groupKey, label));

        if (label === "Xem sản phẩm") row.canView = true;
        if (label === "Bán hàng / POS") row.canSell = true;
        if (label === "Xem đơn hàng được phụ trách") row.canViewOwnOrders = true;
        if (label === "Xem tất cả đơn hàng") row.canViewBranchOrders = true;
        if (label === "Tạo đơn hàng") row.canCreateOrder = true;
        if (label === "Duyệt đơn hàng") row.canApproveOrder = true;
        if (label === "Hủy đơn hàng") row.canCancelOrder = true;
        if (label === "Xem đơn trả hàng" || label === "Tạo đơn trả hàng") row.canHandleReturn = true;
        if (label === "Xem tồn kho") row.canViewStock = true;
        if (label === "Quản kho") row.canManageStock = true;
        if (label.includes("phiếu kiểm hàng")) row.canStocktake = true;
        if (label.includes("phiếu chuyển") || label === "Xác nhận chuyển") row.canTransferStock = true;
        if (label === "Nhận hàng vào kho") row.canReceiveStock = true;
        if (label.includes("khách hàng")) row.canViewCustomer = true;
        if (label === "Tạo khách hàng" || label === "Sửa khách hàng") row.canEditCustomer = true;
        if (label === "Xuất file sản phẩm" || label === "Tải Excel sản phẩm") row.canExportProductExcel = true;
        if (label === "Nhập file sản phẩm") row.canImportProductExcel = true;
        if (label === "Xuất file đơn hàng" || label === "Tải Excel đơn hàng") row.canExportOrderExcel = true;
        if (label === "Tải Excel tồn kho") row.canExportInventoryExcel = true;
        if (label === "Xuất file khách hàng" || label === "Tải Excel khách hàng") row.canExportCustomerExcel = true;
        if (label.includes("Báo cáo")) row.canViewReport = true;
        if (label === "Xem giá trị tồn kho") row.canViewMoney = true;
      }
    }

    row.permissionKeys = Array.from(new Set(keys.filter(Boolean)));
    return row;
  }

  private permissionKeysFromPermissionRow(row: Record<string, any>) {
    const keys: string[] = [];

    for (const [field, permissionKeys] of Object.entries(this.LEGACY_BOOLEAN_PERMISSION_MAP)) {
      if (row?.[field]) keys.push(...permissionKeys);
    }

    if (Array.isArray(row?.permissionKeys)) {
      keys.push(...row.permissionKeys.map((key: any) => String(key || "").trim()).filter(Boolean));
    }

    return Array.from(new Set(keys.filter(Boolean)));
  }

  private async permissionRowForRole(roleCode: string) {
    const normalized = this.normalizeRole(roleCode || "retail-staff");
    const template = await this.prisma.staffRoleTemplate.findUnique({
      where: { roleCode: normalized },
    });

    if (!template) return this.legacyPermissionRowForRole(normalized);
    return this.permissionRowFromTemplatePermissions(template.permissions);
  }

  private async syncPermissionsForAuthUser(user: any) {
    const branchRoles = Array.isArray(user?.branchRoles) ? user.branchRoles : [];
    if (!branchRoles.length) return user;

    let changed = false;
    const existingRows = Array.isArray(user?.branchPermissions) ? user.branchPermissions : [];

    for (const branchRole of branchRoles) {
      const staffId = String(user.id || "").trim();
      const branchId = String(branchRole.branchId || "").trim();
      const roleCode = this.normalizeRole(branchRole.roleCode || user.role || "retail-staff");
      if (!staffId || !branchId) continue;

      const existing = existingRows.find((row: any) => String(row.branchId) === branchId);
      if (existing && !this.shouldAutoRefreshPermissionRow(existing)) continue;

      const permissionRow = await this.permissionRowForRole(roleCode);

      await this.prisma.staffBranchPermission.upsert({
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

      changed = true;
    }

    if (!changed) return user;
    return this.prisma.staffUser.findUnique({
      where: { id: user.id },
      include: {
        roles: true,
        branchRoles: { include: { branch: true } },
        branchPermissions: true,
      },
    });
  }

  private readonly LEGACY_BOOLEAN_PERMISSION_MAP: Record<string, string[]> = {
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

  private buildPermissionKeys(user: any) {
    const keys: string[] = [];

    if (this.getRoleCodes(user).some((role) => role === "owner" || role === "admin")) {
      keys.push("*");
    }

    const rows = Array.isArray(user.branchPermissions) ? user.branchPermissions : [];

    for (const row of rows) {
      if (Array.isArray(row.permissionKeys)) {
        keys.push(...row.permissionKeys.map((key: any) => String(key || "").trim()).filter(Boolean));
      }

      for (const [field, permissionKeys] of Object.entries(this.LEGACY_BOOLEAN_PERMISSION_MAP)) {
        if (row?.[field]) keys.push(...permissionKeys);
      }
    }

    return Array.from(new Set(keys.filter(Boolean)));
  }

  private async buildAccessToken(user: any) {
    const authUser =
      user.roles && user.branchRoles && user.branchPermissions
        ? user
        : await this.getStaffAuthData(user.id);

    const roles = this.getRoleCodes(authUser);

    return jwt.sign(
      {
        id: authUser.id,
        sub: authUser.id,
        code: authUser.code,
        role: this.normalizeRole(authUser.role),
        roles,
        branchId: authUser.branchId,
        branchName: authUser.branchName,
        name: authUser.name,
        type: "staff",
        branchRoles: authUser.branchRoles || [],
        branchPermissions: authUser.branchPermissions || [],
        permissions: this.buildPermissionKeys(authUser),
      },
      this.jwtSecret,
      { expiresIn: "15m" }
    );
  }

  private async buildRefreshToken(user: any) {
    const authUser =
      user.roles && user.branchRoles && user.branchPermissions
        ? user
        : await this.getStaffAuthData(user.id);

    return jwt.sign(
      {
        sub: authUser.id,
        code: authUser.code,
        type: "refresh",
      },
      this.refreshSecret,
      { expiresIn: "30d" }
    );
  }

  private async buildAuthResponse(user: any) {
    const freshUser = await this.getStaffAuthData(user.id);
    const accessToken = await this.buildAccessToken(freshUser);
    const refreshToken = await this.buildRefreshToken(freshUser);

    return {
      token: accessToken,
      accessToken,
      refreshToken,
      user: await this.buildUser(freshUser),
    };
  }

  private async buildUser(user: any) {
    const authUser =
      user.roles && user.branchRoles && user.branchPermissions
        ? user
        : await this.getStaffAuthData(user.id);

    const roles = this.getRoleCodes(authUser);

    return {
      id: authUser.id,
      code: authUser.code,
      name: authUser.name,
      role: this.normalizeRole(authUser.role),
      roles,
      branchId: authUser.branchId,
      branchName: authUser.branchName,
      branchRoles: authUser.branchRoles || [],
      branchPermissions: authUser.branchPermissions || [],
      permissions: this.buildPermissionKeys(authUser),
      type: "staff",
      status: authUser.isActive ? "active" : "inactive",
      lastLoginAt: authUser.lastLoginAt,
    };
  }

  async login(codeInput: string, password: string) {
    if (!codeInput || !password) {
      throw new UnauthorizedException("Thiếu thông tin đăng nhập");
    }

    const code = codeInput.trim();

    const user = await this.prisma.staffUser.findFirst({
      where: {
        OR: [{ code }, { username: code }, { email: code }],
      },
      include: {
        roles: true,
        branchRoles: { include: { branch: true } },
        branchPermissions: true,
      },
    });

    if (!user || !user.isActive) {
      throw new UnauthorizedException(
        "Tài khoản không tồn tại hoặc đã bị khóa."
      );
    }

    if (!user.passwordHash) {
      throw new UnauthorizedException("Tài khoản chưa có mật khẩu.");
    }

    const match = await bcrypt.compare(password, user.passwordHash);

    if (!match) {
      throw new UnauthorizedException("Sai mã đăng nhập hoặc mật khẩu.");
    }

    await this.prisma.staffUser.update({
      where: { id: user.id },
      data: { lastLoginAt: new Date() },
    });

    const freshUser = await this.getStaffAuthData(user.id);

    if (freshUser.secondPasswordEnabled) {
      const tempToken = jwt.sign(
        {
          id: freshUser.id,
          sub: freshUser.id,
          code: freshUser.code,
          type: "second-password",
        },
        this.jwtSecret,
        { expiresIn: "5m" }
      );

      return {
        needsSecondPassword: true,
        tempToken,
        user: await this.buildUser(freshUser),
      };
    }

    return this.buildAuthResponse(freshUser);
  }

  async verifySecondPassword(tempToken: string, secondPassword: string) {
    if (!tempToken || !secondPassword) {
      throw new UnauthorizedException("PIN bảo mật không đúng.");
    }

    let payload: any;

    try {
      payload = jwt.verify(tempToken, this.jwtSecret);
    } catch {
      throw new UnauthorizedException("Phiên xác thực lớp 2 đã hết hạn.");
    }

    if (payload.type !== "second-password") {
      throw new UnauthorizedException("Token không hợp lệ.");
    }

    const user = await this.prisma.staffUser.findUnique({
      where: { id: payload.sub },
      include: {
        roles: true,
        branchRoles: { include: { branch: true } },
        branchPermissions: true,
      },
    });

    if (!user || !user.isActive || !user.secondPasswordHash) {
      throw new UnauthorizedException("Tài khoản không hợp lệ.");
    }

    const match = await bcrypt.compare(secondPassword, user.secondPasswordHash);

    if (!match) {
      throw new UnauthorizedException("Mật khẩu lớp 2 không đúng.");
    }

    await this.prisma.staffUser.update({
      where: { id: user.id },
      data: { lastLoginAt: new Date() },
    });

    return this.buildAuthResponse(user);
  }

async refresh(refreshToken: string) {
  if (!refreshToken) {
    throw new UnauthorizedException("Missing refresh token");
  }

  let payload: any;

  try {
    payload = jwt.verify(refreshToken, this.refreshSecret);
  } catch {
    throw new UnauthorizedException("Phiên đăng nhập đã hết hạn.");
  }

  if (payload.type !== "refresh" || !payload.sub) {
    throw new UnauthorizedException("Refresh token không hợp lệ.");
  }

  const user = await this.getStaffAuthData(payload.sub);

  // ✅ FIX: không build 2 lần
  const accessToken = await this.buildAccessToken(user);

  return {
    token: accessToken,
    accessToken,
    user: await this.buildUser(user),
  };
}

  async me(userId: string) {
    const user = await this.prisma.staffUser.findUnique({
      where: { id: userId },
      include: {
        roles: true,
        branchRoles: { include: { branch: true } },
        branchPermissions: true,
      },
    });

    if (!user || !user.isActive) {
      throw new UnauthorizedException("Tài khoản không hợp lệ.");
    }

    return {
      ...(await this.buildUser(user)),
      status: "active",
    };
  }

  async changeMyPassword(
    userId: string,
    oldPassword: string,
    newPassword: string
  ) {
    const strongPasswordRegex =
      /^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[^A-Za-z\d]).{8,}$/;

    if (!oldPassword || !newPassword) {
      throw new BadRequestException("Thiếu mật khẩu cũ hoặc mật khẩu mới.");
    }

    if (!strongPasswordRegex.test(newPassword)) {
      throw new BadRequestException(
        "Mật khẩu mới phải có ít nhất 8 ký tự, gồm chữ hoa, chữ thường, số và ký tự đặc biệt."
      );
    }

    const user = await this.prisma.staffUser.findUnique({
      where: { id: userId },
    });

    if (!user || !user.isActive || !user.passwordHash) {
      throw new UnauthorizedException("Tài khoản không hợp lệ.");
    }

    const match = await bcrypt.compare(oldPassword, user.passwordHash);

    if (!match) {
      throw new UnauthorizedException("Mật khẩu cũ không đúng.");
    }

    const hash = await bcrypt.hash(newPassword, 10);

    await this.prisma.staffUser.update({
      where: { id: userId },
      data: { passwordHash: hash },
    });

    return { message: "Đổi mật khẩu thành công" };
  }
}