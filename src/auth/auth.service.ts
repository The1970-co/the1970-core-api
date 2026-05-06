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

    return user;
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