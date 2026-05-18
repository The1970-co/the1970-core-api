import {
  BadRequestException,
  Injectable,
  UnauthorizedException,
} from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";
import * as bcrypt from "bcrypt";
import * as jwt from "jsonwebtoken";
import { createHash } from "crypto";

@Injectable()
export class AuthService {
  constructor(private prisma: PrismaService) {}

  private readonly jwtSecret = process.env.JWT_SECRET || "dev-secret";
  private readonly refreshSecret =
    process.env.JWT_REFRESH_SECRET || process.env.JWT_SECRET || "dev-secret";

  private normalizeRole(role: any) {
    return String(role || "").trim().toLowerCase();
  }

  private getRoleCodes(user: any) {
    const rolesFromRelation = Array.isArray(user?.roles)
      ? user.roles
          .map((r: any) => this.normalizeRole(r.roleCode || r))
          .filter(Boolean)
      : [];

    const legacyRole = this.normalizeRole(user?.role);
    return Array.from(new Set([...rolesFromRelation, legacyRole].filter(Boolean)));
  }

  private isOwnerOrAdmin(user: any) {
    const roles = this.getRoleCodes(user);
    return roles.includes("owner") || roles.includes("admin");
  }


  private getBranchIds(user: any) {
    const ids = new Set<string>();

    const add = (value: any) => {
      const id = String(value || "").trim();
      if (id) ids.add(id);
    };

    add(user?.branchId);

    if (Array.isArray(user?.branchRoles)) {
      user.branchRoles.forEach((row: any) => add(row?.branchId || row?.branch?.id));
    }

    if (Array.isArray(user?.branchPermissions)) {
      user.branchPermissions.forEach((row: any) => add(row?.branchId || row?.branch?.id));
    }

    return Array.from(ids);
  }

  private getBranchLabel(user: any, branchId: string) {
    const target = String(branchId || "").trim();
    if (!target) return "";

    const roleRow = Array.isArray(user?.branchRoles)
      ? user.branchRoles.find((row: any) => String(row?.branchId || row?.branch?.id || "").trim() === target)
      : null;

    const permissionRow = Array.isArray(user?.branchPermissions)
      ? user.branchPermissions.find((row: any) => String(row?.branchId || row?.branch?.id || "").trim() === target)
      : null;

    return (
      roleRow?.branch?.name ||
      roleRow?.branchName ||
      permissionRow?.branch?.name ||
      permissionRow?.branchName ||
      (String(user?.branchId || "").trim() === target ? user?.branchName : "") ||
      target
    );
  }

  private getRoleForBranch(user: any, branchId: string) {
    const target = String(branchId || "").trim();
    if (!target) return this.normalizeRole(user?.role);

    const row = Array.isArray(user?.branchRoles)
      ? user.branchRoles.find((item: any) => String(item?.branchId || item?.branch?.id || "").trim() === target)
      : null;

    return this.normalizeRole(row?.roleCode || row?.role || user?.role);
  }

  private buildBranchOptions(user: any) {
    return this.getBranchIds(user).map((branchId) => ({
      branchId,
      branchName: this.getBranchLabel(user, branchId),
      role: this.getRoleForBranch(user, branchId),
    }));
  }

  private hashToken(token: string) {
    return createHash("sha256").update(token).digest("hex");
  }

  private getDeviceInfo(meta?: { userAgent?: string; ipAddress?: string }) {
    return String(meta?.userAgent || "").slice(0, 500) || null;
  }

  private getIpAddress(meta?: { userAgent?: string; ipAddress?: string }) {
    return String(meta?.ipAddress || "").slice(0, 80) || null;
  }

  private getStaffInclude() {
    return {
      roles: true,
      branchRoles: { include: { branch: true } },
      branchPermissions: { include: { branch: true } },
    } as const;
  }

  private async getActiveStaff(userId: string) {
    const user = await this.prisma.staffUser.findUnique({
      where: { id: userId },
      include: this.getStaffInclude(),
    });

    if (!user || !user.isActive) {
      throw new UnauthorizedException("Tài khoản không hợp lệ.");
    }

    return user;
  }

  private buildPermissionKeys(user: any) {
    if (this.isOwnerOrAdmin(user)) return ["*"];

    const keys = new Set<string>();
    const rows = Array.isArray(user?.branchPermissions)
      ? user.branchPermissions
      : [];

    const addKeys = (values: any[]) => {
      if (!Array.isArray(values)) return;
      values
        .map((key: any) => String(key || "").trim())
        .filter(Boolean)
        .forEach((key: string) => keys.add(key));
    };

    const removeKeys = (values: any[]) => {
      if (!Array.isArray(values)) return;
      values
        .map((key: any) => String(key || "").trim())
        .filter(Boolean)
        .forEach((key: string) => keys.delete(key));
    };

    for (const row of rows) {
      addKeys(row?.permissionKeys);
      addKeys(row?.extraPermissionKeys);
      removeKeys(row?.deniedPermissionKeys);
    }

    return Array.from(keys);
  }

  private async buildAccessToken(user: any, session: any) {
    return jwt.sign(
      {
        sub: user.id,
        id: user.id,
        sid: session.id,
        sv: user.sessionVersion || 1,
        type: "access",
      },
      this.jwtSecret,
      { expiresIn: "15m" },
    );
  }

  private async buildRefreshToken(user: any, session: any) {
    return jwt.sign(
      {
        sub: user.id,
        sid: session.id,
        sv: user.sessionVersion || 1,
        type: "refresh",
      },
      this.refreshSecret,
      { expiresIn: "30d" },
    );
  }

  private async createSession(user: any, meta?: { userAgent?: string; ipAddress?: string }) {
    const expiresAt = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000);

    return this.prisma.staffSession.create({
      data: {
        staffId: user.id,
        refreshTokenHash: "pending",
        deviceInfo: this.getDeviceInfo(meta),
        ipAddress: this.getIpAddress(meta),
        sessionVersion: user.sessionVersion || 1,
        expiresAt,
      },
    });
  }

  private async issueTokens(user: any, session: any) {
    const accessToken = await this.buildAccessToken(user, session);
    const refreshToken = await this.buildRefreshToken(user, session);

    await this.prisma.staffSession.update({
      where: { id: session.id },
      data: {
        refreshTokenHash: this.hashToken(refreshToken),
        sessionVersion: user.sessionVersion || 1,
        expiresAt: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000),
        revokedAt: null,
      },
    });

    return { accessToken, refreshToken };
  }

  private async buildAuthResponse(user: any, meta?: { userAgent?: string; ipAddress?: string }) {
    const freshUser = await this.getActiveStaff(user.id);
    const session = await this.createSession(freshUser, meta);
    const tokens = await this.issueTokens(freshUser, session);

    return {
      token: tokens.accessToken,
      accessToken: tokens.accessToken,
      refreshToken: tokens.refreshToken,
      user: await this.buildUser(freshUser),
    };
  }

  async buildUser(user: any) {
    const authUser =
      user?.roles && user?.branchRoles && user?.branchPermissions
        ? user
        : await this.getActiveStaff(user.id || user.sub);

    const branchIds = this.getBranchIds(authUser);
    const activeBranchId = authUser.branchId || branchIds[0] || null;

    return {
      id: authUser.id,
      code: authUser.code,
      name: authUser.name,
      role: this.normalizeRole(authUser.role),
      roles: this.getRoleCodes(authUser),
      branchId: authUser.branchId,
      branchName: authUser.branchName,
      activeBranchId,
      branchIds,
      branchOptions: this.buildBranchOptions(authUser),
      branchRoles: authUser.branchRoles || [],
      branchPermissions: authUser.branchPermissions || [],
      permissions: this.buildPermissionKeys(authUser),
      sessionVersion: authUser.sessionVersion || 1,
      type: "staff",
      status: authUser.isActive ? "active" : "inactive",
      lastLoginAt: authUser.lastLoginAt,
    };
  }

  async login(
    codeInput: string,
    password: string,
    meta?: { userAgent?: string; ipAddress?: string },
  ) {
    if (!codeInput || !password) {
      throw new UnauthorizedException("Thiếu thông tin đăng nhập");
    }

    const code = codeInput.trim();

    const user = await this.prisma.staffUser.findFirst({
      where: {
        OR: [{ code }, { username: code }, { email: code }],
      },
      include: this.getStaffInclude(),
    });

    if (!user || !user.isActive) {
      throw new UnauthorizedException("Tài khoản không tồn tại hoặc đã bị khóa.");
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

    const freshUser = await this.getActiveStaff(user.id);

    if (freshUser.secondPasswordEnabled) {
      const tempToken = jwt.sign(
        {
          id: freshUser.id,
          sub: freshUser.id,
          code: freshUser.code,
          type: "second-password",
        },
        this.jwtSecret,
        { expiresIn: "5m" },
      );

      return {
        needsSecondPassword: true,
        tempToken,
        user: await this.buildUser(freshUser),
      };
    }

    return this.buildAuthResponse(freshUser, meta);
  }

  async verifySecondPassword(
    tempToken: string,
    secondPassword: string,
    meta?: { userAgent?: string; ipAddress?: string },
  ) {
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
      include: this.getStaffInclude(),
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

    return this.buildAuthResponse(user, meta);
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

    if (payload.type !== "refresh" || !payload.sub || !payload.sid) {
      throw new UnauthorizedException("Refresh token không hợp lệ.");
    }

    const session = await this.prisma.staffSession.findUnique({
      where: { id: String(payload.sid) },
      include: { staff: { include: this.getStaffInclude() } },
    });

    if (!session || session.revokedAt || session.expiresAt <= new Date()) {
      throw new UnauthorizedException("Phiên đăng nhập đã hết hạn.");
    }

    if (session.refreshTokenHash !== this.hashToken(refreshToken)) {
      await this.prisma.staffSession.update({
        where: { id: session.id },
        data: { revokedAt: new Date() },
      });
      throw new UnauthorizedException("Phiên đăng nhập không hợp lệ.");
    }

    if (!session.staff?.isActive) {
      throw new UnauthorizedException("Tài khoản không hợp lệ.");
    }

    if ((session.staff.sessionVersion || 1) !== Number(payload.sv || session.sessionVersion || 1)) {
      await this.prisma.staffSession.update({
        where: { id: session.id },
        data: { revokedAt: new Date() },
      });
      throw new UnauthorizedException("Quyền đã thay đổi, vui lòng đăng nhập lại.");
    }

    const tokens = await this.issueTokens(session.staff, session);

    return {
      token: tokens.accessToken,
      accessToken: tokens.accessToken,
      refreshToken: tokens.refreshToken,
      user: await this.buildUser(session.staff),
    };
  }

  async logout(refreshToken?: string, sessionId?: string) {
    if (sessionId) {
      await this.prisma.staffSession
        .update({ where: { id: sessionId }, data: { revokedAt: new Date() } })
        .catch(() => null);
      return { message: "Đăng xuất thành công" };
    }

    if (refreshToken) {
      let payload: any = null;
      try {
        payload = jwt.verify(refreshToken, this.refreshSecret);
      } catch {
        payload = null;
      }

      if (payload?.sid) {
        await this.prisma.staffSession
          .update({ where: { id: String(payload.sid) }, data: { revokedAt: new Date() } })
          .catch(() => null);
      }
    }

    return { message: "Đăng xuất thành công" };
  }

  async me(userId: string) {
    const user = await this.getActiveStaff(userId);
    return {
      ...(await this.buildUser(user)),
      status: "active",
    };
  }

  async changeMyPassword(userId: string, oldPassword: string, newPassword: string) {
    const strongPasswordRegex =
      /^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[^A-Za-z\d]).{8,}$/;

    if (!oldPassword || !newPassword) {
      throw new BadRequestException("Thiếu mật khẩu cũ hoặc mật khẩu mới.");
    }

    if (!strongPasswordRegex.test(newPassword)) {
      throw new BadRequestException(
        "Mật khẩu mới phải có ít nhất 8 ký tự, gồm chữ hoa, chữ thường, số và ký tự đặc biệt.",
      );
    }

    const user = await this.prisma.staffUser.findUnique({ where: { id: userId } });

    if (!user || !user.isActive || !user.passwordHash) {
      throw new UnauthorizedException("Tài khoản không hợp lệ.");
    }

    const match = await bcrypt.compare(oldPassword, user.passwordHash);
    if (!match) {
      throw new UnauthorizedException("Mật khẩu cũ không đúng.");
    }

    const hash = await bcrypt.hash(newPassword, 10);

    await this.prisma.$transaction(async (tx) => {
      await tx.staffUser.update({
        where: { id: userId },
        data: {
          passwordHash: hash,
          sessionVersion: { increment: 1 },
        },
      });

      await tx.staffSession.updateMany({
        where: { staffId: userId, revokedAt: null },
        data: { revokedAt: new Date() },
      });
    });

    return { message: "Đổi mật khẩu thành công" };
  }
}
