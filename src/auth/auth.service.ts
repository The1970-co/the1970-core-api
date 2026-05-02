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

  private async getStaffAuthData(userId: string) {
    const user = await this.prisma.staffUser.findUnique({
      where: { id: userId },
      include: {
        roles: true,
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

    return Array.from(new Set([...rolesFromRelation, legacyRole].filter(Boolean)));
  }

  private async buildToken(user: any) {
    const authUser = user.roles && user.branchPermissions
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
        branchPermissions: authUser.branchPermissions || [],
      },
      process.env.JWT_SECRET || "dev-secret",
      { expiresIn: "7d" }
    );
  }

  private async buildUser(user: any) {
    const authUser = user.roles && user.branchPermissions
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
      branchPermissions: authUser.branchPermissions || [],
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
        OR: [
          { code },
          { username: code },
          { email: code },
        ],
      },
      include: {
        roles: true,
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
        process.env.JWT_SECRET || "dev-secret",
        { expiresIn: "5m" }
      );

      return {
        needsSecondPassword: true,
        tempToken,
        user: await this.buildUser(freshUser),
      };
    }

    return {
      token: await this.buildToken(freshUser),
      user: await this.buildUser(freshUser),
    };
  }

  async verifySecondPassword(tempToken: string, secondPassword: string) {
    if (!tempToken || !secondPassword) {
      throw new UnauthorizedException("PIN bảo mật không đúng.");
    }

    let payload: any;

    try {
      payload = jwt.verify(tempToken, process.env.JWT_SECRET || "dev-secret");
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

    const freshUser = await this.getStaffAuthData(user.id);

    return {
      token: await this.buildToken(freshUser),
      user: await this.buildUser(freshUser),
    };
  }

  async me(userId: string) {
    const user = await this.prisma.staffUser.findUnique({
      where: { id: userId },
      include: {
        roles: true,
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