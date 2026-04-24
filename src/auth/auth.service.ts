import { Injectable, UnauthorizedException } from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";
import * as bcrypt from "bcrypt";
import * as jwt from "jsonwebtoken";

@Injectable()
export class AuthService {
  constructor(private prisma: PrismaService) {}

  async login(codeInput: string, password: string) {
    if (!codeInput || !password) {
      throw new UnauthorizedException("Thiếu thông tin đăng nhập");
    }

    const code = codeInput.trim();
    console.log("login input:", code);

    const user = await this.prisma.staffUser.findFirst({
      where: { code },
    });

    console.log("found staff user:", user);

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

    const token = jwt.sign(
      {
        id: user.id,
        sub: user.id,
        code: user.code,
        role: String(user.role).toLowerCase(),
        branchId: user.branchId,
        branchName: user.branchName,
        name: user.name,
        type: "staff",
      },
      process.env.JWT_SECRET || "dev-secret",
      { expiresIn: "7d" }
    );

    return {
      token,
      user: {
        id: user.id,
        code: user.code,
        name: user.name,
        role: String(user.role).toLowerCase(),
        branchId: user.branchId,
        branchName: user.branchName,
        type: "staff",
        status: user.isActive ? "active" : "inactive",
      },
    };
  }

  async me(userId: string) {
    const user = await this.prisma.staffUser.findUnique({
      where: { id: userId },
      select: {
        id: true,
        code: true,
        name: true,
        role: true,
        branchId: true,
        branchName: true,
        isActive: true,
        lastLoginAt: true,
      },
    });

    if (!user || !user.isActive) {
      throw new UnauthorizedException("Tài khoản không hợp lệ.");
    }

    return {
      id: user.id,
      code: user.code,
      name: user.name,
      role: String(user.role).toLowerCase(),
      branchId: user.branchId,
      branchName: user.branchName,
      type: "staff",
      status: "active",
      lastLoginAt: user.lastLoginAt,
    };
  }
}