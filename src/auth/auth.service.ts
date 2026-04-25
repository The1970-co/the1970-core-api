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

  private buildToken(user: any) {
    return jwt.sign(
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
  }

  private buildUser(user: any) {
    return {
      id: user.id,
      code: user.code,
      name: user.name,
      role: String(user.role).toLowerCase(),
      branchId: user.branchId,
      branchName: user.branchName,
      type: "staff",
      status: user.isActive ? "active" : "inactive",
    };
  }

  async login(codeInput: string, password: string) {
    if (!codeInput || !password) {
      throw new UnauthorizedException("Thiếu thông tin đăng nhập");
    }

    const code = codeInput.trim();

    const user = await this.prisma.staffUser.findFirst({
      where: { code },
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

    if (user.secondPasswordEnabled) {
      const tempToken = jwt.sign(
        {
          id: user.id,
          sub: user.id,
          code: user.code,
          type: "second-password",
        },
        process.env.JWT_SECRET || "dev-secret",
        { expiresIn: "5m" }
      );

      return {
        needsSecondPassword: true,
        tempToken,
        user: this.buildUser(user),
      };
    }

    return {
      token: this.buildToken(user),
      user: this.buildUser(user),
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

    return {
      token: this.buildToken(user),
      user: this.buildUser(user),
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