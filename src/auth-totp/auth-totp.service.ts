import { BadRequestException, Injectable } from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";
import * as speakeasy from "speakeasy";
import * as QRCode from "qrcode";

@Injectable()
export class AuthTotpService {
  constructor(private readonly prisma: PrismaService) {}

  private async resolveAdminFromJwtUser(jwtUser: any) {
    const userId = jwtUser?.id ? String(jwtUser.id) : "";
    const email = jwtUser?.email ? String(jwtUser.email).trim().toLowerCase() : "";

    if (userId) {
      const byId = await this.prisma.adminUser.findUnique({
        where: { id: userId },
        select: {
          id: true,
          email: true,
          fullName: true,
          role: true,
          totpSecret: true,
          totpEnabled: true,
        },
      });

      if (byId) return byId;
    }

    if (email) {
      const byEmail = await this.prisma.adminUser.findUnique({
        where: { email },
        select: {
          id: true,
          email: true,
          fullName: true,
          role: true,
          totpSecret: true,
          totpEnabled: true,
        },
      });

      if (byEmail) return byEmail;
    }

    throw new BadRequestException(
      "Token hiện tại không khớp với tài khoản AdminUser. Cần đăng nhập bằng tài khoản admin."
    );
  }

  async setup(jwtUser: any) {
    const admin = await this.resolveAdminFromJwtUser(jwtUser);

    const secret = speakeasy.generateSecret({
      name: `The 1970 (${admin.email})`,
      issuer: "The 1970",
      length: 20,
    });

    await this.prisma.adminUser.update({
      where: { id: admin.id },
      data: {
        totpSecret: secret.base32,
        totpEnabled: false,
      },
    });

    const qrCodeDataUrl = await QRCode.toDataURL(secret.otpauth_url || "");

    return {
      ok: true,
      secret: secret.base32,
      otpauthUrl: secret.otpauth_url,
      qrCodeDataUrl,
      message: "Quét QR bằng Google Authenticator.",
    };
  }

  async verifySetup(jwtUser: any, code: string) {
    const admin = await this.resolveAdminFromJwtUser(jwtUser);

    if (!admin.totpSecret) {
      throw new BadRequestException("Chưa có secret authen. Hãy tạo mã QR trước.");
    }

    const verified = speakeasy.totp.verify({
      secret: admin.totpSecret,
      encoding: "base32",
      token: String(code).trim(),
      window: 1,
    });

    if (!verified) {
      throw new BadRequestException("Mã authen không đúng.");
    }

    await this.prisma.adminUser.update({
      where: { id: admin.id },
      data: {
        totpEnabled: true,
      },
    });

    return {
      ok: true,
      message: "Đã bật Google Authenticator.",
    };
  }

  async verifyOwnerCode(code: string) {
    const owner = await this.prisma.adminUser.findFirst({
      where: {
        role: {
          in: ["OWNER", "ADMIN"],
        },
        totpEnabled: true,
        totpSecret: {
          not: null,
        },
      },
      orderBy: {
        createdAt: "asc",
      },
      select: {
        id: true,
        totpSecret: true,
        fullName: true,
      },
    });

    if (!owner || !owner.totpSecret) {
      throw new BadRequestException("Chủ chưa bật Google Authenticator.");
    }

    const token = String(code || "").trim();
    const nowSeconds = Math.floor(Date.now() / 1000);

    // Mã Google Authenticator vẫn đổi mỗi 30 giây, nhưng riêng thao tác
    // xác nhận sửa COD cho phép dùng mã hiện tại hoặc 9 mã trước đó.
    // Tổng thời gian nhập tối đa xấp xỉ 5 phút và KHÔNG nhận mã tương lai.
    let verified = false;
    for (let step = 0; step <= 9; step += 1) {
      const expectedToken = speakeasy.totp({
        secret: owner.totpSecret,
        encoding: "base32",
        step: 30,
        time: nowSeconds - step * 30,
      });

      if (expectedToken === token) {
        verified = true;
        break;
      }
    }

    if (!verified) {
      throw new BadRequestException(
        "Mã authen không đúng hoặc đã quá thời hạn 5 phút.",
      );
    }

    return {
      ok: true,
      approverId: owner.id,
      approverName: owner.fullName || "Owner",
    };
  }
}