import { Injectable, BadRequestException } from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";
import { CreateStaffDto } from "./dto/create-staff.dto";
import { UpdateStaffStatusDto } from "./dto/update-staff-status.dto";
import * as bcrypt from "bcrypt";

@Injectable()
export class StaffService {
  constructor(private readonly prisma: PrismaService) {}

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

    const roles = Array.isArray((dto as any).roles)
      ? (dto as any).roles.map((r: string) => String(r).trim()).filter(Boolean)
      : [];

    const legacyRole = String((dto as any).role || roles[0] || "").trim();

    if (!legacyRole) {
      throw new BadRequestException("Thiếu role");
    }

    const existingByCode = await this.prisma.staffUser.findUnique({
      where: { code: dto.code.trim() },
    });

    if (existingByCode) {
      throw new BadRequestException("Mã nhân viên đã tồn tại");
    }

    const email = String((dto as any).email || "").trim() || null;
    const username = String((dto as any).username || "").trim() || null;

    if (email) {
      const existingEmail = await this.prisma.staffUser.findUnique({
        where: { email },
      });

      if (existingEmail) {
        throw new BadRequestException("Email nhân viên đã tồn tại");
      }
    }

    if (username) {
      const existingUsername = await this.prisma.staffUser.findUnique({
        where: { username },
      });

      if (existingUsername) {
        throw new BadRequestException("Tên đăng nhập đã tồn tại");
      }
    }

    const hash = await bcrypt.hash(dto.password, 10);

    let branchId: string | null = null;
    let branchName: string | null = null;

    if (dto.branchId) {
      const branch = await this.prisma.branch.findUnique({
        where: { id: String(dto.branchId).trim() },
        select: {
          id: true,
          name: true,
        },
      });

      if (!branch) {
        throw new BadRequestException("Chi nhánh không tồn tại");
      }

      branchId = branch.id;
      branchName = branch.name;
    }

    const created = await this.prisma.staffUser.create({
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

    const finalRoles = roles.length ? roles : [legacyRole];

    await this.prisma.staffUserRole.createMany({
      data: finalRoles.map((roleCode: string) => ({
        staffId: created.id,
        roleCode: roleCode.toLowerCase(),
      })),
      skipDuplicates: true,
    });

    if (branchId) {
      await this.prisma.staffBranchPermission.create({
        data: {
          staffId: created.id,
          branchId,
          ...this.defaultBranchPermissions(legacyRole),
        },
      });
    }

    return this.findOne(created.id);
  }

  async findAll() {
    return this.prisma.staffUser.findMany({
      orderBy: [{ createdAt: "desc" }],
      include: {
        roles: true,
        branchPermissions: {
          include: {
            branch: true,
          },
        },
      },
    });
  }

  async findOne(id: string) {
    return this.prisma.staffUser.findUnique({
      where: { id },
      include: {
        roles: true,
        branchPermissions: {
          include: {
            branch: true,
          },
        },
      },
    });
  }

  async update(id: string, dto: any) {
    const current = await this.prisma.staffUser.findUnique({
      where: { id },
    });

    if (!current) {
      throw new BadRequestException("Nhân viên không tồn tại");
    }

    const roleInput = String(dto.role || current.role || "").trim();

    if (!roleInput) {
      throw new BadRequestException("Thiếu role");
    }

    let branchId: string | null = current.branchId || null;
    let branchName: string | null = current.branchName || null;

    if (dto.branchId !== undefined) {
      if (dto.branchId) {
        const branch = await this.prisma.branch.findUnique({
          where: { id: String(dto.branchId).trim() },
          select: {
            id: true,
            name: true,
          },
        });

        if (!branch) {
          throw new BadRequestException("Chi nhánh không tồn tại");
        }

        branchId = branch.id;
        branchName = branch.name;
      } else {
        branchId = null;
        branchName = null;
      }
    }

    const email = dto.email !== undefined
      ? String(dto.email || "").trim() || null
      : current.email;

    const username = dto.username !== undefined
      ? String(dto.username || "").trim() || null
      : current.username;

    if (email && email !== current.email) {
      const existingEmail = await this.prisma.staffUser.findUnique({
        where: { email },
      });

      if (existingEmail && existingEmail.id !== id) {
        throw new BadRequestException("Email nhân viên đã tồn tại");
      }
    }

    if (username && username !== current.username) {
      const existingUsername = await this.prisma.staffUser.findUnique({
        where: { username },
      });

      if (existingUsername && existingUsername.id !== id) {
        throw new BadRequestException("Tên đăng nhập đã tồn tại");
      }
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

  async updatePermissions(staffId: string, dto: any) {
    const staff = await this.prisma.staffUser.findUnique({
      where: { id: staffId },
    });

    if (!staff) {
      throw new BadRequestException("Nhân viên không tồn tại");
    }

    const roles = Array.isArray(dto.roles)
      ? dto.roles.map((r: string) => String(r).trim().toLowerCase()).filter(Boolean)
      : [];

    if (!roles.length) {
      throw new BadRequestException("Cần chọn ít nhất 1 vai trò");
    }

    const branchPermissions = Array.isArray(dto.branchPermissions)
      ? dto.branchPermissions
      : [];

    const branchIds = branchPermissions
      .map((b: any) => String(b.branchId || "").trim())
      .filter(Boolean);

    if (branchIds.length) {
      const existingBranches = await this.prisma.branch.findMany({
        where: { id: { in: branchIds } },
        select: { id: true },
      });

      const existingIds = new Set(existingBranches.map((b) => b.id));

      const missingBranch = branchIds.find((id: string) => !existingIds.has(id));

      if (missingBranch) {
        throw new BadRequestException(`Chi nhánh không tồn tại: ${missingBranch}`);
      }
    }

    await this.prisma.$transaction(async (tx) => {
      await tx.staffUserRole.deleteMany({
        where: { staffId },
      });

      await tx.staffUserRole.createMany({
        data: roles.map((roleCode: string) => ({
          staffId,
          roleCode,
        })),
        skipDuplicates: true,
      });

      await tx.staffBranchPermission.deleteMany({
        where: { staffId },
      });

      if (branchPermissions.length) {
        await tx.staffBranchPermission.createMany({
          data: branchPermissions.map((b: any) => ({
            staffId,
            branchId: String(b.branchId).trim(),

            canView: Boolean(b.canView ?? true),

            canSell: Boolean(b.canSell),
            canCreateOrder: Boolean(b.canCreateOrder),
            canApproveOrder: Boolean(b.canApproveOrder),
            canCancelOrder: Boolean(b.canCancelOrder),
            canHandleReturn: Boolean(b.canHandleReturn),

            canViewStock: Boolean(b.canViewStock),
            canManageStock: Boolean(b.canManageStock),
            canStocktake: Boolean(b.canStocktake),
            canTransferStock: Boolean(b.canTransferStock),
            canReceiveStock: Boolean(b.canReceiveStock),

            canViewCustomer: Boolean(b.canViewCustomer),
            canEditCustomer: Boolean(b.canEditCustomer),

            canViewReport: Boolean(b.canViewReport),
            canViewMoney: Boolean(b.canViewMoney),

            note: b.note ? String(b.note) : null,
          })),
          skipDuplicates: true,
        });
      }

      await tx.staffUser.update({
        where: { id: staffId },
        data: {
          role: roles[0],
        },
      });
    });

    return this.findOne(staffId);
  }

  async updateStatus(id: string, dto: UpdateStaffStatusDto) {
    return this.prisma.staffUser.update({
      where: { id },
      data: {
        isActive: dto.status === "ACTIVE",
      },
    });
  }

  async updatePassword(id: string, newPassword: string) {
    if (!newPassword || newPassword.length < 4) {
      throw new BadRequestException("Mật khẩu tối thiểu 4 ký tự");
    }

    const hash = await bcrypt.hash(newPassword, 10);

    return this.prisma.staffUser.update({
      where: { id },
      data: {
        passwordHash: hash,
      },
    });
  }

  async updateSecondPassword(id: string, secondPassword: string) {
    if (!secondPassword || secondPassword.trim().length < 6) {
      throw new BadRequestException("Mật khẩu lớp 2 tối thiểu 6 ký tự.");
    }

    const hash = await bcrypt.hash(secondPassword.trim(), 10);

    await this.prisma.staffUser.update({
      where: { id },
      data: {
        secondPasswordHash: hash,
        secondPasswordEnabled: true,
      },
    });

    return { message: "Đã cập nhật mật khẩu lớp 2." };
  }

  private defaultBranchPermissions(role: string) {
    const r = String(role || "").toLowerCase();

    const isOwner = r === "owner";
    const isAdmin = r === "admin";
    const isFulltime = r === "fulltime";
    const isRetail = r === "retail-staff";
    const isStockAuditor = r === "stock-auditor";
    const isBranchManager = r === "branch-manager";

    const high = isOwner || isAdmin;
    const manager = high || isBranchManager;
    const orderStaff = manager || isFulltime || isRetail;
    const stockStaff = manager || isFulltime || isStockAuditor;

    return {
      canView: true,

      canSell: orderStaff,
      canCreateOrder: orderStaff,
      canApproveOrder: manager || isFulltime,
      canCancelOrder: manager || isFulltime,
      canHandleReturn: orderStaff,

      canViewStock: stockStaff,
      canManageStock: manager || isFulltime,
      canStocktake: stockStaff,
      canTransferStock: manager || isFulltime,
      canReceiveStock: stockStaff,

      canViewCustomer: orderStaff || manager,
      canEditCustomer: manager || isFulltime,

      canViewReport: manager,
      canViewMoney: high,
    };
  }
}