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
};

type BranchRoleInput = {
  branchId: string;
  roleCode: string;
};


const UNIQUE = (values: Array<string | undefined | null>) =>
  Array.from(new Set(values.map((v) => String(v || "").trim()).filter(Boolean)));

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

function permissionKeysFromLegacyBooleans(row: Record<string, any>) {
  const keys: string[] = [];
  for (const [field, permissionKeys] of Object.entries(LEGACY_BOOLEAN_TO_PERMISSION_KEYS)) {
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
    return String(role || "").trim().toLowerCase();
  }

  private normalizeRoles(input: any): string[] {
    const raw = Array.isArray(input)
      ? input
      : input
        ? [input]
        : [];

    return Array.from(
      new Set(raw.map((role) => this.normalizeRole(role)).filter(Boolean)),
    );
  }

  private validateRole(roleCode: string) {
    const normalized = this.normalizeRole(roleCode);
    if (!normalized || !ROLE_TEMPLATES[normalized]) {
      throw new BadRequestException(`Role không hợp lệ: ${roleCode || "trống"}`);
    }
    return normalized;
  }

  private permissionsForRole(roleCode: string) {
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
    };
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
      throw new BadRequestException(`Chi nhánh không tồn tại: ${missingBranch}`);
    }
  }

  private normalizeBranchRoles(input: any[]): BranchRoleInput[] {
    if (!Array.isArray(input)) return [];

    const map = new Map<string, BranchRoleInput>();

    input.forEach((item) => {
      const branchId = String(item?.branchId || "").trim();
      const roleCode = this.validateRole(item?.roleCode || item?.role || item?.roleId);

      if (!branchId) return;
      map.set(branchId, { branchId, roleCode });
    });

    return Array.from(map.values());
  }

  private deriveLegacyRolesFromBranchRoles(branchRoles: BranchRoleInput[], fallbackRole?: string) {
    const roles = branchRoles.map((row) => row.roleCode);
    if (!roles.length && fallbackRole) roles.push(this.normalizeRole(fallbackRole));
    return Array.from(new Set(roles.filter(Boolean)));
  }

  private async replaceBranchRolesAndPermissions(tx: any, staffId: string, branchRoles: BranchRoleInput[]) {
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

    await tx.staffBranchPermission.createMany({
      data: branchRoles.map((row) => ({
        staffId,
        branchId: row.branchId,
        ...this.permissionsForRole(row.roleCode),
        note: `Auto generated from role ${row.roleCode}`,
      })),
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

    const branchRoles = this.normalizeBranchRoles((dto as any).branchRoles || []);
    const rolesFromDto = this.normalizeRoles((dto as any).roles);
    const legacyRole = this.validateRole((dto as any).role || rolesFromDto[0] || branchRoles[0]?.roleCode);

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
      const existingEmail = await this.prisma.staffUser.findUnique({ where: { email } });
      if (existingEmail) throw new BadRequestException("Email nhân viên đã tồn tại");
    }

    if (username) {
      const existingUsername = await this.prisma.staffUser.findUnique({ where: { username } });
      if (existingUsername) throw new BadRequestException("Tên đăng nhập đã tồn tại");
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

    await this.assertBranchesExist(initialBranchRoles.map((row) => row.branchId));

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

      const finalRoles = this.deriveLegacyRolesFromBranchRoles(initialBranchRoles, legacyRole);

      if (finalRoles.length) {
        await tx.staffUserRole.createMany({
          data: finalRoles.map((roleCode) => ({ staffId: staff.id, roleCode })),
          skipDuplicates: true,
        });
      }

      await this.replaceBranchRolesAndPermissions(tx, staff.id, initialBranchRoles);

      return staff;
    });

    return this.findOne(created.id);
  }

  async findAll() {
    return this.prisma.staffUser.findMany({
      orderBy: [{ createdAt: "desc" }],
      include: {
        roles: true,
        branchRoles: { include: { branch: true } },
        branchPermissions: { include: { branch: true } },
      },
    });
  }

  async findOne(id: string) {
    return this.prisma.staffUser.findUnique({
      where: { id },
      include: {
        roles: true,
        branchRoles: { include: { branch: true } },
        branchPermissions: { include: { branch: true } },
      },
    });
  }

  async update(id: string, dto: any) {
    const current = await this.prisma.staffUser.findUnique({ where: { id } });

    if (!current) throw new BadRequestException("Nhân viên không tồn tại");

    const roleInput = this.validateRole(dto.role || current.role || "retail-staff");

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

    const email = dto.email !== undefined ? String(dto.email || "").trim() || null : current.email;
    const username = dto.username !== undefined ? String(dto.username || "").trim().toLowerCase() || current.username : current.username;

    if (email && email !== current.email) {
      const existingEmail = await this.prisma.staffUser.findUnique({ where: { email } });
      if (existingEmail && existingEmail.id !== id) throw new BadRequestException("Email nhân viên đã tồn tại");
    }

    if (username && username !== current.username) {
      const existingUsername = await this.prisma.staffUser.findUnique({ where: { username } });
      if (existingUsername && existingUsername.id !== id) throw new BadRequestException("Tên đăng nhập đã tồn tại");
    }

    await this.prisma.staffUser.update({
      where: { id },
      data: {
        code: dto.code !== undefined ? String(dto.code).trim() : current.code,
        name: dto.name !== undefined ? String(dto.name).trim() : current.name,
        username,
        email,
        phone: dto.phone !== undefined ? String(dto.phone || "").trim() || null : current.phone,
        address: dto.address !== undefined ? String(dto.address || "").trim() || null : current.address,
        note: dto.note !== undefined ? String(dto.note || "").trim() || null : current.note,
        role: roleInput,
        branchId,
        branchName,
      },
    });

    return this.findOne(id);
  }

  async updateBranchRoles(staffId: string, dto: any) {
    const staff = await this.prisma.staffUser.findUnique({ where: { id: staffId } });
    if (!staff) throw new BadRequestException("Nhân viên không tồn tại");

    const branchRoles = this.normalizeBranchRoles(dto.branchRoles || []);
    await this.assertBranchesExist(branchRoles.map((row) => row.branchId));

    const legacyRoles = this.deriveLegacyRolesFromBranchRoles(branchRoles, staff.role || undefined);
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
        },
      });
    });

    return this.findOne(staffId);
  }

  private sanitizeBranchPermissionInput(row: any) {
    const clean: any = this.permissionsForRole(row.roleCode || row.role || "retail-staff");

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
      permissionKeys: Array.isArray(row.permissionKeys) ? row.permissionKeys : [],
    });

    return clean;
  }

  async updatePermissions(staffId: string, dto: any) {
    if (Array.isArray(dto.branchRoles) && !Array.isArray(dto.branchPermissions)) {
      return this.updateBranchRoles(staffId, dto);
    }

    const staff = await this.prisma.staffUser.findUnique({ where: { id: staffId } });
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
    });

    return this.findOne(staffId);
  }

  async updateStatus(id: string, dto: UpdateStaffStatusDto) {
    return this.prisma.staffUser.update({
      where: { id },
      data: { isActive: dto.status === "ACTIVE" },
    });
  }

  async updatePassword(id: string, newPassword: string) {
    if (!newPassword || newPassword.length < 4) {
      throw new BadRequestException("Mật khẩu tối thiểu 4 ký tự");
    }

    const hash = await bcrypt.hash(newPassword, 10);

    return this.prisma.staffUser.update({
      where: { id },
      data: { passwordHash: hash },
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
}
