import { BadRequestException, Injectable, NotFoundException } from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";

type SnapshotPayload = {
  version: 1;
  createdAt: string;
  roleTemplates: any[];
  staffUsers: any[];
  staffUserRoles: any[];
  staffBranchRoles: any[];
  staffBranchPermissions: any[];
  departments: any[];
  staffDepartments: any[];
};

@Injectable()
export class RbacSnapshotsService {
  constructor(private readonly prisma: PrismaService) {}

  private userName(user?: any) {
    return user?.name || user?.code || user?.username || null;
  }

  private async ensureDepartmentTables(tx?: any) {
    const client = tx || this.prisma;

    await client.$executeRawUnsafe(`
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

    await client.$executeRawUnsafe(`
      CREATE TABLE IF NOT EXISTS "StaffDepartment" (
        "id" TEXT PRIMARY KEY,
        "staffId" TEXT NOT NULL,
        "departmentId" TEXT NOT NULL,
        "isHead" BOOLEAN NOT NULL DEFAULT FALSE,
        "createdAt" TIMESTAMP NOT NULL DEFAULT NOW(),
        CONSTRAINT "StaffDepartment_staff_department_unique" UNIQUE ("staffId", "departmentId")
      );
    `);
  }

  private async readDepartments() {
    await this.ensureDepartmentTables();

    const departments = await this.prisma.$queryRawUnsafe<any[]>(`
      SELECT * FROM "Department" ORDER BY "createdAt" ASC
    `);

    const staffDepartments = await this.prisma.$queryRawUnsafe<any[]>(`
      SELECT * FROM "StaffDepartment" ORDER BY "createdAt" ASC
    `);

    return { departments, staffDepartments };
  }

  async list() {
    return this.prisma.rbacSnapshot.findMany({
      orderBy: { createdAt: "desc" },
      select: {
        id: true,
        name: true,
        description: true,
        createdById: true,
        createdByName: true,
        createdAt: true,
        restoredAt: true,
        restoredById: true,
        restoredByName: true,
      },
      take: 30,
    });
  }

  async latest() {
    return this.prisma.rbacSnapshot.findFirst({
      orderBy: { createdAt: "desc" },
      select: {
        id: true,
        name: true,
        description: true,
        createdById: true,
        createdByName: true,
        createdAt: true,
        restoredAt: true,
        restoredById: true,
        restoredByName: true,
      },
    });
  }

  async createSnapshot(body: { name?: string; description?: string }, user?: any) {
    const { departments, staffDepartments } = await this.readDepartments();

    const payload: SnapshotPayload = {
      version: 1,
      createdAt: new Date().toISOString(),
      roleTemplates: await this.prisma.staffRoleTemplate.findMany({
        orderBy: { roleCode: "asc" },
      }),
      staffUsers: await this.prisma.staffUser.findMany({
        select: {
          id: true,
          code: true,
          name: true,
          role: true,
          branchId: true,
          branchName: true,
          isActive: true,
          sessionVersion: true,
        },
        orderBy: { createdAt: "asc" },
      }),
      staffUserRoles: await this.prisma.staffUserRole.findMany({
        orderBy: { staffId: "asc" },
      }),
      staffBranchRoles: await this.prisma.staffBranchRole.findMany({
        orderBy: [{ staffId: "asc" }, { branchId: "asc" }],
      }),
      staffBranchPermissions: await this.prisma.staffBranchPermission.findMany({
        orderBy: [{ staffId: "asc" }, { branchId: "asc" }],
      }),
      departments,
      staffDepartments,
    };

    const name =
      String(body?.name || "").trim() ||
      `operation-rbac-freeze-${new Date().toISOString().slice(0, 10)}`;

    return this.prisma.rbacSnapshot.create({
      data: {
        name,
        description: String(body?.description || "").trim() || null,
        payload: payload as any,
        createdById: user?.id || user?.sub || null,
        createdByName: this.userName(user),
      },
      select: {
        id: true,
        name: true,
        description: true,
        createdAt: true,
        createdById: true,
        createdByName: true,
      },
    });
  }

  async restoreSnapshot(id: string, user?: any) {
    const snapshot = await this.prisma.rbacSnapshot.findUnique({ where: { id } });
    if (!snapshot) throw new NotFoundException("Không tìm thấy bản đóng băng phân quyền.");

    const payload = snapshot.payload as SnapshotPayload;
    if (!payload || payload.version !== 1) {
      throw new BadRequestException("Bản snapshot không hợp lệ.");
    }

    await this.prisma.$transaction(async (tx) => {
      await this.ensureDepartmentTables(tx);

      await tx.staffBranchPermission.deleteMany({});
      await tx.staffBranchRole.deleteMany({});
      await tx.staffUserRole.deleteMany({});
      await tx.staffRoleTemplate.deleteMany({});
      await tx.$executeRawUnsafe(`DELETE FROM "StaffDepartment"`);
      await tx.$executeRawUnsafe(`DELETE FROM "Department"`);

      if (payload.roleTemplates?.length) {
        await tx.staffRoleTemplate.createMany({
          data: payload.roleTemplates.map((row: any) => ({
            roleCode: row.roleCode,
            name: row.name,
            scope: row.scope,
            description: row.description,
            note: row.note,
            permissions: row.permissions,
            createdAt: row.createdAt ? new Date(row.createdAt) : undefined,
            updatedAt: row.updatedAt ? new Date(row.updatedAt) : undefined,
          })),
          skipDuplicates: true,
        });
      }

      if (payload.staffUserRoles?.length) {
        await tx.staffUserRole.createMany({
          data: payload.staffUserRoles.map((row: any) => ({
            id: row.id,
            staffId: row.staffId,
            roleCode: row.roleCode,
            createdAt: row.createdAt ? new Date(row.createdAt) : undefined,
          })),
          skipDuplicates: true,
        });
      }

      if (payload.staffBranchRoles?.length) {
        await tx.staffBranchRole.createMany({
          data: payload.staffBranchRoles.map((row: any) => ({
            id: row.id,
            staffId: row.staffId,
            branchId: row.branchId,
            roleCode: row.roleCode,
            createdAt: row.createdAt ? new Date(row.createdAt) : undefined,
            updatedAt: row.updatedAt ? new Date(row.updatedAt) : undefined,
          })),
          skipDuplicates: true,
        });
      }

      if (payload.staffBranchPermissions?.length) {
        await tx.staffBranchPermission.createMany({
          data: payload.staffBranchPermissions.map((row: any) => ({
            id: row.id,
            staffId: row.staffId,
            branchId: row.branchId,
            permissionKeys: Array.isArray(row.permissionKeys) ? row.permissionKeys : [],
            extraPermissionKeys: Array.isArray(row.extraPermissionKeys) ? row.extraPermissionKeys : [],
            deniedPermissionKeys: Array.isArray(row.deniedPermissionKeys) ? row.deniedPermissionKeys : [],
            canView: Boolean(row.canView),
            canSell: Boolean(row.canSell),
            canViewOwnOrders: Boolean(row.canViewOwnOrders),
            canViewBranchOrders: Boolean(row.canViewBranchOrders),
            canCreateOrder: Boolean(row.canCreateOrder),
            canApproveOrder: Boolean(row.canApproveOrder),
            canCancelOrder: Boolean(row.canCancelOrder),
            canHandleReturn: Boolean(row.canHandleReturn),
            canViewStock: Boolean(row.canViewStock),
            canManageStock: Boolean(row.canManageStock),
            canStocktake: Boolean(row.canStocktake),
            canTransferStock: Boolean(row.canTransferStock),
            canReceiveStock: Boolean(row.canReceiveStock),
            canViewCustomer: Boolean(row.canViewCustomer),
            canEditCustomer: Boolean(row.canEditCustomer),
            canExportProductExcel: Boolean(row.canExportProductExcel),
            canImportProductExcel: Boolean(row.canImportProductExcel),
            canExportOrderExcel: Boolean(row.canExportOrderExcel),
            canExportInventoryExcel: Boolean(row.canExportInventoryExcel),
            canExportCustomerExcel: Boolean(row.canExportCustomerExcel),
            canViewReport: Boolean(row.canViewReport),
            canViewMoney: Boolean(row.canViewMoney),
            note: row.note || "Restored from RBAC snapshot",
            createdAt: row.createdAt ? new Date(row.createdAt) : undefined,
            updatedAt: row.updatedAt ? new Date(row.updatedAt) : undefined,
          })),
          skipDuplicates: true,
        });
      }

      for (const row of payload.departments || []) {
        await tx.$executeRawUnsafe(
          `INSERT INTO "Department" ("id", "name", "code", "description", "color", "isActive", "createdAt", "updatedAt")
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
           ON CONFLICT ("id") DO UPDATE SET
             "name" = EXCLUDED."name",
             "code" = EXCLUDED."code",
             "description" = EXCLUDED."description",
             "color" = EXCLUDED."color",
             "isActive" = EXCLUDED."isActive",
             "updatedAt" = NOW()`,
          row.id,
          row.name,
          row.code,
          row.description || null,
          row.color || "#6366f1",
          row.isActive !== false,
          row.createdAt ? new Date(row.createdAt) : new Date(),
          row.updatedAt ? new Date(row.updatedAt) : new Date(),
        );
      }

      for (const row of payload.staffDepartments || []) {
        await tx.$executeRawUnsafe(
          `INSERT INTO "StaffDepartment" ("id", "staffId", "departmentId", "isHead", "createdAt")
           VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT ("staffId", "departmentId") DO NOTHING`,
          row.id,
          row.staffId,
          row.departmentId,
          Boolean(row.isHead),
          row.createdAt ? new Date(row.createdAt) : new Date(),
        );
      }

      await tx.staffUser.updateMany({ data: { sessionVersion: { increment: 1 } } });
      await tx.staffSession.updateMany({ where: { revokedAt: null }, data: { revokedAt: new Date() } });

      await tx.rbacSnapshot.update({
        where: { id },
        data: {
          restoredAt: new Date(),
          restoredById: user?.id || user?.sub || null,
          restoredByName: this.userName(user),
        },
      });
    }, { maxWait: 10000, timeout: 30000 });

    return {
      success: true,
      message: "Đã rollback phân quyền về bản đóng băng.",
      snapshotId: id,
      name: snapshot.name,
    };
  }
}
