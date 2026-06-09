import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from "@nestjs/common";
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

  private normalizeDate(value: any) {
    if (!value) return undefined;
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? undefined : date;
  }

  private async readDepartments() {
    try {
      const departments = await this.prisma.department.findMany({
        orderBy: { name: "asc" },
      });

      const staffDepartments = await this.prisma.staffDepartment.findMany({
        orderBy: { staffId: "asc" },
      });

      return { departments, staffDepartments };
    } catch {
      // Nếu production chưa có dữ liệu phòng ban hoặc schema phòng ban khác bản cũ,
      // snapshot vẫn phải chạy được để đóng băng role/permission chính.
      return { departments: [], staffDepartments: [] };
    }
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

  async createSnapshot(
    body: { name?: string; description?: string },
    user?: any,
  ) {
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


  async deleteSnapshot(id: string) {
    const snapshot = await this.prisma.rbacSnapshot.findUnique({
      where: { id },
      select: { id: true, name: true },
    });

    if (!snapshot) {
      throw new NotFoundException("Không tìm thấy bản đóng băng phân quyền.");
    }

    await this.prisma.rbacSnapshot.delete({
      where: { id },
    });

    return {
      success: true,
      message: "Đã xoá snapshot rollback.",
      snapshotId: snapshot.id,
      name: snapshot.name,
    };
  }

  async restoreSnapshot(id: string, user?: any) {
    const snapshot = await this.prisma.rbacSnapshot.findUnique({ where: { id } });

    if (!snapshot) {
      throw new NotFoundException("Không tìm thấy bản đóng băng phân quyền.");
    }

    const payload = snapshot.payload as SnapshotPayload;

    if (!payload || payload.version !== 1) {
      throw new BadRequestException("Bản snapshot không hợp lệ.");
    }

    await this.prisma.$transaction(
      async (tx) => {
        await tx.staffBranchPermission.deleteMany({});
        await tx.staffBranchRole.deleteMany({});
        await tx.staffUserRole.deleteMany({});
        await tx.staffRoleTemplate.deleteMany({});

        try {
          await tx.staffDepartment.deleteMany({});
          await tx.department.deleteMany({});
        } catch {
          // Không chặn rollback quyền chính nếu bảng phòng ban ở DB cũ khác schema.
        }

        if (payload.roleTemplates?.length) {
          await tx.staffRoleTemplate.createMany({
            data: payload.roleTemplates.map((row: any) => ({
              roleCode: row.roleCode,
              name: row.name,
              scope: row.scope,
              description: row.description,
              note: row.note,
              permissions: row.permissions,
              createdAt: this.normalizeDate(row.createdAt),
              updatedAt: this.normalizeDate(row.updatedAt),
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
              createdAt: this.normalizeDate(row.createdAt),
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
              createdAt: this.normalizeDate(row.createdAt),
              updatedAt: this.normalizeDate(row.updatedAt),
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
              extraPermissionKeys: Array.isArray(row.extraPermissionKeys)
                ? row.extraPermissionKeys
                : [],
              deniedPermissionKeys: Array.isArray(row.deniedPermissionKeys)
                ? row.deniedPermissionKeys
                : [],
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
              note: row.note || "Restored from RBAC snapshot",
              createdAt: this.normalizeDate(row.createdAt),
              updatedAt: this.normalizeDate(row.updatedAt),
            })),
            skipDuplicates: true,
          });
        }

        try {
          if (payload.departments?.length) {
            await tx.department.createMany({
              data: payload.departments.map((row: any) => ({
                id: row.id,
                name: row.name,
                code: row.code,
                description: row.description || null,
                color: row.color || "#6366f1",
                isActive: row.isActive !== false,
                createdAt: this.normalizeDate(row.createdAt),
                updatedAt: this.normalizeDate(row.updatedAt),
              })),
              skipDuplicates: true,
            });
          }

          if (payload.staffDepartments?.length) {
            await tx.staffDepartment.createMany({
              data: payload.staffDepartments.map((row: any) => ({
                id: row.id,
                staffId: row.staffId,
                departmentId: row.departmentId,
                isHead: Boolean(row.isHead),
                createdAt: this.normalizeDate(row.createdAt),
              })),
              skipDuplicates: true,
            });
          }
        } catch {
          // Snapshot phòng ban là phụ. Rollback quyền chính vẫn phải thành công.
        }

        await tx.staffUser.updateMany({
          data: {
            sessionVersion: { increment: 1 },
          },
        });

        await tx.staffSession.updateMany({
          where: { revokedAt: null },
          data: { revokedAt: new Date() },
        });

        await tx.rbacSnapshot.update({
          where: { id },
          data: {
            restoredAt: new Date(),
            restoredById: user?.id || user?.sub || null,
            restoredByName: this.userName(user),
          },
        });
      },
      {
        maxWait: 10000,
        timeout: 30000,
      },
    );

    return {
      success: true,
      message: "Đã rollback phân quyền về bản đóng băng.",
      snapshotId: id,
      name: snapshot.name,
    };
  }
}
