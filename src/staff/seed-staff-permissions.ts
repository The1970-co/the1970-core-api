// NOTE: File này hiện chỉ seed quyền chi nhánh theo role.
// Quyền menu dạng PermissionKey như promotions.view / products.view đang nằm ở lib/authz phía frontend.

import { PrismaService } from "../prisma/prisma.service";

const prisma = new PrismaService();

function defaultBranchPermissions(role: string) {
  const r = String(role || "").toLowerCase();

  return {
    canView: true,

    canSell: r === "retail-staff" || r === "fulltime" || r === "admin" || r === "owner",
    canCreateOrder: r === "retail-staff" || r === "fulltime" || r === "admin" || r === "owner",
    canApproveOrder: r === "fulltime" || r === "admin" || r === "owner",
    canCancelOrder: r === "fulltime" || r === "admin" || r === "owner",
    canHandleReturn: r === "retail-staff" || r === "fulltime" || r === "admin" || r === "owner",

    canViewStock: r === "stock-auditor" || r === "fulltime" || r === "admin" || r === "owner",
    canManageStock: r === "fulltime" || r === "admin" || r === "owner",
    canStocktake: r === "stock-auditor" || r === "fulltime" || r === "admin" || r === "owner",
    canTransferStock: r === "fulltime" || r === "admin" || r === "owner",
    canReceiveStock: r === "stock-auditor" || r === "fulltime" || r === "admin" || r === "owner",

    canViewCustomer: r === "retail-staff" || r === "fulltime" || r === "admin" || r === "owner",
    canEditCustomer: r === "fulltime" || r === "admin" || r === "owner",

    canViewReport: r === "admin" || r === "owner",
    canViewMoney: r === "admin" || r === "owner",
  };
}

async function main() {
  const staff = await prisma.staffUser.findMany();

  for (const user of staff) {
    const roleCode = String(user.role || "retail-staff").toLowerCase();

    await prisma.staffUserRole.upsert({
      where: {
        staffId_roleCode: {
          staffId: user.id,
          roleCode,
        },
      },
      update: {},
      create: {
        staffId: user.id,
        roleCode,
      },
    });

    if (user.branchId) {
      await prisma.staffBranchPermission.upsert({
        where: {
          staffId_branchId: {
            staffId: user.id,
            branchId: user.branchId,
          },
        },
        update: defaultBranchPermissions(roleCode),
        create: {
          staffId: user.id,
          branchId: user.branchId,
          ...defaultBranchPermissions(roleCode),
        },
      });
    }
  }

  console.log("Seed staff permissions done.");
}

main()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });