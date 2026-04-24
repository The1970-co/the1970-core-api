import "dotenv/config";
import { PrismaClient } from "@prisma/client";
import { Pool } from "pg";
import * as bcrypt from "bcrypt";

const connectionString = process.env.DATABASE_URL;

if (!connectionString) {
  throw new Error("Thiếu DATABASE_URL trong file .env");
}

const pool = new Pool({
  connectionString,
});

const prisma = new PrismaClient();
async function main() {
  const passwordHash = await bcrypt.hash("123456", 10);

  const users = [
    {
      code: "ADMIN",
      name: "Admin",
      role: "admin",
      branchId: null,
      branchName: "All",
    },
    {
      code: "NV10",
      name: "KIỀU ANH",
      role: "retail-staff",
      branchId: "b1",
      branchName: "Hoàn Kiếm",
    },
    {
      code: "NV11",
      name: "LAN HBT",
      role: "retail-staff",
      branchId: "b2",
      branchName: "Hai Bà Trưng",
    },
    {
      code: "NV12",
      name: "MINH QO",
      role: "retail-staff",
      branchId: "quoc-oai",
      branchName: "Quốc Oai",
    },
  ];

  for (const user of users) {
    await prisma.staffUser.upsert({
      where: { code: user.code },
      update: {
        name: user.name,
        role: user.role,
        branchId: user.branchId,
        branchName: user.branchName,
        passwordHash,
        isActive: true,
      },
      create: {
        code: user.code,
        name: user.name,
        role: user.role,
        branchId: user.branchId,
        branchName: user.branchName,
        passwordHash,
        isActive: true,
      },
    });
  }

  console.log("✅ Seed StaffUser xong");
  console.log("ADMIN / 123456");
  console.log("NV10 / 123456");
  console.log("NV11 / 123456");
  console.log("NV12 / 123456");
}

main()
  .catch((error) => {
    console.error("❌ Seed lỗi:", error);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
    await pool.end();
  });