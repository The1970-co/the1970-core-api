import "dotenv/config";
import { Pool } from "pg";
import { PrismaClient } from "@prisma/client";
import { v3 } from "vietnam-divisions-js";

const connectionString = process.env.DATABASE_URL;

if (!connectionString) {
  throw new Error("Thiếu DATABASE_URL trong environment");
}

const pool = new Pool({ connectionString });

const prisma = new PrismaClient();

function normalizeVietnamese(input: string) {
  return String(input || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/đ/g, "d")
    .replace(/Đ/g, "D")
    .toLowerCase()
    .trim();
}

function pickProvinceCode(province: any) {
  return String(
    province?.code ??
      province?.provinceCode ??
      ""
  ).trim();
}

function pickProvinceId(province: any) {
  return String(
    province?.idProvince ??   // ⭐ THÊM DÒNG NÀY
    province?.id ??
    province?.provinceId ??
    province?.province_id ??
    ""
  ).trim();
}

function pickProvinceName(province: any) {
  return String(
    province?.name ??
      province?.fullName ??
      province?.provinceName ??
      ""
  ).trim();
}

function pickWardCode(ward: any) {
  return String(
    ward?.code ??
      ward?.communeCode ??
          ward?.idCommune ??   // thêm dòng này
      ward?.wardCode ??
      ""
  ).trim();
}

function pickWardName(ward: any) {
  return String(
    ward?.name ??
      ward?.fullName ??
      ward?.communeName ??
      ward?.wardName ??
      ""
  ).trim();
}

async function main() {
  console.log("🚀 Loading provinces from vietnam-divisions-js...");
  const provincesRaw = await v3.getAllProvinces();
  const provinces = (provincesRaw as any[]) || [];

  console.log(`📍 Found ${provinces.length} provinces`);

  for (const [index, province] of provinces.entries()) {
    const code = pickProvinceCode(province); // HCM, HNI...
    const name = pickProvinceName(province);

    if (!code || !name) {
      console.log("⚠️ Skip province because missing code/name:", province);
      continue;
    }

    await prisma.administrativeProvince.upsert({
      where: { code },
      update: {
        name,
        normalized: normalizeVietnamese(name),
        isActive: true,
        sortOrder: index + 1,
      },
      create: {
        code,
        name,
        normalized: normalizeVietnamese(name),
        isActive: true,
        sortOrder: index + 1,
      },
    });
  }

  console.log("🚀 Loading wards from all provinces...");
  let totalWards = 0;

  for (const province of provinces) {
    const provinceCode = pickProvinceCode(province); // để lưu DB
    const provinceId = pickProvinceId(province); // để gọi package

    if (!provinceCode || !provinceId) {
      console.log("⚠️ Skip province because missing code/id:", province);
      continue;
    }

    const wardsRaw = await v3.getCommunesByProvinceId(provinceId);
    const wards = (wardsRaw as any[]) || [];

    for (const [idx, ward] of wards.entries()) {
      const wardCode = pickWardCode(ward);
      const wardName = pickWardName(ward);

      if (!wardCode || !wardName) {
        console.log("⚠️ Skip ward because missing code/name:", ward);
        continue;
      }

      await prisma.administrativeWard.upsert({
        where: { code: wardCode },
        update: {
          provinceCode, // FK sang AdministrativeProvince.code
          name: wardName,
          normalized: normalizeVietnamese(wardName),
          isActive: true,
          sortOrder: idx + 1,
        },
        create: {
          code: wardCode,
          provinceCode,
          name: wardName,
          normalized: normalizeVietnamese(wardName),
          isActive: true,
          sortOrder: idx + 1,
        },
      });

      totalWards += 1;
    }

    console.log(`✅ ${provinceCode} (${provinceId}): ${wards.length} wards`);
  }

  console.log(
    `🎉 Done. Seeded ${provinces.length} provinces and ${totalWards} wards.`
  );
}

main()
  .catch((e) => {
    console.error("❌ Seed address error:", e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
    await pool.end();
  });