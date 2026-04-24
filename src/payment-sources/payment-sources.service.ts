import { Injectable } from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";

@Injectable()
export class PaymentSourcesService {
  constructor(private readonly prisma: PrismaService) {}

  findAll() {
    return this.prisma.paymentSource.findMany({
      where: { isActive: true },
      orderBy: [{ sortOrder: "asc" }, { name: "asc" }],
    });
  }
  
async create(data: any) {
  return this.prisma.paymentSource.upsert({
    where: { code: String(data.code).trim() },
    update: {
      name: String(data.name).trim(),
      type: String(data.type).trim(),
      branchId: data.branchId ? String(data.branchId) : null,
      isActive: data.isActive !== false,
      sortOrder: Number(data.sortOrder || 0),
      note: data.note ? String(data.note) : null,
    },
    create: {
      code: String(data.code).trim(),
      name: String(data.name).trim(),
      type: String(data.type).trim(),
      branchId: data.branchId ? String(data.branchId) : null,
      isActive: data.isActive !== false,
      sortOrder: Number(data.sortOrder || 0),
      note: data.note ? String(data.note) : null,
    },
  });
}
}