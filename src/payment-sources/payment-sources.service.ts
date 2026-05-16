import { BadRequestException, Injectable, NotFoundException } from "@nestjs/common";
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

  private normalizePayload(data: any) {
    const code = String(data?.code || "").trim();
    const name = String(data?.name || "").trim();

    if (!code) {
      throw new BadRequestException("Thiếu mã nguồn tiền.");
    }

    if (!name) {
      throw new BadRequestException("Thiếu tên hiển thị nguồn tiền.");
    }

    return {
      code,
      name,
      type: String(data?.type || "OTHER").trim(),
      branchId: data?.branchId ? String(data.branchId) : null,
      isActive: data?.isActive !== false,
      sortOrder: Number(data?.sortOrder || 0),
      note: data?.note ? String(data.note).trim() : null,
    };
  }

  async create(data: any) {
    const payload = this.normalizePayload(data);

    return this.prisma.paymentSource.upsert({
      where: { code: payload.code },
      update: payload,
      create: payload,
    });
  }

  async update(id: string, data: any) {
    const paymentSourceId = String(id || "").trim();

    if (!paymentSourceId) {
      throw new BadRequestException("Thiếu ID nguồn tiền.");
    }

    const existing = await this.prisma.paymentSource.findUnique({
      where: { id: paymentSourceId },
    });

    if (!existing) {
      throw new NotFoundException("Không tìm thấy nguồn tiền.");
    }

    const payload = this.normalizePayload(data);

    return this.prisma.paymentSource.update({
      where: { id: paymentSourceId },
      data: payload,
    });
  }
}
