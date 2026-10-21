import { BadRequestException, ForbiddenException, Injectable, NotFoundException } from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";

@Injectable()
export class PaymentSourcesService {
  constructor(private readonly prisma: PrismaService) {}

  private isAdmin(user: any) {
    const role = String(user?.role?.code || user?.role?.name || user?.role || "").toUpperCase();
    const roles = Array.isArray(user?.roles) ? user.roles.map((item: any) => String(item?.code || item).toUpperCase()) : [];
    return ["OWNER", "ADMIN", "SUPER_ADMIN"].includes(role) || roles.some((r: string) => ["OWNER", "ADMIN", "SUPER_ADMIN"].includes(r));
  }

  private async ensurePosRuleTable() {
    await this.prisma.$executeRawUnsafe(`CREATE TABLE IF NOT EXISTS "PosPaymentRule" (
      "id" TEXT PRIMARY KEY,
      "branchId" TEXT NOT NULL,
      "kind" TEXT NOT NULL,
      "operator" TEXT NOT NULL DEFAULT 'gte',
      "threshold" INTEGER NOT NULL DEFAULT 0,
      "paymentSourceId" TEXT NOT NULL,
      "qrAccountId" TEXT NOT NULL DEFAULT '',
      "priority" INTEGER NOT NULL DEFAULT 0,
      "enabled" BOOLEAN NOT NULL DEFAULT TRUE,
      "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
      "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
    )`);
  }

  async getPosRules(user: any) {
    if (!user) throw new ForbiddenException("Cần đăng nhập.");
    await this.ensurePosRuleTable();
    return this.prisma.$queryRawUnsafe<any[]>(`SELECT "id", "branchId", "kind", "operator", "threshold", "paymentSourceId", "qrAccountId", "priority", "enabled" FROM "PosPaymentRule" ORDER BY "priority" ASC, "createdAt" ASC`);
  }

  async savePosRules(user: any, input: any) {
    if (!this.isAdmin(user)) throw new ForbiddenException("Chỉ Admin/Owner được cấu hình thanh toán POS.");
    const raw = input?.items;
    if (!Array.isArray(raw) || raw.length > 200) throw new BadRequestException("Danh sách quy tắc không hợp lệ (tối đa 200).");
    const kinds = new Set(["amount", "purchase_count", "first_purchase", "returning"]);
    const ids = new Set<string>();
    const items = raw.map((row: any, i: number) => {
      const id = String(row?.id || "").trim();
      const branchId = String(row?.branchId || "").trim();
      const paymentSourceId = String(row?.paymentSourceId || "").trim();
      const kind = String(row?.kind || "");
      const operator = String(row?.operator || "gte");
      const threshold = Number(row?.threshold ?? 0);
      const qrAccountId = String(row?.qrAccountId || "").trim();
      if (!id || id.length > 128 || ids.has(id) || !branchId || !paymentSourceId || !kinds.has(kind) || !["gte", "lte"].includes(operator) || !Number.isSafeInteger(threshold) || threshold < 0 || qrAccountId.length > 128) {
        throw new BadRequestException(`Quy tắc ${i + 1} không hợp lệ.`);
      }
      ids.add(id);
      return { id, branchId, paymentSourceId, kind, operator, threshold, qrAccountId, priority: i + 1, enabled: row?.enabled !== false };
    });
    for (const item of items) {
      const src = await this.prisma.paymentSource.findFirst({ where: { id: item.paymentSourceId, isActive: true } });
      if (!src || (src.branchId && src.branchId !== item.branchId)) throw new BadRequestException(`Nguồn tiền không hợp lệ tại chi nhánh của quy tắc ${item.id}.`);
      const branch = await this.prisma.branch.findUnique({ where: { id: item.branchId } });
      if (!branch) throw new BadRequestException(`Chi nhánh không tồn tại: ${item.branchId}`);
    }
    await this.ensurePosRuleTable();
    await this.prisma.$transaction(async (tx: any) => {
      await tx.$executeRawUnsafe(`DELETE FROM "PosPaymentRule"`);
      for (const r of items) {
        await tx.$executeRawUnsafe(`INSERT INTO "PosPaymentRule" ("id", "branchId", "kind", "operator", "threshold", "paymentSourceId", "qrAccountId", "priority", "enabled") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, r.id, r.branchId, r.kind, r.operator, r.threshold, r.paymentSourceId, r.qrAccountId, r.priority, r.enabled);
      }
    });
    return this.getPosRules(user);
  }

  async getPosCustomerHistory(user: any, phone: string) {
    if (!user) throw new ForbiddenException("Cần đăng nhập.");
    const normalized = String(phone || "").replace(/\D/g, "");
    if (normalized.length < 9 || normalized.length > 15) throw new BadRequestException("Số điện thoại không hợp lệ.");
    // Tính từ đơn thực tế toàn hệ thống, loại đơn mới/chưa duyệt và đơn hủy.
    // Dùng đúng số điện thoại; không ghép khách bằng tên.
    const rows = await this.prisma.$queryRawUnsafe<Array<{ count: bigint }>>(
      `SELECT COUNT(DISTINCT "id")::bigint AS count FROM "Order" WHERE (regexp_replace(COALESCE("customerPhone", ''), '[^0-9]', '', 'g') = $1 OR regexp_replace(COALESCE("shippingPhone", ''), '[^0-9]', '', 'g') = $1) AND "status"::text IN ('APPROVED', 'PACKING', 'SHIPPED', 'COMPLETED')`, normalized
    );
    return { completedOrderCount: Number(rows?.[0]?.count || 0) };
  }

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
