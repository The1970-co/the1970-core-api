import { BadRequestException, Injectable, NotFoundException } from "@nestjs/common";
import { DesignSampleStatus, FabricReceiptStatus, Prisma } from "@prisma/client";
import { PrismaService } from "../prisma/prisma.service";

type Actor = { id?: string | null; name?: string | null; fullName?: string | null; email?: string | null };

const SAMPLE_SEASONS = ["Xuân Hạ", "Thu Đông", "Đông Xuân", "Xuân Hè"] as const;
const DEFAULT_FABRIC_COMPOSITIONS = [
  "Cotton",
  "Linen",
  "Tencel",
  "Lyocell",
  "Viscose",
  "Rayon",
  "Modal",
  "Bamboo",
  "Polyester",
  "Nylon",
  "Spandex",
  "Elastane",
  "Wool",
  "Cashmere",
  "Silk",
  "Acrylic",
] as const;

@Injectable()
export class SampleFabricService {
  constructor(private readonly prisma: PrismaService) {}

  private actor(user?: Actor) {
    return {
      id: String(user?.id || "") || null,
      name: String(user?.name || user?.fullName || user?.email || "").trim() || null,
    };
  }

  private userHas(user: any, permission: string) {
    const role = String(user?.role || "").trim().toUpperCase();
    if (role === "OWNER" || role === "ADMIN") return true;
    const keys = new Set<string>();
    for (const value of [...(user?.permissions || []), ...(user?.permissionKeys || [])]) {
      if (value) keys.add(String(value));
    }
    for (const row of user?.branchPermissions || []) {
      for (const value of [...(row?.permissionKeys || []), ...(row?.extraPermissionKeys || [])]) {
        if (value) keys.add(String(value));
      }
      for (const value of row?.deniedPermissionKeys || []) keys.delete(String(value));
    }
    return keys.has("*") || keys.has(permission);
  }

  private n(value: any) {
    if (value === null || value === undefined || value === "") return null;
    const parsed = Number(String(value).replace(",", "."));
    return Number.isFinite(parsed) ? parsed : null;
  }

  private titleCase(value: any) {
    return String(value || "")
      .trim()
      .replace(/\s+/g, " ")
      .split(" ")
      .map((part) => part ? part.charAt(0).toLocaleUpperCase("vi-VN") + part.slice(1).toLocaleLowerCase("vi-VN") : "")
      .join(" ");
  }

  private normalizeSampleCode(value: any) {
    return String(value || "").trim().toUpperCase().replace(/\s+/g, "");
  }

  private parseCompositionTokens(value?: string | null) {
    return String(value || "")
      .split(",")
      .map((item) => this.titleCase(item))
      .filter(Boolean);
  }

  private async nextCode(prefix: string, model: "sample" | "receipt") {
    const day = new Date().toISOString().slice(2, 10).replace(/-/g, "");
    const base = `${prefix}${day}`;
    const rows = model === "sample"
      ? await this.prisma.designSample.count({ where: { code: { startsWith: base } } })
      : await this.prisma.fabricReceipt.count({ where: { receiptCode: { startsWith: base } } });
    return `${base}-${String(rows + 1).padStart(3, "0")}`;
  }

  private async nextFabricSupplierCode() {
    for (let index = 1; index < 100000; index += 1) {
      const code = `NCCV${String(index).padStart(4, "0")}`;
      const exists = await this.prisma.fabricSupplier.findUnique({ where: { code }, select: { id: true } });
      if (!exists) return code;
    }
    throw new BadRequestException("Không thể sinh mã nhà cung cấp vải.");
  }

  async listFabricSuppliers() {
    return this.prisma.fabricSupplier.findMany({
      where: { isActive: true },
      select: { id: true, code: true, name: true, phone: true, email: true, address: true, note: true },
      orderBy: { name: "asc" },
    });
  }

  async createFabricSupplier(body: any) {
    const name = this.titleCase(body?.name);
    if (!name) throw new BadRequestException("Thiếu tên nhà cung cấp vải.");

    const sameName = await this.prisma.fabricSupplier.findFirst({
      where: { name: { equals: name, mode: "insensitive" }, isActive: true },
      select: { id: true, code: true, name: true, phone: true, email: true, address: true, note: true },
    });
    if (sameName) return sameName;

    const code = this.normalizeSampleCode(body?.code) || await this.nextFabricSupplierCode();
    const codeExists = await this.prisma.fabricSupplier.findUnique({ where: { code }, select: { id: true } });
    if (codeExists) throw new BadRequestException(`Mã nhà cung cấp vải ${code} đã tồn tại.`);

    return this.prisma.fabricSupplier.create({
      data: {
        name,
        code,
        phone: String(body?.phone || "").trim() || null,
        email: String(body?.email || "").trim() || null,
        address: String(body?.address || "").trim() || null,
        note: String(body?.note || "").trim() || null,
      },
      select: { id: true, code: true, name: true, phone: true, email: true, address: true, note: true },
    });
  }

  async checkSampleCode(codeInput: any, excludeId?: string) {
    const code = this.normalizeSampleCode(codeInput);
    if (!code) return { available: true, code: "", source: null, message: "" };

    const sample = await this.prisma.designSample.findFirst({
      where: {
        code: { equals: code, mode: "insensitive" },
        ...(excludeId ? { id: { not: excludeId } } : {}),
      },
      select: { id: true, code: true, name: true },
    });
    if (sample) {
      return {
        available: false,
        code,
        source: "design_sample",
        message: `Mã ${code} đã có trong Quản lý mẫu mã (${sample.name}).`,
      };
    }

    const product = await this.prisma.product.findFirst({
      where: {
        OR: [
          { slug: { equals: code, mode: "insensitive" } },
          { variants: { some: { sku: { equals: code, mode: "insensitive" } } } },
          { variants: { some: { sku: { startsWith: `${code}-`, mode: "insensitive" } } } },
        ],
      },
      select: { id: true, name: true, slug: true },
    });
    if (product) {
      return {
        available: false,
        code,
        source: "product",
        message: `Mã ${code} đã tồn tại trong danh sách sản phẩm (${product.name}).`,
      };
    }

    return { available: true, code, source: null, message: `Mã ${code} có thể sử dụng.` };
  }

  private async assertSampleCodeAvailable(code: string, excludeId?: string) {
    const result = await this.checkSampleCode(code, excludeId);
    if (!result.available) throw new BadRequestException(result.message);
  }

  async sampleMeta() {
    const [suppliers, staff, categories, productCategories, sampleCategories, compositionRows] = await Promise.all([
      this.listFabricSuppliers(),
      this.prisma.staffUser.findMany({
        where: { isActive: true },
        select: { id: true, code: true, name: true, branchId: true },
        orderBy: { name: "asc" },
      }),
      this.prisma.category.findMany({
        where: { isActive: true },
        select: { name: true },
        orderBy: [{ sortOrder: "asc" }, { name: "asc" }],
      }),
      this.prisma.product.findMany({
        where: { category: { not: null } },
        distinct: ["category"],
        select: { category: true },
      }),
      this.prisma.designSample.findMany({
        where: { category: { not: null } },
        distinct: ["category"],
        select: { category: true },
      }),
      this.prisma.designSample.findMany({
        where: { fabricComposition: { not: null } },
        select: { fabricComposition: true },
      }),
    ]);

    const productGroups = Array.from(new Set([
      ...categories.map((row) => this.titleCase(row.name)),
      ...productCategories.map((row) => this.titleCase(row.category)),
      ...sampleCategories.map((row) => this.titleCase(row.category)),
    ].filter(Boolean))).sort((a, b) => a.localeCompare(b, "vi"));

    const fabricCompositions = Array.from(new Set([
      ...DEFAULT_FABRIC_COMPOSITIONS,
      ...compositionRows.flatMap((row) => this.parseCompositionTokens(row.fabricComposition)),
    ])).sort((a, b) => a.localeCompare(b, "vi"));

    return {
      suppliers,
      staff,
      seasons: SAMPLE_SEASONS,
      productGroups,
      fabricCompositions,
    };
  }

  async fabricMeta() {
    const [suppliers, branches, samples] = await Promise.all([
      this.listFabricSuppliers(),
      this.prisma.branch.findMany({ where: { isActive: true }, select: { id: true, name: true }, orderBy: { name: "asc" } }),
      this.prisma.designSample.findMany({
        select: { id: true, code: true, name: true, year: true, fabricBoardCode: true, fabricCode: true },
        orderBy: [{ year: "desc" }, { updatedAt: "desc" }],
      }),
    ]);
    return { suppliers, branches, samples };
  }

  async listSamples(query?: { q?: string; year?: string; status?: string; supplierId?: string }) {
    const q = String(query?.q || "").trim();
    const year = Number(query?.year || 0);
    const status = String(query?.status || "").trim() as DesignSampleStatus;
    const where: Prisma.DesignSampleWhereInput = {
      ...(year ? { year } : {}),
      ...(status ? { status } : {}),
      ...(query?.supplierId ? { supplierId: query.supplierId } : {}),
      ...(q ? {
        OR: [
          { code: { contains: q, mode: "insensitive" } },
          { name: { contains: q, mode: "insensitive" } },
          { category: { contains: q, mode: "insensitive" } },
          { fabricBoardCode: { contains: q, mode: "insensitive" } },
          { fabricCode: { contains: q, mode: "insensitive" } },
          { fabricComposition: { contains: q, mode: "insensitive" } },
        ],
      } : {}),
    };

    return this.prisma.designSample.findMany({
      where,
      include: {
        supplier: { select: { id: true, code: true, name: true } },
        colors: { orderBy: { createdAt: "asc" } },
        images: { orderBy: { createdAt: "desc" } },
        progressLogs: { orderBy: { createdAt: "desc" }, take: 8 },
        _count: { select: { fabricReceipts: true } },
      },
      orderBy: [{ year: "desc" }, { updatedAt: "desc" }],
    });
  }

  async createSample(body: any, user?: Actor) {
    const actor = this.actor(user);
    let code = this.normalizeSampleCode(body?.code);
    if (!code) code = await this.nextCode("MS", "sample");
    await this.assertSampleCodeAvailable(code);

    const name = String(body?.name || "").trim();
    if (!name) throw new BadRequestException("Thiếu tên mẫu.");

    const season = String(body?.season || "").trim();
    if (season && !SAMPLE_SEASONS.includes(season as any)) {
      throw new BadRequestException("Mùa / BST không hợp lệ.");
    }

    const status = (body?.status || "IDEA") as DesignSampleStatus;
    const colors = Array.isArray(body?.colors) ? body.colors : [];
    const images = Array.isArray(body?.images) ? body.images : [];
    const composition = this.parseCompositionTokens(body?.fabricComposition).join(", ") || null;

    return this.prisma.designSample.create({
      data: {
        code,
        name,
        year: Number(body?.year || new Date().getFullYear()),
        season: season || null,
        category: this.titleCase(body?.category) || null,
        supplierId: body?.supplierId || null,
        fabricBoardCode: body?.fabricBoardCode || null,
        fabricCode: body?.fabricCode || null,
        fabricComposition: composition,
        status,
        assigneeStaffId: body?.assigneeStaffId || null,
        assigneeName: body?.assigneeName || null,
        nextAction: body?.nextAction || null,
        dueDate: body?.dueDate ? new Date(body.dueDate) : null,
        coverImageUrl: body?.coverImageUrl || images?.[0]?.url || null,
        note: body?.note || null,
        technicalNote: body?.technicalNote || null,
        createdById: actor.id,
        createdByName: actor.name,
        colors: {
          create: colors.filter((x: any) => x?.name).map((x: any) => ({
            name: this.titleCase(x.name),
            code: x.code || null,
            status: (x.status || status) as DesignSampleStatus,
            note: x.note || null,
            imageUrl: x.imageUrl || null,
          })),
        },
        images: { create: images.filter((x: any) => x?.url).map((x: any) => ({ url: x.url, caption: x.caption || null })) },
        progressLogs: { create: { toStatus: status, note: "Tạo mẫu", actorId: actor.id, actorName: actor.name } },
      },
      include: { supplier: true, colors: true, images: true, progressLogs: true },
    });
  }

  async updateSample(id: string, body: any, user?: Actor) {
    const current = await this.prisma.designSample.findUnique({ where: { id }, include: { colors: true } });
    if (!current) throw new NotFoundException("Không tìm thấy mẫu.");
    const actor = this.actor(user);
    const nextStatus = (body?.status || current.status) as DesignSampleStatus;
    const season = body?.season !== undefined ? String(body.season || "").trim() : undefined;
    if (season && !SAMPLE_SEASONS.includes(season as any)) throw new BadRequestException("Mùa / BST không hợp lệ.");

    return this.prisma.$transaction(async (tx) => {
      if (Array.isArray(body?.colors)) {
        await tx.designSampleColor.deleteMany({ where: { designSampleId: id } });
        if (body.colors.length) {
          await tx.designSampleColor.createMany({
            data: body.colors.filter((x: any) => x?.name).map((x: any) => ({
              designSampleId: id,
              name: this.titleCase(x.name),
              code: x.code || null,
              status: (x.status || nextStatus) as DesignSampleStatus,
              note: x.note || null,
              imageUrl: x.imageUrl || null,
            })),
          });
        }
      }

      if (Array.isArray(body?.images)) {
        await tx.designSampleImage.deleteMany({ where: { designSampleId: id } });
        if (body.images.length) {
          await tx.designSampleImage.createMany({
            data: body.images.filter((x: any) => x?.url).map((x: any) => ({ designSampleId: id, url: x.url, caption: x.caption || null })),
          });
        }
      }

      if (nextStatus !== current.status) {
        await tx.designSampleProgressLog.create({
          data: { designSampleId: id, fromStatus: current.status, toStatus: nextStatus, note: body?.progressNote || body?.note || null, actorId: actor.id, actorName: actor.name },
        });
      }

      return tx.designSample.update({
        where: { id },
        data: {
          ...(body?.name !== undefined ? { name: String(body.name).trim() } : {}),
          ...(body?.year !== undefined ? { year: Number(body.year) } : {}),
          ...(body?.season !== undefined ? { season: season || null } : {}),
          ...(body?.category !== undefined ? { category: this.titleCase(body.category) || null } : {}),
          ...(body?.supplierId !== undefined ? { supplierId: body.supplierId || null } : {}),
          ...(body?.fabricBoardCode !== undefined ? { fabricBoardCode: body.fabricBoardCode || null } : {}),
          ...(body?.fabricCode !== undefined ? { fabricCode: body.fabricCode || null } : {}),
          ...(body?.fabricComposition !== undefined ? { fabricComposition: this.parseCompositionTokens(body.fabricComposition).join(", ") || null } : {}),
          ...(body?.status !== undefined ? { status: nextStatus } : {}),
          ...(body?.assigneeStaffId !== undefined ? { assigneeStaffId: body.assigneeStaffId || null } : {}),
          ...(body?.assigneeName !== undefined ? { assigneeName: body.assigneeName || null } : {}),
          ...(body?.nextAction !== undefined ? { nextAction: body.nextAction || null } : {}),
          ...(body?.dueDate !== undefined ? { dueDate: body.dueDate ? new Date(body.dueDate) : null } : {}),
          ...(body?.coverImageUrl !== undefined ? { coverImageUrl: body.coverImageUrl || null } : {}),
          ...(body?.note !== undefined ? { note: body.note || null } : {}),
          ...(body?.technicalNote !== undefined ? { technicalNote: body.technicalNote || null } : {}),
        },
        include: { supplier: true, colors: true, images: true, progressLogs: { orderBy: { createdAt: "desc" } } },
      });
    });
  }

  async deleteSample(id: string) {
    const found = await this.prisma.designSample.findUnique({ where: { id }, select: { id: true, _count: { select: { fabricReceipts: true } } } });
    if (!found) throw new NotFoundException("Không tìm thấy mẫu.");
    if (found._count.fabricReceipts > 0) throw new BadRequestException("Mẫu đã có phiếu vải về, không thể xoá. Chuyển trạng thái Tạm dừng thay vì xoá.");
    return this.prisma.designSample.delete({ where: { id } });
  }

  async listFabricReceipts(query: { q?: string; status?: string; branchId?: string; supplierId?: string } | undefined, user?: any) {
    const q = String(query?.q || "").trim();
    const status = String(query?.status || "").trim() as FabricReceiptStatus;
    const rows = await this.prisma.fabricReceipt.findMany({
      where: {
        ...(status ? { status } : {}),
        ...(query?.branchId ? { branchId: query.branchId } : {}),
        ...(query?.supplierId ? { supplierId: query.supplierId } : {}),
        ...(q ? { OR: [
          { receiptCode: { contains: q, mode: "insensitive" } },
          { fabricBoardCode: { contains: q, mode: "insensitive" } },
          { fabricCode: { contains: q, mode: "insensitive" } },
          { fabricName: { contains: q, mode: "insensitive" } },
          { colorName: { contains: q, mode: "insensitive" } },
          { colorCode: { contains: q, mode: "insensitive" } },
          { lotCode: { contains: q, mode: "insensitive" } },
        ] } : {}),
      },
      include: {
        supplier: { select: { id: true, code: true, name: true } },
        branch: { select: { id: true, name: true } },
        designSample: { select: { id: true, code: true, name: true, year: true } },
        rolls: { orderBy: { createdAt: "asc" } },
        measurements: { orderBy: { createdAt: "desc" } },
        images: { orderBy: { createdAt: "desc" } },
      },
      orderBy: { updatedAt: "desc" },
    });
    const canViewCost = this.userHas(user, "fabric_receipt.cost.view") || this.userHas(user, "fabric_receipt.cost.edit");
    return canViewCost ? rows : rows.map((row: any) => ({ ...row, unitPrice: null }));
  }

  async createFabricReceipt(body: any, user?: Actor) {
    const actor = this.actor(user);
    const receiptCode = String(body?.receiptCode || "").trim() || await this.nextCode("NV", "receipt");
    const rolls = Array.isArray(body?.rolls) ? body.rolls : [];
    const status = (body?.status || "DRAFT") as FabricReceiptStatus;

    return this.prisma.fabricReceipt.create({
      data: {
        receiptCode,
        designSampleId: body?.designSampleId || null,
        supplierId: body?.supplierId || null,
        branchId: body?.branchId || null,
        fabricBoardCode: body?.fabricBoardCode || null,
        fabricCode: body?.fabricCode || null,
        fabricName: body?.fabricName || null,
        colorName: body?.colorName || null,
        colorCode: body?.colorCode || null,
        lotCode: body?.lotCode || null,
        supplierDeclaredM: this.n(body?.supplierDeclaredM),
        supplierDeclaredKg: this.n(body?.supplierDeclaredKg),
        actualM: this.n(body?.actualM),
        actualKg: this.n(body?.actualKg),
        rollCount: Number(body?.rollCount || rolls.length || 0),
        expectedGsm: this.n(body?.expectedGsm),
        status,
        receivedAt: body?.receivedAt ? new Date(body.receivedAt) : null,
        note: body?.note || null,
        createdById: actor.id,
        createdByName: actor.name,
        rolls: { create: rolls.map((x: any) => ({
          rollCode: x.rollCode || null,
          supplierDeclaredM: this.n(x.supplierDeclaredM),
          supplierDeclaredKg: this.n(x.supplierDeclaredKg),
          actualM: this.n(x.actualM),
          actualKg: this.n(x.actualKg),
          defectNote: x.defectNote || null,
          passed: x.passed !== false,
        })) },
      },
      include: { supplier: true, branch: true, designSample: true, rolls: true, measurements: true, images: true },
    });
  }

  async updateFabricReceipt(id: string, body: any) {
    const found = await this.prisma.fabricReceipt.findUnique({ where: { id } });
    if (!found) throw new NotFoundException("Không tìm thấy phiếu vải về.");
    if (found.status === "COMPLETED") throw new BadRequestException("Phiếu đã hoàn tất. Cần quyền duyệt/sửa đặc biệt nếu muốn điều chỉnh nghiệp vụ.");

    return this.prisma.$transaction(async (tx) => {
      if (Array.isArray(body?.rolls)) {
        await tx.fabricReceiptRoll.deleteMany({ where: { fabricReceiptId: id } });
        if (body.rolls.length) {
          await tx.fabricReceiptRoll.createMany({ data: body.rolls.map((x: any) => ({
            fabricReceiptId: id,
            rollCode: x.rollCode || null,
            supplierDeclaredM: this.n(x.supplierDeclaredM),
            supplierDeclaredKg: this.n(x.supplierDeclaredKg),
            actualM: this.n(x.actualM),
            actualKg: this.n(x.actualKg),
            defectNote: x.defectNote || null,
            passed: x.passed !== false,
          })) });
        }
      }

      return tx.fabricReceipt.update({
        where: { id },
        data: {
          ...(body?.designSampleId !== undefined ? { designSampleId: body.designSampleId || null } : {}),
          ...(body?.supplierId !== undefined ? { supplierId: body.supplierId || null } : {}),
          ...(body?.branchId !== undefined ? { branchId: body.branchId || null } : {}),
          ...(body?.fabricBoardCode !== undefined ? { fabricBoardCode: body.fabricBoardCode || null } : {}),
          ...(body?.fabricCode !== undefined ? { fabricCode: body.fabricCode || null } : {}),
          ...(body?.fabricName !== undefined ? { fabricName: body.fabricName || null } : {}),
          ...(body?.colorName !== undefined ? { colorName: body.colorName || null } : {}),
          ...(body?.colorCode !== undefined ? { colorCode: body.colorCode || null } : {}),
          ...(body?.lotCode !== undefined ? { lotCode: body.lotCode || null } : {}),
          ...(body?.supplierDeclaredM !== undefined ? { supplierDeclaredM: this.n(body.supplierDeclaredM) } : {}),
          ...(body?.supplierDeclaredKg !== undefined ? { supplierDeclaredKg: this.n(body.supplierDeclaredKg) } : {}),
          ...(body?.actualM !== undefined ? { actualM: this.n(body.actualM) } : {}),
          ...(body?.actualKg !== undefined ? { actualKg: this.n(body.actualKg) } : {}),
          ...(body?.rollCount !== undefined || Array.isArray(body?.rolls) ? { rollCount: Number(body?.rollCount || body?.rolls?.length || 0) } : {}),
          ...(body?.expectedGsm !== undefined ? { expectedGsm: this.n(body.expectedGsm) } : {}),
          ...(body?.status !== undefined ? { status: body.status as FabricReceiptStatus } : {}),
          ...(body?.receivedAt !== undefined ? { receivedAt: body.receivedAt ? new Date(body.receivedAt) : null } : {}),
          ...(body?.note !== undefined ? { note: body.note || null } : {}),
        },
        include: { supplier: true, branch: true, designSample: true, rolls: true, measurements: true, images: true },
      });
    });
  }

  async setFabricReceiptCost(id: string, body: any) {
    const found = await this.prisma.fabricReceipt.findUnique({ where: { id }, select: { id: true } });
    if (!found) throw new NotFoundException("Không tìm thấy phiếu vải về.");
    return this.prisma.fabricReceipt.update({
      where: { id },
      data: { unitPrice: this.n(body?.unitPrice), priceUnit: body?.priceUnit || "METER" },
    });
  }

  async addMeasurement(id: string, body: any, user?: Actor) {
    const receipt = await this.prisma.fabricReceipt.findUnique({ where: { id }, select: { id: true } });
    if (!receipt) throw new NotFoundException("Không tìm thấy phiếu vải về.");
    const areaCm2 = this.n(body?.areaCm2) || 100;
    const weightGrams = this.n(body?.weightGrams);
    if (!weightGrams || weightGrams <= 0) throw new BadRequestException("Cân nặng mẫu phải lớn hơn 0g.");
    if (areaCm2 <= 0) throw new BadRequestException("Diện tích mẫu phải lớn hơn 0cm².");
    const gsm = Math.round((weightGrams * 10000 / areaCm2) * 100) / 100;
    const actor = this.actor(user);

    const measurement = await this.prisma.fabricMeasurement.create({
      data: {
        fabricReceiptId: id,
        rollId: body?.rollId || null,
        areaCm2,
        weightGrams,
        gsm,
        positionLabel: body?.positionLabel || null,
        imageUrl: body?.imageUrl || null,
        note: body?.note || null,
        measuredById: actor.id,
        measuredByName: actor.name,
      },
    });

    const agg = await this.prisma.fabricMeasurement.aggregate({ where: { fabricReceiptId: id }, _avg: { gsm: true } });
    await this.prisma.fabricReceipt.update({ where: { id }, data: { measuredGsm: agg._avg.gsm, status: "INSPECTING" } });
    return measurement;
  }

  async addFabricImage(id: string, body: any) {
    const url = String(body?.url || "").trim();
    if (!url) throw new BadRequestException("Thiếu URL ảnh.");
    return this.prisma.fabricReceiptImage.create({
      data: { fabricReceiptId: id, rollId: body?.rollId || null, type: body?.type || "FABRIC", url, caption: body?.caption || null },
    });
  }

  async completeFabricReceipt(id: string) {
    const found = await this.prisma.fabricReceipt.findUnique({ where: { id }, include: { measurements: true } });
    if (!found) throw new NotFoundException("Không tìm thấy phiếu vải về.");
    if (found.actualM === null && found.actualKg === null) throw new BadRequestException("Chưa nhập số lượng thực nhận.");
    return this.prisma.fabricReceipt.update({ where: { id }, data: { status: "COMPLETED", completedAt: new Date() } });
  }

  async approveVariance(id: string, user?: Actor) {
    const actor = this.actor(user);
    return this.prisma.fabricReceipt.update({
      where: { id },
      data: { varianceApproved: true, varianceApprovedBy: actor.name || actor.id, varianceApprovedAt: new Date() },
    });
  }

  async deleteFabricReceipt(id: string) {
    const found = await this.prisma.fabricReceipt.findUnique({ where: { id } });
    if (!found) throw new NotFoundException("Không tìm thấy phiếu vải về.");
    if (found.status === "COMPLETED") throw new BadRequestException("Không xoá phiếu đã hoàn tất.");
    return this.prisma.fabricReceipt.delete({ where: { id } });
  }
}
