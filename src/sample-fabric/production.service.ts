import { BadRequestException, Injectable, NotFoundException } from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";

type Actor = { id?: string; sub?: string; name?: string; fullName?: string; email?: string };

type SourceType = "SAMPLE" | "PRODUCT";

@Injectable()
export class ProductionService {
  constructor(private readonly prisma: PrismaService) {}

  private actor(user?: Actor) {
    return {
      id: String(user?.id || user?.sub || "") || null,
      name: String(user?.name || user?.fullName || user?.email || "Hệ thống"),
    };
  }

  private userHas(user: any, permission: string) {
    const roles = [user?.role, ...(Array.isArray(user?.roles) ? user.roles : [])]
      .map((x) => String(x || "").trim().toLowerCase())
      .filter(Boolean);
    if (roles.includes("owner") || roles.includes("admin")) return true;

    const keys = new Set<string>();
    for (const value of [...(user?.permissions || []), ...(user?.permissionKeys || [])]) if (value) keys.add(String(value));
    const rows = Array.isArray(user?.branchPermissions) ? user.branchPermissions : [];
    const activeBranchId = String(user?.activeBranchId || user?.branchId || "").trim();
    const scopedRows = activeBranchId
      ? rows.filter((row: any) => String(row?.branchId || row?.branch?.id || "").trim() === activeBranchId)
      : [];
    const rowsToUse = scopedRows.length ? scopedRows : rows;
    for (const row of rowsToUse) {
      for (const value of [...(row?.permissionKeys || []), ...(row?.extraPermissionKeys || [])]) if (value) keys.add(String(value));
      for (const value of row?.deniedPermissionKeys || []) keys.delete(String(value));
    }
    return keys.has("*") || keys.has(permission);
  }

  private accessorySupplierForUser(row: any, user?: any) {
    if (!row) return row;
    if (this.userHas(user, "accessories.supplier_identity.view")) return row;
    return { id: row.id, code: row.code, name: null, phone: null, email: null, address: null, note: null, isActive: row.isActive };
  }

  private accessoryForUser(row: any, user?: any) {
    if (!row) return row;
    return {
      ...row,
      ...(this.userHas(user, "accessories.cost.view") ? {} : { unitPrice: null }),
    };
  }

  private n(value: any) {
    if (value === null || value === undefined || value === "") return null;
    const clean = typeof value === "string" ? value.replace(/[^\d,.\-]/g, "").replace(",", ".") : value;
    const n = Number(clean);
    return Number.isFinite(n) ? n : null;
  }

  private normalizeProductionSize(value: any) {
    const raw = String(value || "").trim().toUpperCase().replace(/\s+/g, "");
    if (raw === "2XL") return "XXL";
    return raw;
  }

  private normalizeAccessorySpecifications(typeName: string, value: any) {
    const specs = value && typeof value === "object" && !Array.isArray(value) ? { ...value } : {};
    if (String(typeName || "").trim() !== "Mác Size") return specs;

    const sizeKind = String(specs.sizeKind || specs.sizeType || "").trim().toUpperCase();
    if (sizeKind !== "SHIRT" && sizeKind !== "PANTS") {
      throw new BadRequestException("Mác Size phải chọn loại size Áo hoặc Quần.");
    }

    const size = this.normalizeProductionSize(specs.size || specs.sizeLabel);
    const allowed = sizeKind === "PANTS"
      ? ["29", "30", "31", "32", "34", "36"]
      : ["XS", "S", "M", "L", "XL", "XXL"];
    if (!allowed.includes(size)) {
      throw new BadRequestException(`Size ${size || "trống"} không hợp lệ cho ${sizeKind === "PANTS" ? "mác size quần" : "mác size áo"}.`);
    }

    return { ...specs, sizeKind, size };
  }

  private accessoryTaggedSize(item: any) {
    if (String(item?.typeName || "").trim() !== "Mác Size") return null;
    const specs = item?.specifications && typeof item.specifications === "object" ? item.specifications : {};
    const explicit = this.normalizeProductionSize((specs as any).size || (specs as any).sizeLabel);
    if (explicit) return explicit;

    // Hỗ trợ dữ liệu Mác Size cũ chưa có specifications.size: chỉ suy ra khi size nằm ở cuối tên NPL.
    const name = String(item?.name || "").trim().toUpperCase();
    const matched = name.match(/(?:^|[-–—\s])((?:2?X?XL)|XS|S|M|L|29|30|31|32|34|36)\s*$/i);
    return matched?.[1] ? this.normalizeProductionSize(matched[1]) : null;
  }

  private totalForTaggedSize(totalsBySize: Record<string, number>, taggedSize: string) {
    const target = this.normalizeProductionSize(taggedSize);
    return Object.entries(totalsBySize).reduce(
      (sum, [size, qty]) => sum + (this.normalizeProductionSize(size) === target ? Number(qty || 0) : 0),
      0,
    );
  }

  private normalizeColorCode(value: any) {
    const raw = String(value || "").trim();
    if (!raw) return null;
    return `#${raw.replace(/^#+/, "")}`;
  }

  private initial(name: string) {
    const raw = String(name || "")
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/đ/g, "d")
      .replace(/Đ/g, "D");
    return (raw.match(/[A-Za-z0-9]/)?.[0] || "X").toUpperCase();
  }

  private productCode(product: any) {
    const skus = Array.isArray(product?.variants)
      ? product.variants.map((x: any) => String(x?.sku || "").trim()).filter(Boolean)
      : [];
    if (skus.length) return String(skus[0]).split("-")[0].toUpperCase();
    return String(product?.slug || product?.id || "").trim().toUpperCase();
  }

  private async nextSimpleCode(model: "factory" | "supplier" | "item", name: string) {
    const delegate: any =
      model === "factory"
        ? this.prisma.productionPartner
        : model === "supplier"
          ? this.prisma.productionAccessorySupplier
          : this.prisma.productionAccessoryItem;
    const rows = await delegate.findMany({ select: { code: true } });
    const max = rows.reduce(
      (m: number, r: any) => Math.max(m, Number(String(r.code || "").match(/\d+/)?.[0] || 0)),
      0,
    );
    return `${String(max + 1).padStart(3, "0")}-${this.initial(name)}`;
  }

  private async nextOrderCode() {
    const d = new Date();
    const suffix = `${String(d.getDate()).padStart(2, "0")}${String(d.getMonth() + 1).padStart(2, "0")}${d.getFullYear()}`;
    const rows = await this.prisma.productionOrder.findMany({
      where: { code: { endsWith: suffix } },
      select: { code: true },
    });
    const max = rows.reduce(
      (m: number, r: any) => Math.max(m, Number(String(r.code).match(/^SX-(\d+)-/)?.[1] || 0)),
      0,
    );
    return `SX-${String(max + 1).padStart(3, "0")}-${suffix}`;
  }

  async meta(user?: any) {
    const [samples, products, factories, accessories, rolls] = await Promise.all([
      this.prisma.designSample.findMany({
        where: { status: { not: "ON_HOLD" } },
        select: {
          id: true,
          code: true,
          name: true,
          year: true,
          season: true,
          category: true,
          coverImageUrl: true,
          fabricColorName: true,
          fabricColorCode: true,
        },
        orderBy: { updatedAt: "desc" },
      }),
      this.prisma.product.findMany({
        select: {
          id: true,
          name: true,
          slug: true,
          imageUrl: true,
          category: true,
          variants: { select: { sku: true, size: true, color: true }, take: 20 },
        },
        orderBy: { updatedAt: "desc" },
        take: 1000,
      }),
      this.prisma.productionPartner.findMany({ where: { isActive: true }, orderBy: { name: "asc" } }),
      this.prisma.productionAccessoryItem.findMany({
        where: { isActive: true },
        orderBy: [{ typeName: "asc" }, { name: "asc" }],
      }),
      this.availableFabricRolls(),
    ]);
    return {
      samples,
      products: products.map((p: any) => ({ ...p, code: this.productCode(p) })),
      factories,
      accessories: accessories.map((row: any) => this.accessoryForUser(row, user)),
      rolls,
    };
  }

  async availableFabricRolls(orderId?: string, qInput?: string) {
    const q = String(qInput || "").trim().toLowerCase();
    const rows = await this.prisma.fabricReceiptRoll.findMany({
      where: { fabricReceipt: { status: { in: ["RECEIVING", "INSPECTING", "COMPLETED"] } } },
      include: {
        fabricReceipt: {
          select: {
            id: true,
            receiptCode: true,
            fabricName: true,
            fabricCode: true,
            fabricBoardCode: true,
            colorName: true,
            colorCode: true,
          },
        },
        images: { orderBy: { createdAt: "desc" }, take: 1 },
      },
      orderBy: { createdAt: "desc" },
    });

    const allocated = await this.prisma.productionOrderRoll.groupBy({
      by: ["fabricReceiptRollId"],
      _sum: { allocatedM: true, allocatedKg: true },
    });
    const currentRows = orderId
      ? await this.prisma.productionOrderRoll.findMany({
          where: { productionOrderId: orderId },
          select: { fabricReceiptRollId: true, allocatedM: true, allocatedKg: true },
        })
      : [];

    const used = new Map(allocated.map((x: any) => [x.fabricReceiptRollId, x._sum]));
    const current = new Map(currentRows.map((x: any) => [x.fabricReceiptRollId, x]));

    return rows
      .map((r: any) => {
        const sum: any = used.get(r.id) || {};
        const own: any = current.get(r.id) || {};
        const actualM = Number(r.actualM || 0);
        const actualKg = Number(r.actualKg || 0);
        const usedOtherM = Math.max(0, Number(sum.allocatedM || 0) - Number(own.allocatedM || 0));
        const usedOtherKg = Math.max(0, Number(sum.allocatedKg || 0) - Number(own.allocatedKg || 0));
        const remainingM = Math.max(0, actualM - usedOtherM);
        const remainingKg = Math.max(0, actualKg - usedOtherKg);
        const row = {
          id: r.id,
          fabricReceiptId: r.fabricReceiptId,
          receiptCode: r.fabricReceipt?.receiptCode,
          fabricName: r.fabricReceipt?.fabricName,
          fabricCode: r.fabricReceipt?.fabricCode,
          fabricBoardCode: r.fabricReceipt?.fabricBoardCode,
          rollCode: r.rollCode,
          colorName: r.colorName || r.fabricReceipt?.colorName || null,
          colorCode: this.normalizeColorCode(r.colorCode || r.fabricReceipt?.colorCode),
          actualM,
          actualKg,
          usedOtherM,
          usedOtherKg,
          remainingM,
          remainingKg,
          isDepleted: actualM > 0 && remainingM <= 0.0001,
          missingActual: actualM <= 0,
          imageUrl: r.images?.[0]?.url || null,
        };
        return row;
      })
      .filter((r: any) => {
        if (!q) return true;
        return [
          r.receiptCode,
          r.fabricName,
          r.fabricCode,
          r.fabricBoardCode,
          r.rollCode,
          r.colorName,
          r.colorCode,
        ].some((v) => String(v || "").toLowerCase().includes(q));
      });
  }

  listFactories() {
    return this.prisma.productionPartner.findMany({ where: { isActive: true }, orderBy: { name: "asc" } });
  }

  async createFactory(body: any) {
    const name = String(body?.name || "").trim();
    if (!name) throw new BadRequestException("Thiếu tên nhà may.");
    return this.prisma.productionPartner.create({
      data: {
        code: String(body?.code || "").trim().toUpperCase() || (await this.nextSimpleCode("factory", name)),
        name,
        contactName: body?.contactName || null,
        phone: body?.phone || null,
        email: body?.email || null,
        address: body?.address || null,
        note: body?.note || null,
      },
    });
  }

  async updateFactory(id: string, body: any) {
    return this.prisma.productionPartner.update({
      where: { id },
      data: {
        ...(body?.code !== undefined ? { code: String(body.code || "").trim().toUpperCase() } : {}),
        ...(body?.name !== undefined ? { name: String(body.name || "").trim() } : {}),
        ...(body?.contactName !== undefined ? { contactName: body.contactName || null } : {}),
        ...(body?.phone !== undefined ? { phone: body.phone || null } : {}),
        ...(body?.email !== undefined ? { email: body.email || null } : {}),
        ...(body?.address !== undefined ? { address: body.address || null } : {}),
        ...(body?.note !== undefined ? { note: body.note || null } : {}),
      },
    });
  }

  deactivateFactory(id: string) {
    return this.prisma.productionPartner.update({ where: { id }, data: { isActive: false } });
  }

  async listAccessorySuppliers(user?: any) {
    const rows = await this.prisma.productionAccessorySupplier.findMany({ where: { isActive: true }, orderBy: { name: "asc" } });
    return rows.map((row: any) => this.accessorySupplierForUser(row, user));
  }

  async createAccessorySupplier(body: any) {
    const name = String(body?.name || "").trim();
    if (!name) throw new BadRequestException("Thiếu tên NCC NPL.");
    return this.prisma.productionAccessorySupplier.create({
      data: {
        code: String(body?.code || "").trim().toUpperCase() || (await this.nextSimpleCode("supplier", name)),
        name,
        phone: body?.phone || null,
        email: body?.email || null,
        address: body?.address || null,
        note: body?.note || null,
      },
    });
  }

  async updateAccessorySupplier(id: string, body: any) {
    return this.prisma.productionAccessorySupplier.update({
      where: { id },
      data: {
        ...(body?.code !== undefined ? { code: String(body.code || "").trim().toUpperCase() } : {}),
        ...(body?.name !== undefined ? { name: String(body.name || "").trim() } : {}),
        ...(body?.phone !== undefined ? { phone: body.phone || null } : {}),
        ...(body?.email !== undefined ? { email: body.email || null } : {}),
        ...(body?.address !== undefined ? { address: body.address || null } : {}),
        ...(body?.note !== undefined ? { note: body.note || null } : {}),
      },
    });
  }

  deactivateAccessorySupplier(id: string) {
    return this.prisma.productionAccessorySupplier.update({ where: { id }, data: { isActive: false } });
  }

  async listAccessoryTemplates() {
    return this.prisma.productionAccessoryTemplate.findMany({
      where: { isActive: true },
      include: { items: { orderBy: { sortOrder: "asc" } } },
      orderBy: [{ updatedAt: "desc" }, { name: "asc" }],
    });
  }

  async createAccessoryTemplate(body: any, user?: any) {
    const name = String(body?.name || "").trim();
    if (!name) throw new BadRequestException("Thiếu tên mẫu NPL.");
    const duplicate = await this.prisma.productionAccessoryTemplate.findUnique({ where: { name }, select: { id: true } });
    if (duplicate) throw new BadRequestException("Tên mẫu NPL đã tồn tại. Hãy đặt tên khác.");
    const rows = Array.isArray(body?.items) ? body.items : [];
    if (!rows.length) throw new BadRequestException("Mẫu NPL chưa có phụ kiện.");
    const actor = this.actor(user);
    return this.prisma.productionAccessoryTemplate.create({
      data: {
        name,
        productKind: body?.productKind || "OTHER",
        sourceType: String(body?.sourceType || "MANUAL").trim().toUpperCase(),
        sourceFileName: String(body?.sourceFileName || "").trim() || null,
        createdById: actor.id,
        createdByName: actor.name,
        items: {
          create: rows.map((x: any, index: number) => ({
            accessoryItemId: String(x?.accessoryItemId || "").trim() || null,
            accessoryCodeSnapshot: String(x?.accessoryCodeSnapshot || "").trim() || null,
            accessoryNameSnapshot: String(x?.accessoryNameSnapshot || "").trim() || null,
            qtyPerProduct: Number(this.n(x?.qtyPerProduct) || 0),
            wastePercent: Number(this.n(x?.wastePercent) || 0),
            sizeScoped: x?.sizeScoped === true,
            note: String(x?.note || "").trim() || null,
            sortOrder: index,
          })),
        },
      },
      include: { items: { orderBy: { sortOrder: "asc" } } },
    });
  }

  async updateAccessoryTemplate(id: string, body: any) {
    const current = await this.prisma.productionAccessoryTemplate.findUnique({ where: { id }, select: { id: true } });
    if (!current) throw new NotFoundException("Không tìm thấy mẫu NPL.");
    const name = body?.name !== undefined ? String(body.name || "").trim() : undefined;
    if (name === "") throw new BadRequestException("Tên mẫu NPL không được để trống.");
    if (name) {
      const duplicate = await this.prisma.productionAccessoryTemplate.findFirst({ where: { name, id: { not: id } }, select: { id: true } });
      if (duplicate) throw new BadRequestException("Tên mẫu NPL đã tồn tại. Hãy đặt tên khác.");
    }
    return this.prisma.$transaction(async (tx: any) => {
      await tx.productionAccessoryTemplate.update({
        where: { id },
        data: {
          ...(name !== undefined ? { name } : {}),
          ...(body?.productKind !== undefined ? { productKind: body.productKind || "OTHER" } : {}),
          ...(body?.sourceType !== undefined ? { sourceType: String(body.sourceType || "MANUAL").trim().toUpperCase() } : {}),
          ...(body?.sourceFileName !== undefined ? { sourceFileName: String(body.sourceFileName || "").trim() || null } : {}),
        },
      });
      if (Array.isArray(body?.items)) {
        await tx.productionAccessoryTemplateItem.deleteMany({ where: { templateId: id } });
        if (body.items.length) await tx.productionAccessoryTemplateItem.createMany({
          data: body.items.map((x: any, index: number) => ({
            templateId: id,
            accessoryItemId: String(x?.accessoryItemId || "").trim() || null,
            accessoryCodeSnapshot: String(x?.accessoryCodeSnapshot || "").trim() || null,
            accessoryNameSnapshot: String(x?.accessoryNameSnapshot || "").trim() || null,
            qtyPerProduct: Number(this.n(x?.qtyPerProduct) || 0),
            wastePercent: Number(this.n(x?.wastePercent) || 0),
            sizeScoped: x?.sizeScoped === true,
            note: String(x?.note || "").trim() || null,
            sortOrder: index,
          })),
        });
      }
      return tx.productionAccessoryTemplate.findUnique({ where: { id }, include: { items: { orderBy: { sortOrder: "asc" } } } });
    });
  }

  async deleteAccessoryTemplate(id: string) {
    const row = await this.prisma.productionAccessoryTemplate.findUnique({ where: { id }, select: { id: true } });
    if (!row) throw new NotFoundException("Không tìm thấy mẫu NPL.");
    return this.prisma.productionAccessoryTemplate.update({ where: { id }, data: { isActive: false } });
  }

  async listAccessories(query?: any, user?: any) {
    const q = String(query?.q || "").trim();
    const rows = await this.prisma.productionAccessoryItem.findMany({
      where: {
        isActive: true,
        ...(query?.type ? { typeName: query.type } : {}),
        ...(q
          ? {
              OR: [
                { code: { contains: q, mode: "insensitive" } },
                { name: { contains: q, mode: "insensitive" } },
                { typeName: { contains: q, mode: "insensitive" } },
              ],
            }
          : {}),
      },
      orderBy: [{ typeName: "asc" }, { name: "asc" }],
    });
    return rows.map((row: any) => this.accessoryForUser(row, user));
  }

  async createAccessory(body: any, user?: any) {
    const name = String(body?.name || "").trim();
    const typeName = String(body?.typeName || "").trim();
    if (!name || !typeName) throw new BadRequestException("Thiếu tên hoặc loại NPL.");
    const specifications = this.normalizeAccessorySpecifications(typeName, body?.specifications);
    const created = await this.prisma.productionAccessoryItem.create({
      data: {
        code: String(body?.code || "").trim().toUpperCase() || (await this.nextSimpleCode("item", name)),
        name,
        typeName,
        imageUrl: body?.imageUrl || null,
        unit: body?.unit || "PIECE",
        stockQty: this.n(body?.stockQty) || 0,
        unitPrice: this.n(body?.unitPrice),
        supplierId: body?.supplierId || null,
        specifications,
        note: body?.note || null,
      },
    });
    return this.accessoryForUser(created, user);
  }

  async updateAccessory(id: string, body: any, user?: any) {
    const current = await this.prisma.productionAccessoryItem.findUnique({ where: { id } });
    if (!current) throw new NotFoundException("Không tìm thấy NPL.");
    const nextTypeName = body?.typeName !== undefined ? String(body.typeName || "").trim() : String(current.typeName || "").trim();
    const shouldNormalizeSpecs = body?.typeName !== undefined || body?.specifications !== undefined;
    const normalizedSpecifications = shouldNormalizeSpecs
      ? this.normalizeAccessorySpecifications(
          nextTypeName,
          body?.specifications !== undefined ? body.specifications : current.specifications,
        )
      : undefined;
    const updated = await this.prisma.productionAccessoryItem.update({
      where: { id },
      data: {
        ...(body?.code !== undefined ? { code: String(body.code || "").trim().toUpperCase() } : {}),
        ...(body?.name !== undefined ? { name: String(body.name || "").trim() } : {}),
        ...(body?.typeName !== undefined ? { typeName: nextTypeName } : {}),
        ...(body?.imageUrl !== undefined ? { imageUrl: body.imageUrl || null } : {}),
        ...(body?.unit !== undefined ? { unit: body.unit } : {}),
        ...(body?.stockQty !== undefined ? { stockQty: this.n(body.stockQty) || 0 } : {}),
        ...(body?.unitPrice !== undefined ? { unitPrice: this.n(body.unitPrice) } : {}),
        ...(body?.supplierId !== undefined ? { supplierId: body.supplierId || null } : {}),
        ...(shouldNormalizeSpecs ? { specifications: normalizedSpecifications } : {}),
        ...(body?.note !== undefined ? { note: body.note || null } : {}),
      },
    });
    return this.accessoryForUser(updated, user);
  }

  async adjustAccessoryStock(id: string, body: any, user?: any) {
    const item = await this.prisma.productionAccessoryItem.findUnique({ where: { id } });
    if (!item) throw new NotFoundException("Không tìm thấy NPL.");
    const qty = Number(this.n(body?.qty) || 0);
    const current = Number(item.stockQty || 0);
    const mode = String(body?.mode || "ADD").toUpperCase();
    const next = mode === "SET" ? qty : mode === "SUBTRACT" ? current - qty : current + qty;
    if (next < 0) throw new BadRequestException("Tồn NPL không thể âm.");
    const updated = await this.prisma.productionAccessoryItem.update({ where: { id }, data: { stockQty: next } });
    return this.accessoryForUser(updated, user);
  }

  async getSampleSpec(designSampleId: string) {
    const [spec, materials] = await Promise.all([
      this.prisma.sampleProductionSpec.findUnique({ where: { designSampleId } }),
      this.prisma.sampleAccessorySpec.findMany({ where: { designSampleId }, orderBy: { createdAt: "asc" } }),
    ]);
    return { spec, materials };
  }

  async saveSampleSpec(designSampleId: string, body: any) {
    if (!(await this.prisma.designSample.findUnique({ where: { id: designSampleId }, select: { id: true } }))) {
      throw new NotFoundException("Không tìm thấy mẫu.");
    }
    await this.prisma.sampleProductionSpec.upsert({
      where: { designSampleId },
      create: {
        designSampleId,
        productKind: body?.productKind || "OTHER",
        fabricWidthCm: this.n(body?.fabricWidthCm),
        fabricConsumptionM: this.n(body?.fabricConsumptionM),
        fabricWastePercent: this.n(body?.fabricWastePercent) || 0,
        sizeSet: body?.sizeSet || null,
        defaultSizeRatio: body?.defaultSizeRatio || null,
        note: body?.note || null,
      },
      update: {
        productKind: body?.productKind || "OTHER",
        fabricWidthCm: this.n(body?.fabricWidthCm),
        fabricConsumptionM: this.n(body?.fabricConsumptionM),
        fabricWastePercent: this.n(body?.fabricWastePercent) || 0,
        sizeSet: body?.sizeSet || null,
        defaultSizeRatio: body?.defaultSizeRatio || null,
        note: body?.note || null,
      },
    });
    if (Array.isArray(body?.materials)) {
      await this.prisma.$transaction(async (tx: any) => {
        await tx.sampleAccessorySpec.deleteMany({ where: { designSampleId } });
        if (body.materials.length) {
          await tx.sampleAccessorySpec.createMany({
            data: body.materials
              .filter((x: any) => x.accessoryItemId)
              .map((x: any) => ({
                designSampleId,
                accessoryItemId: x.accessoryItemId,
                qtyPerProduct: Number(this.n(x.qtyPerProduct) || 0),
                wastePercent: Number(this.n(x.wastePercent) || 0),
                sizeScoped: x.sizeScoped === true,
                note: x.note || null,
              })),
          });
        }
      });
    }
    return this.getSampleSpec(designSampleId);
  }

  async listOrders(query?: any) {
    const q = String(query?.q || "").trim();
    const rows = await this.prisma.productionOrder.findMany({
      where: {
        ...(query?.status ? { status: query.status } : {}),
        ...(q
          ? {
              OR: [
                { code: { contains: q, mode: "insensitive" } },
                { sourceCode: { contains: q, mode: "insensitive" } },
                { sourceName: { contains: q, mode: "insensitive" } },
              ],
            }
          : {}),
      },
      orderBy: { updatedAt: "desc" },
    });
    const factoryIds = [...new Set(rows.map((x: any) => x.productionPartnerId))];
    const sampleIds = [...new Set(rows.map((x: any) => x.designSampleId).filter(Boolean))];
    const productIds = [...new Set(rows.map((x: any) => x.productId).filter(Boolean))];
    const [factories, samples, products] = await Promise.all([
      factoryIds.length ? this.prisma.productionPartner.findMany({ where: { id: { in: factoryIds } }, select: { id: true, code: true, name: true } }) : [],
      sampleIds.length ? this.prisma.designSample.findMany({ where: { id: { in: sampleIds } }, select: { id: true, code: true, name: true, coverImageUrl: true } }) : [],
      productIds.length ? this.prisma.product.findMany({ where: { id: { in: productIds } }, select: { id: true, name: true, slug: true, imageUrl: true, variants: { select: { sku: true }, take: 10 } } }) : [],
    ]);
    return rows.map((r: any) => {
      const sample = r.designSampleId ? samples.find((x: any) => x.id === r.designSampleId) : null;
      const product = r.productId ? products.find((x: any) => x.id === r.productId) : null;
      const code = r.sourceCode || sample?.code || (product ? this.productCode(product) : "");
      const name = r.sourceName || sample?.name || product?.name || null;
      const imageUrl = r.sourceImageUrl || sample?.coverImageUrl || product?.imageUrl || null;
      return {
        ...r, sourceCode: code, sourceName: name, sourceImageUrl: imageUrl,
        sample: sample ? { ...sample, code, name } : null,
        source: { type: r.sourceType, id: r.designSampleId || r.productId, code, name, imageUrl },
        factory: factories.find((x: any) => x.id === r.productionPartnerId) || null,
      };
    });
  }

  async getOrder(id: string) {
    const order = await this.prisma.productionOrder.findUnique({ where: { id } });
    if (!order) throw new NotFoundException("Không tìm thấy lệnh SX.");
    const [factory, rolls, sizes, materials, accessorySpecs, cutHistory] = await Promise.all([
      this.prisma.productionPartner.findUnique({ where: { id: order.productionPartnerId } }),
      this.prisma.productionOrderRoll.findMany({ where: { productionOrderId: id }, orderBy: { createdAt: "asc" } }),
      this.prisma.productionSizePlan.findMany({
        where: { productionOrderId: id },
        orderBy: [{ colorName: "asc" }, { size: "asc" }],
      }),
      this.prisma.productionMaterialCalc.findMany({
        where: { productionOrderId: id },
        orderBy: [{ accessoryName: "asc" }, { sizeLabel: "asc" }],
      }),
      this.prisma.productionOrderAccessorySpec.findMany({
        where: { productionOrderId: id },
        orderBy: { createdAt: "asc" },
      }),
      this.prisma.productionCutQtyHistory.findMany({
        where: { productionOrderId: id },
        orderBy: { createdAt: "desc" },
        take: 200,
      }),
    ]);
    const legacySample = order.designSampleId ? await this.prisma.designSample.findUnique({ where: { id: order.designSampleId }, select: { id: true, code: true, name: true, coverImageUrl: true } }) : null;
    const legacyProduct = order.productId ? await this.prisma.product.findUnique({ where: { id: order.productId }, select: { id: true, name: true, slug: true, imageUrl: true, variants: { select: { sku: true }, take: 10 } } }) : null;
    const sourceCode = order.sourceCode || legacySample?.code || (legacyProduct ? this.productCode(legacyProduct) : "");
    const sourceName = order.sourceName || legacySample?.name || legacyProduct?.name || null;
    const sourceImageUrl = order.sourceImageUrl || legacySample?.coverImageUrl || legacyProduct?.imageUrl || null;
    return {
      ...order, sourceCode, sourceName, sourceImageUrl,
      source: { type: order.sourceType, id: order.designSampleId || order.productId, code: sourceCode, name: sourceName, imageUrl: sourceImageUrl },
      sample: legacySample ? { ...legacySample, code: sourceCode, name: sourceName, coverImageUrl: sourceImageUrl } : null,
      factory, rolls, sizes, materials, accessorySpecs, cutHistory,
    };
  }

  async createOrder(body: any, user?: Actor) {
    const sourceType = String(body?.sourceType || "SAMPLE").toUpperCase() as SourceType;
    const sourceId = String(body?.sourceId || body?.designSampleId || body?.productId || "").trim();
    const productionPartnerId = String(body?.productionPartnerId || "").trim();
    if (!sourceId) throw new BadRequestException("Chưa chọn mã sản xuất.");
    if (!productionPartnerId) throw new BadRequestException("Chưa chọn nhà may.");

    const factory = await this.prisma.productionPartner.findUnique({ where: { id: productionPartnerId } });
    if (!factory) throw new NotFoundException("Không tìm thấy nhà may.");

    let sample: any = null;
    let product: any = null;
    let sampleSpec: any = null;
    let sourceCode = "";
    let sourceName = "";
    let sourceImageUrl: string | null = null;
    let designSampleId: string | null = null;
    let productId: string | null = null;

    if (sourceType === "PRODUCT") {
      product = await this.prisma.product.findUnique({
        where: { id: sourceId },
        select: { id: true, name: true, slug: true, imageUrl: true, variants: { select: { sku: true }, take: 20 } },
      });
      if (!product) throw new NotFoundException("Không tìm thấy sản phẩm cũ.");
      sourceCode = this.productCode(product);
      sourceName = product.name;
      sourceImageUrl = product.imageUrl || null;
      productId = product.id;
    } else {
      sample = await this.prisma.designSample.findUnique({ where: { id: sourceId } });
      if (!sample) throw new NotFoundException("Không tìm thấy mẫu triển khai.");
      sampleSpec = await this.prisma.sampleProductionSpec.findUnique({ where: { designSampleId: sourceId } });
      sourceCode = sample.code;
      sourceName = sample.name;
      sourceImageUrl = sample.coverImageUrl || null;
      designSampleId = sample.id;
    }

    const actor = this.actor(user);
    const order = await this.prisma.productionOrder.create({
      data: {
        code: String(body?.code || "").trim().toUpperCase() || (await this.nextOrderCode()),
        sourceType,
        designSampleId,
        productId,
        sourceCode,
        sourceName,
        sourceImageUrl,
        productionPartnerId,
        status: body?.status || "DRAFT",
        plannedStartAt: body?.plannedStartAt ? new Date(body.plannedStartAt) : null,
        dueDate: body?.dueDate ? new Date(body.dueDate) : null,
        productKind: body?.productKind || sampleSpec?.productKind || "OTHER",
        fabricWidthCm: this.n(body?.fabricWidthCm) ?? sampleSpec?.fabricWidthCm ?? null,
        fabricConsumptionM: this.n(body?.fabricConsumptionM) ?? sampleSpec?.fabricConsumptionM ?? null,
        fabricWastePercent: this.n(body?.fabricWastePercent) ?? sampleSpec?.fabricWastePercent ?? 0,
        sizeSet: body?.sizeSet || sampleSpec?.sizeSet || null,
        sizeRatio: body?.sizeRatio || sampleSpec?.defaultSizeRatio || null,
        plannedQtyOverride: body?.plannedQtyOverride ? Number(body.plannedQtyOverride) : null,
        note: body?.note || null,
        createdById: actor.id,
        createdByName: actor.name,
      },
    });

    if (designSampleId) {
      const specs = await this.prisma.sampleAccessorySpec.findMany({ where: { designSampleId } });
      if (specs.length) {
        await this.prisma.productionOrderAccessorySpec.createMany({
          data: specs.map((x: any) => ({
            productionOrderId: order.id,
            accessoryItemId: x.accessoryItemId,
            qtyPerProduct: x.qtyPerProduct,
            wastePercent: x.wastePercent,
            sizeScoped: x.sizeScoped,
            note: x.note,
          })),
        });
      }
    }

    return this.getOrder(order.id);
  }

  async updateOrder(id: string, body: any) {
    return this.prisma.productionOrder.update({
      where: { id },
      data: {
        ...(body?.productionPartnerId !== undefined ? { productionPartnerId: body.productionPartnerId } : {}),
        ...(body?.status !== undefined ? { status: body.status } : {}),
        ...(body?.plannedStartAt !== undefined
          ? { plannedStartAt: body.plannedStartAt ? new Date(body.plannedStartAt) : null }
          : {}),
        ...(body?.dueDate !== undefined ? { dueDate: body.dueDate ? new Date(body.dueDate) : null } : {}),
        ...(body?.productKind !== undefined ? { productKind: body.productKind } : {}),
        ...(body?.fabricWidthCm !== undefined ? { fabricWidthCm: this.n(body.fabricWidthCm) } : {}),
        ...(body?.fabricConsumptionM !== undefined ? { fabricConsumptionM: this.n(body.fabricConsumptionM) } : {}),
        ...(body?.fabricWastePercent !== undefined
          ? { fabricWastePercent: this.n(body.fabricWastePercent) || 0 }
          : {}),
        ...(body?.sizeSet !== undefined ? { sizeSet: body.sizeSet || null } : {}),
        ...(body?.sizeRatio !== undefined ? { sizeRatio: body.sizeRatio || null } : {}),
        ...(body?.plannedQtyOverride !== undefined
          ? { plannedQtyOverride: body.plannedQtyOverride ? Number(body.plannedQtyOverride) : null }
          : {}),
        ...(body?.note !== undefined ? { note: body.note || null } : {}),
      },
    });
  }

  async saveOrderSpec(id: string, body: any) {
    if (!(await this.prisma.productionOrder.findUnique({ where: { id }, select: { id: true } }))) {
      throw new NotFoundException("Không tìm thấy lệnh SX.");
    }
    await this.prisma.$transaction(async (tx: any) => {
      await tx.productionOrder.update({
        where: { id },
        data: {
          ...(body?.productKind !== undefined ? { productKind: body.productKind } : {}),
          ...(body?.fabricWidthCm !== undefined ? { fabricWidthCm: this.n(body.fabricWidthCm) } : {}),
          ...(body?.fabricConsumptionM !== undefined ? { fabricConsumptionM: this.n(body.fabricConsumptionM) } : {}),
          ...(body?.fabricWastePercent !== undefined
            ? { fabricWastePercent: this.n(body.fabricWastePercent) || 0 }
            : {}),
          ...(body?.sizeSet !== undefined ? { sizeSet: body.sizeSet || null } : {}),
          ...(body?.sizeRatio !== undefined ? { sizeRatio: body.sizeRatio || null } : {}),
        },
      });
      if (Array.isArray(body?.materials)) {
        await tx.productionOrderAccessorySpec.deleteMany({ where: { productionOrderId: id } });
        const rows = body.materials.filter((x: any) => x?.accessoryItemId);
        if (rows.length) {
          await tx.productionOrderAccessorySpec.createMany({
            data: rows.map((x: any) => ({
              productionOrderId: id,
              accessoryItemId: x.accessoryItemId,
              qtyPerProduct: Number(this.n(x.qtyPerProduct) || 0),
              wastePercent: Number(this.n(x.wastePercent) || 0),
              sizeScoped: x.sizeScoped === true,
              note: x.note || null,
            })),
          });
        }
      }
    });
    return this.getOrder(id);
  }

  async setOrderRolls(id: string, body: any) {
    if (!(await this.prisma.productionOrder.findUnique({ where: { id }, select: { id: true } }))) {
      throw new NotFoundException("Không tìm thấy lệnh SX.");
    }
    const rows = Array.isArray(body?.rolls) ? body.rolls : [];
    const ids = rows.map((x: any) => x.fabricReceiptRollId).filter(Boolean);
    const src = ids.length
      ? await this.prisma.fabricReceiptRoll.findMany({
          where: { id: { in: ids } },
          include: { fabricReceipt: true, images: { orderBy: { createdAt: "desc" }, take: 1 } },
        })
      : [];

    const allocatedElsewhere = ids.length
      ? await this.prisma.productionOrderRoll.groupBy({
          by: ["fabricReceiptRollId"],
          where: { fabricReceiptRollId: { in: ids }, productionOrderId: { not: id } },
          _sum: { allocatedM: true, allocatedKg: true },
        })
      : [];
    const usedMap = new Map(allocatedElsewhere.map((x: any) => [x.fabricReceiptRollId, x._sum]));

    await this.prisma.$transaction(async (tx: any) => {
      await tx.productionOrderRoll.deleteMany({ where: { productionOrderId: id } });
      if (rows.length) {
        await tx.productionOrderRoll.createMany({
          data: rows.map((x: any) => {
            const r: any = src.find((y: any) => y.id === x.fabricReceiptRollId);
            if (!r) throw new BadRequestException("Cây vải không tồn tại.");
            const used: any = usedMap.get(r.id) || {};
            const availableM = Math.max(0, Number(r.actualM || 0) - Number(used.allocatedM || 0));
            const availableKg = Math.max(0, Number(r.actualKg || 0) - Number(used.allocatedKg || 0));
            const allocatedM = Number(this.n(x.allocatedM) ?? availableM);
            const allocatedKg = Number(this.n(x.allocatedKg) ?? Math.min(Number(r.actualKg || 0), availableKg));
            if (allocatedM > availableM + 0.0001) {
              throw new BadRequestException(`Cây ${r.rollCode || r.id} chỉ còn ${availableM}m.`);
            }
            if (allocatedM <= 0) throw new BadRequestException(`Cây ${r.rollCode || r.id} đã xuất hết hoặc chưa có mét thực nhận.`);
            return {
              productionOrderId: id,
              fabricReceiptRollId: r.id,
              fabricReceiptId: r.fabricReceiptId,
              rollCode: r.rollCode,
              colorName: r.colorName || r.fabricReceipt?.colorName || null,
              colorCode: this.normalizeColorCode(r.colorCode || r.fabricReceipt?.colorCode),
              availableM,
              availableKg,
              allocatedM,
              allocatedKg,
              imageUrl: r.images?.[0]?.url || null,
            };
          }),
        });
      }
    });
    return this.getOrder(id);
  }

  private distribute(total: number, ratio: Record<string, number>) {
    const entries = Object.entries(ratio).filter(([, v]) => Number(v) > 0);
    const sum = entries.reduce((s, [, v]) => s + Number(v), 0);
    if (!sum || total <= 0) return Object.fromEntries(entries.map(([k]) => [k, 0]));
    const exact = entries.map(([k, v]) => ({ k, x: (total * Number(v)) / sum }));
    const result: Record<string, number> = Object.fromEntries(exact.map(({ k, x }) => [k, Math.floor(x)]));
    let remain = total - Object.values(result).reduce((a, b) => a + b, 0);
    exact.sort((a, b) => b.x - Math.floor(b.x) - (a.x - Math.floor(a.x)));
    for (let i = 0; remain > 0; i = (i + 1) % exact.length, remain--) result[exact[i].k] += 1;
    return result;
  }

  private async calculateMaterialsFromSizePlans(id: string, sizeRowsInput?: any[]) {
    const order = await this.prisma.productionOrder.findUnique({ where: { id } });
    if (!order) throw new NotFoundException("Không tìm thấy lệnh SX.");
    const sizeRows = sizeRowsInput || await this.prisma.productionSizePlan.findMany({ where: { productionOrderId: id } });

    // Sau khi có bảng cắt, NPL luôn bám số CẮT THỰC TẾ. Nếu một dòng cũ chưa có actualQty thì tạm dùng plannedQty.
    const totalsBySize: Record<string, number> = {};
    let totalQty = 0;
    for (const row of sizeRows as any[]) {
      const qty = Number(row.actualQty ?? row.plannedQty ?? 0);
      totalQty += qty;
      const size = this.normalizeProductionSize(row.size);
      totalsBySize[size] = (totalsBySize[size] || 0) + qty;
    }

    let specs = await this.prisma.productionOrderAccessorySpec.findMany({ where: { productionOrderId: id } });
    if (!specs.length && order.designSampleId) {
      const sampleSpecs = await this.prisma.sampleAccessorySpec.findMany({ where: { designSampleId: order.designSampleId } });
      specs = sampleSpecs.map((x: any) => ({ ...x, productionOrderId: id }));
    }

    const ids = specs.map((x: any) => x.accessoryItemId);
    const items = ids.length ? await this.prisma.productionAccessoryItem.findMany({ where: { id: { in: ids } } }) : [];
    const materials: any[] = [];

    for (const spec of specs as any[]) {
      const item: any = items.find((x: any) => x.id === spec.accessoryItemId);
      if (!item) continue;
      const per = Number(spec.qtyPerProduct || 0);
      const waste = Number(spec.wastePercent || 0);
      const stock = Number(item.stockQty || 0);
      const taggedSize = this.accessoryTaggedSize(item);
      const makeRow = (baseQty: number, sizeLabel: string | null) => {
        const requiredQty = Math.ceil(baseQty * (1 + waste / 100) * 1000) / 1000;
        return {
          accessoryItemId: item.id,
          accessoryCode: item.code,
          accessoryName: item.name,
          unit: item.unit,
          sizeLabel,
          qtyPerProduct: per,
          wastePercent: waste,
          baseQty,
          requiredQty,
          stockQtySnapshot: stock,
          shortageQty: Math.max(0, requiredQty - stock),
        };
      };

      if (String(item.typeName || "").trim() === "Mác Size") {
        if (!taggedSize) throw new BadRequestException(`NPL ${item.code} · ${item.name} là Mác Size nhưng chưa được gán size trong kho NPL.`);
        const sizeQty = this.totalForTaggedSize(totalsBySize, taggedSize);
        // Luôn giữ dòng Mác Size đã chọn trong bảng NPL, kể cả size đó không có trong lệnh hiện tại.
        // Trước đây sizeQty = 0 bị bỏ qua nên nhập Excel/mẫu xong sang bước 5 trông như mất NPL.
        materials.push(makeRow(sizeQty * per, taggedSize));
      } else if (spec.sizeScoped && Object.keys(totalsBySize).length) {
        for (const [size, qty] of Object.entries(totalsBySize)) materials.push(makeRow(Number(qty) * per, size));
      } else {
        materials.push(makeRow(totalQty * per, null));
      }
    }

    await this.prisma.$transaction(async (tx: any) => {
      await tx.productionMaterialCalc.deleteMany({ where: { productionOrderId: id } });
      if (materials.length) await tx.productionMaterialCalc.createMany({ data: materials.map((x) => ({ productionOrderId: id, ...x })) });
    });
    return { totalQty, totalsBySize, materials };
  }

  async calculateOrder(id: string, user?: Actor) {
    const order = await this.prisma.productionOrder.findUnique({ where: { id } });
    if (!order) throw new NotFoundException("Không tìm thấy lệnh SX.");
    const rolls = await this.prisma.productionOrderRoll.findMany({ where: { productionOrderId: id } });
    const consumption = Number(order.fabricConsumptionM || 0);
    if (consumption <= 0) throw new BadRequestException("Chưa nhập định mức vải / sản phẩm.");
    if (!rolls.length) throw new BadRequestException("Chưa chọn cây vải.");

    const effective = consumption * (1 + Number(order.fabricWastePercent || 0) / 100);
    const grouped = new Map<string, any>();
    for (const r of rolls as any[]) {
      const code = this.normalizeColorCode(r.colorCode) || "";
      const key = `${r.colorName || "Không màu"}|||${code}`;
      const row = grouped.get(key) || { colorName: r.colorName || "Không màu", colorCode: code || null, meters: 0 };
      row.meters += Number(r.allocatedM || 0);
      grouped.set(key, row);
    }

    const ratio = (order.sizeRatio && typeof order.sizeRatio === "object" ? order.sizeRatio : {}) as Record<string, number>;
    const colors = [...grouped.values()].map((x: any) => {
      const plannedQty = Math.floor(x.meters / effective);
      return { ...x, plannedQty, sizes: this.distribute(plannedQty, ratio) };
    });

    const existing = await this.prisma.productionSizePlan.findMany({ where: { productionOrderId: id } });
    const existingMap = new Map(existing.map((x: any) => [`${x.colorName}|||${x.colorCode || ""}|||${this.normalizeProductionSize(x.size)}`, x]));
    const actor = this.actor(user);
    const nextRows = colors.flatMap((c: any) => Object.entries(c.sizes).map(([rawSize, qty]) => {
      const size = this.normalizeProductionSize(rawSize);
      const key = `${c.colorName}|||${c.colorCode || ""}|||${size}`;
      const old: any = existingMap.get(key);
      return {
        productionOrderId: id,
        colorName: c.colorName,
        colorCode: c.colorCode || null,
        size,
        ratio: Number(ratio[rawSize] || ratio[size] || 0),
        plannedQty: Number(qty),
        // Lần đầu: TT = DK để nhân viên chỉ sửa phần lệch. Những lần tính lại DK: giữ nguyên TT đã nhập.
        actualQty: old?.actualQty ?? Number(qty),
      };
    }));
    const nextKeys = new Set(nextRows.map((x: any) => `${x.colorName}|||${x.colorCode || ""}|||${x.size}`));

    await this.prisma.$transaction(async (tx: any) => {
      for (const row of nextRows as any[]) {
        const old: any = existingMap.get(`${row.colorName}|||${row.colorCode || ""}|||${row.size}`);
        await tx.productionSizePlan.upsert({
          where: { productionOrderId_colorName_size: { productionOrderId: id, colorName: row.colorName, size: row.size } },
          create: row,
          update: { colorCode: row.colorCode, ratio: row.ratio, plannedQty: row.plannedQty, actualQty: old?.actualQty ?? row.actualQty },
        });
        if (!old || Number(old.plannedQty || 0) !== Number(row.plannedQty || 0)) {
          await tx.productionCutQtyHistory.create({ data: {
            productionOrderId: id, colorName: row.colorName, colorCode: row.colorCode, size: row.size,
            plannedQty: row.plannedQty, previousActualQty: old?.actualQty ?? null, actualQty: old?.actualQty ?? row.actualQty,
            changeType: old ? "PLANNED_RECALCULATE" : "INITIAL_CALCULATE", createdById: actor.id, createdByName: actor.name,
          }});
        }
      }
      const stale = existing.filter((x: any) => !nextKeys.has(`${x.colorName}|||${x.colorCode || ""}|||${this.normalizeProductionSize(x.size)}`));
      if (stale.length) await tx.productionSizePlan.deleteMany({ where: { id: { in: stale.map((x: any) => x.id) } } });
      if (order.status === "DRAFT") await tx.productionOrder.update({ where: { id }, data: { status: "PLANNING" } });
    });

    const sizeRows = await this.prisma.productionSizePlan.findMany({ where: { productionOrderId: id }, orderBy: [{ colorName: "asc" }, { size: "asc" }] });
    const npl = await this.calculateMaterialsFromSizePlans(id, sizeRows);
    const totalPlannedQty = sizeRows.reduce((sum: number, x: any) => sum + Number(x.plannedQty || 0), 0);
    const totalActualQty = sizeRows.reduce((sum: number, x: any) => sum + Number(x.actualQty ?? x.plannedQty ?? 0), 0);
    return { totalQty: totalPlannedQty, totalPlannedQty, totalActualQty, effectiveConsumptionM: effective, colors: this.groupCutRows(sizeRows), materials: npl.materials };
  }

  private groupCutRows(rows: any[]) {
    const grouped = new Map<string, any>();
    for (const row of rows) {
      const key = `${row.colorName}|||${row.colorCode || ""}`;
      const x = grouped.get(key) || { colorName: row.colorName, colorCode: row.colorCode || null, plannedQty: 0, actualQty: 0, sizes: {} };
      const plannedQty = Number(row.plannedQty || 0);
      const actualQty = Number(row.actualQty ?? row.plannedQty ?? 0);
      x.plannedQty += plannedQty;
      x.actualQty += actualQty;
      x.sizes[row.size] = { plannedQty, actualQty };
      grouped.set(key, x);
    }
    return [...grouped.values()];
  }

  async saveActualCutQuantities(id: string, body: any, user?: Actor) {
    const order = await this.prisma.productionOrder.findUnique({ where: { id } });
    if (!order) throw new NotFoundException("Không tìm thấy lệnh SX.");
    const incoming = Array.isArray(body?.rows) ? body.rows : [];
    if (!incoming.length) throw new BadRequestException("Chưa có số lượng cắt thực tế để lưu.");
    const plans = await this.prisma.productionSizePlan.findMany({ where: { productionOrderId: id } });
    if (!plans.length) throw new BadRequestException("Hãy tính sản lượng dự kiến trước.");
    const actor = this.actor(user);

    await this.prisma.$transaction(async (tx: any) => {
      for (const input of incoming) {
        const colorName = String(input?.colorName || "").trim();
        const size = this.normalizeProductionSize(input?.size);
        const qtyRaw = this.n(input?.actualQty);
        if (!colorName || !size || qtyRaw === null || qtyRaw < 0 || !Number.isInteger(qtyRaw)) throw new BadRequestException("Số lượng cắt thực tế phải là số nguyên từ 0 trở lên.");
        const plan: any = plans.find((x: any) => x.colorName === colorName && this.normalizeProductionSize(x.size) === size);
        if (!plan) throw new BadRequestException(`Không tìm thấy kế hoạch cắt ${colorName} · size ${size}.`);
        const nextActual = Number(qtyRaw);
        const prevActual = Number(plan.actualQty ?? plan.plannedQty ?? 0);
        if (nextActual === prevActual) continue;
        await tx.productionSizePlan.update({ where: { id: plan.id }, data: { actualQty: nextActual } });
        await tx.productionCutQtyHistory.create({ data: {
          productionOrderId: id, colorName: plan.colorName, colorCode: plan.colorCode, size: plan.size,
          plannedQty: Number(plan.plannedQty || 0), previousActualQty: prevActual, actualQty: nextActual,
          changeType: "ACTUAL_UPDATE", createdById: actor.id, createdByName: actor.name,
        }});
      }
    });

    const sizeRows = await this.prisma.productionSizePlan.findMany({ where: { productionOrderId: id }, orderBy: [{ colorName: "asc" }, { size: "asc" }] });
    const npl = await this.calculateMaterialsFromSizePlans(id, sizeRows);
    const totalPlannedQty = sizeRows.reduce((sum: number, x: any) => sum + Number(x.plannedQty || 0), 0);
    const totalActualQty = sizeRows.reduce((sum: number, x: any) => sum + Number(x.actualQty ?? x.plannedQty ?? 0), 0);
    const cutHistory = await this.prisma.productionCutQtyHistory.findMany({ where: { productionOrderId: id }, orderBy: { createdAt: "desc" }, take: 200 });
    return { totalQty: totalPlannedQty, totalPlannedQty, totalActualQty, colors: this.groupCutRows(sizeRows), materials: npl.materials, cutHistory };
  }

  async sendOrder(id: string) {
    const order = await this.prisma.productionOrder.findUnique({ where: { id } });
    if (!order) throw new NotFoundException("Không tìm thấy lệnh SX.");
    const [rollCount, sizeCount] = await Promise.all([
      this.prisma.productionOrderRoll.count({ where: { productionOrderId: id } }),
      this.prisma.productionSizePlan.count({ where: { productionOrderId: id } }),
    ]);
    if (!rollCount) throw new BadRequestException("Chưa chọn cây vải.");
    if (!sizeCount) throw new BadRequestException("Hãy tính sản lượng trước khi gửi lệnh SX.");
    await this.prisma.productionOrder.update({ where: { id }, data: { status: "SENT", sentAt: new Date() } });
    if (order.designSampleId) {
      await this.prisma.designSample
        .update({
          where: { id: order.designSampleId },
          data: { status: "IN_PRODUCTION", nextAction: "Đã gửi lệnh sản xuất" },
        })
        .catch(() => null);
    }
    return this.getOrder(id);
  }

  async printPayload(id: string) {
    return {
      ...(await this.getOrder(id)),
      generatedAt: new Date().toISOString(),
      confirmation: { the1970: "The 1970 xác nhận", factory: "Nhà may / xưởng xác nhận" },
    };
  }
}
