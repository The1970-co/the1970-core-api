import { BadRequestException, ForbiddenException, Injectable, NotFoundException } from "@nestjs/common";
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

  private isAdminUser(user: any) {
    const roles = [user?.role, ...(Array.isArray(user?.roles) ? user.roles : [])]
      .map((x) => String(x || "").trim().toLowerCase())
      .filter(Boolean);
    return roles.includes("owner") || roles.includes("admin");
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

  private fixedAccessorySize(note?: any) {
    const matched = String(note || "").match(/\[\[FIXED_SIZE:([^\]]+)\]\]/i);
    return matched?.[1] ? this.normalizeProductionSize(matched[1]) : null;
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
    const canViewSamples = this.userHas(user, "production.source.sample.view");
    const canUseNplStep = this.userHas(user, "production.step2");
    const canUseFabricStep = this.userHas(user, "production.step3");
    const [samples, products, factories, accessories, rolls] = await Promise.all([
      canViewSamples
        ? this.prisma.designSample.findMany({
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
          })
        : Promise.resolve([]),
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
      canUseNplStep
        ? this.prisma.productionAccessoryItem.findMany({
            where: { isActive: true },
            orderBy: [{ typeName: "asc" }, { name: "asc" }],
          })
        : Promise.resolve([]),
      canUseFabricStep ? this.availableFabricRolls() : Promise.resolve([]),
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

    const activeAllocationOrders = await this.prisma.productionOrder.findMany({
      where: { status: { not: "CANCELLED" } },
      select: { id: true },
    });
    const activeAllocationOrderIds: string[] = activeAllocationOrders.map((x: any) => String(x.id));
    const allocated = activeAllocationOrderIds.length
      ? await this.prisma.productionOrderRoll.groupBy({
          by: ["fabricReceiptRollId"],
          where: { productionOrderId: { in: activeAllocationOrderIds } },
          _sum: { allocatedM: true, allocatedKg: true },
        })
      : [];
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


  async hardDeleteAccessory(id: string) {
    const current = await this.prisma.productionAccessoryItem.findUnique({ where: { id } });
    if (!current) throw new NotFoundException("Không tìm thấy NPL.");

    // Xoá cứng mã NPL khỏi kho. Các bảng lịch sử đã có snapshot mã/tên
    // (phiếu nhập NPL, kết quả tính NPL) được giữ lại để không mất chứng từ cũ.
    // Các cấu hình đang tham chiếu trực tiếp NPL phải được gỡ để không còn id mồ côi.
    return this.prisma.$transaction(async (tx: any) => {
      const removed = {
        sampleSpecs: await tx.sampleAccessorySpec.deleteMany({ where: { accessoryItemId: id } }),
        orderSpecs: await tx.productionOrderAccessorySpec.deleteMany({ where: { accessoryItemId: id } }),
        templateItems: await tx.productionAccessoryTemplateItem.deleteMany({ where: { accessoryItemId: id } }),
      };

      await tx.productionAccessoryItem.delete({ where: { id } });

      return {
        success: true,
        id,
        code: current.code,
        name: current.name,
        removedReferences: {
          sampleSpecs: removed.sampleSpecs.count,
          orderSpecs: removed.orderSpecs.count,
          templateItems: removed.templateItems.count,
        },
      };
    });
  }


  private async nextAccessoryReceiptCode() { const d=new Date(); const suffix=`${String(d.getDate()).padStart(2,"0")}${String(d.getMonth()+1).padStart(2,"0")}${d.getFullYear()}`; const rows=await this.prisma.productionAccessoryReceipt.findMany({where:{code:{endsWith:suffix}},select:{code:true}}); const max=rows.reduce((m:number,r:any)=>Math.max(m,Number(String(r.code||"").match(/^PN-NPL-(\d+)-/)?.[1]||0)),0); return `PN-NPL-${String(max+1).padStart(3,"0")}-${suffix}`; }
  async listAccessoryReceipts(){return this.prisma.productionAccessoryReceipt.findMany({include:{items:{orderBy:{sortOrder:"asc"}}},orderBy:{receivedAt:"desc"},take:200});}
  async getAccessoryReceipt(id:string){const row=await this.prisma.productionAccessoryReceipt.findUnique({where:{id},include:{items:{orderBy:{sortOrder:"asc"}}}});if(!row)throw new NotFoundException("Không tìm thấy phiếu nhập NPL.");return row;}
  async createAccessoryReceipt(body:any,user?:any){
    const rows=Array.isArray(body?.items)?body.items.filter((x:any)=>x?.accessoryItemId&&Number(this.n(x?.qty)||0)>0):[];
    if(!rows.length)throw new BadRequestException("Phiếu nhập NPL chưa có mặt hàng.");
    const ids: string[] = Array.from(new Set<string>(rows.map((x: any) => String(x.accessoryItemId))));
    const items:any[]=await this.prisma.productionAccessoryItem.findMany({where:{id:{in:ids},isActive:true}});
    if(items.length!==ids.length)throw new BadRequestException("Có NPL không tồn tại hoặc đã ngừng sử dụng.");
    const actor=this.actor(user);
    const code=String(body?.code||"").trim().toUpperCase()||await this.nextAccessoryReceiptCode();
    return this.prisma.productionAccessoryReceipt.create({
      data:{
        code,supplierId:body?.supplierId||null,receivedAt:body?.receivedAt?new Date(body.receivedAt):new Date(),
        receivedById:body?.receivedById||actor.id,receivedByName:String(body?.receivedByName||actor.name||"").trim()||null,
        note:body?.note||null,status:"DRAFT",createdById:actor.id,createdByName:actor.name,
        items:{create:rows.map((x:any,i:number)=>{const item=items.find((y:any)=>y.id===x.accessoryItemId);return{accessoryItemId:item.id,accessoryCodeSnapshot:item.code,accessoryNameSnapshot:item.name,unit:item.unit,qty:Number(this.n(x.qty)||0),unitPrice:this.n(x.unitPrice),note:x.note||null,sortOrder:i+1}})}
      },
      include:{items:{orderBy:{sortOrder:"asc"}}}
    });
  }

  async postAccessoryReceipt(id:string,user?:any){
    const actor=this.actor(user);
    return this.prisma.$transaction(async(tx:any)=>{
      const receipt=await tx.productionAccessoryReceipt.findUnique({where:{id},include:{items:{orderBy:{sortOrder:"asc"}}}});
      if(!receipt)throw new NotFoundException("Không tìm thấy phiếu nhập NPL.");
      if(String(receipt.status||"DRAFT")==="POSTED")return receipt;
      if(!receipt.items.length)throw new BadRequestException("Phiếu nhập NPL chưa có mặt hàng.");
      for(const row of receipt.items){
        await tx.productionAccessoryItem.update({where:{id:row.accessoryItemId},data:{stockQty:{increment:Number(row.qty||0)}}});
      }
      return tx.productionAccessoryReceipt.update({
        where:{id},
        data:{status:"POSTED",postedAt:new Date(),postedById:actor.id,postedByName:actor.name},
        include:{items:{orderBy:{sortOrder:"asc"}}}
      });
    });
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

    const orderIds: string[] = rows.map((x: any) => String(x.id));
    const factoryIds: string[] = Array.from(new Set<string>(rows.map((x: any) => String(x.productionPartnerId)).filter(Boolean)));
    const sampleIds: string[] = Array.from(new Set<string>(rows.map((x: any) => String(x.designSampleId || "")).filter(Boolean)));
    const productIds: string[] = Array.from(new Set<string>(rows.map((x: any) => String(x.productId || "")).filter(Boolean)));

    const [factories, samples, products, rollRows, sizeRows, accessoryRows, materialRows] = await Promise.all([
      factoryIds.length ? this.prisma.productionPartner.findMany({ where: { id: { in: factoryIds } }, select: { id: true, code: true, name: true } }) : [],
      sampleIds.length ? this.prisma.designSample.findMany({ where: { id: { in: sampleIds } }, select: { id: true, code: true, name: true, coverImageUrl: true } }) : [],
      productIds.length ? this.prisma.product.findMany({ where: { id: { in: productIds } }, select: { id: true, name: true, slug: true, imageUrl: true, variants: { select: { sku: true }, take: 10 } } }) : [],
      orderIds.length ? this.prisma.productionOrderRoll.findMany({ where: { productionOrderId: { in: orderIds } }, select: { productionOrderId: true, allocatedM: true } }) : [],
      orderIds.length ? this.prisma.productionSizePlan.findMany({ where: { productionOrderId: { in: orderIds } }, select: { productionOrderId: true, plannedQty: true, actualQty: true, size: true } }) : [],
      orderIds.length ? this.prisma.productionOrderAccessorySpec.findMany({ where: { productionOrderId: { in: orderIds } }, select: { productionOrderId: true } }) : [],
      orderIds.length ? this.prisma.productionMaterialCalc.findMany({ where: { productionOrderId: { in: orderIds } }, select: { productionOrderId: true } }) : [],
    ]);

    const summaryByOrder = new Map<string, any>();
    for (const id of orderIds) {
      summaryByOrder.set(id, {
        rollCount: 0,
        allocatedM: 0,
        sizeRowCount: 0,
        totalPlannedQty: 0,
        totalActualQty: 0,
        accessorySpecCount: 0,
        materialCalcCount: 0,
      });
    }
    for (const row of rollRows as any[]) {
      const s = summaryByOrder.get(row.productionOrderId);
      if (!s) continue;
      s.rollCount += 1;
      s.allocatedM += Number(row.allocatedM || 0);
    }
    for (const row of sizeRows as any[]) {
      const s = summaryByOrder.get(row.productionOrderId);
      if (!s) continue;
      s.sizeRowCount += 1;
      s.totalPlannedQty += Number(row.plannedQty || 0);
      s.totalActualQty += Number(row.actualQty ?? row.plannedQty ?? 0);
    }
    for (const row of accessoryRows as any[]) {
      const s = summaryByOrder.get(row.productionOrderId);
      if (s) s.accessorySpecCount += 1;
    }
    for (const row of materialRows as any[]) {
      const s = summaryByOrder.get(row.productionOrderId);
      if (s) s.materialCalcCount += 1;
    }

    return rows.map((r: any) => {
      const sample = r.designSampleId ? samples.find((x: any) => x.id === r.designSampleId) : null;
      const product = r.productId ? products.find((x: any) => x.id === r.productId) : null;
      const code = r.sourceCode || sample?.code || (product ? this.productCode(product) : "");
      const name = r.sourceName || sample?.name || product?.name || null;
      const imageUrl = r.sourceImageUrl || sample?.coverImageUrl || product?.imageUrl || null;
      const summary = summaryByOrder.get(r.id) || {};
      const sizeSet = Array.isArray(r.sizeSet) ? r.sizeSet : [];
      const sizeRatio = r.sizeRatio && typeof r.sizeRatio === "object" && !Array.isArray(r.sizeRatio) ? r.sizeRatio : {};
      const sizeRatioText = sizeSet
        .map((size: string) => `${size}:${Number((sizeRatio as any)?.[size] || 0)}`)
        .join(" · ");

      return {
        ...r,
        sourceCode: code,
        sourceName: name,
        sourceImageUrl: imageUrl,
        sample: sample ? { ...sample, code, name } : null,
        source: { type: r.sourceType, id: r.designSampleId || r.productId, code, name, imageUrl },
        factory: factories.find((x: any) => x.id === r.productionPartnerId) || null,
        progress: {
          nplDone: Number(summary.accessorySpecCount || 0) > 0,
          fabricDone: Number(summary.rollCount || 0) > 0,
          sizeDone: sizeSet.length > 0 && Object.keys(sizeRatio as any).length > 0,
          calculationDone: Number(summary.sizeRowCount || 0) > 0,
          sent: ["SENT", "CUTTING", "SEWING", "QC", "COMPLETED"].includes(String(r.status || "")),
          rollCount: Number(summary.rollCount || 0),
          allocatedM: Number(summary.allocatedM || 0),
          nplCount: Number(summary.accessorySpecCount || 0),
          materialCalcCount: Number(summary.materialCalcCount || 0),
          totalPlannedQty: Number(summary.totalPlannedQty || 0),
          totalActualQty: Number(summary.totalActualQty || 0),
          sizeRatioText,
        },
      };
    });
  }

  async getOrder(id: string, user?: any) {
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
    const totalPlannedQty = sizes.reduce((sum: number, x: any) => sum + Number(x.plannedQty || 0), 0);
    const totalActualQty = sizes.reduce((sum: number, x: any) => sum + Number(x.actualQty ?? x.plannedQty ?? 0), 0);
    const lining = this.liningSummary(order, rolls, totalPlannedQty, totalActualQty);
    const nplIssueState = await this.nplIssueState(id, materials);
    const costSummary = await this.productionCostSummary(id, totalActualQty, nplIssueState.materials, rolls, user, order.productionExtraCosts, order.productionPriceMultiplier);
    return {
      ...order, sourceCode, sourceName, sourceImageUrl,
      source: { type: order.sourceType, id: order.designSampleId || order.productId, code: sourceCode, name: sourceName, imageUrl: sourceImageUrl },
      sample: legacySample ? { ...legacySample, code: sourceCode, name: sourceName, coverImageUrl: sourceImageUrl } : null,
      factory, rolls, sizes, materials: nplIssueState.materials, accessorySpecs, cutHistory, lining, costSummary, nplIssueHistory: nplIssueState.nplIssueHistory, nplIssueCount: nplIssueState.nplIssueCount, nextNplIssueRound: nplIssueState.nextRoundNo,
    };
  }

  async createOrder(body: any, user?: Actor) {
    const sourceType = String(body?.sourceType || "SAMPLE").toUpperCase() as SourceType;
    const sourceId = String(body?.sourceId || body?.designSampleId || body?.productId || "").trim();
    const productionPartnerId = String(body?.productionPartnerId || "").trim();
    if (!sourceId) throw new BadRequestException("Chưa chọn mã sản xuất.");
    if (!productionPartnerId) throw new BadRequestException("Chưa chọn nhà may.");
    if (sourceType === "SAMPLE" && !this.userHas(user, "production.source.sample.view")) {
      throw new ForbiddenException("Bạn không có quyền xem hoặc tạo lệnh từ Mẫu mới / Triển khai mẫu.");
    }

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
        ...(body?.liningFabricConsumptionM !== undefined ? { liningFabricConsumptionM: this.n(body.liningFabricConsumptionM) } : {}),
        ...(body?.liningFabricWastePercent !== undefined
          ? { liningFabricWastePercent: this.n(body.liningFabricWastePercent) || 0 }
          : {}),
        ...(body?.liningFabricComponents !== undefined ? { liningFabricComponents: body.liningFabricComponents || null } : {}),
        ...(body?.liningFabricAssignments !== undefined ? { liningFabricAssignments: body.liningFabricAssignments || null } : {}),
        ...(body?.sizeSet !== undefined ? { sizeSet: body.sizeSet || null } : {}),
        ...(body?.sizeRatio !== undefined ? { sizeRatio: body.sizeRatio || null } : {}),
        ...(body?.plannedQtyOverride !== undefined
          ? { plannedQtyOverride: body.plannedQtyOverride ? Number(body.plannedQtyOverride) : null }
          : {}),
        ...(body?.note !== undefined ? { note: body.note || null } : {}),
      },
    });
  }

  async saveOrderSpec(id: string, body: any, user?: any) {
    if (!(await this.prisma.productionOrder.findUnique({ where: { id }, select: { id: true } }))) {
      throw new NotFoundException("Không tìm thấy lệnh SX.");
    }

    const touchesNpl = Array.isArray(body?.materials);
    const touchesLiningAssignments = Object.prototype.hasOwnProperty.call(body || {}, "liningFabricAssignments");
    const touchesSizeOrFabric = [
      "fabricWidthCm",
      "fabricConsumptionM",
      "fabricWastePercent",
      "liningFabricConsumptionM",
      "liningFabricWastePercent",
      "liningFabricComponents",
      "sizeSet",
      "sizeRatio",
    ].some((key) => Object.prototype.hasOwnProperty.call(body || {}, key));

    if (touchesNpl && !this.userHas(user, "production.step2")) {
      throw new ForbiddenException("Bạn không có quyền thao tác Bước 2 · Nguyên phụ liệu.");
    }
    if (touchesSizeOrFabric && !this.userHas(user, "production.step4")) {
      throw new ForbiddenException("Bạn không có quyền thao tác Bước 4 · Size, tỷ lệ và định mức vải.");
    }
    if (touchesLiningAssignments && !this.userHas(user, "production.step4")) {
      throw new ForbiddenException("Bạn không có quyền gán cây vải lót ở Bước 4.");
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
          ...(body?.liningFabricConsumptionM !== undefined ? { liningFabricConsumptionM: this.n(body.liningFabricConsumptionM) } : {}),
          ...(body?.liningFabricWastePercent !== undefined
            ? { liningFabricWastePercent: this.n(body.liningFabricWastePercent) || 0 }
            : {}),
          ...(body?.liningFabricComponents !== undefined ? { liningFabricComponents: body.liningFabricComponents || null } : {}),
          ...(body?.liningFabricAssignments !== undefined ? { liningFabricAssignments: body.liningFabricAssignments || null } : {}),
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
              fabricRole: String(x?.fabricRole || "MAIN").toUpperCase() === "LINING" ? "LINING" : "MAIN",
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

      const fixedSize = this.fixedAccessorySize(spec.note);
      if (String(item.typeName || "").trim() === "Mác Size") {
        if (!taggedSize) throw new BadRequestException(`NPL ${item.code} · ${item.name} là Mác Size nhưng chưa được gán size trong kho NPL.`);
        const sizeQty = this.totalForTaggedSize(totalsBySize, taggedSize);
        // Mác Size luôn bám size đã cấu hình ngay trên mã NPL.
        materials.push(makeRow(sizeQty * per, taggedSize));
      } else if (fixedSize) {
        // NPL cố định một size (VD khóa 72cm chỉ dùng cho size L): chỉ tính đúng sản lượng size đó.
        const sizeQty = this.totalForTaggedSize(totalsBySize, fixedSize);
        materials.push(makeRow(sizeQty * per, fixedSize));
      } else if (spec.sizeScoped && Object.keys(totalsBySize).length) {
        // Chế độ "Theo tất cả size": sinh một dòng cho mỗi size.
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

  private nplIssueKey(accessoryItemId: any, sizeLabel: any) {
    return `${String(accessoryItemId || "")}|||${String(sizeLabel || "").trim().toUpperCase()}`;
  }

  private async nplIssueState(id: string, materialsInput?: any[]) {
    const materials = materialsInput || await this.prisma.productionMaterialCalc.findMany({
      where: { productionOrderId: id },
      orderBy: [{ accessoryName: "asc" }, { sizeLabel: "asc" }],
    });

    const [issueRows, issueItems, notes] = await Promise.all([
      this.prisma.productionNplIssue.findMany({
        where: { productionOrderId: id },
        orderBy: { roundNo: "desc" },
      }),
      this.prisma.productionNplIssueItem.findMany({
        where: { productionOrderId: id },
        orderBy: { createdAt: "asc" },
      }),
      this.prisma.productionNplIssueNote.findMany({
        where: { productionOrderId: id },
      }),
    ]);

    const accessoryIds = [...new Set((materials || []).map((x: any) => String(x.accessoryItemId || "")).filter(Boolean))];
    const items = accessoryIds.length ? await this.prisma.productionAccessoryItem.findMany({
      where: { id: { in: accessoryIds } },
      select: { id: true, stockQty: true },
    }) : [];
    const stockById = new Map(items.map((x: any) => [String(x.id), Number(x.stockQty || 0)]));

    const issuedByKey = new Map<string, number>();
    for (const row of issueItems as any[]) {
      const key = this.nplIssueKey(row.accessoryItemId, row.sizeKey || row.sizeLabel);
      issuedByKey.set(key, (issuedByKey.get(key) || 0) + Number(row.issuedQty || 0));
    }
    const noteByKey = new Map((notes as any[]).map((x: any) => [this.nplIssueKey(x.accessoryItemId, x.sizeKey), String(x.note || "")]));

    const augmented = (materials || []).map((m: any) => {
      const key = this.nplIssueKey(m.accessoryItemId, m.sizeLabel);
      const issuedQty = issuedByKey.get(key) || 0;
      const requiredQty = Number(m.requiredQty || 0);
      const remainingToIssue = Math.max(0, requiredQty - issuedQty);
      const stockQtyCurrent = stockById.get(String(m.accessoryItemId)) || 0;
      return {
        ...m,
        issuedQty,
        remainingToIssue,
        stockQtyCurrent,
        shortageQty: Math.max(0, remainingToIssue - stockQtyCurrent),
        issueNote: noteByKey.get(key) || "",
      };
    });

    const itemsByIssue = new Map<string, any[]>();
    for (const item of issueItems as any[]) {
      const arr = itemsByIssue.get(String(item.issueId)) || [];
      arr.push(item);
      itemsByIssue.set(String(item.issueId), arr);
    }
    const history = (issueRows as any[]).map((x: any) => ({
      ...x,
      items: itemsByIssue.get(String(x.id)) || [],
      totalLines: (itemsByIssue.get(String(x.id)) || []).length,
    }));

    return {
      materials: augmented,
      nplIssueHistory: history,
      nplIssueCount: history.length,
      nextRoundNo: history.length ? Math.max(...history.map((x: any) => Number(x.roundNo || 0))) + 1 : 1,
    };
  }

  async saveNplIssueNotes(id: string, body: any, user?: Actor) {
    const order = await this.prisma.productionOrder.findUnique({ where: { id }, select: { id: true } });
    if (!order) throw new NotFoundException("Không tìm thấy lệnh SX.");
    const rows = Array.isArray(body?.rows) ? body.rows : [];
    const actor = this.actor(user);
    for (const row of rows) {
      const accessoryItemId = String(row?.accessoryItemId || "").trim();
      const sizeKey = String(row?.sizeLabel || "").trim().toUpperCase();
      if (!accessoryItemId) continue;
      await this.prisma.productionNplIssueNote.upsert({
        where: { productionOrderId_accessoryItemId_sizeKey: { productionOrderId: id, accessoryItemId, sizeKey } },
        create: {
          productionOrderId: id,
          accessoryItemId,
          sizeKey,
          note: String(row?.note || "").trim() || null,
          updatedById: actor.id,
          updatedByName: actor.name,
        },
        update: {
          note: String(row?.note || "").trim() || null,
          updatedById: actor.id,
          updatedByName: actor.name,
        },
      });
    }
    const state = await this.nplIssueState(id);
    return state;
  }

  async createNplIssue(id: string, body: any, user?: Actor) {
    const order = await this.prisma.productionOrder.findUnique({ where: { id }, select: { id: true } });
    if (!order) throw new NotFoundException("Không tìm thấy lệnh SX.");

    const requested = (Array.isArray(body?.rows) ? body.rows : [])
      .map((x: any) => ({
        accessoryItemId: String(x?.accessoryItemId || "").trim(),
        sizeLabel: String(x?.sizeLabel || "").trim().toUpperCase() || null,
        qty: Number(this.n(x?.qty) || 0),
        note: String(x?.note || "").trim() || null,
      }))
      .filter((x: any) => x.accessoryItemId && x.qty > 0);

    if (!requested.length) throw new BadRequestException("Chưa nhập số lượng NPL cần xuất lần này.");

    const materials = await this.prisma.productionMaterialCalc.findMany({ where: { productionOrderId: id } });
    if (!materials.length) throw new BadRequestException("Chưa có bảng tính NPL. Hãy tính sản lượng ở Bước 5 trước.");

    const state = await this.nplIssueState(id, materials);
    const stateMap = new Map((state.materials || []).map((x: any) => [this.nplIssueKey(x.accessoryItemId, x.sizeLabel), x]));
    const actor = this.actor(user);

    const result = await this.prisma.$transaction(async (tx: any) => {
      const maxRound = await tx.productionNplIssue.aggregate({
        where: { productionOrderId: id },
        _max: { roundNo: true },
      });
      const roundNo = Number(maxRound?._max?.roundNo || 0) + 1;
      const issue = await tx.productionNplIssue.create({
        data: {
          productionOrderId: id,
          roundNo,
          note: String(body?.note || "").trim() || null,
          createdById: actor.id,
          createdByName: actor.name,
        },
      });

      for (const row of requested) {
        const key = this.nplIssueKey(row.accessoryItemId, row.sizeLabel);
        const material: any = stateMap.get(key);
        if (!material) throw new BadRequestException(`Không tìm thấy dòng NPL ${row.accessoryItemId}${row.sizeLabel ? ` · size ${row.sizeLabel}` : ""}.`);

        const remaining = Number(material.remainingToIssue || 0);
        if (row.qty > remaining + 0.0001) {
          throw new BadRequestException(`${material.accessoryCode || ""} · ${material.accessoryName}: lần này chỉ còn phải cấp ${remaining}.`);
        }

        const stockItem = await tx.productionAccessoryItem.findUnique({
          where: { id: row.accessoryItemId },
          select: { id: true, stockQty: true },
        });
        if (!stockItem) throw new BadRequestException(`Không tìm thấy NPL ${material.accessoryCode || row.accessoryItemId}.`);
        const stockBefore = Number(stockItem.stockQty || 0);
        if (row.qty > stockBefore + 0.0001) {
          throw new BadRequestException(`${material.accessoryCode || ""} · ${material.accessoryName}: kho chỉ còn ${stockBefore}, không thể cấp ${row.qty}.`);
        }

        const stockAfter = Math.max(0, stockBefore - row.qty);
        await tx.productionAccessoryItem.update({
          where: { id: row.accessoryItemId },
          data: { stockQty: { decrement: row.qty } },
        });

        const issuedBefore = Number(material.issuedQty || 0);
        const remainingAfter = Math.max(0, Number(material.requiredQty || 0) - issuedBefore - row.qty);
        await tx.productionNplIssueItem.create({
          data: {
            issueId: issue.id,
            productionOrderId: id,
            accessoryItemId: row.accessoryItemId,
            accessoryCode: material.accessoryCode || null,
            accessoryName: material.accessoryName,
            sizeLabel: row.sizeLabel,
            sizeKey: row.sizeLabel || "",
            unit: material.unit,
            requiredQtyAtIssue: Number(material.requiredQty || 0),
            issuedBeforeQty: issuedBefore,
            issuedQty: row.qty,
            remainingAfterQty: remainingAfter,
            stockBeforeQty: stockBefore,
            stockAfterQty: stockAfter,
            note: row.note,
          },
        });

        await tx.productionNplIssueNote.upsert({
          where: { productionOrderId_accessoryItemId_sizeKey: { productionOrderId: id, accessoryItemId: row.accessoryItemId, sizeKey: row.sizeLabel || "" } },
          create: {
            productionOrderId: id,
            accessoryItemId: row.accessoryItemId,
            sizeKey: row.sizeLabel || "",
            note: row.note,
            updatedById: actor.id,
            updatedByName: actor.name,
          },
          update: {
            note: row.note,
            updatedById: actor.id,
            updatedByName: actor.name,
          },
        });
      }
      return issue;
    });

    const fresh = await this.nplIssueState(id);
    return { success: true, issue: result, ...fresh };
  }

  private normalizeLiningComponents(order: any) {
    const raw = Array.isArray(order?.liningFabricComponents) ? order.liningFabricComponents : [];
    const rows = raw
      .map((x: any, index: number) => {
        const key = String(x?.key || x?.id || `LINING_${index + 1}`).trim();
        const name = String(x?.name || x?.label || key).trim();
        const unit = String(x?.unit || "M").trim().toUpperCase() === "G" ? "G" : "M";
        const consumption = Math.max(0, Number(this.n(x?.consumption) || 0));
        const wastePercent = Math.max(0, Number(this.n(x?.wastePercent) || 0));
        const enabled = x?.enabled !== false && consumption > 0;
        return { key, name, unit, consumption, wastePercent, enabled };
      })
      .filter((x: any) => x.key && x.name && x.enabled);

    // Tương thích lệnh V18 cũ: nếu chưa cấu hình theo từng phần thì dùng định mức lót tổng cũ.
    if (!rows.length && Number(order?.liningFabricConsumptionM || 0) > 0) {
      rows.push({
        key: "LEGACY_LINING",
        name: "Vải lót",
        unit: "M",
        consumption: Number(order.liningFabricConsumptionM || 0),
        wastePercent: Number(order.liningFabricWastePercent || 0),
        enabled: true,
      });
    }
    return rows;
  }

  private liningSummary(order: any, rolls: any[], totalPlannedQty: number, totalActualQty: number) {
    const liningRolls = rolls.filter((r: any) => String(r.fabricRole || "MAIN").toUpperCase() === "LINING");
    const rollMap = new Map(liningRolls.map((r: any) => [String(r.fabricReceiptRollId), r]));
    const components = this.normalizeLiningComponents(order);
    const rawAssignments = order?.liningFabricAssignments && typeof order.liningFabricAssignments === "object" && !Array.isArray(order.liningFabricAssignments)
      ? order.liningFabricAssignments as Record<string, any>
      : {};

    const assignments: Record<string, string[]> = {};
    for (const component of components) {
      const supplied = Array.isArray(rawAssignments?.[component.key]) ? rawAssignments[component.key] : [];
      const ids = supplied.map((x: any) => String(x || "")).filter((id: string) => rollMap.has(id));
      // Lệnh V18 cũ: định mức tổng mặc định dùng toàn bộ cây lót đã chọn.
      assignments[component.key] = component.key === "LEGACY_LINING" && !ids.length
        ? liningRolls.map((r: any) => String(r.fabricReceiptRollId))
        : Array.from(new Set(ids));
    }

    const groupMap = new Map<string, any>();
    for (const component of components) {
      const rollIds = assignments[component.key] || [];
      const assignmentKey = `${component.unit}|||${[...rollIds].sort().join("|") || "UNASSIGNED"}`;
      const effectivePerProduct = component.consumption * (1 + component.wastePercent / 100);
      const current = groupMap.get(assignmentKey) || {
        key: assignmentKey,
        unit: component.unit,
        rollIds,
        components: [],
        effectivePerProduct: 0,
      };
      current.components.push(component);
      current.effectivePerProduct += effectivePerProduct;
      groupMap.set(assignmentKey, current);
    }

    const groups = [...groupMap.values()].map((group: any) => {
      const selectedRolls = group.rollIds.map((id: string) => rollMap.get(id)).filter(Boolean);
      const allocated = group.unit === "G"
        ? selectedRolls.reduce((sum: number, r: any) => sum + Number(r.allocatedKg || 0) * 1000, 0)
        : selectedRolls.reduce((sum: number, r: any) => sum + Number(r.allocatedM || 0), 0);
      const possibleQty = group.effectivePerProduct > 0 ? Math.floor(allocated / group.effectivePerProduct) : 0;
      const requiredPlanned = group.effectivePerProduct * Math.max(0, Number(totalPlannedQty || 0));
      const requiredActual = group.effectivePerProduct * Math.max(0, Number(totalActualQty || 0));
      const shortagePlanned = Math.max(0, requiredPlanned - allocated);
      const shortageActual = Math.max(0, requiredActual - allocated);
      return {
        ...group,
        allocated,
        possibleQty,
        requiredPlanned,
        requiredActual,
        shortagePlanned,
        shortageActual,
        shortagePlannedQty: Math.max(0, Number(totalPlannedQty || 0) - possibleQty),
        shortageActualQty: Math.max(0, Number(totalActualQty || 0) - possibleQty),
        rollCount: selectedRolls.length,
        rolls: selectedRolls.map((r: any) => ({
          id: r.fabricReceiptRollId,
          rollCode: r.rollCode || null,
          colorName: r.colorName || null,
          allocatedM: Number(r.allocatedM || 0),
          allocatedKg: Number(r.allocatedKg || 0),
        })),
      };
    });
    const groupByKey = new Map(groups.map((x: any) => [x.key, x]));

    const parts = components.map((component: any) => {
      const rollIds = assignments[component.key] || [];
      const groupKey = `${component.unit}|||${[...rollIds].sort().join("|") || "UNASSIGNED"}`;
      const group: any = groupByKey.get(groupKey);
      const effectivePerProduct = component.consumption * (1 + component.wastePercent / 100);
      return {
        ...component,
        rollIds,
        assigned: rollIds.length > 0,
        effectivePerProduct,
        requiredPlanned: effectivePerProduct * Math.max(0, Number(totalPlannedQty || 0)),
        requiredActual: effectivePerProduct * Math.max(0, Number(totalActualQty || 0)),
        groupKey,
        groupAllocated: Number(group?.allocated || 0),
        groupPossibleQty: Number(group?.possibleQty || 0),
        groupShortageActual: Number(group?.shortageActual || 0),
        groupShortageActualQty: Number(group?.shortageActualQty || 0),
        rolls: group?.rolls || [],
      };
    });

    return {
      enabled: components.length > 0 || liningRolls.length > 0,
      rollCount: liningRolls.length,
      components: parts,
      groups,
      allAssigned: components.length > 0 && parts.every((x: any) => x.assigned),
      enoughForActual: groups.length > 0 && groups.every((x: any) => x.rollIds.length > 0 && Number(x.shortageActual || 0) <= 0.0001),
    };
  }

  private canViewProductionCost(user?: any) {
    return !!user
      && this.isAdminUser(user)
      && this.userHas(user, "fabric_receipt.cost.view")
      && this.userHas(user, "accessories.cost.view");
  }

  private async productionCostSummary(
    id: string,
    totalActualQty: number,
    materials: any[],
    orderRolls: any[],
    user?: any,
    extraCostsRaw?: any,
    priceMultiplierRaw?: any,
  ) {
    if (!this.canViewProductionCost(user)) {
      return {
        canView: false,
        totalActualQty,
        complete: false,
        missingPriceCount: 0,
        mainFabricCostVnd: null,
        liningFabricCostVnd: null,
        accessoryCostVnd: null,
        totalMaterialCostVnd: null,
        materialCostPerProductVnd: null,
        fabricLines: [],
        accessoryLines: [],
      };
    }

    const rollIds = [...new Set((orderRolls || []).map((x: any) => String(x.fabricReceiptRollId || "")).filter(Boolean))];
    const fabricRows = rollIds.length
      ? await this.prisma.fabricReceiptRoll.findMany({
          where: { id: { in: rollIds } },
          select: {
            id: true,
            fabricReceiptId: true,
            fabricCode: true,
            rollCode: true,
            colorName: true,
            actualM: true,
            actualKg: true,
            supplierDeclaredM: true,
            supplierDeclaredKg: true,
            unitPriceCny: true,
            priceUnit: true,
            fabricReceipt: {
              select: {
                receiptCode: true,
                fabricName: true,
                fabricCode: true,
                exchangeRateToVnd: true,
                unitPriceVnd: true,
                priceUnit: true,
                fabricCosts: {
                  select: {
                    fabricCode: true,
                    chinaShippingCny: true,
                    vietnamShippingVnd: true,
                  },
                },
              },
            },
          },
        })
      : [];

    const receiptIds = [...new Set(fabricRows.map((x: any) => String(x.fabricReceiptId || "")).filter(Boolean))];
    const siblingRolls = receiptIds.length
      ? await this.prisma.fabricReceiptRoll.findMany({
          where: { fabricReceiptId: { in: receiptIds } },
          select: { id: true, fabricReceiptId: true, fabricCode: true },
        })
      : [];

    const siblingCount = new Map<string, number>();
    for (const row of siblingRolls as any[]) {
      const key = `${row.fabricReceiptId}|||${String(row.fabricCode || "").trim().toUpperCase()}`;
      siblingCount.set(key, (siblingCount.get(key) || 0) + 1);
    }

    const allocationByRoll = new Map((orderRolls || []).map((x: any) => [String(x.fabricReceiptRollId), x]));
    const fabricLines = fabricRows.map((roll: any) => {
      const allocation: any = allocationByRoll.get(String(roll.id)) || {};
      const role = String(allocation.fabricRole || "MAIN").toUpperCase() === "LINING" ? "LINING" : "MAIN";
      const receipt = roll.fabricReceipt || {};
      const rate = Number(receipt.exchangeRateToVnd || 0);
      const code = String(roll.fabricCode || receipt.fabricCode || "").trim().toUpperCase();
      const priceUnit = String(roll.priceUnit || receipt.priceUnit || "METER").toUpperCase();
      const fullM = Number(roll.actualM ?? roll.supplierDeclaredM ?? 0);
      const fullKg = Number(roll.actualKg ?? roll.supplierDeclaredKg ?? 0);
      const usedM = Number(allocation.allocatedM || 0);
      const usedKg = Number(allocation.allocatedKg || 0);

      let usedQty = 0;
      let fullQty = 0;
      let qtyUnit = "m";
      if (priceUnit === "KG") {
        usedQty = usedKg;
        fullQty = fullKg;
        qtyUnit = "kg";
      } else if (priceUnit === "ROLL") {
        qtyUnit = "cây";
        if (fullM > 0 && usedM > 0) {
          usedQty = Math.min(1, usedM / fullM);
          fullQty = 1;
        } else if (fullKg > 0 && usedKg > 0) {
          usedQty = Math.min(1, usedKg / fullKg);
          fullQty = 1;
        } else {
          usedQty = 1;
          fullQty = 1;
        }
      } else {
        usedQty = usedM;
        fullQty = fullM;
        qtyUnit = "m";
      }

      const unitPriceCny = roll.unitPriceCny === null || roll.unitPriceCny === undefined ? null : Number(roll.unitPriceCny);
      const legacyUnitPriceVnd = receipt.unitPriceVnd === null || receipt.unitPriceVnd === undefined ? null : Number(receipt.unitPriceVnd);
      let goodsFullVnd: number | null = null;
      let priceSource = "";

      if (unitPriceCny !== null && rate > 0) {
        const pricedQty = priceUnit === "ROLL" ? 1 : fullQty;
        goodsFullVnd = pricedQty * unitPriceCny * rate;
        priceSource = "Giá cây × tỷ giá";
      } else if (legacyUnitPriceVnd !== null) {
        const receiptPriceUnit = String(receipt.priceUnit || priceUnit).toUpperCase();
        const pricedQty = receiptPriceUnit === "ROLL" ? 1 : (receiptPriceUnit === "KG" ? fullKg : fullM);
        goodsFullVnd = pricedQty * legacyUnitPriceVnd;
        priceSource = "Giá VND phiếu vải";
      }

      const costRow = Array.isArray(receipt.fabricCosts)
        ? receipt.fabricCosts.find((x: any) => String(x.fabricCode || "").trim().toUpperCase() === code)
        : null;
      const chinaShippingVnd = Number(costRow?.chinaShippingCny || 0) * rate;
      const vietnamShippingVnd = Number(costRow?.vietnamShippingVnd || 0);
      const countKey = `${roll.fabricReceiptId}|||${code}`;
      const codeRollCount = Math.max(1, siblingCount.get(countKey) || 1);
      const shippingPerRollVnd = (chinaShippingVnd + vietnamShippingVnd) / codeRollCount;

      const fraction = priceUnit === "ROLL"
        ? Math.max(0, Math.min(1, usedQty))
        : fullQty > 0
          ? Math.max(0, Math.min(1, usedQty / fullQty))
          : 0;

      const missingPrice = goodsFullVnd === null || !Number.isFinite(goodsFullVnd);
      const costVnd = missingPrice ? 0 : Math.max(0, goodsFullVnd! * fraction + shippingPerRollVnd * fraction);

      return {
        role,
        fabricReceiptRollId: roll.id,
        receiptCode: receipt.receiptCode || null,
        rollCode: roll.rollCode || null,
        fabricCode: code || null,
        fabricName: receipt.fabricName || null,
        colorName: roll.colorName || null,
        priceUnit,
        usedQty,
        qtyUnit,
        usedM,
        usedKg,
        costVnd,
        missingPrice,
        priceSource,
      };
    });

    const accessoryIds = [...new Set((materials || []).map((x: any) => String(x.accessoryItemId || "")).filter(Boolean))];
    const accessoryItems = accessoryIds.length
      ? await this.prisma.productionAccessoryItem.findMany({
          where: { id: { in: accessoryIds } },
          select: { id: true, code: true, name: true, unit: true, unitPrice: true },
        })
      : [];
    const accessoryMap = new Map(accessoryItems.map((x: any) => [String(x.id), x]));

    const accessoryLines = (materials || []).map((m: any) => {
      const item: any = accessoryMap.get(String(m.accessoryItemId || ""));
      const unitPrice = item?.unitPrice === null || item?.unitPrice === undefined ? null : Number(item.unitPrice);
      const requiredQty = Number(m.requiredQty || 0);
      const missingPrice = unitPrice === null || !Number.isFinite(unitPrice);
      return {
        accessoryItemId: m.accessoryItemId,
        accessoryCode: m.accessoryCode || item?.code || null,
        accessoryName: m.accessoryName || item?.name || null,
        sizeLabel: m.sizeLabel || null,
        unit: m.unit || item?.unit || null,
        requiredQty,
        unitPriceVnd: missingPrice ? null : unitPrice,
        costVnd: missingPrice ? 0 : requiredQty * unitPrice!,
        missingPrice,
      };
    });

    let extraCosts = (Array.isArray(extraCostsRaw) ? extraCostsRaw : [])
      .map((x: any, index: number) => ({
        id: String(x?.id || `EXTRA_${index + 1}`),
        type: String(x?.type || "OTHER").toUpperCase(),
        label: String(x?.label || "").trim() || "Phụ phí khác",
        amountVnd: Math.max(0, Number(this.n(x?.amountVnd) || 0)),
        note: String(x?.note || "").trim() || null,
      }))
      .filter((x: any) => x.amountVnd > 0 || x.label);

    if (!extraCosts.some((x: any) => x.type === "FACTORY_LABOR")) {
      extraCosts = [{ id: "FACTORY_LABOR", type: "FACTORY_LABOR", label: "Gia công nhà may / SP", amountVnd: 0, note: null }, ...extraCosts];
    }

    const factoryLaborRow = extraCosts.find((x: any) => x.type === "FACTORY_LABOR");
    const factoryLaborUnitVnd = Number(factoryLaborRow?.amountVnd || 0);
    const factoryLaborCostVnd = factoryLaborUnitVnd * Math.max(0, totalActualQty);
    const otherExtraCosts = extraCosts.filter((x: any) => x.type !== "FACTORY_LABOR");
    const otherExtraCostVnd = otherExtraCosts.reduce((sum: number, x: any) => sum + Number(x.amountVnd || 0), 0);

    const mainFabricCostVnd = fabricLines.filter((x: any) => x.role === "MAIN").reduce((sum: number, x: any) => sum + Number(x.costVnd || 0), 0);
    const liningFabricCostVnd = fabricLines.filter((x: any) => x.role === "LINING").reduce((sum: number, x: any) => sum + Number(x.costVnd || 0), 0);
    const accessoryCostVnd = accessoryLines.reduce((sum: number, x: any) => sum + Number(x.costVnd || 0), 0);
    const baseMaterialCostVnd = mainFabricCostVnd + liningFabricCostVnd + accessoryCostVnd;
    const totalProductionCostVnd = baseMaterialCostVnd + factoryLaborCostVnd + otherExtraCostVnd;
    const productionCostPerProductVnd = totalActualQty > 0 ? totalProductionCostVnd / totalActualQty : null;
    const priceMultiplier = Math.max(0.1, Number(this.n(priceMultiplierRaw) || 2.2));
    const estimatedSalePriceVnd = productionCostPerProductVnd === null ? null : productionCostPerProductVnd * priceMultiplier;
    const missingPriceCount = fabricLines.filter((x: any) => x.missingPrice).length + accessoryLines.filter((x: any) => x.missingPrice).length;
    const complete = totalActualQty > 0 && missingPriceCount === 0;

    return {
      canView: true,
      totalActualQty,
      complete,
      missingPriceCount,
      mainFabricCostVnd,
      liningFabricCostVnd,
      accessoryCostVnd,
      baseMaterialCostVnd,
      factoryLaborUnitVnd,
      factoryLaborCostVnd,
      otherExtraCostVnd,
      extraCostVnd: factoryLaborCostVnd + otherExtraCostVnd,
      totalMaterialCostVnd: totalProductionCostVnd,
      totalProductionCostVnd,
      materialCostPerProductVnd: productionCostPerProductVnd,
      productionCostPerProductVnd,
      priceMultiplier,
      estimatedSalePriceVnd,
      extraCosts,
      fabricLines,
      accessoryLines,
      note: "Giá gốc/SP = (vải chính + vải lót + NPL + gia công nhà may + phụ phí khác) / số lượng cắt thực tế. Giá bán ước tính = giá gốc/SP × hệ số.",
    };
  }

  async calculateOrder(id: string, user?: Actor) {
    const order = await this.prisma.productionOrder.findUnique({ where: { id } });
    if (!order) throw new NotFoundException("Không tìm thấy lệnh SX.");
    const rolls = await this.prisma.productionOrderRoll.findMany({ where: { productionOrderId: id } });
    const mainRolls = (rolls as any[]).filter((r: any) => String(r.fabricRole || "MAIN").toUpperCase() !== "LINING");
    const liningRolls = (rolls as any[]).filter((r: any) => String(r.fabricRole || "MAIN").toUpperCase() === "LINING");
    const consumption = Number(order.fabricConsumptionM || 0);
    if (consumption <= 0) throw new BadRequestException("Chưa nhập định mức vải chính / sản phẩm.");
    if (!mainRolls.length) throw new BadRequestException("Chưa chọn cây vải chính.");
    const liningComponents = this.normalizeLiningComponents(order);
    if (liningRolls.length && !liningComponents.length) {
      throw new BadRequestException("Đã chọn cây vải lót nhưng chưa cấu hình định mức lót thân/tay/túi/cổ ở Bước 4.");
    }

    const effective = consumption * (1 + Number(order.fabricWastePercent || 0) / 100);
    const grouped = new Map<string, any>();
    for (const r of mainRolls as any[]) {
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
    const lining = this.liningSummary(order, rolls, totalPlannedQty, totalActualQty);
    const issueState = await this.nplIssueState(id, npl.materials);
    const costSummary = await this.productionCostSummary(id, totalActualQty, issueState.materials, rolls, user, order.productionExtraCosts, order.productionPriceMultiplier);
    return { totalQty: totalPlannedQty, totalPlannedQty, totalActualQty, effectiveConsumptionM: effective, colors: this.groupCutRows(sizeRows), materials: issueState.materials, lining, costSummary, nplIssueHistory: issueState.nplIssueHistory, nextNplIssueRound: issueState.nextRoundNo };
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
    const rolls = await this.prisma.productionOrderRoll.findMany({ where: { productionOrderId: id } });
    const freshOrder = await this.prisma.productionOrder.findUnique({ where: { id } });
    const lining = this.liningSummary(freshOrder || order, rolls, totalPlannedQty, totalActualQty);
    const issueState = await this.nplIssueState(id, npl.materials);
    const costSummary = await this.productionCostSummary(id, totalActualQty, issueState.materials, rolls, user, (freshOrder || order).productionExtraCosts, (freshOrder || order).productionPriceMultiplier);
    return { totalQty: totalPlannedQty, totalPlannedQty, totalActualQty, colors: this.groupCutRows(sizeRows), materials: issueState.materials, cutHistory, lining, costSummary, nplIssueHistory: issueState.nplIssueHistory, nextNplIssueRound: issueState.nextRoundNo };
  }

  async cancelOrder(id: string, user?: any) {
    const order = await this.prisma.productionOrder.findUnique({ where: { id } });
    if (!order) throw new NotFoundException("Không tìm thấy lệnh SX.");
    if (String(order.status) === "COMPLETED") {
      throw new BadRequestException("Lệnh đã hoàn thành, không thể huỷ.");
    }
    const actor = this.actor(user);
    await this.prisma.$transaction(async (tx: any) => {
      // Giữ nguyên cây vải/NPL/size/lịch sử để tra cứu. availableFabricRolls sẽ bỏ qua allocation của lệnh CANCELLED.
      await tx.productionOrder.update({
        where: { id },
        data: {
          status: "CANCELLED",
          note: [String(order.note || "").trim(), `Đã huỷ bởi ${actor.name || "—"} lúc ${new Date().toISOString()}`]
            .filter(Boolean)
            .join("\n"),
        },
      });
    });
    return this.getOrder(id);
  }

  async saveProductionExtraCosts(id: string, body: any, user?: any) {
    if (!this.isAdminUser(user)) {
      throw new ForbiddenException("Chỉ Admin / Owner được duyệt và cấu hình chi phí ở Bước 6.");
    }
    const order = await this.prisma.productionOrder.findUnique({ where: { id }, select: { id: true } });
    if (!order) throw new NotFoundException("Không tìm thấy lệnh SX.");

    let rows = (Array.isArray(body?.items) ? body.items : [])
      .map((x: any, index: number) => ({
        id: String(x?.id || `EXTRA_${Date.now()}_${index}`),
        type: String(x?.type || "OTHER").toUpperCase(),
        label: String(x?.label || "").trim() || "Phụ phí khác",
        amountVnd: Math.max(0, Number(this.n(x?.amountVnd) || 0)),
        note: String(x?.note || "").trim() || null,
      }))
      .filter((x: any) => x.label);

    if (!rows.some((x: any) => x.type === "FACTORY_LABOR")) {
      rows = [{ id: "FACTORY_LABOR", type: "FACTORY_LABOR", label: "Gia công nhà may / SP", amountVnd: 0, note: null }, ...rows];
    }

    const productionPriceMultiplier = Math.max(0.1, Number(this.n(body?.priceMultiplier) || 2.2));

    await this.prisma.productionOrder.update({
      where: { id },
      data: {
        productionExtraCosts: rows,
        productionPriceMultiplier,
      },
    });
    return this.getOrder(id, user);
  }


  async deleteOrder(id: string) {
    const order = await this.prisma.productionOrder.findUnique({ where: { id }, select: { id: true, status: true } });
    if (!order) throw new NotFoundException("Không tìm thấy lệnh SX.");
    if (!["DRAFT", "PLANNING", "CANCELLED"].includes(String(order.status))) {
      throw new BadRequestException("Chỉ được xoá lệnh chưa triển khai, đang lên kế hoạch hoặc đã huỷ.");
    }
    await this.prisma.$transaction(async (tx: any) => {
      await tx.productionOrderRoll.deleteMany({ where: { productionOrderId: id } });
      await tx.productionCutQtyHistory.deleteMany({ where: { productionOrderId: id } });
      await tx.productionSizePlan.deleteMany({ where: { productionOrderId: id } });
      await tx.productionNplIssueItem.deleteMany({ where: { productionOrderId: id } });
      await tx.productionNplIssue.deleteMany({ where: { productionOrderId: id } });
      await tx.productionNplIssueNote.deleteMany({ where: { productionOrderId: id } });
      await tx.productionMaterialCalc.deleteMany({ where: { productionOrderId: id } });
      await tx.productionOrderAccessorySpec.deleteMany({ where: { productionOrderId: id } });
      await tx.productionOrder.delete({ where: { id } });
    });
    return { success: true, id };
  }

  async sendOrder(id: string, user?: any) {
    if (!this.isAdminUser(user)) {
      throw new ForbiddenException("Chỉ Admin / Owner được gửi lệnh tại Bước 6.");
    }
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
    return this.getOrder(id, user);
  }

  async printPayload(id: string) {
    return {
      ...(await this.getOrder(id)),
      generatedAt: new Date().toISOString(),
      confirmation: { the1970: "The 1970 xác nhận", factory: "Nhà may / xưởng xác nhận" },
    };
  }
}
