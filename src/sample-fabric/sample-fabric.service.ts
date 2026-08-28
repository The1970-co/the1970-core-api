
import { BadRequestException, Injectable, NotFoundException } from "@nestjs/common";
import {
  DesignSampleImageType,
  DesignSampleStatus,
  FabricBoardImageType,
  FabricReceiptStatus,
  FabricSampleDispatchStatus,
  Prisma,
} from "@prisma/client";
import { PrismaService } from "../prisma/prisma.service";

type Actor = { id?: string | null; name?: string | null; fullName?: string | null; email?: string | null };

const SAMPLE_SEASONS = ["Xuân Hạ", "Thu Đông", "Đông Xuân", "Xuân Hè"] as const;
const DEFAULT_FABRIC_COMPOSITIONS = [
  "Cotton","Linen","Tencel","Lyocell","Viscose","Rayon","Modal","Bamboo",
  "Polyester","Nylon","Spandex","Elastane","Wool","Cashmere","Silk","Acrylic",
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

  private n(value: any) {
    if (value === null || value === undefined || value === "") return null;
    const parsed = Number(String(value).trim().replace(",", ".").replace(/[^0-9.-]/g, ""));
    return Number.isFinite(parsed) ? parsed : null;
  }

  private titleCase(value: any) {
    return String(value || "").trim().replace(/\s+/g, " ").split(" ")
      .map((part) => part ? part.charAt(0).toLocaleUpperCase("vi-VN") + part.slice(1).toLocaleLowerCase("vi-VN") : "")
      .join(" ");
  }

  private normalizeSampleCode(value: any) {
    return String(value || "").trim().toUpperCase().replace(/\s+/g, "");
  }

  private normalizeColorCode(value: any) {
    const raw = String(value || "").trim();
    if (!raw) return null;
    return `#${raw.replace(/^#+/, "")}`;
  }

  private normalizeColorCodes(value: any) {
    const values = String(value || "")
      .split(/[;,\s]+/)
      .map((x) => x.trim())
      .filter(Boolean)
      .map((x) => this.normalizeColorCode(x))
      .filter(Boolean);
    return values.length ? Array.from(new Set(values)).join(", ") : null;
  }

  private parseTokens(value: any) {
    if (Array.isArray(value)) return value.map((x) => this.titleCase(x)).filter(Boolean);
    return String(value || "").split(",").map((x) => this.titleCase(x)).filter(Boolean);
  }

  private parseCompositionTokens(value?: string | null) {
    return this.parseTokens(value);
  }

  private async nextCode(prefix: string, model: "sample" | "receipt") {
    const day = new Date().toISOString().slice(2, 10).replace(/-/g, "");
    const base = `${prefix}${day}`;
    const rows = model === "sample"
      ? await this.prisma.designSample.count({ where: { code: { startsWith: base } } })
      : await this.prisma.fabricReceipt.count({ where: { receiptCode: { startsWith: base } } });
    return `${base}-${String(rows + 1).padStart(3, "0")}`;
  }

  private supplierInitial(name: any) {
    const raw = String(name || "")
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/Đ/g, "D")
      .replace(/đ/g, "d")
      .toUpperCase();
    return raw.match(/[A-Z0-9]/)?.[0] || "X";
  }

  private async nextFabricSupplierCode(name: any) {
    const initial = this.supplierInitial(name);
    for (let index = 1; index < 100000; index += 1) {
      const code = `${String(index).padStart(3, "0")}-${initial}`;
      const exists = await this.prisma.fabricSupplier.findUnique({ where: { code }, select: { id: true } });
      if (!exists) return code;
    }
    throw new BadRequestException("Không thể sinh mã nhà cung cấp vải.");
  }

  private async normalizeLegacyFabricSupplierCodes() {
    const rows = await this.prisma.fabricSupplier.findMany({
      where: { isActive: true },
      select: { id: true, code: true, name: true },
      orderBy: { name: "asc" },
    });
    for (const row of rows) {
      if (/^\d{3}-[A-Z0-9]$/.test(String(row.code || ""))) continue;
      const code = await this.nextFabricSupplierCode(row.name);
      await this.prisma.fabricSupplier.update({ where: { id: row.id }, data: { code } });
    }
  }

  private isAdminUser(user?: any) {
    const role = String(user?.role || "").trim().toUpperCase();
    return role === "OWNER" || role === "ADMIN";
  }

  private supplierForUser(supplier: any, user?: any) {
    if (!supplier) return supplier;
    if (user === undefined || this.userHas(user, "fabric_receipt.supplier_identity.view")) return supplier;
    return { id: supplier.id, code: supplier.code, name: null, phone: null, email: null, address: null, note: null };
  }

  private rollPriceQty(roll:any) {
    const unit=String(roll?.priceUnit || "METER").toUpperCase();
    if (unit === "ROLL") return 1;
    if (unit === "KG") return Number(this.n(roll?.actualKg) || this.n(roll?.supplierDeclaredKg) || 0);
    return Number(this.n(roll?.actualM) || this.n(roll?.supplierDeclaredM) || 0);
  }

  private receiptCostSummary(row:any) {
    const rate=Number(this.n(row?.exchangeRateToVnd)||0);
    const sourceRolls=Array.isArray(row?.rolls)?row.rolls:[];
    const sourceCosts=Array.isArray(row?.fabricCosts)?row.fabricCosts:[];
    const codeStats=new Map<string,any>();
    for(const roll of sourceRolls){const code=String(roll?.fabricCode||row?.fabricCode||"").trim().toUpperCase();if(!code)continue;const current=codeStats.get(code)||{rollCount:0,totalKg:0,goodsCny:0};const qty=this.rollPriceQty(roll),unitPriceCny=Number(this.n(roll?.unitPriceCny)||0);current.rollCount+=1;current.totalKg+=Number(this.n(roll?.actualKg)||this.n(roll?.supplierDeclaredKg)||0);current.goodsCny+=qty*unitPriceCny;codeStats.set(code,current)}
    const fabricCosts=sourceCosts.map((cost:any)=>{const code=String(cost?.fabricCode||"").trim().toUpperCase(),stats=codeStats.get(code)||{rollCount:0,totalKg:0,goodsCny:0},chinaShippingCny=Number(this.n(cost?.chinaShippingCny)||0),chinaShippingVnd=chinaShippingCny*rate,vnRate=Number(this.n(cost?.vietnamShippingRateVndPerKg)||0),legacyVn=Number(this.n(cost?.vietnamShippingVnd)||0),vietnamShippingVnd=vnRate>0?stats.totalKg*vnRate:legacyVn,totalShippingVnd=chinaShippingVnd+vietnamShippingVnd,goodsVnd=stats.goodsCny*rate;return{...cost,vietnamShippingRateVndPerKg:vnRate,vietnamShippingVnd,rollCount:stats.rollCount,totalKg:stats.totalKg,goodsCny:stats.goodsCny,goodsVnd,chinaShippingVnd,totalShippingVnd,shippingPerRollVnd:stats.rollCount?totalShippingVnd/stats.rollCount:0,fabricPerRollVnd:stats.rollCount?goodsVnd/stats.rollCount:0,landedPerRollVnd:stats.rollCount?(goodsVnd+totalShippingVnd)/stats.rollCount:0}});
    const byCode=new Map(fabricCosts.map((x:any)=>[String(x.fabricCode||"").toUpperCase(),x]));
    const rolls=sourceRolls.map((roll:any)=>{const qty=this.rollPriceQty(roll),unitPriceCny=Number(this.n(roll?.unitPriceCny)||0),lineAmountCny=qty*unitPriceCny,lineAmountVnd=lineAmountCny*rate,code=String(roll?.fabricCode||row?.fabricCode||"").trim().toUpperCase(),cc:any=byCode.get(code),allocatedShippingVnd=Number(cc?.shippingPerRollVnd||0);return{...roll,priceQty:qty,lineAmountCny,lineAmountVnd,allocatedShippingVnd,landedCostVnd:lineAmountVnd+allocatedShippingVnd}});
    const goodsCny=rolls.reduce((sum:number,x:any)=>sum+Number(x.lineAmountCny||0),0),goodsVnd=goodsCny*rate,chinaShippingCny=fabricCosts.reduce((sum:number,x:any)=>sum+Number(this.n(x?.chinaShippingCny)||0),0),chinaShippingVnd=chinaShippingCny*rate,vietnamShippingVnd=fabricCosts.reduce((sum:number,x:any)=>sum+Number(x?.vietnamShippingVnd||0),0),totalShippingVnd=chinaShippingVnd+vietnamShippingVnd;
    return {rolls,fabricCosts,summary:{exchangeRateToVnd:rate,goodsCny,goodsVnd,chinaShippingCny,chinaShippingVnd,vietnamShippingVnd,totalShippingVnd,grandTotalVnd:goodsVnd+totalShippingVnd}};
  }

  private receiptForUser(row: any, user?: any) {
    if (!row) return row;
    const canViewCost = user === undefined || this.userHas(user, "fabric_receipt.cost.view") || this.userHas(user, "fabric_receipt.cost.edit");
    const rollTotals = Array.isArray(row.rolls) && row.rolls.length ? this.receiptTotalsFromRolls(row.rolls, row) : null;
    const priced=this.receiptCostSummary(row);
    const fabricConfigs=(Array.isArray(row.fabricConfigs)?row.fabricConfigs:[]).map((x:any)=>({...x,supplier:this.supplierForUser(x.supplier,user)}));
    if (canViewCost) return {...row,...(rollTotals||{}),rolls:priced.rolls,fabricCosts:priced.fabricCosts,fabricConfigs,costSummary:priced.summary,supplier:this.supplierForUser(row.supplier,user)};
    return {...row,...(rollTotals||{}),rolls:(Array.isArray(row.rolls)?row.rolls:[]).map((x:any)=>({...x,unitPriceCny:null,priceUnit:null,lineAmountCny:null,lineAmountVnd:null,priceQty:null})),fabricCosts:[],fabricConfigs,costSummary:null,supplier:this.supplierForUser(row.supplier,user),unitPrice:null,priceUnit:null,priceCurrency:null,exchangeRateToVnd:null,unitPriceVnd:null};
  }

  async listFabricSuppliers(user?: any) {
    await this.normalizeLegacyFabricSupplierCodes();
    const rows = await this.prisma.fabricSupplier.findMany({
      where: { isActive: true },
      select: { id: true, code: true, name: true, phone: true, email: true, address: true, note: true },
      orderBy: { name: "asc" },
    });
    return rows.map((row) => this.supplierForUser(row, user));
  }

  async createFabricSupplier(body: any) {
    const name = this.titleCase(body?.name);
    if (!name) throw new BadRequestException("Thiếu tên nhà cung cấp vải.");
    const sameName = await this.prisma.fabricSupplier.findFirst({
      where: { name: { equals: name, mode: "insensitive" }, isActive: true },
      select: { id: true, code: true, name: true, phone: true, email: true, address: true, note: true },
    });
    if (sameName) {
      if (!/^\d{3}-[A-Z0-9]$/.test(String(sameName.code || ""))) {
        const code = await this.nextFabricSupplierCode(name);
        return this.prisma.fabricSupplier.update({
          where: { id: sameName.id }, data: { code },
          select: { id: true, code: true, name: true, phone: true, email: true, address: true, note: true },
        });
      }
      return sameName;
    }
    const code = this.normalizeSampleCode(body?.code) || await this.nextFabricSupplierCode(name);
    const codeExists = await this.prisma.fabricSupplier.findUnique({ where: { code }, select: { id: true } });
    if (codeExists) throw new BadRequestException(`Mã nhà cung cấp vải ${code} đã tồn tại.`);
    return this.prisma.fabricSupplier.create({
      data: {
        name, code,
        phone: String(body?.phone || "").trim() || null,
        email: String(body?.email || "").trim() || null,
        address: String(body?.address || "").trim() || null,
        note: String(body?.note || "").trim() || null,
      },
      select: { id: true, code: true, name: true, phone: true, email: true, address: true, note: true },
    });
  }

  async updateFabricSupplier(id: string, body: any) {
    const existing = await this.prisma.fabricSupplier.findUnique({ where: { id } });
    if (!existing) throw new NotFoundException("Không tìm thấy nhà cung cấp vải.");

    const name = body?.name !== undefined ? this.titleCase(body.name) : existing.name;
    if (!name) throw new BadRequestException("Thiếu tên nhà cung cấp vải.");

    let code = body?.code !== undefined ? this.normalizeSampleCode(body.code) : existing.code;
    if (!code) code = await this.nextFabricSupplierCode(name);

    const duplicateCode = await this.prisma.fabricSupplier.findFirst({
      where: { code, NOT: { id } },
      select: { id: true },
    });
    if (duplicateCode) throw new BadRequestException(`Mã nhà cung cấp vải ${code} đã tồn tại.`);

    const duplicateName = await this.prisma.fabricSupplier.findFirst({
      where: { name: { equals: name, mode: "insensitive" }, isActive: true, NOT: { id } },
      select: { id: true },
    });
    if (duplicateName) throw new BadRequestException(`Nhà cung cấp vải ${name} đã tồn tại.`);

    return this.prisma.fabricSupplier.update({
      where: { id },
      data: {
        name,
        code,
        ...(body?.phone !== undefined ? { phone: String(body.phone || "").trim() || null } : {}),
        ...(body?.email !== undefined ? { email: String(body.email || "").trim() || null } : {}),
        ...(body?.address !== undefined ? { address: String(body.address || "").trim() || null } : {}),
        ...(body?.note !== undefined ? { note: String(body.note || "").trim() || null } : {}),
      },
      select: { id: true, code: true, name: true, phone: true, email: true, address: true, note: true },
    });
  }

  async deactivateFabricSupplier(id: string) {
    const existing = await this.prisma.fabricSupplier.findUnique({
      where: { id },
      select: { id: true, code: true, name: true, isActive: true },
    });
    if (!existing) throw new NotFoundException("Không tìm thấy nhà cung cấp vải.");
    if (!existing.isActive) return { success: true, id };

    await this.prisma.fabricSupplier.update({
      where: { id },
      data: { isActive: false },
    });
    return { success: true, id };
  }


  private receiptDateSuffix(value?: any) {
    const raw = String(value || "").trim();
    const match = raw.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (match) return `${match[3]}${match[2]}${match[1]}`;
    const parts = new Intl.DateTimeFormat("en-GB", {
      timeZone: "Asia/Ho_Chi_Minh", day: "2-digit", month: "2-digit", year: "numeric",
    }).formatToParts(new Date());
    const map = Object.fromEntries(parts.map((part) => [part.type, part.value]));
    return `${map.day}${map.month}${map.year}`;
  }

  private async nextFabricReceiptCode(receivedAt?: any) {
    const suffix = this.receiptDateSuffix(receivedAt);
    const rows = await this.prisma.fabricReceipt.findMany({
      where: { receiptCode: { startsWith: "NV-" } },
      select: { receiptCode: true },
    });
    const max = rows.reduce((current, row) => {
      const match = String(row.receiptCode || "").match(/^NV-(\d+)-\d{8}$/);
      return Math.max(current, Number(match?.[1] || 0));
    }, 0);
    return `NV-${String(max + 1).padStart(3, "0")}-${suffix}`;
  }

  async previewFabricReceiptCode(receivedAt?: any) {
    return { code: await this.nextFabricReceiptCode(receivedAt) };
  }

  private async productGroupsAndCompositions() {
    const [categories, productCategories, sampleCategories, sampleComps, boardComps, boards] = await Promise.all([
      this.prisma.category.findMany({ where: { isActive: true }, select: { name: true }, orderBy: [{ sortOrder: "asc" }, { name: "asc" }] }),
      this.prisma.product.findMany({ where: { category: { not: null } }, distinct: ["category"], select: { category: true } }),
      this.prisma.designSample.findMany({ where: { category: { not: null } }, distinct: ["category"], select: { category: true } }),
      this.prisma.designSample.findMany({ where: { fabricComposition: { not: null } }, select: { fabricComposition: true } }),
      this.prisma.fabricBoard.findMany({ where: { composition: { not: null } }, select: { composition: true } }),
      this.prisma.fabricBoard.findMany({ select: { productGroups: true } }),
    ]);
    const productGroups = Array.from(new Set([
      ...categories.map((x) => this.titleCase(x.name)),
      ...productCategories.map((x) => this.titleCase(x.category)),
      ...sampleCategories.map((x) => this.titleCase(x.category)),
      ...boards.flatMap((x) => x.productGroups.map((y) => this.titleCase(y))),
    ].filter(Boolean))).sort((a, b) => a.localeCompare(b, "vi"));
    const fabricCompositions = Array.from(new Set([
      ...DEFAULT_FABRIC_COMPOSITIONS,
      ...sampleComps.flatMap((x) => this.parseCompositionTokens(x.fabricComposition)),
      ...boardComps.flatMap((x) => this.parseCompositionTokens(x.composition)),
    ])).sort((a, b) => a.localeCompare(b, "vi"));
    return { productGroups, fabricCompositions };
  }

  // -------------------------
  // FABRIC LIBRARY / BẢNG VẢI
  // -------------------------
  async fabricLibraryMeta() {
    const [suppliers, staff, vocab] = await Promise.all([
      this.listFabricSuppliers(),
      this.prisma.staffUser.findMany({
        where: { isActive: true },
        select: { id: true, code: true, name: true, branchId: true },
        orderBy: { name: "asc" },
      }),
      this.productGroupsAndCompositions(),
    ]);
    return { suppliers, staff, seasons: SAMPLE_SEASONS, ...vocab };
  }

  async listFabricBoards(query?: { q?: string; supplierId?: string; season?: string; productGroup?: string }) {
    const q = String(query?.q || "").trim();
    return this.prisma.fabricBoard.findMany({
      where: {
        isActive: true,
        ...(query?.supplierId ? { supplierId: query.supplierId } : {}),
        ...(query?.season ? { seasons: { has: query.season } } : {}),
        ...(query?.productGroup ? { productGroups: { has: this.titleCase(query.productGroup) } } : {}),
        ...(q ? { OR: [
          { boardCode: { contains: q, mode: "insensitive" } },
          { fabricCode: { contains: q, mode: "insensitive" } },
          { name: { contains: q, mode: "insensitive" } },
          { composition: { contains: q, mode: "insensitive" } },
          { supplier: { name: { contains: q, mode: "insensitive" } } },
          { colors: { some: { OR: [
            { name: { contains: q, mode: "insensitive" } },
            { code: { contains: q, mode: "insensitive" } },
          ] } } },
        ] } : {}),
      },
      include: {
        supplier: { select: { id: true, code: true, name: true } },
        colors: { orderBy: { createdAt: "asc" } },
        images: { orderBy: { createdAt: "desc" } },
        _count: { select: { designSamples: true, sampleDispatches: true, fabricReceipts: true } },
      },
      orderBy: { updatedAt: "desc" },
    });
  }

  async getFabricBoard(id: string) {
    const board = await this.prisma.fabricBoard.findUnique({
      where: { id },
      include: {
        supplier: true,
        colors: { orderBy: { createdAt: "asc" } },
        images: { orderBy: { createdAt: "desc" } },
        designSamples: {
          include: { producedProduct: { select: { id: true, name: true, slug: true, imageUrl: true } } },
          orderBy: { updatedAt: "desc" },
        },
        sampleDispatches: {
          include: {
            designSample: { select: { id: true, code: true, name: true, status: true, year: true } },
            fabricColor: { select: { id: true, name: true, code: true } },
          },
          orderBy: { sentAt: "desc" },
        },
        fabricReceipts: {
          select: { id: true, receiptCode: true, status: true, receivedAt: true, actualM: true, actualKg: true },
          orderBy: { updatedAt: "desc" },
        },
      },
    });
    if (!board) throw new NotFoundException("Không tìm thấy bảng vải.");
    return board;
  }

  async createFabricBoard(body: any, user?: Actor) {
    const actor = this.actor(user);
    const supplierId = String(body?.supplierId || "").trim();
    const boardCode = this.normalizeSampleCode(body?.boardCode);
    if (!supplierId) throw new BadRequestException("Chưa chọn nhà cung cấp vải.");
    if (!boardCode) throw new BadRequestException("Thiếu mã bảng vải.");
    const fabricCode = this.normalizeSampleCode(body?.fabricCode) || null;
    const duplicate = await this.prisma.fabricBoard.findFirst({
      where: { supplierId, boardCode, fabricCode },
      select: { id: true },
    });
    if (duplicate) throw new BadRequestException(`Bảng vải ${boardCode}${fabricCode ? ` / ${fabricCode}` : ""} đã tồn tại ở NCC này.`);
    const colors = Array.isArray(body?.colors) ? body.colors : [];
    const images = Array.isArray(body?.images) ? body.images : [];
    return this.prisma.fabricBoard.create({
      data: {
        supplierId,
        boardCode,
        fabricCode,
        name: this.titleCase(body?.name) || null,
        composition: this.parseCompositionTokens(body?.composition).join(", ") || null,
        expectedGsm: this.n(body?.expectedGsm),
        seasons: Array.from(new Set(this.parseTokens(body?.seasons).filter((x) => SAMPLE_SEASONS.includes(x as any)))),
        productGroups: Array.from(new Set(this.parseTokens(body?.productGroups))),
        coverImageUrl: body?.coverImageUrl || images?.[0]?.url || null,
        note: String(body?.note || "").trim() || null,
        createdById: actor.id,
        createdByName: actor.name,
        colors: { create: colors.filter((x: any) => x?.name).map((x: any) => ({
          name: this.titleCase(x.name),
          code: this.normalizeColorCode(x.code),
          imageUrl: x.imageUrl || null,
          note: x.note || null,
        })) },
        images: { create: images.filter((x: any) => x?.url).map((x: any) => ({
          type: (x.type || "BOARD") as FabricBoardImageType,
          url: x.url,
          caption: x.caption || null,
        })) },
      },
      include: { supplier: true, colors: true, images: true },
    });
  }

  async updateFabricBoard(id: string, body: any) {
    const current = await this.prisma.fabricBoard.findUnique({ where: { id }, include: { colors: true } });
    if (!current) throw new NotFoundException("Không tìm thấy bảng vải.");
    return this.prisma.$transaction(async (tx) => {
      if (Array.isArray(body?.colors)) {
        await tx.fabricBoardColor.deleteMany({ where: { fabricBoardId: id } });
        if (body.colors.length) {
          await tx.fabricBoardColor.createMany({
            data: body.colors.filter((x: any) => x?.name).map((x: any) => ({
              fabricBoardId: id,
              name: this.titleCase(x.name),
              code: this.normalizeColorCode(x.code),
              imageUrl: x.imageUrl || null,
              note: x.note || null,
            })),
          });
        }
      }
      if (Array.isArray(body?.images)) {
        await tx.fabricBoardImage.deleteMany({ where: { fabricBoardId: id } });
        if (body.images.length) {
          await tx.fabricBoardImage.createMany({
            data: body.images.filter((x: any) => x?.url).map((x: any) => ({
              fabricBoardId: id,
              type: (x.type || "BOARD") as FabricBoardImageType,
              url: x.url,
              caption: x.caption || null,
            })),
          });
        }
      }
      return tx.fabricBoard.update({
        where: { id },
        data: {
          ...(body?.supplierId !== undefined ? { supplierId: body.supplierId } : {}),
          ...(body?.boardCode !== undefined ? { boardCode: this.normalizeSampleCode(body.boardCode) } : {}),
          ...(body?.fabricCode !== undefined ? { fabricCode: this.normalizeSampleCode(body.fabricCode) || null } : {}),
          ...(body?.name !== undefined ? { name: this.titleCase(body.name) || null } : {}),
          ...(body?.composition !== undefined ? { composition: this.parseCompositionTokens(body.composition).join(", ") || null } : {}),
          ...(body?.expectedGsm !== undefined ? { expectedGsm: this.n(body.expectedGsm) } : {}),
          ...(body?.seasons !== undefined ? { seasons: Array.from(new Set(this.parseTokens(body.seasons).filter((x) => SAMPLE_SEASONS.includes(x as any)))) } : {}),
          ...(body?.productGroups !== undefined ? { productGroups: Array.from(new Set(this.parseTokens(body.productGroups))) } : {}),
          ...(body?.coverImageUrl !== undefined ? { coverImageUrl: body.coverImageUrl || null } : {}),
          ...(body?.note !== undefined ? { note: body.note || null } : {}),
        },
        include: { supplier: true, colors: true, images: true },
      });
    });
  }

  async deleteFabricBoard(id: string) {
    const found = await this.prisma.fabricBoard.findUnique({
      where: { id },
      include: { _count: { select: { designSamples: true, sampleDispatches: true, fabricReceipts: true } } },
    });
    if (!found) throw new NotFoundException("Không tìm thấy bảng vải.");
    const used = found._count.designSamples + found._count.sampleDispatches + found._count.fabricReceipts;
    if (used > 0) throw new BadRequestException("Bảng vải đã có lịch sử sử dụng/gửi mẫu/vải về nên không thể xoá. Có thể ngừng dùng thay vì xoá.");
    return this.prisma.fabricBoard.delete({ where: { id } });
  }

  // -------------------------
  // SAMPLE DEVELOPMENT
  // -------------------------
  async checkSampleCode(codeInput: any, excludeId?: string) {
    const code = this.normalizeSampleCode(codeInput);
    if (!code) return { available: true, code: "", source: null, message: "" };
    const sample = await this.prisma.designSample.findFirst({
      where: { code: { equals: code, mode: "insensitive" }, ...(excludeId ? { id: { not: excludeId } } : {}) },
      select: { id: true, code: true, name: true },
    });
    if (sample) return { available: false, code, source: "design_sample", message: `Mã ${code} đã có trong Triển khai mẫu (${sample.name}).` };
    const product = await this.prisma.product.findFirst({
      where: { OR: [
        { slug: { equals: code, mode: "insensitive" } },
        { variants: { some: { sku: { equals: code, mode: "insensitive" } } } },
        { variants: { some: { sku: { startsWith: `${code}-`, mode: "insensitive" } } } },
      ] },
      select: { id: true, name: true, slug: true },
    });
    if (product) return { available: false, code, source: "product", message: `Mã ${code} đã tồn tại trong danh sách sản phẩm (${product.name}).` };
    return { available: true, code, source: null, message: `Mã ${code} có thể sử dụng.` };
  }

  private async assertSampleCodeAvailable(code: string, excludeId?: string) {
    const result = await this.checkSampleCode(code, excludeId);
    if (!result.available) throw new BadRequestException(result.message);
  }

  async sampleMeta() {
    const [staff, boards, factories, samplePeople, vocab] = await Promise.all([
      this.prisma.staffUser.findMany({
        where: { isActive: true },
        select: { id: true, code: true, name: true, branchId: true },
        orderBy: { name: "asc" },
      }),
      this.prisma.fabricBoard.findMany({
        where: { isActive: true },
        include: { supplier: { select: { id: true, code: true, name: true } }, colors: { orderBy: { createdAt: "asc" } } },
        orderBy: { updatedAt: "desc" },
      }),
      this.prisma.supplier.findMany({
        where: { isActive: true },
        select: { id: true, code: true, name: true, phone: true },
        orderBy: [{ name: "asc" }, { code: "asc" }],
      }),
      this.prisma.sampleTechnicalPerson.findMany({
        where: { isActive: true },
        orderBy: [{ role: "asc" }, { name: "asc" }],
      }),
      this.productGroupsAndCompositions(),
    ]);
    return { staff, boards, factories, samplePeople, seasons: SAMPLE_SEASONS, ...vocab };
  }

  async listIdeaBoards() {
    return this.prisma.designSampleIdeaBoard.findMany({
      include: {
        samples: {
          include: {
            designSample: {
              select: {
                id: true,
                code: true,
                name: true,
                year: true,
                category: true,
                status: true,
                coverImageUrl: true,
                createdAt: true,
                updatedAt: true,
                images: { select: { id: true, type: true, url: true, caption: true }, orderBy: { createdAt: "desc" }, take: 4 },
              },
            },
          },
          orderBy: [{ sortOrder: "asc" }, { createdAt: "desc" }],
        },
      },
      orderBy: [{ updatedAt: "desc" }, { name: "asc" }],
    });
  }

  async createIdeaBoard(body: any, user?: Actor) {
    const name = String(body?.name || "").trim();
    if (!name) throw new BadRequestException("Thiếu tên bảng ý tưởng.");
    const duplicate = await this.prisma.designSampleIdeaBoard.findFirst({
      where: { name: { equals: name, mode: "insensitive" } },
      select: { id: true },
    });
    if (duplicate) throw new BadRequestException("Tên bảng ý tưởng đã tồn tại.");
    const actor = this.actor(user);
    return this.prisma.designSampleIdeaBoard.create({
      data: {
        name,
        description: String(body?.description || "").trim() || null,
        createdById: actor.id,
        createdByName: actor.name,
      },
    });
  }

  async updateIdeaBoard(id: string, body: any) {
    const found = await this.prisma.designSampleIdeaBoard.findUnique({ where: { id } });
    if (!found) throw new NotFoundException("Không tìm thấy bảng ý tưởng.");
    const name = body?.name !== undefined ? String(body.name || "").trim() : found.name;
    if (!name) throw new BadRequestException("Thiếu tên bảng ý tưởng.");
    const duplicate = await this.prisma.designSampleIdeaBoard.findFirst({
      where: { name: { equals: name, mode: "insensitive" }, NOT: { id } },
      select: { id: true },
    });
    if (duplicate) throw new BadRequestException("Tên bảng ý tưởng đã tồn tại.");
    return this.prisma.designSampleIdeaBoard.update({
      where: { id },
      data: {
        ...(body?.name !== undefined ? { name } : {}),
        ...(body?.description !== undefined ? { description: String(body.description || "").trim() || null } : {}),
      },
    });
  }

  async deleteIdeaBoard(id: string) {
    const found = await this.prisma.designSampleIdeaBoard.findUnique({ where: { id }, select: { id: true } });
    if (!found) throw new NotFoundException("Không tìm thấy bảng ý tưởng.");
    await this.prisma.designSampleIdeaBoard.delete({ where: { id } });
    return { success: true, id };
  }

  async setSampleIdeaBoards(sampleId: string, body: any) {
    const sample = await this.prisma.designSample.findUnique({ where: { id: sampleId }, select: { id: true } });
    if (!sample) throw new NotFoundException("Không tìm thấy mẫu.");
    const boardIds = Array.from(new Set((Array.isArray(body?.boardIds) ? body.boardIds : []).map((x: any) => String(x || "").trim()).filter(Boolean))) as string[];
    if (boardIds.length) {
      const found = await this.prisma.designSampleIdeaBoard.findMany({ where: { id: { in: boardIds } }, select: { id: true } });
      if (found.length !== boardIds.length) throw new BadRequestException("Có bảng ý tưởng không còn tồn tại.");
    }
    await this.prisma.$transaction(async (tx) => {
      await tx.designSampleIdeaBoardItem.deleteMany({ where: { designSampleId: sampleId } });
      if (boardIds.length) {
        await tx.designSampleIdeaBoardItem.createMany({
          data: boardIds.map((boardId, index) => ({ boardId, designSampleId: sampleId, sortOrder: index })),
          skipDuplicates: true,
        });
      }
    });
    return this.prisma.designSample.findUnique({
      where: { id: sampleId },
      include: { ideaBoards: { include: { board: true }, orderBy: { createdAt: "asc" } } },
    });
  }

  async listSamplePeople() {
    return this.prisma.sampleTechnicalPerson.findMany({
      where: { isActive: true },
      orderBy: [{ role: "asc" }, { name: "asc" }],
    });
  }

  async createSamplePerson(body: any) {
    const name = String(body?.name || "").trim();
    if (!name) throw new BadRequestException("Thiếu tên người.");
    const role = String(body?.role || "").trim().toUpperCase();
    if (!["SAMPLE_MAKER","PATTERN_MAKER","BOTH"].includes(role)) throw new BadRequestException("Vai trò người ra mẫu / thiết kế rập không hợp lệ.");
    const existing = await this.prisma.sampleTechnicalPerson.findFirst({
      where: { name: { equals: name, mode: "insensitive" }, role: role as any, isActive: true },
    });
    if (existing) return existing;
    return this.prisma.sampleTechnicalPerson.create({
      data: {
        name,
        role: role as any,
        phone: String(body?.phone || "").trim() || null,
        note: String(body?.note || "").trim() || null,
      },
    });
  }

  private async sampleTechnicalPersonSnapshot(id?: string | null, allowedRoles: string[] = []) {
    const clean = String(id || "").trim();
    if (!clean) return null;
    const person = await this.prisma.sampleTechnicalPerson.findFirst({ where: { id: clean, isActive: true } });
    if (!person) throw new BadRequestException("Người ra mẫu / thiết kế rập không còn tồn tại.");
    if (allowedRoles.length && person.role !== "BOTH" && !allowedRoles.includes(person.role)) throw new BadRequestException("Người được chọn không đúng vai trò.");
    return person;
  }

  private async sampleBoardSnapshot(fabricBoardId?: string | null) {
    if (!fabricBoardId) return null;
    return this.prisma.fabricBoard.findUnique({
      where: { id: fabricBoardId },
      include: { supplier: { select: { id: true } } },
    });
  }

  private async sampleFactorySnapshot(sampleFactoryId?: string | null) {
    const id = String(sampleFactoryId || "").trim();
    if (!id) return null;
    const supplier = await this.prisma.supplier.findFirst({
      where: { id, isActive: true },
      select: {
        id: true,
        code: true,
        name: true,
        phone: true,
        email: true,
        address: true,
      },
    });
    if (!supplier) throw new BadRequestException("Nhà may làm mẫu không còn tồn tại hoặc đã ngưng dùng.");
    return supplier;
  }

  async listSamples(query?: { q?: string; year?: string; status?: string; fabricBoardId?: string }) {
    const q = String(query?.q || "").trim();
    const year = Number(query?.year || 0);
    const status = String(query?.status || "").trim() as DesignSampleStatus;
    const rows = await this.prisma.designSample.findMany({
      where: {
        ...(year ? { year } : {}),
        ...(status ? { status } : {}),
        ...(query?.fabricBoardId ? { fabricBoardId: query.fabricBoardId } : {}),
        ...(q ? { OR: [
          { code: { contains: q, mode: "insensitive" } },
          { name: { contains: q, mode: "insensitive" } },
          { category: { contains: q, mode: "insensitive" } },
          { fabricBoard: { boardCode: { contains: q, mode: "insensitive" } } },
          { fabricBoard: { fabricCode: { contains: q, mode: "insensitive" } } },
          { sampleDispatches: { some: { recipientName: { contains: q, mode: "insensitive" } } } },
        ] } : {}),
      },
      include: {
        fabricBoard: { include: { supplier: { select: { id: true, code: true, name: true } } } },
        fabricColor: true,
        producedProduct: { select: { id: true, name: true, slug: true, imageUrl: true } },
        colors: { orderBy: { createdAt: "asc" } },
        images: { orderBy: { createdAt: "desc" } },
        progressLogs: { orderBy: { createdAt: "desc" }, take: 12 },
        sampleDispatches: { include: { fabricColor: true }, orderBy: { sentAt: "desc" } },
        ideaBoards: { include: { board: true }, orderBy: { createdAt: "asc" } },
        _count: { select: { fabricReceipts: true } },
      },
      orderBy: [{ year: "desc" }, { updatedAt: "desc" }],
    });

    const missingCodes = rows.filter((x) => !x.producedProductId).map((x) => x.code);
    const matches = missingCodes.length ? await this.prisma.product.findMany({
      where: { OR: missingCodes.flatMap((code) => [
        { slug: { equals: code, mode: "insensitive" as const } },
        { variants: { some: { sku: { startsWith: `${code}-`, mode: "insensitive" as const } } } },
      ]) },
      select: { id: true, name: true, slug: true, imageUrl: true, variants: { select: { sku: true }, take: 10 } },
    }) : [];

    return rows.map((row) => {
      const matchedProduct = row.producedProduct || matches.find((product) =>
        product.slug.toUpperCase() === row.code.toUpperCase() ||
        product.variants.some((variant) => String(variant.sku || "").toUpperCase().startsWith(`${row.code.toUpperCase()}-`)),
      ) || null;
      return { ...row, matchedProduct };
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
    if (season && !SAMPLE_SEASONS.includes(season as any)) throw new BadRequestException("Mùa / BST không hợp lệ.");
    const status = (body?.status || "IDEA") as DesignSampleStatus;
    const board = await this.sampleBoardSnapshot(body?.fabricBoardId);
    const sampleFactory = await this.sampleFactorySnapshot(body?.sampleFactoryId);
    const sampleMaker = await this.sampleTechnicalPersonSnapshot(body?.sampleMakerId, ["SAMPLE_MAKER"]);
    const patternMaker = await this.sampleTechnicalPersonSnapshot(body?.patternMakerId, ["PATTERN_MAKER"]);
    const images = Array.isArray(body?.images) ? body.images : [];
    return this.prisma.designSample.create({
      data: {
        code, name,
        year: Number(body?.year || new Date().getFullYear()),
        season: season || null,
        category: this.titleCase(body?.category) || null,
        fabricBoardId: body?.fabricBoardId || null,
        fabricColorId: body?.fabricColorId || null,
        fabricColorName: this.titleCase(body?.fabricColorName) || null,
        fabricColorCode: this.normalizeColorCode(body?.fabricColorCode),
        sampleFactoryId: sampleFactory?.id || null,
        sampleFactoryName: sampleFactory?.name || null,
        sampleMakerId: sampleMaker?.id || null,
        sampleMakerName: sampleMaker?.name || null,
        patternMakerId: patternMaker?.id || null,
        patternMakerName: patternMaker?.name || null,
        supplierId: body?.supplierId || board?.supplierId || null,
        fabricBoardCode: this.normalizeSampleCode(body?.fabricBoardCode) || board?.boardCode || null,
        fabricCode: this.normalizeSampleCode(body?.fabricCode) || board?.fabricCode || null,
        fabricComposition: String(body?.fabricComposition || "").trim() || board?.composition || null,
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
        images: { create: images.filter((x: any) => x?.url).map((x: any) => ({
          type: (x.type || "SAMPLE") as DesignSampleImageType,
          url: x.url,
          caption: x.caption || null,
        })) },
        progressLogs: { create: { toStatus: status, note: "Tạo mẫu", actorId: actor.id, actorName: actor.name } },
      },
      include: { fabricBoard: true, fabricColor: true, images: true, progressLogs: true, sampleDispatches: true, ideaBoards: { include: { board: true } } },
    });
  }

  async createQuickSample(body: any, user?: Actor) {
    const sampleImageUrl = String(body?.sampleImageUrl || "").trim();
    const fabricReferenceImageUrl = String(body?.fabricReferenceImageUrl || "").trim();
    return this.createSample({
      name: body?.name,
      code: body?.code,
      year: body?.year || new Date().getFullYear(),
      season: body?.season || null,
      category: body?.category || null,
      status: body?.status || "IDEA",
      note: body?.note || null,
      coverImageUrl: sampleImageUrl || fabricReferenceImageUrl || null,
      images: [
        ...(sampleImageUrl ? [{ type: "SAMPLE", url: sampleImageUrl, caption: "Ảnh mẫu cần làm" }] : []),
        ...(fabricReferenceImageUrl ? [{ type: "REFERENCE", url: fabricReferenceImageUrl, caption: "Ảnh bảng vải chụp nhanh" }] : []),
      ],
    }, user);
  }

  async updateSample(id: string, body: any, user?: Actor) {
    const current = await this.prisma.designSample.findUnique({ where: { id } });
    if (!current) throw new NotFoundException("Không tìm thấy mẫu.");
    const actor = this.actor(user);
    const nextStatus = (body?.status || current.status) as DesignSampleStatus;
    const nextCode = body?.code !== undefined ? this.normalizeSampleCode(body.code) : current.code;
    if (!nextCode) throw new BadRequestException("Thiếu mã mẫu.");
    if (nextCode !== current.code) await this.assertSampleCodeAvailable(nextCode, id);
    if (body?.season !== undefined) {
      const season = String(body.season || "").trim();
      if (season && !SAMPLE_SEASONS.includes(season as any)) throw new BadRequestException("Mùa / BST không hợp lệ.");
    }
    const board = body?.fabricBoardId !== undefined ? await this.sampleBoardSnapshot(body.fabricBoardId) : null;
    const sampleFactory = body?.sampleFactoryId !== undefined
      ? await this.sampleFactorySnapshot(body.sampleFactoryId)
      : undefined;
    const sampleMaker = body?.sampleMakerId !== undefined
      ? await this.sampleTechnicalPersonSnapshot(body.sampleMakerId, ["SAMPLE_MAKER"])
      : undefined;
    const patternMaker = body?.patternMakerId !== undefined
      ? await this.sampleTechnicalPersonSnapshot(body.patternMakerId, ["PATTERN_MAKER"])
      : undefined;
    return this.prisma.$transaction(async (tx) => {
      if (Array.isArray(body?.images)) {
        await tx.designSampleImage.deleteMany({ where: { designSampleId: id } });
        if (body.images.length) await tx.designSampleImage.createMany({
          data: body.images.filter((x: any) => x?.url).map((x: any) => ({
            designSampleId: id,
            type: (x.type || "SAMPLE") as DesignSampleImageType,
            url: x.url,
            caption: x.caption || null,
          })),
        });
      }
      const updated = await tx.designSample.update({
        where: { id },
        data: {
          ...(body?.code !== undefined ? { code: nextCode } : {}),
          ...(body?.name !== undefined ? { name: String(body.name).trim() } : {}),
          ...(body?.year !== undefined ? { year: Number(body.year) } : {}),
          ...(body?.season !== undefined ? { season: body.season || null } : {}),
          ...(body?.category !== undefined ? { category: this.titleCase(body.category) || null } : {}),
          ...(body?.fabricBoardId !== undefined ? { fabricBoardId: body.fabricBoardId || null } : {}),
          ...(body?.fabricColorId !== undefined ? { fabricColorId: body.fabricColorId || null } : {}),
          ...(body?.fabricColorName !== undefined ? { fabricColorName: this.titleCase(body.fabricColorName) || null } : {}),
          ...(body?.fabricColorCode !== undefined ? { fabricColorCode: this.normalizeColorCode(body.fabricColorCode) } : {}),
          ...(body?.sampleFactoryId !== undefined ? {
            sampleFactoryId: sampleFactory?.id || null,
            sampleFactoryName: sampleFactory?.name || null,
          } : {}),
          ...(body?.sampleMakerId !== undefined ? {
            sampleMakerId: sampleMaker?.id || null,
            sampleMakerName: sampleMaker?.name || null,
          } : {}),
          ...(body?.patternMakerId !== undefined ? {
            patternMakerId: patternMaker?.id || null,
            patternMakerName: patternMaker?.name || null,
          } : {}),
          ...(body?.supplierId !== undefined || body?.fabricBoardId !== undefined ? { supplierId: body?.supplierId || board?.supplierId || null } : {}),
          ...(body?.fabricBoardCode !== undefined || body?.fabricBoardId !== undefined ? { fabricBoardCode: this.normalizeSampleCode(body?.fabricBoardCode) || board?.boardCode || null } : {}),
          ...(body?.fabricCode !== undefined || body?.fabricBoardId !== undefined ? { fabricCode: this.normalizeSampleCode(body?.fabricCode) || board?.fabricCode || null } : {}),
          ...(body?.fabricComposition !== undefined || body?.fabricBoardId !== undefined ? { fabricComposition: String(body?.fabricComposition || "").trim() || board?.composition || null } : {}),
          ...(body?.producedProductId !== undefined ? { producedProductId: body.producedProductId || null } : {}),
          ...(body?.status !== undefined ? { status: nextStatus } : {}),
          ...(body?.assigneeStaffId !== undefined ? { assigneeStaffId: body.assigneeStaffId || null } : {}),
          ...(body?.assigneeName !== undefined ? { assigneeName: body.assigneeName || null } : {}),
          ...(body?.nextAction !== undefined ? { nextAction: body.nextAction || null } : {}),
          ...(body?.dueDate !== undefined ? { dueDate: body.dueDate ? new Date(body.dueDate) : null } : {}),
          ...(body?.coverImageUrl !== undefined ? { coverImageUrl: body.coverImageUrl || null } : {}),
          ...(body?.note !== undefined ? { note: body.note || null } : {}),
          ...(body?.technicalNote !== undefined ? { technicalNote: body.technicalNote || null } : {}),
        },
        include: { fabricBoard: true, fabricColor: true, images: true, progressLogs: true, sampleDispatches: true, ideaBoards: { include: { board: true } } },
      });
      if (nextStatus !== current.status) {
        await tx.designSampleProgressLog.create({
          data: { designSampleId: id, fromStatus: current.status, toStatus: nextStatus, note: body?.progressNote || body?.nextAction || null, actorId: actor.id, actorName: actor.name },
        });
      }
      return updated;
    });
  }

  async deleteSample(id: string) {
    const found = await this.prisma.designSample.findUnique({
      where: { id },
      include: { _count: { select: { fabricReceipts: true } } },
    });
    if (!found) throw new NotFoundException("Không tìm thấy mẫu.");
    if (found._count.fabricReceipts > 0) throw new BadRequestException("Mẫu đã có phiếu vải về, không thể xoá.");
    return this.prisma.designSample.delete({ where: { id } });
  }

  async addSampleImage(id: string, body: any) {
    const url = String(body?.url || "").trim();
    if (!url) throw new BadRequestException("Thiếu URL ảnh.");
    return this.prisma.designSampleImage.create({
      data: {
        designSampleId: id,
        type: (body?.type || "SAMPLE") as DesignSampleImageType,
        url,
        caption: body?.caption || null,
      },
    });
  }

  // -------------------------
  // GỬI ĐI LÀM MẪU
  // -------------------------
  async listSampleDispatches(query?: { q?: string; status?: string; fabricBoardId?: string }) {
    const q = String(query?.q || "").trim();
    const status = String(query?.status || "").trim() as FabricSampleDispatchStatus;
    return this.prisma.fabricSampleDispatch.findMany({
      where: {
        ...(status ? { status } : {}),
        ...(query?.fabricBoardId ? { fabricBoardId: query.fabricBoardId } : {}),
        ...(q ? { OR: [
          { recipientName: { contains: q, mode: "insensitive" } },
          { sentByName: { contains: q, mode: "insensitive" } },
          { designSample: { code: { contains: q, mode: "insensitive" } } },
          { designSample: { name: { contains: q, mode: "insensitive" } } },
          { fabricBoard: { boardCode: { contains: q, mode: "insensitive" } } },
          { fabricBoard: { fabricCode: { contains: q, mode: "insensitive" } } },
          { fabricColor: { code: { contains: q, mode: "insensitive" } } },
          { colorCode: { contains: q, mode: "insensitive" } },
          { colorName: { contains: q, mode: "insensitive" } },
        ] } : {}),
      },
      include: {
        designSample: { include: { images: { orderBy: { createdAt: "desc" } } } },
        fabricBoard: { include: { supplier: { select: { id: true, code: true, name: true } } } },
        fabricColor: true,
      },
      orderBy: { sentAt: "desc" },
    });
  }

  async createSampleDispatch(body: any, user?: Actor) {
    const actor = this.actor(user);
    const fabricBoardId = String(body?.fabricBoardId || "").trim();
    const sampleFactory = await this.sampleFactorySnapshot(body?.sampleFactoryId);
    const recipientName = this.titleCase(sampleFactory?.name || body?.recipientName);
    const recipientContact = sampleFactory?.phone || body?.recipientContact || null;
    if (!fabricBoardId) throw new BadRequestException("Chưa chọn bảng vải.");
    if (!recipientName) throw new BadRequestException("Chưa chọn nhà may / nhà cung cấp làm mẫu.");
    let designSampleId = String(body?.designSampleId || "").trim() || null;

    if (!designSampleId) {
      const sampleName = String(body?.sampleName || "").trim();
      if (!sampleName) throw new BadRequestException("Thiếu tên mẫu.");
      let sampleCode = this.normalizeSampleCode(body?.sampleCode);
      if (!sampleCode) sampleCode = await this.nextCode("MS", "sample");
      await this.assertSampleCodeAvailable(sampleCode);
      const board = await this.sampleBoardSnapshot(fabricBoardId);
      const sample = await this.prisma.designSample.create({
        data: {
          code: sampleCode,
          name: sampleName,
          year: Number(body?.year || new Date().getFullYear()),
          season: body?.season || null,
          category: this.titleCase(body?.category) || null,
          fabricBoardId,
          fabricColorId: body?.fabricColorId || null,
          fabricColorName: this.titleCase(body?.colorName) || null,
          fabricColorCode: this.normalizeColorCode(body?.colorCode),
          sampleFactoryId: sampleFactory?.id || null,
          sampleFactoryName: recipientName,
          supplierId: board?.supplierId || null,
          fabricBoardCode: board?.boardCode || null,
          fabricCode: board?.fabricCode || null,
          fabricComposition: board?.composition || null,
          status: "SAMPLING",
          assigneeStaffId: body?.assigneeStaffId || null,
          assigneeName: body?.assigneeName || null,
          dueDate: body?.dueDate ? new Date(body.dueDate) : null,
          nextAction: `Đang làm mẫu tại ${recipientName}`,
          createdById: actor.id,
          createdByName: actor.name,
          progressLogs: { create: { toStatus: "SAMPLING", note: `Gửi làm mẫu: ${recipientName}`, actorId: actor.id, actorName: actor.name } },
        },
      });
      designSampleId = sample.id;
    }

    const designSample = await this.prisma.designSample.findUnique({ where: { id: designSampleId } });
    if (!designSample) throw new NotFoundException("Không tìm thấy mẫu triển khai.");

    const dispatch = await this.prisma.fabricSampleDispatch.create({
      data: {
        designSampleId,
        fabricBoardId,
        fabricColorId: body?.fabricColorId || designSample.fabricColorId || null,
        colorName: this.titleCase(body?.colorName || designSample.fabricColorName) || null,
        colorCode: this.normalizeColorCode(body?.colorCode || designSample.fabricColorCode),
        recipientName,
        recipientType: this.titleCase(body?.recipientType) || null,
        recipientContact,
        sentAt: body?.sentAt ? new Date(body.sentAt) : new Date(),
        sentById: body?.sentById || actor.id,
        sentByName: body?.sentByName || actor.name,
        dueDate: body?.dueDate ? new Date(body.dueDate) : null,
        status: (body?.status || "SENT") as FabricSampleDispatchStatus,
        note: body?.note || null,
      },
      include: { designSample: true, fabricBoard: true, fabricColor: true },
    });

    await this.prisma.designSample.update({
      where: { id: designSampleId },
      data: {
        status: "SAMPLING",
        sampleFactoryId: sampleFactory?.id || designSample.sampleFactoryId || null,
        sampleFactoryName: recipientName,
        fabricColorName: this.titleCase(body?.colorName || designSample.fabricColorName) || null,
        fabricColorCode: this.normalizeColorCode(body?.colorCode || designSample.fabricColorCode),
        nextAction: `Đang làm mẫu tại ${recipientName}`,
        dueDate: body?.dueDate ? new Date(body.dueDate) : designSample.dueDate,
      },
    });
    return dispatch;
  }

  async updateSampleDispatch(id: string, body: any) {
    const found = await this.prisma.fabricSampleDispatch.findUnique({ where: { id } });
    if (!found) throw new NotFoundException("Không tìm thấy lần gửi mẫu.");
    const status = (body?.status || found.status) as FabricSampleDispatchStatus;
    const updated = await this.prisma.fabricSampleDispatch.update({
      where: { id },
      data: {
        ...(body?.recipientName !== undefined ? { recipientName: this.titleCase(body.recipientName) } : {}),
        ...(body?.recipientType !== undefined ? { recipientType: this.titleCase(body.recipientType) || null } : {}),
        ...(body?.recipientContact !== undefined ? { recipientContact: body.recipientContact || null } : {}),
        ...(body?.colorName !== undefined ? { colorName: this.titleCase(body.colorName) || null } : {}),
        ...(body?.colorCode !== undefined ? { colorCode: this.normalizeColorCode(body.colorCode) } : {}),
        ...(body?.sentAt !== undefined ? { sentAt: new Date(body.sentAt) } : {}),
        ...(body?.sentById !== undefined ? { sentById: body.sentById || null } : {}),
        ...(body?.sentByName !== undefined ? { sentByName: body.sentByName || null } : {}),
        ...(body?.dueDate !== undefined ? { dueDate: body.dueDate ? new Date(body.dueDate) : null } : {}),
        ...(body?.returnedAt !== undefined ? { returnedAt: body.returnedAt ? new Date(body.returnedAt) : null } : {}),
        ...(body?.status !== undefined ? { status } : {}),
        ...(body?.note !== undefined ? { note: body.note || null } : {}),
      },
      include: { designSample: true, fabricBoard: true, fabricColor: true },
    });
    const sampleStatus: DesignSampleStatus =
      status === "APPROVED" ? "APPROVED_FOR_PRODUCTION" :
      status === "REVISING" ? "REVISING" :
      status === "RETURNED" ? "SAMPLE_READY" :
      status === "MAKING" || status === "RECEIVED" || status === "SENT" ? "SAMPLING" :
      "ON_HOLD";
    await this.prisma.designSample.update({
      where: { id: found.designSampleId },
      data: { status: sampleStatus },
    });
    return updated;
  }

  async deleteSampleDispatch(id: string) {
    const found = await this.prisma.fabricSampleDispatch.findUnique({ where: { id } });
    if (!found) throw new NotFoundException("Không tìm thấy lần gửi mẫu.");
    return this.prisma.fabricSampleDispatch.delete({ where: { id } });
  }

  // -------------------------
  // FABRIC RECEIPTS
  // -------------------------
  async fabricMeta(user?: any) {
    const [suppliers, branches, staff, samples, products, boards] = await Promise.all([
      this.listFabricSuppliers(user),
      this.prisma.branch.findMany({ where: { isActive: true }, select: { id: true, name: true }, orderBy: { name: "asc" } }),
      this.prisma.staffUser.findMany({ where: { isActive: true }, select: { id: true, code: true, name: true, branchId: true }, orderBy: { name: "asc" } }),
      this.prisma.designSample.findMany({ select: { id: true, code: true, name: true, year: true, fabricBoardId: true, fabricColorId: true, fabricColorName: true, fabricColorCode: true }, orderBy: [{ year: "desc" }, { updatedAt: "desc" }] }),
      this.prisma.product.findMany({ select: { id:true,name:true,slug:true,imageUrl:true,status:true }, orderBy: { updatedAt:"desc" }, take: 500 }),
      this.prisma.fabricBoard.findMany({ where: { isActive: true }, include: { supplier: true, colors: true }, orderBy: { updatedAt: "desc" } }),
    ]);
    const safeBoards = boards.map((board: any) => ({ ...board, supplier: this.supplierForUser(board.supplier, user) }));
    return { suppliers, branches, staff, samples, products, boards: safeBoards };
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
          { receivedByName: { contains: q, mode: "insensitive" } },
          { note: { contains: q, mode: "insensitive" } },
          { supplier: { is: { code: { contains: q, mode: "insensitive" } } } },
          { supplier: { is: { name: { contains: q, mode: "insensitive" } } } },
          { branch: { is: { name: { contains: q, mode: "insensitive" } } } },
          { designSample: { is: { code: { contains: q, mode: "insensitive" } } } },
          { designSample: { is: { name: { contains: q, mode: "insensitive" } } } },
          { fabricConfigs: { some: { OR: [
            { fabricCode: { contains: q, mode: "insensitive" } },
            { materialName: { contains: q, mode: "insensitive" } },
            { product: { is: { name: { contains: q, mode: "insensitive" } } } },
            { designSample: { is: { code: { contains: q, mode: "insensitive" } } } },
            { designSample: { is: { name: { contains: q, mode: "insensitive" } } } },
          ] } } },
          { rolls: { some: { OR: [
            { rollCode: { contains: q, mode: "insensitive" } },
            { fabricCode: { contains: q, mode: "insensitive" } },
            { colorName: { contains: q, mode: "insensitive" } },
            { colorCode: { contains: q, mode: "insensitive" } },
            { defectNote: { contains: q, mode: "insensitive" } },
          ] } } },
        ] } : {}),
      },
      include: {
        supplier: { select: { id: true, code: true, name: true } },
        branch: { select: { id: true, name: true } },
        designSample: { select: { id: true, code: true, name: true, year: true } },
        product: { select: { id:true,name:true,slug:true,imageUrl:true } },
        fabricBoard: { select: { id: true, boardCode: true, fabricCode: true, name: true } },
        fabricColor: { select: { id: true, name: true, code: true } },
        rolls: { include: { images: { orderBy: { createdAt: "desc" } } }, orderBy: [{ sortOrder: "asc" }, { createdAt: "asc" }] },
        fabricCosts: { orderBy: { fabricCode: "asc" } },
        fabricConfigs: { include: { supplier:true, product: { select: { id:true,name:true,slug:true,imageUrl:true } }, designSample: { select: { id:true,code:true,name:true,year:true } } }, orderBy: { fabricCode: "asc" } },
        colorMaps: { orderBy: [{ fabricCode: "asc" }, { colorName: "asc" }] },
        measurements: { orderBy: { createdAt: "desc" } },
        images: { orderBy: { createdAt: "desc" } },
      },
      orderBy: { updatedAt: "desc" },
    });
    return rows.map((row: any) => this.receiptForUser(row, user));
  }

  private receiptTotalsFromRolls(rolls:any[],body:any){
    const sum=(key:string)=>{
      const entered=rolls.filter(x=>x?.[key]!==null&&x?.[key]!==undefined&&String(x[key]).trim()!=="");
      return entered.length?entered.reduce((total,x)=>total+Number(this.n(x[key])||0),0):this.n(body?.[key]);
    };
    return {supplierDeclaredM:sum("supplierDeclaredM"),supplierDeclaredKg:sum("supplierDeclaredKg"),actualM:sum("actualM"),actualKg:sum("actualKg")};
  }

  private async receiptReceiverSnapshot(staffId?:string|null){
    const id=String(staffId||"").trim();if(!id)return null;
    const row=await this.prisma.staffUser.findFirst({where:{id,isActive:true},select:{id:true,name:true}});
    if(!row)throw new BadRequestException("Nhân viên nhận vải không tồn tại hoặc đã ngừng hoạt động.");
    return row;
  }

  async createFabricReceipt(body: any, user?: Actor) {
    const actor = this.actor(user);
    const receiptCode = String(body?.receiptCode || "").trim() || await this.nextFabricReceiptCode(body?.receivedAt);
    const rolls = Array.isArray(body?.rolls) ? body.rolls : [];
    const status = (body?.status || "DRAFT") as FabricReceiptStatus;
    const canLinkBoard = this.userHas(user, "fabric_receipt.fabric_board_link");
    const board = canLinkBoard && body?.fabricBoardId ? await this.prisma.fabricBoard.findUnique({ where: { id: body.fabricBoardId } }) : null;
    const totals=this.receiptTotalsFromRolls(rolls,body);
    const receiver=await this.receiptReceiverSnapshot(body?.receivedByStaffId);
    const canEditCost=this.userHas(user, "fabric_receipt.cost.edit");
    const color = canLinkBoard && body?.fabricColorId ? await this.prisma.fabricBoardColor.findUnique({ where: { id: body.fabricColorId } }) : null;
    const created = await this.prisma.fabricReceipt.create({
      data: {
        receiptCode,
        designSampleId: body?.designSampleId || null,
        productId: body?.productId || null,
        fabricBoardId: canLinkBoard ? (body?.fabricBoardId || null) : null,
        fabricColorId: canLinkBoard ? (body?.fabricColorId || null) : null,
        supplierId: body?.supplierId || board?.supplierId || null,
        branchId: body?.branchId || null,
        fabricBoardCode: String(body?.fabricBoardCode || board?.boardCode || "").replace(/[^A-Za-z0-9]/g,"").toUpperCase() || null,
        fabricCode: body?.fabricCode || board?.fabricCode || null,
        fabricName: body?.fabricName || board?.name || null,
        colorName: body?.colorName || color?.name || null,
        colorCode: this.normalizeColorCodes(body?.colorCode || color?.code),
        lotCode: body?.lotCode || null,
        supplierDeclaredM: totals.supplierDeclaredM,
        supplierDeclaredKg: totals.supplierDeclaredKg,
        actualM: totals.actualM,
        actualKg: totals.actualKg,
        rollCount: Number(body?.rollCount || rolls.length || 0),
        expectedGsm: this.n(body?.expectedGsm) ?? board?.expectedGsm ?? null,
        status,
        receivedAt: body?.receivedAt ? new Date(body.receivedAt) : null,
        note: body?.note || null,
        receivedByStaffId: receiver?.id || null,
        receivedByName: receiver?.name || null,
        createdById: actor.id,
        createdByName: actor.name,
        rolls: { create: rolls.map((x: any,index:number) => ({
          sortOrder: Number(x.sortOrder || index + 1),
          fabricCode: String(x.fabricCode || body?.fabricCode || "").trim().toUpperCase() || null,
          rollCode: x.rollCode || null,
          colorName: String(x.colorName || body?.colorName || color?.name || "").trim() || null,
          colorCode: this.normalizeColorCode(x.colorCode || body?.colorCode || color?.code),
          supplierDeclaredM: this.n(x.supplierDeclaredM),
          supplierDeclaredKg: this.n(x.supplierDeclaredKg),
          actualM: this.n(x.actualM),
          actualKg: this.n(x.actualKg),
          measuredGsm: this.n(x.measuredGsm),
          unitPriceCny: canEditCost ? this.n(x.unitPriceCny) : null,
          priceUnit: canEditCost && ["METER","KG","ROLL"].includes(String(x.priceUnit||"METER").toUpperCase()) ? String(x.priceUnit||"METER").toUpperCase() as any : "METER",
          defectNote: x.defectNote || null,
          passed: x.passed !== false,
        })) },
        fabricCosts: canEditCost && Array.isArray(body?.fabricCosts) ? { create: body.fabricCosts.map((x:any)=>({
          fabricCode:String(x.fabricCode||"").trim().toUpperCase(),
          chinaShippingCny:this.n(x.chinaShippingCny),
          vietnamShippingRateVndPerKg:this.n(x.vietnamShippingRateVndPerKg),
          vietnamShippingVnd:this.n(x.vietnamShippingVnd),
          note:String(x.note||"").trim()||null,
        })).filter((x:any)=>x.fabricCode) } : undefined,
        fabricConfigs: Array.isArray(body?.fabricConfigs) ? { create: body.fabricConfigs.map((x:any)=>({
          fabricCode:String(x.fabricCode||"").trim().toUpperCase(),
          materialName:String(x.materialName||"").trim()||null,
          supplierId:x.supplierId||null,
          fabricBoardCode:String(x.fabricBoardCode||"").replace(/[^A-Za-z0-9]/g,"").toUpperCase()||null,
          fabricWidthCm:this.n(x.fabricWidthCm),
          productId:x.productId||null,
          designSampleId:x.designSampleId||null,
        })).filter((x:any)=>x.fabricCode) } : undefined,
        colorMaps: Array.isArray(body?.colorMaps) ? { create: body.colorMaps.map((x:any)=>({
          fabricCode:String(x.fabricCode||"").trim().toUpperCase(),
          colorName:String(x.colorName||"").trim(),
          colorCode:this.normalizeColorCode(x.colorCode),
        })).filter((x:any)=>x.fabricCode&&x.colorName) } : undefined,
      },
      include: { supplier: true, branch: true, designSample: true, product: true, fabricBoard: true, fabricColor: true, rolls: { include: { images: true } }, fabricCosts: { orderBy: { fabricCode: "asc" } }, fabricConfigs: { include: { supplier:true, product: true, designSample: true }, orderBy: { fabricCode: "asc" } }, colorMaps: { orderBy: [{ fabricCode: "asc" }, { colorName: "asc" }] }, measurements: true, images: true },
    });
    return this.receiptForUser(created, user);
  }

  async updateFabricReceipt(id: string, body: any, user?: any) {
    const found = await this.prisma.fabricReceipt.findUnique({ where: { id } });
    if (!found) throw new NotFoundException("Không tìm thấy phiếu vải về.");
    if (found.status === "COMPLETED") throw new BadRequestException("Phiếu đã hoàn tất.");
    const updated = await this.prisma.$transaction(async (tx) => {
      const canLinkBoard = this.userHas(user, "fabric_receipt.fabric_board_link");
      const board = canLinkBoard && body?.fabricBoardId ? await tx.fabricBoard.findUnique({ where: { id: body.fabricBoardId } }) : null;
      const color = canLinkBoard && body?.fabricColorId ? await tx.fabricBoardColor.findUnique({ where: { id: body.fabricColorId } }) : null;
      const receiver = body?.receivedByStaffId !== undefined ? await this.receiptReceiverSnapshot(body.receivedByStaffId) : undefined;
      const canEditCost=this.userHas(user, "fabric_receipt.cost.edit");
      const totals = Array.isArray(body?.rolls) ? this.receiptTotalsFromRolls(body.rolls,body) : null;
      if (Array.isArray(body?.rolls)) {
        const keepIds: string[] = [];
        for (const x of body.rolls) {
          const data = {
            sortOrder: Number(x.sortOrder || keepIds.length + 1),
            fabricCode: String(x.fabricCode || body?.fabricCode || "").trim().toUpperCase() || null,
            rollCode: x.rollCode || null,
            colorName: String(x.colorName || body?.colorName || color?.name || "").trim() || null,
            colorCode: this.normalizeColorCode(x.colorCode || body?.colorCode || color?.code),
            supplierDeclaredM: this.n(x.supplierDeclaredM),
            supplierDeclaredKg: this.n(x.supplierDeclaredKg),
            actualM: this.n(x.actualM),
            actualKg: this.n(x.actualKg),
            measuredGsm: this.n(x.measuredGsm),
            ...(canEditCost ? {unitPriceCny:this.n(x.unitPriceCny),priceUnit:(["METER","KG","ROLL"].includes(String(x.priceUnit||"METER").toUpperCase())?String(x.priceUnit||"METER").toUpperCase():"METER") as any}:{}),
            defectNote: x.defectNote || null,
            passed: x.passed !== false,
          };
          if (x.id) {
            const existing = await tx.fabricReceiptRoll.findFirst({ where: { id: x.id, fabricReceiptId: id }, select: { id: true } });
            if (existing) {
              await tx.fabricReceiptRoll.update({ where: { id: x.id }, data });
              keepIds.push(x.id);
              continue;
            }
          }
          const createdRoll = await tx.fabricReceiptRoll.create({ data: { fabricReceiptId: id, ...data } });
          keepIds.push(createdRoll.id);
        }
        await tx.fabricReceiptRoll.deleteMany({ where: { fabricReceiptId: id, ...(keepIds.length ? { id: { notIn: keepIds } } : {}) } });
      }
      if (canEditCost && Array.isArray(body?.fabricCosts)) {
        const keepCostIds:string[]=[];
        for (const x of body.fabricCosts) {
          const fabricCode=String(x?.fabricCode||"").trim().toUpperCase();
          if(!fabricCode) continue;
          const data={fabricCode,chinaShippingCny:this.n(x.chinaShippingCny),vietnamShippingRateVndPerKg:this.n(x.vietnamShippingRateVndPerKg),vietnamShippingVnd:this.n(x.vietnamShippingVnd),note:String(x.note||"").trim()||null};
          if(x.id){
            const exists=await tx.fabricReceiptFabricCost.findFirst({where:{id:x.id,fabricReceiptId:id},select:{id:true}});
            if(exists){await tx.fabricReceiptFabricCost.update({where:{id:x.id},data});keepCostIds.push(x.id);continue;}
          }
          const made=await tx.fabricReceiptFabricCost.create({data:{fabricReceiptId:id,...data}});keepCostIds.push(made.id);
        }
        await tx.fabricReceiptFabricCost.deleteMany({where:{fabricReceiptId:id,...(keepCostIds.length?{id:{notIn:keepCostIds}}:{})}});
      }
      if (Array.isArray(body?.fabricConfigs)) {
        const keepConfigIds:string[]=[];
        for (const x of body.fabricConfigs) {
          const fabricCode=String(x?.fabricCode||"").trim().toUpperCase();
          if(!fabricCode) continue;
          const data={
            fabricCode,
            materialName:String(x?.materialName||"").trim()||null,
            supplierId:x?.supplierId||null,
            fabricBoardCode:String(x?.fabricBoardCode||"").replace(/[^A-Za-z0-9]/g,"").toUpperCase()||null,
            fabricWidthCm:this.n(x?.fabricWidthCm),
            productId:x?.productId||null,
            designSampleId:x?.designSampleId||null,
          };
          if(x.id){
            const exists=await tx.fabricReceiptFabricConfig.findFirst({where:{id:x.id,fabricReceiptId:id},select:{id:true}});
            if(exists){await tx.fabricReceiptFabricConfig.update({where:{id:x.id},data});keepConfigIds.push(x.id);continue;}
          }
          const made=await tx.fabricReceiptFabricConfig.create({data:{fabricReceiptId:id,...data}});
          keepConfigIds.push(made.id);
        }
        await tx.fabricReceiptFabricConfig.deleteMany({where:{fabricReceiptId:id,...(keepConfigIds.length?{id:{notIn:keepConfigIds}}:{})}});
      }
      if (Array.isArray(body?.colorMaps)) {
        const keepColorIds:string[]=[];
        for (const x of body.colorMaps) {
          const fabricCode=String(x?.fabricCode||"").trim().toUpperCase();
          const colorName=String(x?.colorName||"").trim();
          if(!fabricCode||!colorName) continue;
          const data={fabricCode,colorName,colorCode:this.normalizeColorCode(x.colorCode)};
          if(x.id){
            const exists=await tx.fabricReceiptColorMap.findFirst({where:{id:x.id,fabricReceiptId:id},select:{id:true}});
            if(exists){await tx.fabricReceiptColorMap.update({where:{id:x.id},data});keepColorIds.push(x.id);continue;}
          }
          const made=await tx.fabricReceiptColorMap.create({data:{fabricReceiptId:id,...data}});
          keepColorIds.push(made.id);
        }
        await tx.fabricReceiptColorMap.deleteMany({where:{fabricReceiptId:id,...(keepColorIds.length?{id:{notIn:keepColorIds}}:{})}});
      }
      return tx.fabricReceipt.update({
        where: { id },
        data: {
          ...(body?.designSampleId !== undefined ? { designSampleId: body.designSampleId || null } : {}),
          ...(body?.productId !== undefined ? { productId: body.productId || null } : {}),
          ...(canLinkBoard && body?.fabricBoardId !== undefined ? { fabricBoardId: body.fabricBoardId || null } : {}),
          ...(canLinkBoard && body?.fabricColorId !== undefined ? { fabricColorId: body.fabricColorId || null } : {}),
          ...(body?.supplierId !== undefined || board ? { supplierId: body.supplierId || board?.supplierId || null } : {}),
          ...(body?.branchId !== undefined ? { branchId: body.branchId || null } : {}),
          ...(body?.fabricBoardCode !== undefined || board ? { fabricBoardCode: body.fabricBoardCode || board?.boardCode || null } : {}),
          ...(body?.fabricCode !== undefined || board ? { fabricCode: body.fabricCode || board?.fabricCode || null } : {}),
          ...(body?.fabricName !== undefined || board ? { fabricName: body.fabricName || board?.name || null } : {}),
          ...(body?.colorName !== undefined || color ? { colorName: body.colorName || color?.name || null } : {}),
          ...(body?.colorCode !== undefined || color ? { colorCode: this.normalizeColorCodes(body.colorCode || color?.code) } : {}),
          ...(body?.lotCode !== undefined ? { lotCode: body.lotCode || null } : {}),
          ...(totals ? { supplierDeclaredM: totals.supplierDeclaredM, supplierDeclaredKg: totals.supplierDeclaredKg, actualM: totals.actualM, actualKg: totals.actualKg } : {
            ...(body?.supplierDeclaredM !== undefined ? { supplierDeclaredM: this.n(body.supplierDeclaredM) } : {}),
            ...(body?.supplierDeclaredKg !== undefined ? { supplierDeclaredKg: this.n(body.supplierDeclaredKg) } : {}),
            ...(body?.actualM !== undefined ? { actualM: this.n(body.actualM) } : {}),
            ...(body?.actualKg !== undefined ? { actualKg: this.n(body.actualKg) } : {}),
          }),
          ...(body?.rollCount !== undefined || Array.isArray(body?.rolls) ? { rollCount: Number(body?.rollCount || body?.rolls?.length || 0) } : {}),
          ...(body?.expectedGsm !== undefined ? { expectedGsm: this.n(body.expectedGsm) } : {}),
          ...(body?.status !== undefined ? { status: body.status as FabricReceiptStatus } : {}),
          ...(body?.receivedAt !== undefined ? { receivedAt: body.receivedAt ? new Date(body.receivedAt) : null } : {}),
          ...(body?.receivedByStaffId !== undefined ? { receivedByStaffId: receiver?.id || null, receivedByName: receiver?.name || null } : {}),
          ...(body?.note !== undefined ? { note: body.note || null } : {}),
        },
        include: { supplier: true, branch: true, designSample: true, product: true, fabricBoard: true, fabricColor: true, rolls: { include: { images: true } }, fabricCosts: { orderBy: { fabricCode: "asc" } }, fabricConfigs: { include: { supplier:true, product: true, designSample: true }, orderBy: { fabricCode: "asc" } }, colorMaps: { orderBy: [{ fabricCode: "asc" }, { colorName: "asc" }] }, measurements: true, images: true },
      });
    });
    return this.receiptForUser(updated, user);
  }

  async setFabricReceiptCost(id: string, body: any, user?: any) {
    if (!this.userHas(user, "fabric_receipt.cost.edit")) throw new BadRequestException("Không có quyền sửa đơn giá / tỷ giá vải.");
    const found = await this.prisma.fabricReceipt.findUnique({ where: { id }, select: { id: true } });
    if (!found) throw new NotFoundException("Không tìm thấy phiếu vải về.");

    const unitPrice = this.n(body?.unitPrice);
    const priceCurrency = String(body?.priceCurrency || "VND").trim().toUpperCase() === "CNY" ? "CNY" : "VND";
    const exchangeRateToVnd = priceCurrency === "CNY" ? this.n(body?.exchangeRateToVnd) : 1;

    if (priceCurrency === "CNY" && (!exchangeRateToVnd || exchangeRateToVnd <= 0)) {
      throw new BadRequestException("Thiếu tỷ giá CNY → VND.");
    }

    const unitPriceVnd =
      unitPrice === null
        ? null
        : Number(unitPrice) * Number(exchangeRateToVnd || 1);

    return this.prisma.fabricReceipt.update({
      where: { id },
      data: {
        unitPrice,
        priceUnit: body?.priceUnit || "METER",
        priceCurrency,
        exchangeRateToVnd,
        unitPriceVnd,
      },
    });
  }

  async updateFabricReceiptRoll(receiptId: string, rollId: string, body: any) {
    const roll = await this.prisma.fabricReceiptRoll.findFirst({ where: { id: rollId, fabricReceiptId: receiptId } });
    if (!roll) throw new NotFoundException("Không tìm thấy cây vải.");
    return this.prisma.fabricReceiptRoll.update({
      where: { id: rollId },
      data: {
        ...(body?.sortOrder !== undefined ? { sortOrder: Number(body.sortOrder || 0) } : {}),
        ...(body?.fabricCode !== undefined ? { fabricCode: String(body.fabricCode || "").trim().toUpperCase() || null } : {}),
        ...(body?.rollCode !== undefined ? { rollCode: String(body.rollCode || "").trim() || null } : {}),
        ...(body?.colorName !== undefined ? { colorName: String(body.colorName || "").trim() || null } : {}),
        ...(body?.colorCode !== undefined ? { colorCode: this.normalizeColorCode(body.colorCode) } : {}),
        ...(body?.supplierDeclaredM !== undefined ? { supplierDeclaredM: this.n(body.supplierDeclaredM) } : {}),
        ...(body?.supplierDeclaredKg !== undefined ? { supplierDeclaredKg: this.n(body.supplierDeclaredKg) } : {}),
        ...(body?.actualM !== undefined ? { actualM: this.n(body.actualM) } : {}),
        ...(body?.actualKg !== undefined ? { actualKg: this.n(body.actualKg) } : {}),
        ...(body?.defectNote !== undefined ? { defectNote: body.defectNote || null } : {}),
        ...(body?.passed !== undefined ? { passed: body.passed !== false } : {}),
      },
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
        fabricReceiptId: id, rollId: body?.rollId || null, areaCm2, weightGrams, gsm,
        positionLabel: body?.positionLabel || null, imageUrl: body?.imageUrl || null, note: body?.note || null,
        measuredById: actor.id, measuredByName: actor.name,
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

  async cancelFabricReceipt(id: string, user?: Actor) {
    const found = await this.prisma.fabricReceipt.findUnique({ where: { id } });
    if (!found) throw new NotFoundException("Không tìm thấy phiếu vải về.");
    if (found.status === "COMPLETED") throw new BadRequestException("Phiếu đã hoàn tất, không thể huỷ trực tiếp.");
    if (found.status === "CANCELLED") return found;
    const actor = this.actor(user);
    return this.prisma.fabricReceipt.update({
      where: { id },
      data: {
        status: "CANCELLED",
        note: [found.note, `Huỷ phiếu bởi ${actor.name || actor.id} lúc ${new Date().toISOString()}`].filter(Boolean).join("\n"),
      },
    });
  }

  async deleteFabricReceipt(id: string) {
    const found = await this.prisma.fabricReceipt.findUnique({ where: { id } });
    if (!found) throw new NotFoundException("Không tìm thấy phiếu vải về.");
    if (found.status === "COMPLETED") throw new BadRequestException("Không xoá phiếu đã hoàn tất.");
    return this.prisma.fabricReceipt.delete({ where: { id } });
  }
}
