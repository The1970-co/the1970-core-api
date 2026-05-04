import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from "@nestjs/common";

import {
  BranchNotificationType,
  InventoryMovementType,
  Prisma,
  StockTransferDirection,
  StockTransferSourceType,
  TransferStatus,
} from "@prisma/client";

import { Cron } from "@nestjs/schedule";

import { PrismaService } from "../prisma/prisma.service";
import { BranchNotificationsService } from "../notifications/branch-notifications.service";

import { CreateStockTransferDto } from "./dto/create-stock-transfer.dto";
import { GenerateOutboundSuggestionsDto } from "./dto/generate-outbound-suggestions.dto";
import { ListStockTransfersDto } from "./dto/list-stock-transfers.dto";
import { UpdateStockTransferStatusDto } from "./dto/update-stock-transfer-status.dto";
import { CreateSelectedOutboundSuggestionsDto } from "./dto/create-selected-outbound-suggestions.dto";
import { UpdateAutoRebalanceConfigDto } from "./dto/update-auto-rebalance-config.dto";
@Injectable()
export class StockTransferService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly branchNotificationsService: BranchNotificationsService
  ) {}

  private isAdminUser(user?: any) {
    const text = JSON.stringify(user || {}).toLowerCase();
    return text.includes("owner") || text.includes("admin");
  }

  private assertAdminUser(user?: any) {
    if (!this.isAdminUser(user)) {
      throw new BadRequestException("Chỉ admin/owner được xoá phiếu chuyển kho");
    }
  }

private async nextCode(direction: StockTransferDirection) {
  const prefix =
    direction === StockTransferDirection.OUTBOUND_TO_BRANCH ? "CKCN" : "CVK";

  const rows = await this.prisma.stockTransfer.findMany({
    where: {
      transferCode: {
        startsWith: `${prefix}-`,
      },
    },
    select: {
      transferCode: true,
    },
    take: 10000,
  });

  let maxNumber = 0;

  for (const row of rows) {
    const match = String(row.transferCode || "").match(
      new RegExp(`^${prefix}-(\\d+)$`)
    );

    if (!match) continue;

    const currentNumber = Number(match[1] || 0);
    if (Number.isFinite(currentNumber) && currentNumber > maxNumber) {
      maxNumber = currentNumber;
    }
  }

  return `${prefix}-${String(maxNumber + 1).padStart(6, "0")}`;
}

  private resolveDirection(fromBranchId: string, toBranchId: string) {
    if (toBranchId === "QO" && fromBranchId !== "QO") {
      return StockTransferDirection.INBOUND_FROM_BRANCH;
    }

    return StockTransferDirection.OUTBOUND_TO_BRANCH;
  }

  private getLockedFromBranchId(dto: CreateStockTransferDto, user?: any) {
    const userBranchId = String(
      user?.branchId || user?.branch?.id || user?.branchCode || ""
    ).trim();

    const userRoleText = JSON.stringify({
      role: user?.role,
      type: user?.type,
      roles: user?.roles,
      permissions: user?.permissions,
    }).toLowerCase();

    const isAdminOrOwner =
      userRoleText.includes("admin") || userRoleText.includes("owner");

    if (!isAdminOrOwner && userBranchId) {
      return userBranchId;
    }

    const fallbackFromBranchId = String(dto.fromBranchId || "").trim();

    if (!fallbackFromBranchId) {
      throw new BadRequestException("Thiếu kho gửi");
    }

    return fallbackFromBranchId;
  }

  async create(dto: CreateStockTransferDto, user?: any) {
    const lockedFromBranchId = this.getLockedFromBranchId(dto, user);

    if (!dto.items?.length) {
      throw new BadRequestException("Phiếu chuyển kho phải có ít nhất 1 dòng hàng");
    }

    if (!dto.toBranchId) {
      throw new BadRequestException("Thiếu kho nhận hàng");
    }

    if (lockedFromBranchId === dto.toBranchId) {
      throw new BadRequestException("Kho gửi và kho nhận không được trùng nhau");
    }

    if (dto.items.some((item) => Number(item.qty || 0) <= 0)) {
      throw new BadRequestException("Số lượng chuyển phải lớn hơn 0");
    }

    const direction = this.resolveDirection(lockedFromBranchId, dto.toBranchId);

    const variantIds = dto.items.map((item) => item.variantId);

    const variants = await this.prisma.productVariant.findMany({
      where: { id: { in: variantIds } },
      include: { product: true },
    });

    const variantMap = new Map(variants.map((variant) => [variant.id, variant]));
    const missingVariantIds = variantIds.filter((id) => !variantMap.has(id));

    if (missingVariantIds.length > 0) {
      throw new BadRequestException(
        `Không tìm thấy biến thể sản phẩm: ${missingVariantIds.join(", ")}`
      );
    }

    const code = await this.nextCode(direction);

    const transfer = await this.prisma.stockTransfer.create({
      data: {
        transferCode: code,
        direction,
        sourceType: dto.sourceType ?? StockTransferSourceType.MANUAL,
        sourceRefId: dto.sourceRefId,
        fromBranchId: lockedFromBranchId,
        toBranchId: dto.toBranchId,
        note: dto.note,
        status: TransferStatus.DRAFT,
        createdById: dto.createdById,
        createdByName: dto.createdByName,
        items: {
          create: dto.items.map((item) => {
            const variant = variantMap.get(item.variantId);

            return {
              variantId: item.variantId,
              sku: item.sku ?? variant?.sku ?? null,
              productName: item.productName ?? variant?.product?.name ?? null,
              color: item.color ?? variant?.color ?? null,
              size: item.size ?? variant?.size ?? null,
              qty: item.qty,
            };
          }),
        },
      },
      include: {
        items: {
          include: {
            variant: {
              include: {
                product: true,
              },
            },
          },
          orderBy: { createdAt: "asc" },
        },
        fromBranch: true,
        toBranch: true,
      },
    });

    if (
      transfer.direction === StockTransferDirection.OUTBOUND_TO_BRANCH &&
      transfer.toBranchId !== "QO"
    ) {
      await this.branchNotificationsService.createNotification({
        branchId: transfer.toBranchId,
        branchName: transfer.toBranch?.name ?? transfer.toBranchId,
        title: "Phiếu chuyển kho mới",
        message: `QO vừa tạo phiếu ${transfer.transferCode} chuyển hàng ra chi nhánh ${
          transfer.toBranch?.name ?? transfer.toBranchId
        }.`,
        type: BranchNotificationType.TRANSFER_OUT_CREATED,
        transferId: transfer.id,
        transferCode: transfer.transferCode,
      });
    }

    if (
      transfer.direction === StockTransferDirection.INBOUND_FROM_BRANCH &&
      transfer.toBranchId === "QO"
    ) {
      await this.branchNotificationsService.createNotification({
        branchId: "QO",
        branchName: transfer.toBranch?.name ?? "Kho QO",
        title: "Có phiếu chuyển về kho",
        message: `${transfer.fromBranch?.name ?? transfer.fromBranchId} vừa tạo phiếu ${
          transfer.transferCode
        } chuyển hàng về kho QO.`,
        type: BranchNotificationType.TRANSFER_IN_CREATED,
        transferId: transfer.id,
        transferCode: transfer.transferCode,
      });
    }

    return transfer;
  }
private getDateKey(date = new Date()) {
  return date.toISOString().slice(0, 10);
}

private async getOrCreateAutoConfig() {
  const existing = await this.prisma.stockTransferAutoConfig.findFirst({
    orderBy: { createdAt: "asc" },
  });

  if (existing) return existing;

  return this.prisma.stockTransferAutoConfig.create({
    data: {
      isEnabled: false,
      runHour: 9,
      runMinute: 0,
      toBranchIds: ["TH", "XD", "CL"],
      categoryNames: [],
      branchMinTargets: {
        TH: 2,
        XD: 1,
        CL: 1,
      },
      maxPerVariant: 5,
      salesVelocityDays: 14,
      minSoldQty: 0,
    },
  });
}

async getAutoRebalanceConfig() {
  return this.getOrCreateAutoConfig();
}

async updateAutoRebalanceConfig(dto: UpdateAutoRebalanceConfigDto) {
  const config = await this.getOrCreateAutoConfig();

  return this.prisma.stockTransferAutoConfig.update({
    where: { id: config.id },
    data: {
      isEnabled: dto.isEnabled ?? config.isEnabled,
      runHour: dto.runHour ?? config.runHour,
      runMinute: dto.runMinute ?? config.runMinute,
      toBranchIds: dto.toBranchIds ?? config.toBranchIds,
      categoryNames: dto.categoryNames ?? config.categoryNames,
   branchMinTargets:
  dto.branchMinTargets ??
  ((config.branchMinTargets as Prisma.InputJsonValue) || {
    TH: 2,
    XD: 1,
    CL: 1,
  }),
      maxPerVariant: dto.maxPerVariant ?? config.maxPerVariant,
      salesVelocityDays: dto.salesVelocityDays ?? config.salesVelocityDays,
      minSoldQty: dto.minSoldQty ?? config.minSoldQty,
    },
  });
}

async runAutoRebalanceNow() {
  const config = await this.getOrCreateAutoConfig();

  return this.createOutboundTransfersFromSuggestions({
    maxPerVariant: config.maxPerVariant,
    toBranchIds: config.toBranchIds,
    categoryNames: config.categoryNames,
    branchMinTargets: (config.branchMinTargets as Record<string, number>) ?? {
      TH: 2,
      XD: 1,
      CL: 1,
    },
    salesVelocityDays: config.salesVelocityDays,
    minSoldQty: config.minSoldQty,
    createdById: "system-auto",
    createdByName: "Auto Rebalance",
  } as any);
}

// @Cron("*/1 * * * *")
async handleAutoRebalanceCron() {
  return;
  const now = new Date();
  const config = await this.getOrCreateAutoConfig();

  if (!config.isEnabled) return;

  const hour = now.getHours();
  const minute = now.getMinutes();
  const todayKey = this.getDateKey(now);

  if (hour !== config.runHour || minute !== config.runMinute) return;
  if (config.lastRunDateKey === todayKey) return;

  try {
    await this.runAutoRebalanceNow();

    await this.prisma.stockTransferAutoConfig.update({
      where: { id: config.id },
      data: {
        lastRunAt: now,
        lastRunDateKey: todayKey,
      },
    });
  } catch (error) {
    console.error("Auto rebalance cron failed", error);
  }
}
  async list(dto: ListStockTransfersDto) {
    const andConditions: Prisma.StockTransferWhereInput[] = [];

    if (dto.direction) {
      andConditions.push({ direction: dto.direction });
    }

    if (dto.sourceType) {
      andConditions.push({ sourceType: dto.sourceType });
    }

    if (dto.status) {
      andConditions.push({ status: dto.status });
    }

    if (dto.branchId) {
      andConditions.push({
        OR: [{ fromBranchId: dto.branchId }, { toBranchId: dto.branchId }],
      });
    }

    if (dto.keyword) {
      andConditions.push({
        OR: [
          { transferCode: { contains: dto.keyword, mode: "insensitive" } },
          { fromBranch: { name: { contains: dto.keyword, mode: "insensitive" } } },
          { toBranch: { name: { contains: dto.keyword, mode: "insensitive" } } },
          {
            items: {
              some: {
                OR: [
                  { productName: { contains: dto.keyword, mode: "insensitive" } },
                  { sku: { contains: dto.keyword, mode: "insensitive" } },
                  { color: { contains: dto.keyword, mode: "insensitive" } },
                  { size: { contains: dto.keyword, mode: "insensitive" } },
                ],
              },
            },
          },
        ],
      });
    }

    const where: Prisma.StockTransferWhereInput =
      andConditions.length > 0 ? { AND: andConditions } : {};

    const rows = await this.prisma.stockTransfer.findMany({
      where,
      include: {
        items: false,
        fromBranch: true,
        toBranch: true,
      },
      orderBy: { createdAt: "desc" },
      take: 100,
    });

    const transferIds = rows.map((row) => row.id);

    const groupedItems =
      transferIds.length > 0
        ? await this.prisma.stockTransferItem.groupBy({
            by: ["transferId"],
            where: { transferId: { in: transferIds } },
            _count: { _all: true },
            _sum: { qty: true },
          })
        : [];

    const itemStats = new Map(
      groupedItems.map((item) => [
        item.transferId,
        {
          totalLines: item._count._all,
          totalQty: item._sum.qty ?? 0,
        },
      ])
    );

    return rows.map((row) => {
      const stats = itemStats.get(row.id);

      return {
        id: row.id,
        transferCode: row.transferCode,
        code: row.transferCode,
        direction: row.direction,
        sourceType: row.sourceType,
        sourceRefId: row.sourceRefId,
        fromBranchId: row.fromBranchId,
        fromBranchName: row.fromBranch?.name ?? row.fromBranchId,
        toBranchId: row.toBranchId,
        toBranchName: row.toBranch?.name ?? row.toBranchId,
        note: row.note,
        status: row.status,
        createdById: row.createdById,
        createdByName: row.createdByName,
        confirmedById: row.confirmedById,
        confirmedByName: row.confirmedByName,
        confirmedAt: row.confirmedAt,
        completedAt: row.completedAt,
        createdAt: row.createdAt,
        updatedAt: row.updatedAt,
        totalLines: stats?.totalLines ?? 0,
        totalQty: stats?.totalQty ?? 0,
        items: [],
      };
    });
  }

  async detail(id: string) {
    const transfer = await this.prisma.stockTransfer.findUnique({
      where: { id },
      include: {
        items: {
          include: {
            variant: {
              include: {
                product: true,
              },
            },
          },
          orderBy: { createdAt: "asc" },
        },
        fromBranch: true,
        toBranch: true,
        notifications: { orderBy: { createdAt: "asc" } },
      },
    });

    if (!transfer) {
      throw new NotFoundException("Không tìm thấy phiếu chuyển kho");
    }

    const timeline = [
      {
        id: `created-${transfer.id}`,
        label: "Tạo phiếu chuyển kho",
        time: transfer.createdAt,
      },
      ...(transfer.confirmedAt
        ? [
            {
              id: `confirmed-${transfer.id}`,
              label: "Xác nhận phiếu chuyển kho",
              time: transfer.confirmedAt,
            },
          ]
        : []),
      ...(transfer.completedAt
        ? [
            {
              id: `completed-${transfer.id}`,
              label: "Hoàn thành phiếu chuyển kho",
              time: transfer.completedAt,
            },
          ]
        : []),
    ];

    return {
      id: transfer.id,
      transferCode: transfer.transferCode,
      code: transfer.transferCode,
      direction: transfer.direction,
      sourceType: transfer.sourceType,
      sourceRefId: transfer.sourceRefId,
      fromBranchId: transfer.fromBranchId,
      fromBranchName: transfer.fromBranch?.name ?? transfer.fromBranchId,
      toBranchId: transfer.toBranchId,
      toBranchName: transfer.toBranch?.name ?? transfer.toBranchId,
      note: transfer.note,
      status: transfer.status,
      createdById: transfer.createdById,
      createdByName: transfer.createdByName,
      confirmedById: transfer.confirmedById,
      confirmedByName: transfer.confirmedByName,
      confirmedAt: transfer.confirmedAt,
      completedAt: transfer.completedAt,
      createdAt: transfer.createdAt,
      updatedAt: transfer.updatedAt,
      totalLines: transfer.items.length,
      totalQty: transfer.items.reduce((sum, item) => sum + item.qty, 0),
      items: transfer.items,
      notifications: transfer.notifications,
      timeline,
    };
  }

  private normalizeCategoryText(value: any) {
    return String(value || "")
      .trim()
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/\s+/g, " ");
  }

  private getProductCategoryNames(product: any): string[] {
    const values = new Set<string>();

    const pushValue = (value: any) => {
      if (value === null || value === undefined) return;

      if (typeof value === "string" || typeof value === "number") {
        const text = String(value).trim();
        if (text) values.add(text);
        return;
      }

      if (typeof value === "object") {
        pushValue(value.name);
        pushValue(value.title);
        pushValue(value.label);
        pushValue(value.code);
        pushValue(value.id);
      }
    };

    pushValue(product?.categoryName);
    pushValue(product?.category);
    pushValue(product?.categoryId);
    pushValue(product?.categoryCode);
    pushValue(product?.productCategory);

    if (Array.isArray(product?.categories)) {
      for (const category of product.categories) pushValue(category);
    }

    if (Array.isArray(product?.productCategories)) {
      for (const item of product.productCategories) {
        pushValue(item);
        pushValue(item?.category);
      }
    }

    return Array.from(values);
  }

async generateOutboundSuggestions(dto: GenerateOutboundSuggestionsDto) {
  const defaultMinTarget = Number(dto.minTarget ?? 1);
  const maxPerVariant = Number(dto.maxPerVariant ?? 10);
  const season = dto.season ?? "ALL";
  const minSoldQty = Number(dto.minSoldQty ?? 0);
  const salesVelocityDays = Number(dto.salesVelocityDays ?? 14);

  const branchMinTargets = dto.branchMinTargets ?? {
    TH: 2,
    XD: 1,
    CL: 1,
  };

  const toBranchIds = dto.toBranchIds?.length
    ? dto.toBranchIds
    : Object.keys(branchMinTargets);

  const maxTarget = Math.max(
    defaultMinTarget,
    ...Object.values(branchMinTargets).map((v) => Number(v || 0))
  );

  // =========================
  // 1. LẤY TỒN CHI NHÁNH
  // =========================
  const lowInventories = await this.prisma.inventoryItem.findMany({
    where: {
      branchId: { in: toBranchIds },
      availableQty: { lt: maxTarget },
    },
    include: {
      variant: {
        include: {
          product: true,
        },
      },
    },
  });

  const selectedCategoryNames = (dto.categoryNames || [])
    .map((name) => String(name || "").trim())
    .filter(Boolean);

  const selectedCategorySet = new Set(
    selectedCategoryNames.map((name) => this.normalizeCategoryText(name))
  );

  const filtered = lowInventories.filter((item) => {
    // Nếu UI có chọn danh mục thì backend BẮT BUỘC phải filter theo danh mục.
    // Không được quét tất cả như bản cũ.
    if (selectedCategorySet.size > 0) {
      const productCategoryNames = this.getProductCategoryNames(
        (item as any).variant?.product
      );

      const matchedCategory = productCategoryNames.some((name) =>
        selectedCategorySet.has(this.normalizeCategoryText(name))
      );

      if (!matchedCategory) return false;
    }

    const target = Number(branchMinTargets[item.branchId] ?? defaultMinTarget);
    return item.availableQty < target;
  });

  const variantIds = [
    ...new Set(filtered.map((i) => i.variantId)),
  ];

  // =========================
  // 2. TỒN QO
  // =========================
  const qoInventories = await this.prisma.inventoryItem.findMany({
    where: {
      branchId: "QO",
      variantId: { in: variantIds },
      availableQty: { gt: 0 },
    },
  });

  const qoMap = new Map(qoInventories.map((i) => [i.variantId, i]));

  // =========================
  // 3. ANTI DUPLICATE
  // =========================
  const existing = await this.prisma.stockTransferItem.findMany({
    where: {
      variantId: { in: variantIds },
      transfer: {
        sourceType: StockTransferSourceType.AUTO,
        status: {
          notIn: [TransferStatus.COMPLETED, TransferStatus.CANCELLED],
        },
      },
    },
    include: { transfer: true },
  });

  const existSet = new Set(
    existing.map(
      (i) => `${i.transfer.toBranchId}-${i.variantId}`
    )
  );

  // =========================
  // 4. SALES VELOCITY (đơn giản)
  // =========================
  const since = new Date();
  since.setDate(since.getDate() - salesVelocityDays);

  const movements = await this.prisma.inventoryMovement.findMany({
    where: {
      variantId: { in: variantIds },
      branchId: { in: toBranchIds },
      qty: { lt: 0 },
      createdAt: { gte: since },
    },
  });

  const soldMap = new Map<string, number>();

  for (const m of movements) {
    const key = `${m.branchId}-${m.variantId}`;
    soldMap.set(key, (soldMap.get(key) || 0) + Math.abs(m.qty));
  }

  // =========================
  // 5. BUILD SUGGESTION
  // =========================
  const suggestions = filtered
    .map((storeInv) => {
      const qoInv = qoMap.get(storeInv.variantId);
      if (!qoInv) return null;

      const key = `${storeInv.branchId}-${storeInv.variantId}`;
      if (existSet.has(key)) return null;

      const productName = storeInv.variant.product?.name ?? "";
      const nameLower = productName.toLowerCase();

      // ===== SEASON FILTER =====
      if (
        season === "SUMMER" &&
        !(
          nameLower.includes("tee") ||
          nameLower.includes("polo") ||
          nameLower.includes("short") ||
          nameLower.includes("sơ mi")
        )
      )
        return null;

      if (
        season === "WINTER" &&
        !(
          nameLower.includes("hoodie") ||
          nameLower.includes("jacket") ||
          nameLower.includes("áo khoác")
        )
      )
        return null;

      const soldQty = soldMap.get(key) || 0;

      if (minSoldQty > 0 && soldQty < minSoldQty) {
        return null;
      }

      const target = Number(
        branchMinTargets[storeInv.branchId] ?? defaultMinTarget
      );

      const need = target - storeInv.availableQty;
      const velocityBoost = soldQty > 0 ? Math.ceil(soldQty / Math.max(1, Math.ceil(salesVelocityDays / 7))) : 0;
      const aiSuggestedQty = Math.min(
        qoInv.availableQty,
        maxPerVariant,
        Math.max(need, need + Math.min(velocityBoost, Math.max(0, maxPerVariant - need)))
      );
      const qty = Math.max(0, aiSuggestedQty);

      if (qty <= 0) return null;

      const shortageRatio = target > 0 ? need / target : 0;
      const velocityRatio = target > 0 ? soldQty / Math.max(1, target) : soldQty;
      const zeroStockBonus = storeInv.availableQty <= 0 ? 15 : 0;
      const qoEnoughBonus = qoInv.availableQty >= need ? 5 : 0;
      const aiScore = Math.min(
        100,
        Math.max(
          1,
          Math.round(35 + shortageRatio * 35 + Math.min(velocityRatio, 2) * 12 + zeroStockBonus + qoEnoughBonus)
        )
      );

      const priority =
        aiScore >= 85 ? "CRITICAL" : aiScore >= 70 ? "HIGH" : aiScore >= 50 ? "MEDIUM" : "LOW";

      const aiReason =
        priority === "CRITICAL"
          ? `Ưu tiên rất cao: tồn ${storeInv.availableQty}/${target}, bán ${soldQty}/${salesVelocityDays} ngày, QO còn ${qoInv.availableQty}`
          : priority === "HIGH"
            ? `Ưu tiên cao: thiếu ${need}, bán ${soldQty}/${salesVelocityDays} ngày`
            : `Đề xuất bù ngưỡng: thiếu ${need}, QO còn ${qoInv.availableQty}`;

      return {
        variantId: storeInv.variantId,
        sku: storeInv.variant.sku,
        productName,
        color: storeInv.variant.color,
        size: storeInv.variant.size,
        fromBranchId: "QO",
        toBranchId: storeInv.branchId,
        toBranchName: storeInv.branchId,
        qoAvailableQty: qoInv.availableQty,
        storeAvailableQty: storeInv.availableQty,
        branchMinTarget: target,
        soldQty,
        salesVelocityDays,
        suggestedQty: qty,
        priority,
        aiScore,
        aiReason,
        reason: aiReason,
      };
    })
    .filter((item): item is NonNullable<typeof item> => item !== null)
    .sort((a, b) => {
      if (b.aiScore !== a.aiScore) return b.aiScore - a.aiScore;
      if (a.toBranchId !== b.toBranchId) return a.toBranchId.localeCompare(b.toBranchId);
      return a.sku.localeCompare(b.sku);
    });

  const summary = {
    critical: suggestions.filter((item) => item.priority === "CRITICAL").length,
    high: suggestions.filter((item) => item.priority === "HIGH").length,
    medium: suggestions.filter((item) => item.priority === "MEDIUM").length,
    low: suggestions.filter((item) => item.priority === "LOW").length,
  };

  return {
    total: suggestions.length,
    summary,
    suggestions,
  };
}
  async createOutboundTransfersFromSuggestions(dto: GenerateOutboundSuggestionsDto) {
    const result = await this.generateOutboundSuggestions(dto);
const suggestions = result.suggestions.filter(
  (item): item is NonNullable<typeof item> => item !== null
);

    if (!suggestions.length) {
      throw new BadRequestException("Không có đề xuất cấp hàng nào phù hợp");
    }

    const grouped = new Map<string, typeof suggestions>();

    for (const item of suggestions) {
      if (!grouped.has(item.toBranchId)) {
        grouped.set(item.toBranchId, []);
      }

      grouped.get(item.toBranchId)!.push(item);
    }

    const createdTransfers: any[] = [];

    // Level 4 rule: mỗi chi nhánh nhận chỉ tạo 1 phiếu duy nhất.
    // Không chia batch theo số dòng nữa để tránh TH/XD/CL bị sinh nhiều phiếu lẻ.
    for (const [toBranchId, items] of grouped.entries()) {
      const transfer = await this.create({
        direction: StockTransferDirection.OUTBOUND_TO_BRANCH,
        sourceType: StockTransferSourceType.AUTO,
        fromBranchId: "QO",
        toBranchId,
        note: "Auto cấp hàng (1 phiếu / kho)",
        createdById: dto.createdById,
        createdByName: dto.createdByName || "Auto Rebalance",
        items: items.map((item) => ({
          variantId: item.variantId,
          qty: item.suggestedQty,
        })),
      });

      createdTransfers.push(transfer);
    }

    return {
      success: true,
      createdCount: createdTransfers.length,
      transfers: createdTransfers,
    };
  }
  
  async createSelectedOutboundTransfers(dto: CreateSelectedOutboundSuggestionsDto) {
  if (!dto.items?.length) {
    throw new BadRequestException("Chưa có dòng đề xuất nào được chọn");
  }

  const grouped = new Map<string, typeof dto.items>();

  for (const item of dto.items) {
    if (!grouped.has(item.toBranchId)) grouped.set(item.toBranchId, []);
    grouped.get(item.toBranchId)!.push(item);
  }

  const createdTransfers: any[] = [];

  for (const [toBranchId, items] of grouped.entries()) {
    const transfer = await this.create({
      direction: StockTransferDirection.OUTBOUND_TO_BRANCH,
      sourceType: StockTransferSourceType.AUTO,
      fromBranchId: "QO",
      toBranchId,
      note: "Tạo tự động từ danh sách đề xuất đã chọn",
      createdById: dto.createdById,
      createdByName: dto.createdByName || "Auto Rebalance",
      items: items.map((item) => ({
        variantId: item.variantId,
        qty: item.qty,
      })),
    });

    createdTransfers.push(transfer);
  }

  return {
    success: true,
    createdCount: createdTransfers.length,
    transfers: createdTransfers,
  };
}

  async updateDraft(id: string, dto: CreateStockTransferDto, user?: any) {
    const existing = await this.prisma.stockTransfer.findUnique({
      where: { id },
      include: { items: true },
    });

    if (!existing) {
      throw new NotFoundException("Không tìm thấy phiếu chuyển kho");
    }

    if (existing.status !== TransferStatus.DRAFT && existing.status !== TransferStatus.PENDING) {
      throw new BadRequestException("Chỉ được sửa phiếu nháp hoặc phiếu chưa xác nhận");
    }

    const lockedFromBranchId = this.getLockedFromBranchId(dto, user);

    if (!dto.items?.length) {
      throw new BadRequestException("Phiếu chuyển kho phải có ít nhất 1 dòng hàng");
    }

    if (!dto.toBranchId) {
      throw new BadRequestException("Thiếu kho nhận hàng");
    }

    if (lockedFromBranchId === dto.toBranchId) {
      throw new BadRequestException("Kho gửi và kho nhận không được trùng nhau");
    }

    if (dto.items.some((item) => Number(item.qty || 0) <= 0)) {
      throw new BadRequestException("Số lượng chuyển phải lớn hơn 0");
    }

    const direction = this.resolveDirection(lockedFromBranchId, dto.toBranchId);

    const variantIds = dto.items.map((item) => item.variantId);

    const variants = await this.prisma.productVariant.findMany({
      where: { id: { in: variantIds } },
      include: { product: true },
    });

    const variantMap = new Map(variants.map((variant) => [variant.id, variant]));
    const missingVariantIds = variantIds.filter((variantId) => !variantMap.has(variantId));

    if (missingVariantIds.length > 0) {
      throw new BadRequestException(
        `Không tìm thấy biến thể sản phẩm: ${missingVariantIds.join(", ")}`
      );
    }

    return this.prisma.$transaction(async (tx) => {
      await tx.stockTransferItem.deleteMany({
        where: { transferId: id },
      });

      return tx.stockTransfer.update({
        where: { id },
        data: {
          direction,
          sourceType: dto.sourceType ?? existing.sourceType,
          sourceRefId: dto.sourceRefId,
          fromBranchId: lockedFromBranchId,
          toBranchId: dto.toBranchId,
          note: dto.note,
          items: {
            create: dto.items.map((item) => {
              const variant = variantMap.get(item.variantId);

              return {
                variantId: item.variantId,
                sku: item.sku ?? variant?.sku ?? null,
                productName: item.productName ?? variant?.product?.name ?? null,
                color: item.color ?? variant?.color ?? null,
                size: item.size ?? variant?.size ?? null,
                qty: Number(item.qty || 0),
              };
            }),
          },
        },
        include: {
          items: true,
          fromBranch: true,
          toBranch: true,
        },
      });
    });
  }

  private async finalizeReceivedInventoryTransfer(
    tx: Prisma.TransactionClient,
    transfer: {
      id: string;
      transferCode: string;
      fromBranchId: string;
      toBranchId: string;
      items: {
        variantId: string;
        qty: number;
        productName: string | null;
        sku: string | null;
        color: string | null;
        size: string | null;
      }[];
    },
    dto: UpdateStockTransferStatusDto
  ) {
    for (const item of transfer.items) {
      const fromInventory = await tx.inventoryItem.findUnique({
        where: {
          variantId_branchId: {
            variantId: item.variantId,
            branchId: transfer.fromBranchId,
          },
        },
      });

      if (!fromInventory) {
        throw new BadRequestException(
          `Kho gửi chưa có tồn: ${item.productName ?? item.sku ?? item.variantId}`
        );
      }

      if (fromInventory.availableQty < item.qty) {
        throw new BadRequestException(
          `Không đủ tồn để chuyển: ${item.productName ?? item.sku ?? item.variantId}. ` +
            `Tồn hiện tại ${fromInventory.availableQty}, cần chuyển ${item.qty}`
        );
      }

      await tx.inventoryItem.update({
        where: {
          variantId_branchId: {
            variantId: item.variantId,
            branchId: transfer.fromBranchId,
          },
        },
        data: {
          availableQty: {
            decrement: item.qty,
          },
        },
      });

      await tx.inventoryItem.upsert({
        where: {
          variantId_branchId: {
            variantId: item.variantId,
            branchId: transfer.toBranchId,
          },
        },
        update: {
          availableQty: {
            increment: item.qty,
          },
        },
        create: {
          variantId: item.variantId,
          branchId: transfer.toBranchId,
          availableQty: item.qty,
          reservedQty: 0,
          incomingQty: 0,
        },
      });

      await tx.inventoryMovement.create({
        data: {
          variantId: item.variantId,
          branchId: transfer.fromBranchId,
          type: InventoryMovementType.ADJUSTMENT,
          qty: -item.qty,
          refType: "STOCK_TRANSFER_OUT",
          refId: transfer.id,
          note: `Chuyển kho ${transfer.transferCode}: xuất từ ${transfer.fromBranchId} sang ${transfer.toBranchId}`,
          createdById: dto.confirmedById,
        },
      });

      await tx.inventoryMovement.create({
        data: {
          variantId: item.variantId,
          branchId: transfer.toBranchId,
          type: InventoryMovementType.ADJUSTMENT,
          qty: item.qty,
          refType: "STOCK_TRANSFER_IN",
          refId: transfer.id,
          note: `Chuyển kho ${transfer.transferCode}: nhập từ ${transfer.fromBranchId} sang ${transfer.toBranchId}`,
          createdById: dto.confirmedById,
        },
      });
    }
  }

  async updateStatus(id: string, dto: UpdateStockTransferStatusDto) {
    const transfer = await this.prisma.stockTransfer.findUnique({
      where: { id },
      include: {
        items: true,
        fromBranch: true,
        toBranch: true,
      },
    });

    if (!transfer) {
      throw new NotFoundException("Không tìm thấy phiếu chuyển kho");
    }

    if (transfer.status === TransferStatus.COMPLETED) {
      throw new BadRequestException("Phiếu đã hoàn thành, không thể cập nhật trạng thái");
    }

    if (transfer.status === TransferStatus.CANCELLED) {
      throw new BadRequestException("Phiếu đã huỷ, không thể cập nhật trạng thái");
    }

    if (dto.status === TransferStatus.CONFIRMED) {
      if (transfer.status === TransferStatus.CONFIRMED) {
        throw new BadRequestException("Phiếu này đã được xác nhận chuyển trước đó");
      }

      if (transfer.status !== TransferStatus.DRAFT && transfer.status !== TransferStatus.PENDING) {
        throw new BadRequestException("Chỉ được xác nhận chuyển từ phiếu nháp/chờ xác nhận");
      }
    }

    if (dto.status === TransferStatus.COMPLETED && transfer.status !== TransferStatus.CONFIRMED) {
      throw new BadRequestException("Chỉ được xác nhận nhận đủ sau khi phiếu đã được xác nhận chuyển");
    }

    if (dto.status === TransferStatus.CANCELLED && transfer.status === TransferStatus.CONFIRMED) {
      throw new BadRequestException("Phiếu đã xác nhận chuyển, không được huỷ trực tiếp. Nếu cần, tạo phiếu điều chỉnh/hoàn chuyển riêng.");
    }

    const updated = await this.prisma.$transaction(async (tx) => {
      // IMPORTANT: không trừ/cộng kho khi người gửi bấm xác nhận chuyển.
      // Chỉ khi bên nhận bấm xác nhận đã nhận đủ (COMPLETED) mới ghi inventory.
      if (dto.status === TransferStatus.COMPLETED) {
        await this.finalizeReceivedInventoryTransfer(tx, transfer, dto);
      }

      const updateData: Prisma.StockTransferUpdateInput = {
        status: dto.status,
      };

      if (dto.status === TransferStatus.CONFIRMED) {
        updateData.confirmedById = dto.confirmedById;
        updateData.confirmedByName = dto.confirmedByName;
        updateData.confirmedAt = new Date();
      }

      if (dto.status === TransferStatus.COMPLETED) {
        updateData.completedAt = new Date();
      }

      return tx.stockTransfer.update({
        where: { id },
        data: updateData,
        include: {
          items: true,
          fromBranch: true,
          toBranch: true,
        },
      });
    });

    if (
      dto.status === TransferStatus.CONFIRMED &&
      updated.direction === StockTransferDirection.OUTBOUND_TO_BRANCH &&
      updated.toBranchId !== "QO"
    ) {
      await this.branchNotificationsService.createNotification({
        branchId: updated.toBranchId,
        branchName: updated.toBranch?.name ?? updated.toBranchId,
        title: "Phiếu chuyển kho chờ nhận",
        message: `Phiếu ${updated.transferCode} đã được xác nhận chuyển. Chi nhánh ${
          updated.toBranch?.name ?? updated.toBranchId
        } kiểm hàng và bấm xác nhận nhận đủ để nhập kho.`,
        type: BranchNotificationType.TRANSFER_OUT_CONFIRMED,
        transferId: updated.id,
        transferCode: updated.transferCode,
      });
    }

    if (
      dto.status === TransferStatus.CONFIRMED &&
      updated.direction === StockTransferDirection.INBOUND_FROM_BRANCH &&
      updated.toBranchId === "QO"
    ) {
      await this.branchNotificationsService.createNotification({
        branchId: "QO",
        branchName: updated.toBranch?.name ?? "Kho QO",
        title: "Phiếu chuyển về chờ nhận",
        message: `Phiếu ${updated.transferCode} từ ${
          updated.fromBranch?.name ?? updated.fromBranchId
        } đã được xác nhận chuyển về kho QO. Kho QO kiểm hàng và xác nhận nhận đủ để nhập kho.`,
        type: BranchNotificationType.TRANSFER_IN_CONFIRMED,
        transferId: updated.id,
        transferCode: updated.transferCode,
      });
    }

    if (dto.status === TransferStatus.COMPLETED) {
      await this.branchNotificationsService.createNotification({
        branchId: updated.fromBranchId,
        branchName: updated.fromBranch?.name ?? updated.fromBranchId,
        title: "Phiếu chuyển kho đã hoàn tất",
        message: `Phiếu ${updated.transferCode} đã được bên nhận xác nhận đủ. Hệ thống đã trừ kho ${updated.fromBranchId} và cộng kho ${updated.toBranchId}.`,
        type: BranchNotificationType.TRANSFER_OUT_CONFIRMED,
        transferId: updated.id,
        transferCode: updated.transferCode,
      });
    }

    return {
      success: true,
      id: updated.id,
      transferCode: updated.transferCode,
      code: updated.transferCode,
      status: updated.status,
      confirmedAt: updated.confirmedAt,
      completedAt: updated.completedAt,
    };
  }

  async delete(id: string, user?: any) {
    this.assertAdminUser(user);

    const result = await this.bulkDelete([id], user);
    return { success: true, deletedCount: result.deletedCount };
  }

  async bulkDelete(ids: string[], user?: any) {
    this.assertAdminUser(user);

    const uniqueIds = Array.from(
      new Set((ids || []).map((id) => String(id || "").trim()).filter(Boolean))
    );

    if (uniqueIds.length === 0) {
      throw new BadRequestException("Chưa chọn phiếu chuyển kho để xoá");
    }

    const transfers = await this.prisma.stockTransfer.findMany({
      where: { id: { in: uniqueIds } },
      select: { id: true, transferCode: true, status: true },
    });

    const foundIds = new Set(transfers.map((item) => item.id));
    const missingIds = uniqueIds.filter((id) => !foundIds.has(id));

    if (missingIds.length > 0) {
      throw new NotFoundException(`Không tìm thấy phiếu chuyển kho: ${missingIds.join(", ")}`);
    }

    const lockedTransfers = transfers.filter(
      (item) => item.status !== TransferStatus.DRAFT && item.status !== TransferStatus.PENDING
    );

    if (lockedTransfers.length > 0) {
      throw new BadRequestException(
        `Chỉ được xoá phiếu nháp hoặc phiếu chưa xác nhận: ${lockedTransfers
          .map((item) => item.transferCode)
          .join(", ")}`
      );
    }

    await this.prisma.$transaction(async (tx) => {
      await tx.stockTransferItem.deleteMany({
        where: { transferId: { in: uniqueIds } },
      });

      await tx.stockTransfer.deleteMany({
        where: { id: { in: uniqueIds } },
      });
    });

    return { success: true, deletedCount: uniqueIds.length };
  }

}