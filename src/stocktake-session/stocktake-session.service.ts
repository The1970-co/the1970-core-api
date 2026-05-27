import {
  BadRequestException,
  ForbiddenException,
  Injectable,
  NotFoundException,
} from "@nestjs/common";
import { InventoryMovementType } from "@prisma/client";
import { PrismaService } from "src/prisma/prisma.service";
import * as XLSX from "xlsx";
import { CreateStocktakeSessionDto } from "./dto/create-stocktake-session.dto";
import { JoinStocktakeSessionDto } from "./dto/join-stocktake-session.dto";
import { ScanStocktakeDto } from "./dto/scan-stocktake.dto";

@Injectable()
export class StocktakeSessionService {
  constructor(private prisma: PrismaService) {}

  private isOwner(user?: any) {
    const roles = [
      ...(Array.isArray(user?.roles) ? user.roles : []),
      user?.role,
    ]
      .map((role) => String(role || "").toLowerCase())
      .filter(Boolean);

    return (
      roles.includes("owner") ||
      roles.includes("admin") ||
      roles.includes("admin_owner") ||
      roles.includes("admin-owner") ||
      roles.includes("admin/owner") ||
      roles.includes("admin all") ||
      roles.includes("admin-all") ||
      String(user?.roleName || "").toLowerCase().includes("owner") ||
      String(user?.roleName || "").toLowerCase().includes("admin") ||
      String(user?.title || "").toLowerCase().includes("owner") ||
      String(user?.title || "").toLowerCase().includes("admin") ||
      (Array.isArray(user?.permissions) && user.permissions.includes("*")) ||
      (Array.isArray(user?.permissionKeys) && user.permissionKeys.includes("*"))
    );
  }

  private userBranch(user?: any) {
    return (
      user?.branchId ||
      user?.currentBranchId ||
      user?.workingBranchId ||
      user?.defaultBranchId ||
      user?.staffBranchPermission?.branchId ||
      user?.branchPermissions?.find?.((row: any) => row?.branchId)?.branchId ||
      null
    );
  }

  private scopedBranchId(user?: any, requestedBranchId?: string | null) {
    if (this.isOwner(user)) return requestedBranchId || undefined;

    const branchId = this.userBranch(user);
    if (!branchId) throw new ForbiddenException("Tài khoản chưa được gán chi nhánh.");

    if (requestedBranchId && String(requestedBranchId) !== String(branchId)) {
      throw new ForbiddenException("Không có quyền thao tác phiên kiểm kho chi nhánh khác.");
    }

    return branchId;
  }

  private async ensureSessionAccess(sessionId: string, user?: any) {
    if (this.isOwner(user)) return;

    const session = await this.prisma.stocktakeSession.findUnique({
      where: { id: sessionId },
      select: { id: true, branchId: true },
    });

    if (!session) throw new NotFoundException("Không tìm thấy phiên kiểm kho.");

    const branchId = this.userBranch(user);
    if (!branchId) throw new ForbiddenException("Tài khoản chưa được gán chi nhánh.");

    if (String(session.branchId) !== String(branchId)) {
      throw new ForbiddenException("Không có quyền xem hoặc thao tác phiên kiểm kho chi nhánh khác.");
    }
  }

  private normalizeStatus(status?: string | null) {
    return String(status || "")
      .trim()
      .toUpperCase();
  }

  private isPaused(status?: string | null) {
    return this.normalizeStatus(status) === "PAUSED";
  }

  private isClosed(status?: string | null) {
    return ["FINISHED", "APPLIED", "CANCELLED"].includes(
      this.normalizeStatus(status),
    );
  }

  private async createSnapshotForSession(sessionId: string, branchId: string) {
    const existing = await this.prisma.stocktakeSnapshot.count({
      where: { sessionId },
    });

    if (existing > 0) {
      return { created: 0, skipped: existing };
    }

        const sessionForSnapshot = await this.prisma.stocktakeSession.findUnique({
      where: { id: sessionId },
      select: { branchId: true },
    });
if (!branchId) {
      throw new Error('Phiên kiểm chưa có chi nhánh, không thể tạo snapshot kiểm kho.');
    }

const inventoryItems = await this.prisma.inventoryItem.findMany({
      where: { branchId },
      select: {
        variantId: true,
        branchId: true,
        availableQty: true,
      },
    });

    if (!inventoryItems.length) {
      return { created: 0, skipped: 0 };
    }

    await this.prisma.stocktakeSnapshot.createMany({
      data: inventoryItems.map((item) => ({
        sessionId,
        branchId: item.branchId,
        variantId: item.variantId,
        snapshotQty: Number(item.availableQty || 0),
      })),
      skipDuplicates: true,
    });

    return { created: inventoryItems.length, skipped: 0 };
  }

  private async updateRealtimeCount(input: {
    sessionId: string;
    workerId?: string | null;
    branchId: string;
    variantId?: string | null;
    sku: string;
    qtyDelta: number;
    zone?: string | null;
    areaId?: string | null;
    rackId?: string | null;
    rackCode?: string | null;
    locationCode?: string | null;
    status: string;
  }) {
    const existing = await this.prisma.stocktakeCount.findFirst({
      where: {
        sessionId: input.sessionId,
        branchId: input.branchId,
        sku: input.sku,
        workerId: input.workerId || null,
      },
    });

    if (existing) {
      return this.prisma.stocktakeCount.update({
        where: { id: existing.id },
        data: {
          countedQty: { increment: input.qtyDelta },
          eventCount: { increment: 1 },
          variantId: input.variantId || existing.variantId,
          zone: input.zone || existing.zone,
          areaId: input.areaId || existing.areaId,
          rackId: input.rackId || existing.rackId,
          rackCode: input.rackCode || existing.rackCode,
          locationCode: input.locationCode || existing.locationCode,
          status: input.status,
          lastScannedAt: new Date(),
        },
      });
    }

    return this.prisma.stocktakeCount.create({
      data: {
        sessionId: input.sessionId,
        workerId: input.workerId || null,
        branchId: input.branchId,
        variantId: input.variantId || null,
        sku: input.sku,
        countedQty: input.qtyDelta,
        eventCount: 1,
        zone: input.zone || null,
        areaId: input.areaId || null,
        rackId: input.rackId || null,
        rackCode: input.rackCode || null,
        locationCode: input.locationCode || null,
        status: input.status,
        lastScannedAt: new Date(),
      },
    });
  }

  createSession(dto: CreateStocktakeSessionDto, user?: any) {
    const branchId = this.scopedBranchId(user, dto.branchId);

    return this.prisma.stocktakeSession.create({
      data: {
        branchId,
        name: dto.name,
        note: dto.note,
        createdById: dto.createdById,
        status: "DRAFT",
      },
      include: {
        workers: true,
      },
    });
  }

  private buildSessionListWhere(
    branchId?: string,
    filters?: { status?: string; from?: string; to?: string; productQuery?: string; productQ?: string; q?: string; query?: string; sku?: string },
    user?: any,
  ) {
    const where: any = {};

    const scopedBranchId = this.scopedBranchId(user, branchId);
    if (scopedBranchId) where.branchId = scopedBranchId;

    const normalizedStatus = String(filters?.status || "").trim().toUpperCase();
    if (normalizedStatus && normalizedStatus !== "ALL") {
      where.status = normalizedStatus;
    }

    if (filters?.from || filters?.to) {
      where.createdAt = {};
      if (filters.from) where.createdAt.gte = new Date(filters.from);
      if (filters.to) {
        const toDate = new Date(filters.to);
        if (!Number.isNaN(toDate.getTime())) {
          toDate.setHours(23, 59, 59, 999);
          where.createdAt.lte = toDate;
        }
      }
    }

    return where;
  }

  private normalizePagination(input?: { page?: number; limit?: number }) {
    const page = Math.max(1, Number(input?.page || 1));
    const rawLimit = Number(input?.limit || 50);
    const limit = Math.min(100, Math.max(10, rawLimit));
    const skip = (page - 1) * limit;

    return { page, limit, skip };
  }

  private async enrichSessionListRows(sessions: any[]) {
    const sessionIds = sessions.map((session) => session.id);
    if (!sessionIds.length) return [];

    const db = this.prisma as any;

    const [snapshotGroups, countRows] = await Promise.all([
      db.stocktakeSnapshot?.groupBy
        ? db.stocktakeSnapshot.groupBy({
            by: ["sessionId"],
            where: { sessionId: { in: sessionIds } },
            _count: { _all: true },
            _sum: { snapshotQty: true },
          })
        : Promise.resolve([]),
      db.stocktakeCount?.findMany
        ? db.stocktakeCount.findMany({
            where: { sessionId: { in: sessionIds } },
            select: {
              sessionId: true,
              variantId: true,
              sku: true,
              countedQty: true,
              eventCount: true,
              status: true,
            },
          })
        : Promise.resolve([]),
    ]);

    const snapshotStatMap = new Map<
      string,
      { totalSku: number; totalSnapshotQty: number }
    >();

    for (const row of snapshotGroups || []) {
      snapshotStatMap.set(String(row.sessionId), {
        totalSku: Number(row?._count?._all || 0),
        totalSnapshotQty: Number(row?._sum?.snapshotQty || 0),
      });
    }

    type CountAgg = {
      sessionId: string;
      variantId: string | null;
      sku: string;
      countedQty: number;
      eventCount: number;
      status: string;
    };

    const countAggMap = new Map<string, CountAgg>();
    const countedVariantIds = new Set<string>();

    for (const row of countRows || []) {
      const sessionId = String(row.sessionId || "");
      const variantId = row.variantId ? String(row.variantId) : null;
      const sku = String(row.sku || "");
      const key = `${sessionId}::${variantId || `SKU:${sku}`}`;

      const current =
        countAggMap.get(key) ||
        ({
          sessionId,
          variantId,
          sku,
          countedQty: 0,
          eventCount: 0,
          status: String(row.status || ""),
        } satisfies CountAgg);

      current.countedQty += Number(row.countedQty || 0);
      current.eventCount += Number(row.eventCount || 0);
      current.status = String(row.status || current.status || "");
      countAggMap.set(key, current);

      if (variantId) countedVariantIds.add(variantId);
    }

    const countedVariantList = Array.from(countedVariantIds);

    const countedSnapshots =
      countedVariantList.length && db.stocktakeSnapshot?.findMany
        ? await db.stocktakeSnapshot.findMany({
            where: {
              sessionId: { in: sessionIds },
              variantId: { in: countedVariantList },
            },
            select: {
              sessionId: true,
              variantId: true,
              snapshotQty: true,
            },
          })
        : [];

    const countedSnapshotMap = new Map<string, number>();
    for (const snapshot of countedSnapshots || []) {
      countedSnapshotMap.set(
        `${String(snapshot.sessionId)}::${String(snapshot.variantId)}`,
        Number(snapshot.snapshotQty || 0),
      );
    }

    const kpiMap = new Map<
      string,
      {
        countedSku: number;
        notFoundSku: number;
        mismatchSku: number;
        discrepancySku: number;
        matchedSku: number;
        totalCountedQty: number;
        totalDiffQty: number;
      }
    >();

    for (const count of countAggMap.values()) {
      const current =
        kpiMap.get(count.sessionId) ||
        {
          countedSku: 0,
          notFoundSku: 0,
          mismatchSku: 0,
          discrepancySku: 0,
          matchedSku: 0,
          totalCountedQty: 0,
          totalDiffQty: 0,
        };

      current.countedSku += 1;
      current.totalCountedQty += count.countedQty;

      const status = this.normalizeStatus(count.status);
      if (!count.variantId || status === "NOT_FOUND") {
        current.notFoundSku += 1;
      } else {
        const snapshotQty = Number(
          countedSnapshotMap.get(`${count.sessionId}::${count.variantId}`) || 0,
        );
        const diff = count.countedQty - snapshotQty;

        if (diff === 0) {
          current.matchedSku += 1;
        } else {
          current.mismatchSku += 1;
          current.discrepancySku += 1;
          current.totalDiffQty += diff;
        }
      }

      kpiMap.set(count.sessionId, current);
    }

    return sessions.map((session) => {
      const snapshotStat = snapshotStatMap.get(session.id) || {
        totalSku: 0,
        totalSnapshotQty: 0,
      };
      const kpi = kpiMap.get(session.id) || {
        countedSku: 0,
        notFoundSku: 0,
        mismatchSku: 0,
        discrepancySku: 0,
        matchedSku: 0,
        totalCountedQty: 0,
        totalDiffQty: 0,
      };

      const totalSku = snapshotStat.totalSku || kpi.countedSku;
      const uncountedSku = Math.max(totalSku - kpi.countedSku, 0);

      return {
        ...session,
        kpi: {
          totalSnapshotSku: snapshotStat.totalSku,
          totalSku,
          totalRows: totalSku,
          countedSku: kpi.countedSku,
          uncountedSku,
          matchedSku: kpi.matchedSku,
          mismatchSku: kpi.mismatchSku,
          discrepancySku: kpi.discrepancySku,
          notFoundSku: kpi.notFoundSku,
          totalSnapshotQty: snapshotStat.totalSnapshotQty,
          totalCountedQty: kpi.totalCountedQty,
          totalDiffQty: kpi.totalDiffQty,
          totalDiffValue: 0,
          scanEvents: session._count?.scanEvents || 0,
          workerCount: session.workers?.length || 0,
        },
      };
    });
  }


  private normalizeSearchText(value?: string | null) {
    return String(value || "").trim();
  }

  private mergeIdFilter(where: any, sessionIds: string[]) {
    const ids = Array.from(new Set(sessionIds.filter(Boolean)));
    if (!ids.length) {
      where.id = "__NO_STOCKTAKE_PRODUCT_MATCH__";
      return where;
    }

    if (where.id?.in && Array.isArray(where.id.in)) {
      where.id = {
        in: where.id.in.filter((id: string) => ids.includes(id)),
      };
      if (!where.id.in.length) where.id = "__NO_STOCKTAKE_PRODUCT_MATCH__";
      return where;
    }

    where.id = { in: ids };
    return where;
  }

  private async findSessionIdsByProductQuery(productQuery?: string | null) {
    const q = this.normalizeSearchText(productQuery);
    if (!q) return null;

    const normalized = q.toLowerCase();
    const exactLike = q.length >= 3;

    const variantWhere: any = {
      OR: [
        { sku: { contains: q, mode: "insensitive" } },
        { color: { contains: q, mode: "insensitive" } },
        { size: { contains: q, mode: "insensitive" } },
        { product: { name: { contains: q, mode: "insensitive" } } },
      ],
    };

    const variants = await this.prisma.productVariant.findMany({
      where: variantWhere,
      select: {
        id: true,
        sku: true,
        color: true,
        size: true,
        product: {
          select: {
            name: true,
          },
        },
      },
      take: 500,
    });

    const variantIds = variants.map((variant) => variant.id).filter(Boolean);
    const matchedSkus = variants
      .map((variant) => String(variant.sku || "").trim())
      .filter(Boolean);

    const sessionIds = new Set<string>();

    const [countRows, eventRows, snapshotRows] = await Promise.all([
      this.prisma.stocktakeCount.findMany({
        where: {
          OR: [
            ...(variantIds.length ? [{ variantId: { in: variantIds } }] : []),
            { sku: { contains: q, mode: "insensitive" } },
          ],
        },
        select: { sessionId: true },
        take: 5000,
      }),
      this.prisma.stocktakeScanEvent.findMany({
        where: {
          OR: [
            ...(variantIds.length ? [{ variantId: { in: variantIds } }] : []),
            { sku: { contains: q, mode: "insensitive" } },
            { barcode: { contains: q, mode: "insensitive" } },
          ],
        },
        select: { sessionId: true },
        take: 5000,
      }),
      variantIds.length
        ? this.prisma.stocktakeSnapshot.findMany({
            where: { variantId: { in: variantIds } },
            select: { sessionId: true },
            take: 10000,
          })
        : Promise.resolve([]),
    ]);

    for (const row of [...countRows, ...eventRows, ...snapshotRows]) {
      if (row?.sessionId) sessionIds.add(String(row.sessionId));
    }

    return {
      sessionIds: Array.from(sessionIds),
      matchedSkus,
      productQuery: q,
      exactLike,
      normalized,
    };
  }

  async listSessions(
    branchId?: string,
    filters?: { status?: string; from?: string; to?: string; page?: number; limit?: number; productQuery?: string; productQ?: string; q?: string; query?: string; sku?: string },
    user?: any,
  ) {
    const where = this.buildSessionListWhere(branchId, filters, user);
    const productQuery =
      filters?.productQuery || filters?.productQ || filters?.sku || filters?.q || filters?.query || "";

    const productSearch = await this.findSessionIdsByProductQuery(productQuery);
    if (productSearch) {
      this.mergeIdFilter(where, productSearch.sessionIds);
    }

    const { page, limit, skip } = this.normalizePagination(filters);

    const [total, sessions] = await Promise.all([
      this.prisma.stocktakeSession.count({ where }),
      this.prisma.stocktakeSession.findMany({
        where,
        orderBy: { createdAt: "desc" },
        skip,
        take: limit,
        include: {
          workers: {
            orderBy: { createdAt: "asc" },
          },
          _count: {
            select: {
              scanEvents: true,
            },
          },
        },
      }),
    ]);

    const items = await this.enrichSessionListRows(sessions);

    return {
      items,
      total,
      page,
      limit,
      totalPages: Math.max(1, Math.ceil(total / limit)),
      productSearch: productSearch
        ? {
            productQuery: productSearch.productQuery,
            matchedSkus: productSearch.matchedSkus,
            matchedSessionCount: productSearch.sessionIds.length,
          }
        : null,
    };
  }

  async getSessionsOverview(
    branchId?: string,
    filters?: { status?: string; from?: string; to?: string; productQuery?: string; q?: string; query?: string; sku?: string; productQ?: string },
    user?: any,
  ) {
    const where = this.buildSessionListWhere(branchId, filters, user);
    const productQuery =
      filters?.productQuery || filters?.productQ || filters?.sku || filters?.q || filters?.query || "";

    const productSearch = await this.findSessionIdsByProductQuery(productQuery);
    if (productSearch) {
      this.mergeIdFilter(where, productSearch.sessionIds);
    }

    const sessions = await this.prisma.stocktakeSession.findMany({
      where,
      select: {
        id: true,
        status: true,
        workers: {
          select: { id: true },
        },
        _count: {
          select: { scanEvents: true },
        },
      },
    });

    const overview = {
      total: sessions.length,
      running: 0,
      finished: 0,
      applied: 0,
      cancelled: 0,
      totalWorkers: 0,
      totalScanEvents: 0,
    };

    for (const session of sessions) {
      const status = this.normalizeStatus(session.status);

      if (["DRAFT", "IN_PROGRESS", "PAUSED"].includes(status)) {
        overview.running += 1;
      }

      if (status === "FINISHED") overview.finished += 1;
      if (status === "APPLIED") overview.applied += 1;
      if (status === "CANCELLED") overview.cancelled += 1;

      overview.totalWorkers += session.workers?.length || 0;
      overview.totalScanEvents += session._count?.scanEvents || 0;
    }

    return overview;
  }


  async updateSessionNote(
    sessionId: string,
    body: { note?: string | null } = {},
    user?: any,
  ) {
    await this.ensureSessionAccess(sessionId, user);

    const note = String(body?.note ?? "").trim();

    return this.prisma.stocktakeSession.update({
      where: { id: sessionId },
      data: { note },
      include: {
        workers: {
          orderBy: { createdAt: "asc" },
        },
        _count: {
          select: {
            scanEvents: true,
          },
        },
      },
    });
  }

  async getSession(id: string, user?: any) {
    await this.ensureSessionAccess(id, user);
    return this.prisma.stocktakeSession.findUnique({
      where: { id },
      include: {
        workers: {
          orderBy: { createdAt: "asc" },
        },
        areas: {
          orderBy: { createdAt: "asc" },
        },
        scanEvents: {
          orderBy: { createdAt: "desc" },
          take: 100,
        },
      },
    });
  }

  async startSession(id: string, user?: any) {
    await this.ensureSessionAccess(id, user);
    const session = await this.prisma.stocktakeSession.findUnique({
      where: { id },
    });

    if (!session) {
      throw new NotFoundException("Không tìm thấy phiên kiểm kho.");
    }

    await this.createSnapshotForSession(id, session.branchId);

    return this.prisma.stocktakeSession.update({
      where: { id },
      data: {
        status: "IN_PROGRESS",
        startedAt: session.startedAt || new Date(),
      },
      include: {
        workers: true,
        scanEvents: {
          orderBy: { createdAt: "desc" },
          take: 100,
        },
        _count: {
          select: {
            scanEvents: true,
          },
        },
      },
    });
  }

  async pauseSession(id: string, user?: any) {
    await this.ensureSessionAccess(id, user);
    const session = await this.prisma.stocktakeSession.findUnique({
      where: { id },
    });

    if (!session) {
      throw new NotFoundException("Không tìm thấy phiên kiểm kho.");
    }

    if (this.isClosed(session.status)) {
      throw new BadRequestException("Phiên kiểm kho đã kết thúc.");
    }

    return this.prisma.stocktakeSession.update({
      where: { id },
      data: {
        status: "PAUSED",
      },
      include: {
        workers: true,
        scanEvents: {
          orderBy: { createdAt: "desc" },
          take: 100,
        },
        _count: {
          select: {
            scanEvents: true,
          },
        },
      },
    });
  }

  async resumeSession(id: string, user?: any) {
    await this.ensureSessionAccess(id, user);
    const session = await this.prisma.stocktakeSession.findUnique({
      where: { id },
    });

    if (!session) {
      throw new NotFoundException("Không tìm thấy phiên kiểm kho.");
    }

    if (this.isClosed(session.status)) {
      throw new BadRequestException("Phiên kiểm kho đã kết thúc.");
    }

    await this.createSnapshotForSession(id, session.branchId);

    return this.prisma.stocktakeSession.update({
      where: { id },
      data: {
        status: "IN_PROGRESS",
        startedAt: session.startedAt || new Date(),
      },
      include: {
        workers: true,
        scanEvents: {
          orderBy: { createdAt: "desc" },
          take: 100,
        },
        _count: {
          select: {
            scanEvents: true,
          },
        },
      },
    });
  }

  async finishSession(id: string, user?: any) {
    await this.ensureSessionAccess(id, user);
    const session = await this.prisma.stocktakeSession.findUnique({
      where: { id },
    });

    if (!session) {
      throw new NotFoundException("Không tìm thấy phiên kiểm kho.");
    }

    await this.prisma.stocktakeArea.updateMany({
      where: {
        sessionId: id,
        status: "IN_PROGRESS",
      },
      data: {
        status: "FINISHED",
        finishedAt: new Date(),
      },
    });

    return this.prisma.stocktakeSession.update({
      where: { id },
      data: {
        status: "FINISHED",
        finishedAt: new Date(),
      },
    });
  }

  async joinSession(sessionId: string, dto: JoinStocktakeSessionDto, user?: any) {
    await this.ensureSessionAccess(sessionId, user);
    const session = await this.prisma.stocktakeSession.findUnique({
      where: { id: sessionId },
    });

    if (!session) {
      throw new NotFoundException("Không tìm thấy phiên kiểm kho.");
    }

    return this.prisma.stocktakeWorker.create({
      data: {
        sessionId,
        name: dto.name,
        userId: dto.userId,
        zone: dto.zone,
        deviceName: dto.deviceName,
        status: "IN_PROGRESS",
        isActive: true,
        startedAt: new Date(),
      },
    });
  }

  async finishWorker(workerId: string, user?: any) {
    const worker = await this.prisma.stocktakeWorker.findUnique({ where: { id: workerId }, select: { sessionId: true } });
    if (!worker) throw new NotFoundException("Không tìm thấy nhân sự kiểm kho.");
    await this.ensureSessionAccess(worker.sessionId, user);
    return this.prisma.stocktakeWorker.update({
      where: { id: workerId },
      data: {
        status: "FINISHED",
        isActive: false,
        finishedAt: new Date(),
      },
    });
  }

  private normalizeCode(value?: string) {
    return String(value || "").trim();
  }

  private async findTargetRack(dto: ScanStocktakeDto) {
    const rackId = this.normalizeCode(dto.rackId);
    const rackCode = this.normalizeCode(dto.rackCode || dto.locationCode);

    if (rackId) {
      return this.prisma.warehouseRack.findUnique({
        where: { id: rackId },
      });
    }

    if (rackCode) {
      return this.prisma.warehouseRack.findFirst({
        where: {
          OR: [{ code: rackCode }, { rackNo: rackCode }],
        },
      });
    }

    return null;
  }

  private async markAreaAndRackInProgress(dto: ScanStocktakeDto) {
    const now = new Date();

    if (dto.areaId) {
      const area = await this.prisma.stocktakeArea.findUnique({
        where: { id: dto.areaId },
      });

      if (area) {
        await this.prisma.stocktakeArea.update({
          where: { id: dto.areaId },
          data: {
            status: "IN_PROGRESS",
            startedAt: area.startedAt || now,
          },
        });

        if (area.scopeType === "RACK" && area.rackId) {
          await this.prisma.warehouseRack.update({
            where: { id: area.rackId },
            data: { status: "IN_PROGRESS" },
          });
        }

        if (area.scopeType === "AISLE" && area.aisle) {
          await this.prisma.warehouseRack.updateMany({
            where: {
              branchId: area.branchId,
              mapId: area.mapId || undefined,
              aisle: area.aisle,
              isActive: true,
            },
            data: { status: "IN_PROGRESS" },
          });
        }

        if (area.scopeType === "MAP" && area.mapId) {
          await this.prisma.warehouseRack.updateMany({
            where: {
              branchId: area.branchId,
              mapId: area.mapId,
              isActive: true,
            },
            data: { status: "IN_PROGRESS" },
          });
        }
      }
    }

    const targetRack = await this.findTargetRack(dto);

    if (targetRack) {
      await this.prisma.warehouseRack.update({
        where: { id: targetRack.id },
        data: { status: "IN_PROGRESS" },
      });
    }

    return targetRack;
  }

  async scan(dto: ScanStocktakeDto, user?: any) {
    await this.ensureSessionAccess(dto.sessionId, user);
    const code = this.normalizeCode(dto.code);

    if (!code) {
      throw new NotFoundException("Mã scan trống.");
    }

    const session = await this.prisma.stocktakeSession.findUnique({
      where: { id: dto.sessionId },
    });

    if (!session) {
      throw new NotFoundException("Không tìm thấy phiên kiểm kho.");
    }

    if (this.isPaused(session.status)) {
      throw new BadRequestException(
        "Phiên kiểm kho đang tạm dừng. Bấm tiếp tục để scan.",
      );
    }

    if (this.isClosed(session.status)) {
      throw new BadRequestException(
        "Phiên kiểm kho đã kết thúc, không thể scan thêm.",
      );
    }

    const sessionBranchId = session.branchId;

    await this.createSnapshotForSession(dto.sessionId, sessionBranchId);

    const variant = await this.prisma.productVariant.findFirst({
      where: { sku: code },
    });

    const targetRack = await this.markAreaAndRackInProgress(dto);
    const finalLocationCode =
      dto.locationCode || dto.rackCode || targetRack?.code || dto.zone;

    const event = await this.prisma.stocktakeScanEvent.create({
      data: {
        sessionId: dto.sessionId,
        workerId: dto.workerId,
        branchId: sessionBranchId,
        variantId: variant?.id,
        sku: variant?.sku || code,
        barcode: code,
        qtyDelta: dto.qtyDelta ?? 1,
        zone: dto.zone || dto.aisle || targetRack?.aisle,
        locationCode: finalLocationCode,
        status: variant ? "OK" : "NOT_FOUND",
        note: dto.note,
      },
    });

    await this.updateRealtimeCount({
      sessionId: dto.sessionId,
      workerId: dto.workerId || null,
      branchId: sessionBranchId,
      variantId: variant?.id || null,
      sku: variant?.sku || code,
      qtyDelta: dto.qtyDelta ?? 1,
      zone: dto.zone || dto.aisle || targetRack?.aisle || null,
      areaId: dto.areaId || null,
      rackId: dto.rackId || targetRack?.id || null,
      rackCode: dto.rackCode || targetRack?.code || null,
      locationCode: finalLocationCode || null,
      status: variant ? "OK" : "NOT_FOUND",
    });

    await this.prisma.stocktakeSession.update({
      where: { id: dto.sessionId },
      data: {
        status: "IN_PROGRESS",
        startedAt: session.startedAt || new Date(),
      },
    });

    return {
      ok: true,
      event,
      variant,
      rack: targetRack,
      mapShouldRefresh: true,
    };
  }

  async finishArea(
    areaId: string,
    status: "FINISHED" | "MISMATCH" = "FINISHED",
  ) {
    const area = await this.prisma.stocktakeArea.findUnique({
      where: { id: areaId },
    });

    if (!area) {
      throw new NotFoundException("Không tìm thấy khu kiểm kho.");
    }

    if (area.scopeType === "RACK" && area.rackId) {
      await this.prisma.warehouseRack.update({
        where: { id: area.rackId },
        data: { status },
      });
    }

    if (area.scopeType === "AISLE" && area.aisle) {
      await this.prisma.warehouseRack.updateMany({
        where: {
          branchId: area.branchId,
          mapId: area.mapId || undefined,
          aisle: area.aisle,
          isActive: true,
        },
        data: { status },
      });
    }

    if (area.scopeType === "MAP" && area.mapId) {
      await this.prisma.warehouseRack.updateMany({
        where: {
          branchId: area.branchId,
          mapId: area.mapId,
          isActive: true,
        },
        data: { status },
      });
    }

    return this.prisma.stocktakeArea.update({
      where: { id: areaId },
      data: {
        status,
        finishedAt: new Date(),
      },
    });
  }

  async markAreaMismatch(areaId: string) {
    return this.finishArea(areaId, "MISMATCH");
  }

  async getSessionSummary(sessionId: string, user?: any) {
    await this.ensureSessionAccess(sessionId, user);
    const session = await this.prisma.stocktakeSession.findUnique({
      where: { id: sessionId },
    });

    if (!session) {
      throw new NotFoundException("Không tìm thấy phiên kiểm kho.");
    }

    await this.createSnapshotForSession(sessionId, session.branchId);

    let counts = await this.prisma.stocktakeCount.findMany({
      where: { sessionId, branchId: session.branchId },
        orderBy: { lastScannedAt: "asc" },
    });

    // Backfill aggregate cho phiên cũ đã có scanEvents nhưng chưa có StocktakeCount.
    if (!counts.length) {
      const events = await this.prisma.stocktakeScanEvent.findMany({
        where: { sessionId, branchId: session.branchId },
        orderBy: { createdAt: "asc" },
      });

      for (const event of events) {
        await this.updateRealtimeCount({
          sessionId: event.sessionId,
          workerId: event.workerId || null,
          branchId: event.branchId,
          variantId: event.variantId || null,
          sku: event.sku,
          qtyDelta: event.qtyDelta,
          zone: event.zone || null,
          areaId: null,
          rackId: null,
          rackCode: null,
          locationCode: event.locationCode || null,
          status: event.status,
        });
      }

      counts = await this.prisma.stocktakeCount.findMany({
        where: { sessionId, branchId: session.branchId },
        orderBy: { lastScannedAt: "asc" },
      });
    }

    const variantIds = counts
      .map((row) => row.variantId)
      .filter(Boolean) as string[];

    const [snapshots, movements] = await Promise.all([
      variantIds.length
        ? this.prisma.stocktakeSnapshot.findMany({
            where: {
              sessionId,
              variantId: { in: variantIds },
            },
          })
        : Promise.resolve([]),
      variantIds.length
        ? this.prisma.inventoryMovement.groupBy({
            by: ["variantId"],
            where: {
              branchId: session.branchId,
              variantId: { in: variantIds },
              createdAt: {
                gte: session.startedAt || session.createdAt,
              },
              refType: {
                not: "STOCKTAKE",
              },
            },
            _sum: {
              qty: true,
            },
          })
        : Promise.resolve([]),
    ]);

    const snapshotMap = new Map<string, number>(
      snapshots.map(
        (item) =>
          [item.variantId, Number(item.snapshotQty || 0)] as [string, number],
      ),
    );

    const movementMap = new Map<string, number>(
      movements.map(
        (item) =>
          [item.variantId, Number(item._sum.qty || 0)] as [string, number],
      ),
    );

    return counts
      .filter((row) => Number(row.eventCount || 0) > 0 || Number(row.countedQty || 0) !== 0)
      .map((row) => {
        const snapshotQty = row.variantId
          ? Number(snapshotMap.get(row.variantId) || 0)
          : 0;
        const movementDuringStocktake = row.variantId
          ? Number(movementMap.get(row.variantId) || 0)
          : 0;
        const counted = Number(row.countedQty || 0);
        const diff = counted - snapshotQty;
        const finalQty = snapshotQty + diff + movementDuringStocktake;

        return {
          variantId: row.variantId,
          workerId: row.workerId,
          sku: row.sku,
          counted,
          countedQty: counted,
          status: row.status,
          events: row.eventCount,
          eventCount: row.eventCount,
          zone: row.zone,
          areaId: row.areaId,
          rackId: row.rackId,
          rackCode: row.rackCode,
          locationCode: row.locationCode,
          lastScannedAt: row.lastScannedAt,
          snapshotQty,
          system: snapshotQty,
          diff,
          movementDuringStocktake,
          finalQty,
        };
      });
  }

  private normalizeDetailStatus(input: {
    hasCount: boolean;
    countedQty: number;
    snapshotQty: number;
    variantId?: string | null;
    rawStatus?: string | null;
  }) {
    const rawStatus = this.normalizeStatus(input.rawStatus);

    if (rawStatus === "NOT_FOUND" || !input.variantId) {
      return {
        status: "NOT_FOUND",
        statusLabel: "Mã lạ",
        diffType: "NOT_FOUND",
      };
    }

    if (!input.hasCount) {
      return {
        status: "UNCOUNTED",
        statusLabel: "Chưa kiểm",
        diffType: "UNCOUNTED",
      };
    }

    const diff = Number(input.countedQty || 0) - Number(input.snapshotQty || 0);

    if (diff === 0) {
      return {
        status: "MATCH",
        statusLabel: "Khớp",
        diffType: "MATCH",
      };
    }

    return {
      status: "MISMATCH",
      statusLabel: diff > 0 ? "Thừa" : "Thiếu",
      diffType: diff > 0 ? "OVER" : "SHORT",
    };
  }

  private async buildSessionDetail(sessionId: string, ensureSnapshot = true) {
    const session = await this.prisma.stocktakeSession.findUnique({
      where: { id: sessionId },
      include: {
        workers: { orderBy: { createdAt: "asc" } },
        areas: { orderBy: { createdAt: "asc" } },
        _count: { select: { scanEvents: true } },
      },
    });

    if (!session) {
      throw new NotFoundException("Không tìm thấy phiên kiểm kho.");
    }

    if (ensureSnapshot) {
      await this.createSnapshotForSession(sessionId, session.branchId);
    }

    const [snapshots, counts, scanEvents] = await Promise.all([
      this.prisma.stocktakeSnapshot.findMany({
        where: { sessionId, branchId: session.branchId },
        orderBy: { createdAt: "asc" },
      }),
      this.prisma.stocktakeCount.findMany({
        where: { sessionId, branchId: session.branchId },
        orderBy: { lastScannedAt: "asc" },
      }),
      this.prisma.stocktakeScanEvent.findMany({
        where: { sessionId, branchId: session.branchId },
        orderBy: { createdAt: "desc" },
        take: 500,
      }),
    ]);

    const workerMap = new Map(
      (session.workers || []).map((worker) => [worker.id, worker]),
    );

    const countMap = new Map<string, any>();
    for (const count of counts) {
      const key = count.variantId || `SKU:${count.sku}`;
      const current = countMap.get(key) || {
        variantId: count.variantId,
        sku: count.sku,
        countedQty: 0,
        eventCount: 0,
        workerIds: new Set<string>(),
        zone: count.zone,
        areaId: count.areaId,
        rackId: count.rackId,
        rackCode: count.rackCode,
        locationCode: count.locationCode,
        status: count.status,
        lastScannedAt: count.lastScannedAt,
      };

      current.countedQty += Number(count.countedQty || 0);
      current.eventCount += Number(count.eventCount || 0);
      if (count.workerId) current.workerIds.add(count.workerId);
      current.zone = count.zone || current.zone;
      current.areaId = count.areaId || current.areaId;
      current.rackId = count.rackId || current.rackId;
      current.rackCode = count.rackCode || current.rackCode;
      current.locationCode = count.locationCode || current.locationCode;
      current.status = count.status || current.status;
      if (
        count.lastScannedAt &&
        (!current.lastScannedAt || count.lastScannedAt > current.lastScannedAt)
      ) {
        current.lastScannedAt = count.lastScannedAt;
      }
      countMap.set(key, current);
    }

    // Branch-scope cứng cho detail: phiên CL/QO/TH chỉ được tính SKU thuộc branch đó.
    // Với các phiên cũ lỡ chụp toàn hệ thống, lọc lại bằng InventoryItem của session.branchId.
    const branchInventoryItemsForDetail = await this.prisma.inventoryItem.findMany({
      where: { branchId: session.branchId },
      select: { variantId: true, availableQty: true },
    });

    const branchVariantIdSet = new Set(
      branchInventoryItemsForDetail.map((item) => item.variantId),
    );

    const inventoryMap = new Map(
      branchInventoryItemsForDetail.map(
        (item) => [item.variantId, Number(item.availableQty || 0)] as [string, number],
      ),
    );

    const scopedSnapshots = snapshots.filter((snapshot) => {
      if (!snapshot?.variantId) return false;
      return branchVariantIdSet.has(snapshot.variantId) || countMap.has(snapshot.variantId);
    });

    const variantIds = Array.from(
      new Set([
        ...scopedSnapshots.map((item) => item.variantId).filter(Boolean),
        ...counts.map((item) => item.variantId).filter(Boolean),
      ]),
    ) as string[];

    const variants = variantIds.length
      ? await this.prisma.productVariant.findMany({
          where: { id: { in: variantIds } },
          include: { product: true, locations: true },
        })
      : [];

    const variantMap = new Map(variants.map((variant) => [variant.id, variant]));

    // inventoryMap đã được tạo từ toàn bộ InventoryItem của branch phía trên.

    const snapshotVariantIds = new Set(scopedSnapshots.map((item) => item.variantId));
    const extraCounts = Array.from(countMap.values()).filter(
      (item) => !item.variantId || !snapshotVariantIds.has(item.variantId),
    );

    const buildWorkerInfo = (count?: any) => {
      const workerIds = Array.from(count?.workerIds || []) as string[];
      const lastWorkerId = workerIds[workerIds.length - 1] || null;
      const lastWorker = lastWorkerId ? workerMap.get(lastWorkerId) : null;

      return {
        workerIds,
        workerId: lastWorkerId,
        workerName: lastWorker?.name || null,
        workerCount: workerIds.length,
      };
    };

    const rows = [
      ...scopedSnapshots.map((snapshot) => {
        const count = countMap.get(snapshot.variantId);
        const variant = variantMap.get(snapshot.variantId);
        const hasCount = Boolean(count);
        const countedQty = hasCount ? Number(count.countedQty || 0) : 0;
        const snapshotQty = Number(snapshot.snapshotQty || 0);
        const diff = hasCount ? countedQty - snapshotQty : -snapshotQty;
        const unitCost = Number(variant?.costPrice || 0);
        const statusInfo = this.normalizeDetailStatus({
          hasCount,
          countedQty,
          snapshotQty,
          variantId: snapshot.variantId,
          rawStatus: count?.status,
        });
        const workerInfo = buildWorkerInfo(count);

        return {
          sessionId,
          branchId: session.branchId,
          variantId: snapshot.variantId,
          sku: variant?.sku || count?.sku || snapshot.variantId,
          productName: variant?.product?.name || "",
          color: variant?.color || null,
          size: variant?.size || null,
          barcode: (variant as any)?.barcode || null,
          unitCost,
          costPrice: unitCost,
          price: Number(variant?.price || 0),
          snapshotQty,
          countedQty,
          diff,
          currentQty: inventoryMap.get(snapshot.variantId) || 0,
          isCounted: hasCount,
          status: statusInfo.status,
          statusLabel: statusInfo.statusLabel,
          diffType: statusInfo.diffType,
          diffValue: diff * unitCost,
          valueDiff: diff * unitCost,
          eventCount: Number(count?.eventCount || 0),
          ...workerInfo,
          zone: count?.zone || null,
          areaId: count?.areaId || null,
          rackId: count?.rackId || null,
          rackCode: count?.rackCode || null,
          locationCode: count?.locationCode || null,
          lastScannedAt: count?.lastScannedAt || null,
        };
      }),
      ...extraCounts.map((count) => {
        const variant = count.variantId ? variantMap.get(count.variantId) : null;
        const countedQty = Number(count.countedQty || 0);
        const unitCost = Number(variant?.costPrice || 0);
        const statusInfo = this.normalizeDetailStatus({
          hasCount: true,
          countedQty,
          snapshotQty: 0,
          variantId: count.variantId,
          rawStatus: count.status,
        });
        const workerInfo = buildWorkerInfo(count);

        return {
          sessionId,
          branchId: session.branchId,
          variantId: count.variantId || null,
          sku: variant?.sku || count.sku,
          productName: variant?.product?.name || "",
          color: variant?.color || null,
          size: variant?.size || null,
          barcode: (variant as any)?.barcode || count.sku,
          unitCost,
          costPrice: unitCost,
          price: Number(variant?.price || 0),
          snapshotQty: 0,
          countedQty,
          diff: countedQty,
          currentQty: count.variantId ? inventoryMap.get(count.variantId) || 0 : 0,
          isCounted: true,
          status: statusInfo.status,
          statusLabel: statusInfo.statusLabel,
          diffType: statusInfo.diffType,
          diffValue: countedQty * unitCost,
          valueDiff: countedQty * unitCost,
          eventCount: Number(count.eventCount || 0),
          ...workerInfo,
          zone: count.zone || null,
          areaId: count.areaId || null,
          rackId: count.rackId || null,
          rackCode: count.rackCode || null,
          locationCode: count.locationCode || null,
          lastScannedAt: count.lastScannedAt || null,
        };
      }),
    ];

    const countedRows = rows.filter((row) => row.isCounted);
    const uncountedRows = rows.filter((row) => row.status === "UNCOUNTED");
    const matchedRows = rows.filter((row) => row.status === "MATCH");
    const notFoundRows = rows.filter((row) => row.status === "NOT_FOUND");
    const discrepancyRows = rows.filter((row) => row.status === "MISMATCH");
    const overRows = rows.filter((row) => row.diffType === "OVER");
    const shortRows = rows.filter((row) => row.diffType === "SHORT");

    const kpi = {
      totalSnapshotSku: scopedSnapshots.length,
      totalSku: rows.length,
      totalRows: rows.length,
      countedSku: countedRows.length,
      uncountedSku: uncountedRows.length,
      matchedSku: matchedRows.length,
      mismatchSku: discrepancyRows.length,
      discrepancySku: discrepancyRows.length,
      notFoundSku: notFoundRows.length,
      overSku: overRows.length,
      shortSku: shortRows.length,
      totalSnapshotQty: rows.reduce((sum, row) => sum + Number(row.snapshotQty || 0), 0),
      totalCountedQty: rows.reduce((sum, row) => sum + Number(row.countedQty || 0), 0),
      totalDiffQty: discrepancyRows.reduce((sum, row) => sum + Number(row.diff || 0), 0),
      totalDiffValue: discrepancyRows.reduce((sum, row) => sum + Number(row.diffValue || 0), 0),
      scanEvents: scanEvents.length,
      workerCount: session.workers.length,
    };

    return {
      ...session,
      session,
      kpi,
      rows,
      items: rows,
      uncountedRows,
      discrepancyRows,
      logs: scanEvents.map((log) => {
        const worker = log.workerId ? workerMap.get(log.workerId) : null;
        return {
          ...log,
          workerName: worker?.name || null,
        };
      }),
      recentLogs: scanEvents,
    };
  }

  async getSessionDetail(sessionId: string, user?: any) {
    await this.ensureSessionAccess(sessionId, user);
    return this.buildSessionDetail(sessionId);
  }

  async getSessionItems(
    sessionId: string,
    filters?: { status?: string; q?: string },
    user?: any,
  ) {
    await this.ensureSessionAccess(sessionId, user);
    const detail = await this.buildSessionDetail(sessionId);
    const q = String(filters?.q || "")
      .trim()
      .toLowerCase();
    const status = String(filters?.status || "")
      .trim()
      .toUpperCase();

    let rows = detail.rows;

    if (status && status !== "ALL") {
      if (status === "MISMATCH" || status === "DISCREPANCY") {
        rows = rows.filter((row) => row.status === "MISMATCH");
      } else if (status === "COUNTED" || status === "SCANNED") {
        rows = rows.filter(
          (row) => row.isCounted || Number(row.countedQty || 0) > 0,
        );
      } else if (status === "MATCHED") {
        rows = rows.filter((row) => row.status === "MATCH");
      } else {
        rows = rows.filter((row) => row.status === status);
      }
    }

    if (q) {
      rows = rows.filter((row) =>
        [
          row.sku,
          row.productName,
          row.color,
          row.size,
          row.locationCode,
          row.rackCode,
        ]
          .filter(Boolean)
          .some((value) => String(value).toLowerCase().includes(q)),
      );
    }

    const session = await this.prisma.stocktakeSession.findUnique({
      where: { id: sessionId },
    });

    if (!session) {
      return rows;
    }

    return await this.filterRowsBySessionBranchV27(session, rows);
  }

  async getSessionLogs(sessionId: string, user?: any) {
    await this.ensureSessionAccess(sessionId, user);
    const session = await this.prisma.stocktakeSession.findUnique({
      where: { id: sessionId },
    });

    if (!session) {
      throw new NotFoundException("Không tìm thấy phiên kiểm kho.");
    }

    const [events, workers] = await Promise.all([
      this.prisma.stocktakeScanEvent.findMany({
      where: { sessionId, branchId: session.branchId },
        orderBy: { createdAt: "desc" },
      }),
      this.prisma.stocktakeWorker.findMany({
        where: { sessionId },
      }),
    ]);

    const workerMap = new Map(workers.map((worker) => [worker.id, worker]));

    return events.map((event) => ({
      ...event,
      workerName: event.workerId ? workerMap.get(event.workerId)?.name || null : null,
    }));
  }

  async applySession(
    sessionId: string,
    body: { note?: string; createdById?: string } = {},
    user?: any,
  ) {
    await this.ensureSessionAccess(sessionId, user);
    const detail = await this.buildSessionDetail(sessionId);
    const session = detail.session;

    if (this.normalizeStatus(session.status) === "APPLIED") {
      throw new BadRequestException("Phiên kiểm kho này đã được chốt tồn rồi.");
    }

    if (this.normalizeStatus(session.status) === "CANCELLED") {
      throw new BadRequestException(
        "Phiên kiểm kho đã huỷ, không thể chốt tồn.",
      );
    }

    const rowsToApply = detail.rows.filter(
      (row) =>
        row.variantId && row.isCounted && Number(row.diff || 0) !== 0,
    );

    return this.prisma.$transaction(
      async (tx) => {
        let adjustedCount = 0;
        let totalDelta = 0;

        for (const row of rowsToApply) {
          const diff = Number(row.diff || 0);
          const inventoryItem = await tx.inventoryItem.findUnique({
            where: {
              variantId_branchId: {
                variantId: row.variantId,
                branchId: session.branchId,
              },
            },
          });

          if (!inventoryItem) continue;

          const beforeQty = Number(inventoryItem.availableQty || 0);
          const afterQty = beforeQty + diff;

          await tx.inventoryItem.update({
            where: {
              variantId_branchId: {
                variantId: row.variantId,
                branchId: session.branchId,
              },
            },
            data: {
              availableQty: afterQty,
            },
          });

          await tx.inventoryMovement.create({
            data: {
              variantId: row.variantId,
              branchId: session.branchId,
              type: InventoryMovementType.ADJUSTMENT,
              qty: diff,
              beforeQty,
              afterQty,
              refType: "STOCKTAKE_SESSION",
              refId: session.id,
              createdById: body?.createdById || session.createdById || null,
              note: [
                `Chốt kiểm kho: ${session.name}`,
                `SKU: ${row.sku}`,
                `Snapshot: ${row.snapshotQty}`,
                `Đếm thực tế: ${row.countedQty}`,
                body?.note ? `Ghi chú: ${body.note}` : "",
              ]
                .filter(Boolean)
                .join(" | "),
            },
          });

          adjustedCount += 1;
          totalDelta += diff;
        }

        const updatedSession = await tx.stocktakeSession.update({
          where: { id: session.id },
          data: {
            status: "APPLIED",
            finishedAt: session.finishedAt || new Date(),
          },
        });

        return {
          ok: true,
          session: updatedSession,
          adjustedCount,
          totalDelta,
          skippedUncounted: detail.uncountedRows.length,
          discrepancySku: detail.kpi.discrepancySku,
        };
      },
      { maxWait: 10000, timeout: 30000 },
    );
  }

  private toExcelRows(rows: any[]) {
    return rows.map((row) => ({
      SKU: row.sku,
      "Tên sản phẩm": row.productName,
      Màu: row.color || "",
      Size: row.size || "",
      "Tồn snapshot": row.snapshotQty,
      "Đã kiểm": row.countedQty ?? "",
      "Chênh lệch": row.diff ?? "",
      "Trạng thái": row.statusLabel,
      "Giá vốn": row.costPrice || 0,
      "Giá trị lệch": row.valueDiff || 0,
      "Vị trí": row.locationCode || row.rackCode || row.zone || "",
      "Số lần scan": row.eventCount || 0,
      "Người kiểm": row.workerCount || 0,
      "Lần scan cuối": row.lastScannedAt || "",
    }));
  }

  async exportSessionExcel(sessionId: string, user?: any) {
    await this.ensureSessionAccess(sessionId, user);
    const detail = await this.buildSessionDetail(sessionId);
    const session = detail.session;

    const overviewRows = [
      { Chỉ_số: "Mã phiên", Giá_trị: session.id },
      { Chỉ_số: "Tên phiên", Giá_trị: session.name },
      { Chỉ_số: "Chi nhánh", Giá_trị: session.branchId },
      { Chỉ_số: "Trạng thái", Giá_trị: session.status },
      { Chỉ_số: "Bắt đầu", Giá_trị: session.startedAt || "" },
      { Chỉ_số: "Kết thúc", Giá_trị: session.finishedAt || "" },
      { Chỉ_số: "Tổng SKU snapshot", Giá_trị: detail.kpi.totalSnapshotSku },
      { Chỉ_số: "Đã kiểm", Giá_trị: detail.kpi.countedSku },
      { Chỉ_số: "Chưa kiểm", Giá_trị: detail.kpi.uncountedSku },
      { Chỉ_số: "SKU lệch", Giá_trị: detail.kpi.discrepancySku },
      { Chỉ_số: "Tổng lệch SL", Giá_trị: detail.kpi.totalDiffQty },
      { Chỉ_số: "Tổng giá trị lệch", Giá_trị: detail.kpi.totalDiffValue },
    ];

    const logRows = detail.logs.map((log) => ({
      "Thời gian": log.createdAt,
      Worker: log.workerId || "",
      SKU: log.sku,
      Barcode: log.barcode || "",
      "Số lượng": log.qtyDelta,
      Khu: log.zone || "",
      "Vị trí": log.locationCode || "",
      "Trạng thái": log.status,
      "Ghi chú": log.note || "",
    }));

    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(
      workbook,
      XLSX.utils.json_to_sheet(overviewRows),
      "Tong quan",
    );
    XLSX.utils.book_append_sheet(
      workbook,
      XLSX.utils.json_to_sheet(this.toExcelRows(detail.rows)),
      "Toan bo san pham",
    );
    XLSX.utils.book_append_sheet(
      workbook,
      XLSX.utils.json_to_sheet(this.toExcelRows(detail.discrepancyRows)),
      "Chenh lech",
    );
    XLSX.utils.book_append_sheet(
      workbook,
      XLSX.utils.json_to_sheet(this.toExcelRows(detail.uncountedRows)),
      "Chua kiem",
    );
    XLSX.utils.book_append_sheet(
      workbook,
      XLSX.utils.json_to_sheet(logRows),
      "Log scan",
    );

    const buffer = XLSX.write(workbook, {
      type: "buffer",
      bookType: "xlsx",
    });

    const safeName = String(session.name || session.id)
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/[^a-zA-Z0-9-_]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 80);

    return {
      fileName: `kiem-kho-${safeName || session.id}.xlsx`,
      buffer,
    };
  }

  async getWorkerSummary(sessionId: string, workerId: string, user?: any) {
    await this.ensureSessionAccess(sessionId, user);
    const rows = await this.getSessionSummary(sessionId, user);
    return rows.filter((row: any) => row.workerId === workerId);
  }

  async getZoneSummary(sessionId: string, user?: any) {
    await this.ensureSessionAccess(sessionId, user);
    const workers = await this.prisma.stocktakeWorker.findMany({
      where: { sessionId },
      include: {
        scanEvents: true,
      },
      orderBy: { createdAt: "asc" },
    });

    return workers.map((worker) => {
      const totalScans = worker.scanEvents.reduce(
        (sum, event) => sum + event.qtyDelta,
        0,
      );

      return {
        workerId: worker.id,
        name: worker.name,
        zone: worker.zone,
        status: worker.status,
        startedAt: worker.startedAt,
        finishedAt: worker.finishedAt,
        totalScans,
        events: worker.scanEvents.length,
      };
    });
  }
  // OPTIONAL BACKEND PATCH: active session endpoint
  // Dán vào StocktakeSessionService nếu muốn frontend load phiên active theo chi nhánh.
  // Frontend trong gói này đã có localStorage resume, nên endpoint này là nâng cấp thêm.

  async getActiveSession(branchId?: string, user?: any) {
    const scopedBranchId = this.scopedBranchId(user, branchId);
    const session = await this.prisma.stocktakeSession.findFirst({
      where: {
        ...(scopedBranchId ? { branchId: scopedBranchId } : {}),
        status: {
          in: ["IN_PROGRESS", "PAUSED", "DRAFT"],
        },
      },
      orderBy: {
        updatedAt: "desc",
      },
      include: {
        workers: true,
        scanEvents: {
          orderBy: { createdAt: "desc" },
          take: 100,
        },
        _count: {
          select: {
            scanEvents: true,
          },
        },
      },
    });

    return session;
  }

  /* STOCKTAKE_V27_BRANCH_SCOPE_HELPERS */
  private normalizeBranchKeyV27(value: any): string {
    return String(value ?? '').trim().toLowerCase();
  }

  private async getBranchVariantIdsV27(branchId: string): Promise<Set<string>> {
    if (!branchId) return new Set<string>();

    const rows = await this.prisma.inventoryItem.findMany({
      where: { branchId },
      select: { variantId: true },
    });

    return new Set(rows.map((row: any) => String(row.variantId || '')).filter(Boolean));
  }

  private async filterRowsBySessionBranchV27<T extends any>(session: any, rows: T[]): Promise<T[]> {
    const branchVariantIds = await this.getBranchVariantIdsV27(String(session?.branchId || ''));
    if (!branchVariantIds.size) return [];

    return (Array.isArray(rows) ? rows : []).filter((row: any) => {
      const directBranch = row?.branchId || row?.branch?.id || row?.inventoryItem?.branchId;
      if (directBranch) return String(directBranch) === String(session.branchId);

      const variantId = row?.variantId || row?.productVariantId || row?.variant?.id || row?.inventoryItem?.variantId;
      if (variantId) return branchVariantIds.has(String(variantId));

      // Nếu API cũ không có variantId thì giữ các dòng đã có phát sinh scan, bỏ dòng catalog rỗng.
      const snapshotQty = Number(row?.snapshotQty ?? row?.systemQty ?? row?.openingQty ?? 0);
      const countedQty = Number(row?.countedQty ?? row?.counted ?? 0);
      const diff = Number(row?.diff ?? row?.deltaQty ?? (countedQty - snapshotQty));
      const status = String(row?.status || '').toUpperCase();
      return countedQty !== 0 || diff !== 0 || status === 'NOT_FOUND' || Boolean(row?.lastScannedAt || row?.workerId);
    });
  }

}
