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

    // Tìm phiên kiểm kho ở backend, không chỉ lọc trên 50 dòng đã tải ở UI.
    // query/q dành cho phiên: id CUID, mã hiển thị dạng KK-TH-ABC123, tên phiên, ghi chú, nhân viên/máy scan.
    const rawSessionQuery = this.normalizeSearchText(filters?.query || filters?.q);
    if (rawSessionQuery) {
      const compact = rawSessionQuery.replace(/[^a-zA-Z0-9]+/g, "");
      const dashParts = rawSessionQuery.split("-").map((part) => part.trim()).filter(Boolean);
      const lastPart = dashParts[dashParts.length - 1] || "";
      const searchTerms = Array.from(
        new Set([rawSessionQuery, compact, lastPart].map((term) => this.normalizeSearchText(term)).filter(Boolean)),
      );

      where.AND = [
        ...(Array.isArray(where.AND) ? where.AND : []),
        {
          OR: searchTerms.flatMap((term) => [
            { id: { contains: term, mode: "insensitive" } },
            { name: { contains: term, mode: "insensitive" } },
            { note: { contains: term, mode: "insensitive" } },
            { createdById: { contains: term, mode: "insensitive" } },
            {
              workers: {
                some: {
                  OR: [
                    { name: { contains: term, mode: "insensitive" } },
                    { userId: { contains: term, mode: "insensitive" } },
                    { deviceName: { contains: term, mode: "insensitive" } },
                    { zone: { contains: term, mode: "insensitive" } },
                  ],
                },
              },
            },
          ]),
        },
      ];
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

    const [snapshotGroups, countRows, resultRows] = await Promise.all([
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
      db.stocktakeResult?.findMany
        ? db.stocktakeResult.findMany({
            where: { sessionId: { in: sessionIds } },
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

    const resultMap = new Map<string, any>();
    for (const row of resultRows || []) {
      resultMap.set(String(row.sessionId), row);
      if (!snapshotStatMap.has(String(row.sessionId))) {
        snapshotStatMap.set(String(row.sessionId), {
          totalSku: Number(row.totalRows || 0),
          totalSnapshotQty: Number(row.totalSnapshotQty || 0),
        });
      }
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
      const storedResult = resultMap.get(session.id);
      const snapshotStat = snapshotStatMap.get(session.id) || {
        totalSku: 0,
        totalSnapshotQty: 0,
      };
      const kpi = storedResult
        ? {
            countedSku: Number(storedResult.countedSku || 0),
            notFoundSku: Number(storedResult.notFoundSku || 0),
            mismatchSku: Number(storedResult.mismatchSku || 0),
            discrepancySku: Number(storedResult.mismatchSku || 0),
            matchedSku: Number(storedResult.matchedSku || 0),
            totalCountedQty: Number(storedResult.totalCountedQty || 0),
            totalDiffQty: Number(storedResult.totalDiffQty || 0),
            totalDiffValue: Number(storedResult.totalDiffValue || 0),
          }
        : kpiMap.get(session.id) || {
            countedSku: 0,
            notFoundSku: 0,
            mismatchSku: 0,
            discrepancySku: 0,
            matchedSku: 0,
            totalCountedQty: 0,
            totalDiffQty: 0,
            totalDiffValue: 0,
          };

      const totalSku = snapshotStat.totalSku || kpi.countedSku;
      const uncountedSku = storedResult ? 0 : Math.max(totalSku - kpi.countedSku, 0);

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
          totalDiffValue: Number((kpi as any).totalDiffValue || 0),
          snapshotPurged: Boolean(storedResult),
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
      filters?.productQuery || filters?.productQ || filters?.sku || "";

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
      filters?.productQuery || filters?.productQ || filters?.sku || "";

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


  private buildDetailFromStoredResult(session: any, result: any, scanEvents: any[] = []) {
    const rows = (result?.items || []).map((item: any) => {
      const snapshotQty = Number(item.snapshotQty || 0);
      const countedQty = Number(item.countedQty || 0);
      const diff = Number(item.diffQty ?? countedQty - snapshotQty);
      const unitCost = Number(item.unitCost || 0);
      const diffValue = Number(item.diffValue || diff * unitCost);

      return {
        sessionId: item.sessionId || session.id,
        branchId: item.branchId || session.branchId,
        variantId: item.variantId || null,
        sku: item.sku,
        productName: item.productName || "",
        color: item.color || null,
        size: item.size || null,
        barcode: item.barcode || item.sku,
        unitCost,
        costPrice: unitCost,
        snapshotQty,
        countedQty,
        diff,
        beforeApplyQty: item.beforeApplyQty ?? null,
        afterApplyQty: item.afterApplyQty ?? null,
        currentQty: item.afterApplyQty ?? item.beforeApplyQty ?? null,
        isCounted: true,
        status: item.status || "MATCH",
        statusLabel: item.statusLabel || this.normalizeDetailStatus({
          hasCount: true,
          countedQty,
          snapshotQty,
          variantId: item.variantId,
          rawStatus: item.status,
        }).statusLabel,
        diffType: item.diffType || this.normalizeDetailStatus({
          hasCount: true,
          countedQty,
          snapshotQty,
          variantId: item.variantId,
          rawStatus: item.status,
        }).diffType,
        diffValue,
        valueDiff: diffValue,
        eventCount: Number(item.eventCount || 0),
        workerId: item.workerId || null,
        workerName: item.workerName || null,
        workerCount: item.workerId ? 1 : 0,
        zone: item.zone || null,
        areaId: item.areaId || null,
        rackId: item.rackId || null,
        rackCode: item.rackCode || null,
        locationCode: item.locationCode || null,
        lastScannedAt: item.lastScannedAt || null,
      };
    });

    const matchedRows = rows.filter((row: any) => row.status === "MATCH");
    const notFoundRows = rows.filter((row: any) => row.status === "NOT_FOUND");
    const discrepancyRows = rows.filter((row: any) => row.status === "MISMATCH" || Number(row.diff || 0) !== 0);
    const overRows = rows.filter((row: any) => row.diffType === "OVER" || Number(row.diff || 0) > 0);
    const shortRows = rows.filter((row: any) => row.diffType === "SHORT" || Number(row.diff || 0) < 0);

    const kpi = {
      totalSnapshotSku: Number(result.totalRows || rows.length),
      totalSku: Number(result.totalRows || rows.length),
      totalRows: Number(result.totalRows || rows.length),
      countedSku: Number(result.countedSku || rows.length),
      uncountedSku: 0,
      matchedSku: Number(result.matchedSku || matchedRows.length),
      mismatchSku: Number(result.mismatchSku || discrepancyRows.length),
      discrepancySku: Number(result.mismatchSku || discrepancyRows.length),
      notFoundSku: Number(result.notFoundSku || notFoundRows.length),
      overSku: overRows.length,
      shortSku: shortRows.length,
      totalSnapshotQty: Number(result.totalSnapshotQty || rows.reduce((sum: number, row: any) => sum + Number(row.snapshotQty || 0), 0)),
      totalCountedQty: Number(result.totalCountedQty || rows.reduce((sum: number, row: any) => sum + Number(row.countedQty || 0), 0)),
      totalDiffQty: Number(result.totalDiffQty || discrepancyRows.reduce((sum: number, row: any) => sum + Number(row.diff || 0), 0)),
      totalDiffValue: Number(result.totalDiffValue || discrepancyRows.reduce((sum: number, row: any) => sum + Number(row.diffValue || 0), 0)),
      scanEvents: session._count?.scanEvents || scanEvents.length,
      workerCount: session.workers?.length || 0,
      resultMode: true,
      snapshotPurged: true,
    };

    const workerMap = new Map((session.workers || []).map((worker: any) => [worker.id, worker]));

    return {
      ...session,
      session,
      kpi,
      rows,
      items: rows,
      uncountedRows: [],
      discrepancyRows,
      logs: scanEvents.map((log: any) => {
        const worker: any = log.workerId ? workerMap.get(log.workerId) : null;
        return {
          ...log,
          workerName: worker?.name || null,
        };
      }),
      recentLogs: scanEvents,
      result,
    };
  }

  private async persistStocktakeResult(
    tx: any,
    session: any,
    detail: any,
    beforeAfterMap = new Map<string, { beforeQty: number | null; afterQty: number | null }>(),
  ) {
    const rows = Array.isArray(detail?.rows) ? detail.rows : [];
    const countedRows = rows.filter((row: any) => row.isCounted || Number(row.countedQty || 0) !== 0 || row.lastScannedAt || row.workerId);
    const matchedRows = countedRows.filter((row: any) => String(row.status || "").toUpperCase() === "MATCH" && Number(row.diff || 0) === 0);
    const mismatchRows = countedRows.filter((row: any) => Number(row.diff || 0) !== 0 || String(row.status || "").toUpperCase() === "MISMATCH");
    const notFoundRows = countedRows.filter((row: any) => String(row.status || "").toUpperCase() === "NOT_FOUND" || !row.variantId);

    await tx.stocktakeResultItem.deleteMany({ where: { sessionId: session.id } });
    await tx.stocktakeResult.deleteMany({ where: { sessionId: session.id } });

    const result = await tx.stocktakeResult.create({
      data: {
        sessionId: session.id,
        branchId: session.branchId,
        totalRows: countedRows.length,
        countedSku: countedRows.length,
        matchedSku: matchedRows.length,
        mismatchSku: mismatchRows.length,
        notFoundSku: notFoundRows.length,
        totalSnapshotQty: countedRows.reduce((sum: number, row: any) => sum + Number(row.snapshotQty || 0), 0),
        totalCountedQty: countedRows.reduce((sum: number, row: any) => sum + Number(row.countedQty || 0), 0),
        totalDiffQty: mismatchRows.reduce((sum: number, row: any) => sum + Number(row.diff || 0), 0),
        totalDiffValue: mismatchRows.reduce((sum: number, row: any) => sum + Number(row.diffValue ?? row.valueDiff ?? 0), 0),
      },
    });

    if (countedRows.length) {
      await tx.stocktakeResultItem.createMany({
        data: countedRows.map((row: any) => {
          const variantId = row.variantId ? String(row.variantId) : null;
          const beforeAfter = variantId ? beforeAfterMap.get(variantId) : null;
          const snapshotQty = Number(row.snapshotQty || 0);
          const countedQty = Number(row.countedQty || 0);
          const diffQty = Number(row.diff ?? countedQty - snapshotQty);
          const unitCost = Number(row.unitCost ?? row.costPrice ?? 0);
          const diffValue = Number(row.diffValue ?? row.valueDiff ?? diffQty * unitCost);
          const beforeApplyQty = beforeAfter?.beforeQty ?? (Number.isFinite(Number(row.currentQty)) ? Number(row.currentQty) : null);
          const afterApplyQty = beforeAfter?.afterQty ?? (beforeApplyQty === null ? null : beforeApplyQty + diffQty);

          return {
            resultId: result.id,
            sessionId: session.id,
            branchId: row.branchId || session.branchId,
            variantId,
            sku: String(row.sku || ""),
            barcode: row.barcode || null,
            productName: row.productName || null,
            color: row.color || null,
            size: row.size || null,
            snapshotQty,
            countedQty,
            diffQty,
            beforeApplyQty,
            afterApplyQty,
            unitCost,
            diffValue,
            status: row.status || (diffQty === 0 ? "MATCH" : "MISMATCH"),
            statusLabel: row.statusLabel || null,
            diffType: row.diffType || null,
            eventCount: Number(row.eventCount || 0),
            workerId: row.workerId || null,
            workerName: row.workerName || null,
            zone: row.zone || null,
            areaId: row.areaId || null,
            rackId: row.rackId || null,
            rackCode: row.rackCode || null,
            locationCode: row.locationCode || null,
            lastScannedAt: row.lastScannedAt || null,
          };
        }),
      });
    }

    return { resultId: result.id, resultItemCount: countedRows.length };
  }

  private async cleanupAppliedSessionSnapshots(sessionId: string, user?: any) {
    await this.ensureSessionAccess(sessionId, user);
    const session = await this.prisma.stocktakeSession.findUnique({
      where: { id: sessionId },
      include: {
        workers: { orderBy: { createdAt: "asc" } },
        areas: { orderBy: { createdAt: "asc" } },
        _count: { select: { scanEvents: true } },
      },
    });

    if (!session) throw new NotFoundException("Không tìm thấy phiên kiểm kho.");
    if (this.normalizeStatus(session.status) !== "APPLIED") {
      throw new BadRequestException("Chỉ dọn snapshot của phiên đã chốt tồn.");
    }

    const snapshotCount = await this.prisma.stocktakeSnapshot.count({ where: { sessionId } });
    if (!snapshotCount) {
      return { ok: true, sessionId, deletedSnapshots: 0, resultItemCount: 0, alreadyCleaned: true };
    }

    const existingResult = await (this.prisma as any).stocktakeResult?.findUnique?.({
      where: { sessionId },
      select: { id: true },
    });

    const detail = existingResult ? null : await this.buildSessionDetail(sessionId);

    return this.prisma.$transaction(
      async (tx: any) => {
        let resultItemCount = 0;
        if (!existingResult && detail) {
          const stored = await this.persistStocktakeResult(tx, session, detail);
          resultItemCount = stored.resultItemCount;
        }

        const deleted = await tx.stocktakeSnapshot.deleteMany({ where: { sessionId } });
        await tx.stocktakeSession.update({
          where: { id: sessionId },
          data: { snapshotPurgedAt: new Date() },
        });

        return {
          ok: true,
          sessionId,
          deletedSnapshots: deleted.count,
          resultItemCount,
          alreadyHadResult: Boolean(existingResult),
        };
      },
      { maxWait: 10000, timeout: 30000 },
    );
  }

  async cleanupSessionSnapshots(sessionId: string, user?: any) {
    return this.cleanupAppliedSessionSnapshots(sessionId, user);
  }

  async cleanupAppliedSnapshots(body: { sessionIds?: string[] } = {}, user?: any) {
    if (!this.isOwner(user)) {
      throw new ForbiddenException("Chỉ Admin/Owner được dọn snapshot kiểm kho hàng loạt.");
    }

    const sessionIds = Array.from(
      new Set(
        (Array.isArray(body?.sessionIds) ? body.sessionIds : [])
          .map((id) => String(id || "").trim())
          .filter(Boolean),
      ),
    );

    if (!sessionIds.length) {
      throw new BadRequestException("Chưa chọn phiên kiểm kho để dọn snapshot.");
    }

    if (sessionIds.length > 100) {
      throw new BadRequestException("Mỗi lần chỉ dọn tối đa 100 phiên đã chọn.");
    }

    const sessions = await this.prisma.stocktakeSession.findMany({
      where: { id: { in: sessionIds } },
      select: { id: true, status: true, snapshotPurgedAt: true } as any,
    });

    const sessionMap = new Map(sessions.map((session) => [String(session.id), session]));

    let processedSessions = 0;
    let deletedSnapshots = 0;
    let resultItemCount = 0;
    let failed = 0;
    let skipped = 0;

    for (const sessionId of sessionIds) {
      const session = sessionMap.get(sessionId) as any;
      if (!session || this.normalizeStatus(session.status) !== "APPLIED" || session.snapshotPurgedAt) {
        skipped += 1;
        continue;
      }

      const snapshotCount = await this.prisma.stocktakeSnapshot.count({
        where: { sessionId },
      });

      if (!snapshotCount) {
        await this.prisma.stocktakeSession.update({
          where: { id: sessionId },
          data: { snapshotPurgedAt: new Date() } as any,
        });
        skipped += 1;
        continue;
      }

      try {
        const result = await this.cleanupAppliedSessionSnapshots(sessionId, user);
        processedSessions += 1;
        deletedSnapshots += Number(result.deletedSnapshots || 0);
        resultItemCount += Number(result.resultItemCount || 0);
      } catch {
        failed += 1;
      }
    }

    return {
      ok: true,
      processedSessions,
      deletedSnapshots,
      resultItemCount,
      failed,
      skipped,
      selectedSessions: sessionIds.length,
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

    // FAST DETAIL MODE:
    // Chi tiết phiên chỉ cần những SKU đã phát sinh trong phiên (StocktakeCount / ScanEvent),
    // không load toàn bộ StocktakeSnapshot của cả kho nữa. Snapshot chỉ được lookup theo
    // variantId đã kiểm để lấy tồn hệ thống lúc bắt đầu phiên và tính lệch.
    // Biến ensureSnapshot giữ lại để không phá chữ ký hàm cũ, nhưng detail không tự tạo snapshot
    // vì createSnapshotForSession có thể quét toàn kho và làm chậm khi mở chi tiết.
    void ensureSnapshot;

    const storedResult = await (this.prisma as any).stocktakeResult?.findUnique?.({
      where: { sessionId },
      include: { items: { orderBy: { createdAt: "asc" } } },
    });

    if (storedResult) {
      const scanEvents = await this.prisma.stocktakeScanEvent.findMany({
        where: { sessionId, branchId: session.branchId },
        orderBy: { createdAt: "desc" },
        take: 1000,
      });
      return this.buildDetailFromStoredResult(session, storedResult, scanEvents);
    }

    const [countRows, scanEvents] = await Promise.all([
      this.prisma.stocktakeCount.findMany({
        where: { sessionId, branchId: session.branchId },
        orderBy: { lastScannedAt: "asc" },
      }),
      this.prisma.stocktakeScanEvent.findMany({
        where: { sessionId, branchId: session.branchId },
        orderBy: { createdAt: "desc" },
        take: 1000,
      }),
    ]);

    const workerMap = new Map(
      (session.workers || []).map((worker) => [worker.id, worker]),
    );

    type FastCountAgg = {
      variantId: string | null;
      sku: string;
      countedQty: number;
      eventCount: number;
      workerIds: Set<string>;
      zone?: string | null;
      areaId?: string | null;
      rackId?: string | null;
      rackCode?: string | null;
      locationCode?: string | null;
      status?: string | null;
      lastScannedAt?: Date | null;
      barcode?: string | null;
    };

    const countMap = new Map<string, FastCountAgg>();

    const upsertCountAgg = (input: {
      variantId?: string | null;
      sku?: string | null;
      countedQty?: number | null;
      eventCount?: number | null;
      workerId?: string | null;
      zone?: string | null;
      areaId?: string | null;
      rackId?: string | null;
      rackCode?: string | null;
      locationCode?: string | null;
      status?: string | null;
      lastScannedAt?: Date | string | null;
      barcode?: string | null;
    }) => {
      const sku = String(input.sku || "").trim();
      if (!sku && !input.variantId) return;

      const variantId = input.variantId ? String(input.variantId) : null;
      const key = variantId || `SKU:${sku}`;
      const current =
        countMap.get(key) ||
        ({
          variantId,
          sku,
          countedQty: 0,
          eventCount: 0,
          workerIds: new Set<string>(),
          zone: input.zone || null,
          areaId: input.areaId || null,
          rackId: input.rackId || null,
          rackCode: input.rackCode || null,
          locationCode: input.locationCode || null,
          status: input.status || null,
          lastScannedAt: input.lastScannedAt ? new Date(input.lastScannedAt) : null,
          barcode: input.barcode || null,
        } satisfies FastCountAgg);

      current.countedQty += Number(input.countedQty || 0);
      current.eventCount += Number(input.eventCount || 0);
      if (input.workerId) current.workerIds.add(String(input.workerId));
      current.zone = input.zone || current.zone;
      current.areaId = input.areaId || current.areaId;
      current.rackId = input.rackId || current.rackId;
      current.rackCode = input.rackCode || current.rackCode;
      current.locationCode = input.locationCode || current.locationCode;
      current.status = input.status || current.status;
      current.barcode = input.barcode || current.barcode;

      const nextLastScannedAt = input.lastScannedAt ? new Date(input.lastScannedAt) : null;
      if (
        nextLastScannedAt &&
        (!current.lastScannedAt || nextLastScannedAt > current.lastScannedAt)
      ) {
        current.lastScannedAt = nextLastScannedAt;
      }

      countMap.set(key, current);
    };

    for (const count of countRows || []) {
      upsertCountAgg({
        variantId: count.variantId,
        sku: count.sku,
        countedQty: Number(count.countedQty || 0),
        eventCount: Number(count.eventCount || 0),
        workerId: count.workerId,
        zone: count.zone,
        areaId: count.areaId,
        rackId: count.rackId,
        rackCode: count.rackCode,
        locationCode: count.locationCode,
        status: count.status,
        lastScannedAt: count.lastScannedAt,
      });
    }

    // Fallback cho phiên cũ chưa có StocktakeCount: đọc log scan và aggregate trong RAM,
    // không ghi backfill để tránh mở chi tiết mà phát sinh nhiều write.
    if (!countMap.size && scanEvents.length) {
      for (const event of scanEvents) {
        upsertCountAgg({
          variantId: event.variantId,
          sku: event.sku,
          countedQty: Number(event.qtyDelta || 0),
          eventCount: 1,
          workerId: event.workerId,
          zone: event.zone,
          locationCode: event.locationCode,
          status: event.status,
          lastScannedAt: event.createdAt,
          barcode: event.barcode,
        });
      }
    } else {
      // Bổ sung barcode / worker cuối từ log scan cho các count đã có.
      for (const event of scanEvents) {
        const key = event.variantId || `SKU:${event.sku}`;
        const current = countMap.get(key);
        if (!current) continue;
        current.barcode = event.barcode || current.barcode;
        if (event.workerId) current.workerIds.add(String(event.workerId));
        if (!current.lastScannedAt || event.createdAt > current.lastScannedAt) {
          current.lastScannedAt = event.createdAt;
        }
      }
    }

    const counts = Array.from(countMap.values()).filter(
      (row) => Number(row.eventCount || 0) > 0 || Number(row.countedQty || 0) !== 0,
    );

    const variantIds = Array.from(
      new Set(counts.map((row) => row.variantId).filter(Boolean)),
    ) as string[];

    const [snapshots, variants, inventoryItems] = await Promise.all([
      variantIds.length
        ? this.prisma.stocktakeSnapshot.findMany({
            where: {
              sessionId,
              branchId: session.branchId,
              variantId: { in: variantIds },
            },
            select: {
              variantId: true,
              snapshotQty: true,
            },
          })
        : Promise.resolve([]),
      variantIds.length
        ? this.prisma.productVariant.findMany({
            where: { id: { in: variantIds } },
            include: { product: true, locations: true },
          })
        : Promise.resolve([]),
      variantIds.length
        ? this.prisma.inventoryItem.findMany({
            where: { branchId: session.branchId, variantId: { in: variantIds } },
            select: { variantId: true, availableQty: true },
          })
        : Promise.resolve([]),
    ]);

    const snapshotMap = new Map(
      (snapshots || []).map(
        (item) => [item.variantId, Number(item.snapshotQty || 0)] as [string, number],
      ),
    );
    const variantMap = new Map<string, any>(
      (variants || []).map((variant: any) => [String(variant.id), variant] as [string, any]),
    );
    const inventoryMap = new Map(
      (inventoryItems || []).map(
        (item) => [item.variantId, Number(item.availableQty || 0)] as [string, number],
      ),
    );

    const buildWorkerInfo = (count?: FastCountAgg) => {
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

    const rows = counts.map((count) => {
      const variant: any = count.variantId ? variantMap.get(String(count.variantId)) : null;
      const snapshotQty = count.variantId
        ? Number(snapshotMap.get(count.variantId) || 0)
        : 0;
      const countedQty = Number(count.countedQty || 0);
      const diff = countedQty - snapshotQty;
      const unitCost = Number(variant?.costPrice || 0);
      const statusInfo = this.normalizeDetailStatus({
        hasCount: true,
        countedQty,
        snapshotQty,
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
        barcode: count.barcode || count.sku,
        unitCost,
        costPrice: unitCost,
        price: Number(variant?.price || 0),
        snapshotQty,
        countedQty,
        diff,
        currentQty: count.variantId ? Number(inventoryMap.get(count.variantId) || 0) : 0,
        isCounted: true,
        status: statusInfo.status,
        statusLabel: statusInfo.statusLabel,
        diffType: statusInfo.diffType,
        diffValue: diff * unitCost,
        valueDiff: diff * unitCost,
        eventCount: Number(count.eventCount || 0),
        ...workerInfo,
        zone: count.zone || null,
        areaId: count.areaId || null,
        rackId: count.rackId || null,
        rackCode: count.rackCode || null,
        locationCode: count.locationCode || null,
        lastScannedAt: count.lastScannedAt || null,
      };
    });

    const countedRows = rows;
    const uncountedRows: any[] = [];
    const matchedRows = rows.filter((row) => row.status === "MATCH");
    const notFoundRows = rows.filter((row) => row.status === "NOT_FOUND");
    const discrepancyRows = rows.filter((row) => row.status === "MISMATCH");
    const overRows = rows.filter((row) => row.diffType === "OVER");
    const shortRows = rows.filter((row) => row.diffType === "SHORT");

    const kpi = {
      totalSnapshotSku: rows.length,
      totalSku: rows.length,
      totalRows: rows.length,
      countedSku: countedRows.length,
      uncountedSku: 0,
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
      scanEvents: session._count?.scanEvents || scanEvents.length,
      workerCount: session.workers.length,
      fastMode: true,
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
        const beforeAfterMap = new Map<string, { beforeQty: number | null; afterQty: number | null }>();

        for (const row of rowsToApply) {
          const variantId = String(row.variantId || "");
          if (!variantId) continue;

          const diff = Number(row.diff || 0);
          const inventoryItem = await tx.inventoryItem.findUnique({
            where: {
              variantId_branchId: {
                variantId,
                branchId: session.branchId,
              },
            },
          });

          if (!inventoryItem) continue;

          const beforeQty = Number(inventoryItem.availableQty || 0);
          const afterQty = beforeQty + diff;
          beforeAfterMap.set(variantId, { beforeQty, afterQty });

          await tx.inventoryItem.update({
            where: {
              variantId_branchId: {
                variantId,
                branchId: session.branchId,
              },
            },
            data: {
              availableQty: afterQty,
            },
          });

          await tx.inventoryMovement.create({
            data: {
              variantId,
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

        const storedResult = await this.persistStocktakeResult(tx, session, detail, beforeAfterMap);
        const deletedSnapshots = await tx.stocktakeSnapshot.deleteMany({
          where: { sessionId: session.id },
        });

        const appliedAt = new Date();
        const updatedSession = await tx.stocktakeSession.update({
          where: { id: session.id },
          data: {
            status: "APPLIED",
            finishedAt: session.finishedAt || appliedAt,
            appliedAt,
            snapshotPurgedAt: appliedAt,
          },
        });

        return {
          ok: true,
          session: updatedSession,
          adjustedCount,
          totalDelta,
          skippedUncounted: detail.uncountedRows.length,
          discrepancySku: detail.kpi.discrepancySku,
          resultItemCount: storedResult.resultItemCount,
          deletedSnapshots: deletedSnapshots.count,
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
