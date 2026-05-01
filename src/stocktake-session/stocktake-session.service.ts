import { BadRequestException, Injectable, NotFoundException } from "@nestjs/common";
import { PrismaService } from "src/prisma/prisma.service";
import { CreateStocktakeSessionDto } from "./dto/create-stocktake-session.dto";
import { JoinStocktakeSessionDto } from "./dto/join-stocktake-session.dto";
import { ScanStocktakeDto } from "./dto/scan-stocktake.dto";

@Injectable()
export class StocktakeSessionService {
  constructor(private prisma: PrismaService) {}

  private normalizeStatus(status?: string | null) {
    return String(status || "").trim().toUpperCase();
  }

  private isPaused(status?: string | null) {
    return this.normalizeStatus(status) === "PAUSED";
  }

  private isClosed(status?: string | null) {
    return ["FINISHED", "APPLIED", "CANCELLED"].includes(this.normalizeStatus(status));
  }

  private async createSnapshotForSession(sessionId: string, branchId: string) {
    const existing = await this.prisma.stocktakeSnapshot.count({
      where: { sessionId },
    });

    if (existing > 0) {
      return { created: 0, skipped: existing };
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

  createSession(dto: CreateStocktakeSessionDto) {
    return this.prisma.stocktakeSession.create({
      data: {
        branchId: dto.branchId,
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

  listSessions(branchId?: string) {
    return this.prisma.stocktakeSession.findMany({
      where: branchId ? { branchId } : undefined,
      orderBy: { createdAt: "desc" },
      include: {
        workers: true,
        _count: {
          select: {
            scanEvents: true,
          },
        },
      },
    });
  }

  getSession(id: string) {
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

  async startSession(id: string) {
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

  async pauseSession(id: string) {
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

  async resumeSession(id: string) {
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

  async finishSession(id: string) {
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

  async joinSession(sessionId: string, dto: JoinStocktakeSessionDto) {
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

  finishWorker(workerId: string) {
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

  async scan(dto: ScanStocktakeDto) {
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
      throw new BadRequestException("Phiên kiểm kho đang tạm dừng. Bấm tiếp tục để scan.");
    }

    if (this.isClosed(session.status)) {
      throw new BadRequestException("Phiên kiểm kho đã kết thúc, không thể scan thêm.");
    }

    await this.createSnapshotForSession(dto.sessionId, session.branchId);

    const variant = await this.prisma.productVariant.findFirst({
      where: {
        sku: code,
      },
    });

    const targetRack = await this.markAreaAndRackInProgress(dto);
    const finalLocationCode =
      dto.locationCode || dto.rackCode || targetRack?.code || dto.zone;

    const event = await this.prisma.stocktakeScanEvent.create({
      data: {
        sessionId: dto.sessionId,
        workerId: dto.workerId,
        branchId: dto.branchId,
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
      branchId: dto.branchId,
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

  async finishArea(areaId: string, status: "FINISHED" | "MISMATCH" = "FINISHED") {
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

  async getSessionSummary(sessionId: string) {
    const session = await this.prisma.stocktakeSession.findUnique({
      where: { id: sessionId },
    });

    if (!session) {
      throw new NotFoundException("Không tìm thấy phiên kiểm kho.");
    }

    await this.createSnapshotForSession(sessionId, session.branchId);

    let counts = await this.prisma.stocktakeCount.findMany({
      where: { sessionId },
      orderBy: { lastScannedAt: "asc" },
    });

    // Backfill aggregate cho phiên cũ đã có scanEvents nhưng chưa có StocktakeCount.
    if (!counts.length) {
      const events = await this.prisma.stocktakeScanEvent.findMany({
        where: { sessionId },
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
        where: { sessionId },
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
  snapshots.map((item) => [
    item.variantId,
    Number(item.snapshotQty || 0),
  ] as [string, number])
);

const movementMap = new Map<string, number>(
  movements.map((item) => [
    item.variantId,
    Number(item._sum.qty || 0),
  ] as [string, number])
);

    return counts
      .filter((row) => Number(row.countedQty || 0) > 0)
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

    async getWorkerSummary(sessionId: string, workerId: string) {
    const rows = await this.getSessionSummary(sessionId);
    return rows.filter((row: any) => row.workerId === workerId);
  }

  async getZoneSummary(sessionId: string) {
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
        0
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

async getActiveSession(branchId?: string) {
  const session = await this.prisma.stocktakeSession.findFirst({
    where: {
      ...(branchId ? { branchId } : {}),
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

}
