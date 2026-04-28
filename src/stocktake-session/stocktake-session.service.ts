import { Injectable, NotFoundException } from "@nestjs/common";
import { PrismaService } from "src/prisma/prisma.service";
import { CreateStocktakeSessionDto } from "./dto/create-stocktake-session.dto";
import { JoinStocktakeSessionDto } from "./dto/join-stocktake-session.dto";
import { ScanStocktakeDto } from "./dto/scan-stocktake.dto";

@Injectable()
export class StocktakeSessionService {
  constructor(private prisma: PrismaService) {}

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

  startSession(id: string) {
    return this.prisma.stocktakeSession.update({
      where: { id },
      data: {
        status: "IN_PROGRESS",
        startedAt: new Date(),
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
    const events = await this.prisma.stocktakeScanEvent.findMany({
      where: { sessionId },
      orderBy: { createdAt: "asc" },
    });

    const grouped = new Map<string, any>();

    for (const event of events) {
      const key = event.variantId || event.sku;

      const current = grouped.get(key) || {
        variantId: event.variantId,
        sku: event.sku,
        counted: 0,
        status: event.status,
        events: 0,
      };

      current.counted += event.qtyDelta;
      current.events += 1;

      if (event.status !== "OK") {
        current.status = event.status;
      }

      grouped.set(key, current);
    }

    return Array.from(grouped.values()).filter((row) => row.counted > 0);
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
}
