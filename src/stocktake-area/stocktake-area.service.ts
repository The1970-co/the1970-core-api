import { Injectable, NotFoundException } from "@nestjs/common";
import { PrismaService } from "src/prisma/prisma.service";
import { CreateStocktakeAreaDto } from "./dto/create-stocktake-area.dto";

@Injectable()
export class StocktakeAreaService {
  constructor(private prisma: PrismaService) {}

  async create(dto: CreateStocktakeAreaDto) {
    const session = await this.prisma.stocktakeSession.findUnique({
      where: { id: dto.sessionId },
    });

    if (!session) {
      throw new NotFoundException("Không tìm thấy phiên kiểm kho.");
    }

    return this.prisma.stocktakeArea.create({
      data: {
        sessionId: dto.sessionId,
        branchId: dto.branchId,
        mapId: dto.mapId,
        scopeType: dto.scopeType,
        aisle: dto.aisle,
        rackId: dto.rackId,
        rackCode: dto.rackCode,
        label: dto.label,
        status: "PENDING",
      },
    });
  }

  listBySession(sessionId: string) {
    return this.prisma.stocktakeArea.findMany({
      where: { sessionId },
      orderBy: { createdAt: "asc" },
    });
  }

  start(id: string) {
    return this.prisma.stocktakeArea.update({
      where: { id },
      data: {
        status: "IN_PROGRESS",
        startedAt: new Date(),
      },
    });
  }

  finish(id: string) {
    return this.prisma.stocktakeArea.update({
      where: { id },
      data: {
        status: "FINISHED",
        finishedAt: new Date(),
      },
    });
  }

  markMismatch(id: string) {
    return this.prisma.stocktakeArea.update({
      where: { id },
      data: {
        status: "MISMATCH",
      },
    });
  }
}