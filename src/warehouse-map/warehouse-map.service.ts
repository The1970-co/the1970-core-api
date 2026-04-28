import { Injectable, NotFoundException } from "@nestjs/common";
import { PrismaService } from "src/prisma/prisma.service";
import { CreateWarehouseMapDto } from "./dto/create-warehouse-map.dto";
import { CreateRackDto } from "./dto/create-rack.dto";
import { UpdateRackDto } from "./dto/update-rack.dto";
import { AssignVariantLocationDto } from "./dto/assign-variant-location.dto";
import { CreateCustomLayoutDto } from "./dto/create-custom-layout.dto";
import { CreateFloorDto } from "./dto/create-floor.dto";
import { CreateZoneDto } from "./dto/create-zone.dto";
import { CreateDoorDto } from "./dto/create-door.dto";

@Injectable()
export class WarehouseMapService {
    constructor(private prisma: PrismaService) { }

    createMap(dto: CreateWarehouseMapDto) {
        return this.prisma.warehouseMap.create({
            data: {
                branchId: dto.branchId,
                name: dto.name,
                code: dto.code,
                note: dto.note,
                width: dto.width || 1200,
                height: dto.height || 800,
            },
            include: {
                racks: {
                    where: { isActive: true },
                    include: { shelves: true },
                },
            },
        });
    }

    listMaps(branchId?: string) {
        return this.prisma.warehouseMap.findMany({
            where: {
                isActive: true,
                ...(branchId ? { branchId } : {}),
            },
            orderBy: { createdAt: "desc" },
            include: {
                racks: {
                    where: { isActive: true },
                    include: { shelves: true },
                    orderBy: [{ zone: "asc" }, { aisle: "asc" }, { rackNo: "asc" }],
                },
            },
        });
    }

    getMap(id: string) {
        return this.prisma.warehouseMap.findUnique({
            where: { id },
            include: {
                racks: {
                    where: { isActive: true },
                    include: {
                        shelves: {
                            orderBy: { floorNo: "asc" },
                        },
                        variantLocations: true,
                    },
                    orderBy: [{ zone: "asc" }, { aisle: "asc" }, { rackNo: "asc" }],
                },
            },
        });
    }
private buildRackCode(dto: {
  mapCode: string;
  zone: string;
  aisle: string;
  rackNo: string;
}) {
  return `${dto.mapCode}-${dto.zone}-${dto.aisle}-${dto.rackNo}`;
}

    async createRack(dto: CreateRackDto) {
        const map = await this.prisma.warehouseMap.findUnique({
            where: { id: dto.mapId },
        });

        if (!map) {
            throw new NotFoundException("Không tìm thấy sơ đồ kho.");
        }

        const floors = dto.floors || 5;
        const code = this.buildRackCode({
            mapCode: map.code,
            zone: dto.zone,
            aisle: dto.aisle,
            rackNo: dto.rackNo,
        });

        const existingActiveRack = await this.prisma.warehouseRack.findFirst({
            where: {
                code,
                isActive: true,
            },
            include: {
                shelves: {
                    orderBy: { floorNo: "asc" },
                },
            },
        });

        if (existingActiveRack) {
            return existingActiveRack;
        }

        const existingInactiveRack = await this.prisma.warehouseRack.findFirst({
            where: {
                code,
                isActive: false,
            },
        });

        if (existingInactiveRack) {
            return this.prisma.$transaction(async (tx) => {
                const rack = await tx.warehouseRack.update({
                    where: { id: existingInactiveRack.id },
                    data: {
                        mapId: dto.mapId,
                        branchId: dto.branchId,
                        name: dto.name,
                        zone: dto.zone,
                        aisle: dto.aisle,
                        rackNo: dto.rackNo,
                        floors,
                        x: dto.x ?? 0,
                        y: dto.y ?? 0,
                        w: dto.w ?? 120,
                        h: dto.h ?? 260,
                        rotation: dto.rotation ?? 0,
                        note: dto.note,
                        status: "PENDING",
                        isActive: true,
                    },
                });

                for (let i = 1; i <= floors; i++) {
                    const shelfCode = `${rack.code}-T${String(i).padStart(2, "0")}`;

                    await tx.warehouseShelf.upsert({
                        where: { code: shelfCode },
                        update: {
                            rackId: rack.id,
                            floorNo: i,
                            label: `Tầng ${i}`,
                        },
                        create: {
                            rackId: rack.id,
                            floorNo: i,
                            code: shelfCode,
                            label: `Tầng ${i}`,
                        },
                    });
                }

                return tx.warehouseRack.findUnique({
                    where: { id: rack.id },
                    include: {
                        shelves: {
                            orderBy: { floorNo: "asc" },
                        },
                    },
                });
            });
        }

        return this.prisma.$transaction(async (tx) => {
            const rack = await tx.warehouseRack.create({
                data: {
                    mapId: dto.mapId,
                    branchId: dto.branchId,
                    code,
                    name: dto.name,
                    zone: dto.zone,
                    aisle: dto.aisle,
                    rackNo: dto.rackNo,
                    floors,
                    x: dto.x ?? 0,
                    y: dto.y ?? 0,
                    w: dto.w ?? 120,
                    h: dto.h ?? 260,
                    rotation: dto.rotation ?? 0,
                    note: dto.note,
                },
            });

            for (let i = 1; i <= floors; i++) {
                const shelfCode = `${rack.code}-T${String(i).padStart(2, "0")}`;

                await tx.warehouseShelf.upsert({
                    where: { code: shelfCode },
                    update: {
                        rackId: rack.id,
                        floorNo: i,
                        label: `Tầng ${i}`,
                    },
                    create: {
                        rackId: rack.id,
                        floorNo: i,
                        code: shelfCode,
                        label: `Tầng ${i}`,
                    },
                });
            }

            return tx.warehouseRack.findUnique({
                where: { id: rack.id },
                include: {
                    shelves: {
                        orderBy: { floorNo: "asc" },
                    },
                },
            });
        });
    }

async createQuickLayout(mapId: string) {
  const map = await this.prisma.warehouseMap.findUnique({
    where: { id: mapId },
  });

  if (!map) {
    throw new NotFoundException("Không tìm thấy sơ đồ kho.");
  }

  let created = 0;
  let updated = 0;

  for (let aisleIndex = 1; aisleIndex <= 4; aisleIndex++) {
    for (let rackIndex = 1; rackIndex <= 12; rackIndex++) {
      const aisle = `D${String(aisleIndex).padStart(2, "0")}`;
      const rackNo = `K${String(rackIndex).padStart(2, "0")}`;

      const code = this.buildRackCode({
        mapCode: map.code,
        zone: "A",
        aisle,
        rackNo,
      });

      const x = 80 + (aisleIndex - 1) * 230;
      const y = 60 + (rackIndex - 1) * 42;

      const existing = await this.prisma.warehouseRack.findFirst({
        where: { code },
      });

      if (existing) {
        await this.prisma.warehouseRack.update({
          where: { id: existing.id },
          data: {
            mapId,
            branchId: map.branchId,
            name: `Dãy ${aisleIndex} - Kệ ${rackIndex}`,
            zone: "A",
            aisle,
            rackNo,
            floors: 5,
            x,
            y,
            w: 160,
            h: 34,
            rotation: 0,
            status: "PENDING",
            isActive: true,
          },
        });

        updated += 1;
        continue;
      }

      const rack = await this.prisma.warehouseRack.create({
        data: {
          mapId,
          branchId: map.branchId,
          code,
          name: `Dãy ${aisleIndex} - Kệ ${rackIndex}`,
          zone: "A",
          aisle,
          rackNo,
          floors: 5,
          x,
          y,
          w: 160,
          h: 34,
          rotation: 0,
          status: "PENDING",
          isActive: true,
        },
      });

      for (let floor = 1; floor <= 5; floor++) {
        await this.prisma.warehouseShelf.upsert({
          where: {
            code: `${code}-T${String(floor).padStart(2, "0")}`,
          },
          update: {
            rackId: rack.id,
            floorNo: floor,
            label: `Tầng ${floor}`,
          },
          create: {
            rackId: rack.id,
            code: `${code}-T${String(floor).padStart(2, "0")}`,
            floorNo: floor,
            label: `Tầng ${floor}`,
          },
        });
      }

      created += 1;
    }
  }

  return {
    ok: true,
    created,
    updated,
    total: 48,
    message: `Layout đã đủ 48 kệ. Tạo mới ${created}, cập nhật ${updated}.`,
  };
}

    async resetLayout(mapId: string) {
        const map = await this.prisma.warehouseMap.findUnique({
            where: { id: mapId },
        });

        if (!map) {
            throw new NotFoundException("Không tìm thấy sơ đồ kho.");
        }

        await this.prisma.warehouseRack.updateMany({
            where: { mapId },
            data: {
                isActive: false,
                status: "PENDING",
            },
        });

        return {
            ok: true,
            message: "Đã reset kệ trong sơ đồ. Có thể tạo layout lại.",
        };
    }

    async updateRack(id: string, dto: UpdateRackDto) {
        return this.prisma.warehouseRack.update({
            where: { id },
            data: dto,
            include: {
                shelves: {
                    orderBy: { floorNo: "asc" },
                },
            },
        });
    }

    async deleteRack(id: string) {
        return this.prisma.warehouseRack.update({
            where: { id },
            data: {
                isActive: false,
                status: "PENDING",
            },
        });
    }

    async assignVariant(dto: AssignVariantLocationDto) {
        return this.prisma.productVariantLocation.create({
            data: {
                variantId: dto.variantId,
                rackId: dto.rackId,
                shelfId: dto.shelfId,
                isPrimary: dto.isPrimary || false,
                note: dto.note,
            },
        });
    }

    async getZones(branchId: string) {
        const racks = await this.prisma.warehouseRack.findMany({
            where: {
                branchId,
                isActive: true,
            },
            orderBy: [{ zone: "asc" }, { aisle: "asc" }, { rackNo: "asc" }],
        });

        const map = new Map<string, any>();

        for (const rack of racks) {
            const code = `${rack.branchId}-${rack.zone}`;

            if (!map.has(code)) {
                map.set(code, {
                    code,
                    branchId: rack.branchId,
                    zone: rack.zone,
                    label: `Khu ${rack.zone}`,
                    racks: 0,
                });
            }

            map.get(code).racks += 1;
        }

        return Array.from(map.values());
    }
    private normalizeAisle(value: string) {
  const clean = String(value || "").trim().toUpperCase();
  if (!clean) return "A";
  if (/^D\d+$/i.test(clean)) return clean.toUpperCase();
  return clean.replace(/^DÃY\s*/i, "");
}

async createCustomLayout(mapId: string, dto: CreateCustomLayoutDto) {
  const map = await this.prisma.warehouseMap.findUnique({
    where: { id: mapId },
  });

  if (!map) {
    throw new NotFoundException("Không tìm thấy sơ đồ kho.");
  }

  const zone = dto.zone || "A";

  if (dto.resetBeforeCreate) {
    await this.prisma.warehouseRack.updateMany({
      where: { mapId },
      data: {
        isActive: false,
        status: "PENDING",
      },
    });
  }

  let created = 0;
  let updated = 0;

  const aisles = (dto.aisles || []).filter((item) => item.rackCount > 0);

  for (let aisleIndex = 0; aisleIndex < aisles.length; aisleIndex++) {
    const item = aisles[aisleIndex];
    const aisle = this.normalizeAisle(item.aisle);
    const rackCount = Number(item.rackCount || 0);
    const floors = Number(item.floors || 5);

    for (let rackIndex = 1; rackIndex <= rackCount; rackIndex++) {
      const rackNo = `K${String(rackIndex).padStart(2, "0")}`;
      const code = this.buildRackCode({
        mapCode: map.code,
        zone,
        aisle,
        rackNo,
      });

      const x = 80 + aisleIndex * 260;
      const y = 60 + (rackIndex - 1) * 52;

      const existing = await this.prisma.warehouseRack.findFirst({
        where: { code },
      });

      if (existing) {
        await this.prisma.warehouseRack.update({
          where: { id: existing.id },
          data: {
            mapId,
            branchId: map.branchId,
            name: `Dãy ${aisle} - Kệ ${rackIndex}`,
            zone,
            aisle,
            rackNo,
            floors,
            x,
            y,
            w: 180,
            h: 40,
            rotation: 0,
            status: "PENDING",
            isActive: true,
          },
        });

        for (let floor = 1; floor <= floors; floor++) {
          await this.prisma.warehouseShelf.upsert({
            where: { code: `${code}-T${String(floor).padStart(2, "0")}` },
            update: {
              rackId: existing.id,
              floorNo: floor,
              label: `Tầng ${floor}`,
            },
            create: {
              rackId: existing.id,
              code: `${code}-T${String(floor).padStart(2, "0")}`,
              floorNo: floor,
              label: `Tầng ${floor}`,
            },
          });
        }

        updated += 1;
        continue;
      }

      const rack = await this.prisma.warehouseRack.create({
        data: {
          mapId,
          branchId: map.branchId,
          code,
          name: `Dãy ${aisle} - Kệ ${rackIndex}`,
          zone,
          aisle,
          rackNo,
          floors,
          x,
          y,
          w: 180,
          h: 40,
          rotation: 0,
          status: "PENDING",
          isActive: true,
        },
      });

      for (let floor = 1; floor <= floors; floor++) {
        await this.prisma.warehouseShelf.create({
          data: {
            rackId: rack.id,
            code: `${code}-T${String(floor).padStart(2, "0")}`,
            floorNo: floor,
            label: `Tầng ${floor}`,
          },
        });
      }

      created += 1;
    }
  }

  return {
    ok: true,
    created,
    updated,
    total: created + updated,
    message: `Đã lưu layout kho: tạo mới ${created}, cập nhật ${updated}.`,
  };
}

    async getFullMap(mapId: string) {
        const map = await this.prisma.warehouseMap.findUnique({
            where: { id: mapId },
            include: {
                racks: {
                    where: { isActive: true },
                    include: {
                        shelves: {
                            orderBy: { floorNo: "asc" },
                        },
                        variantLocations: true,
                    },
                    orderBy: [{ zone: "asc" }, { aisle: "asc" }, { rackNo: "asc" }],
                },
                floors: {
                    orderBy: { level: "asc" },
                    include: {
                        zones: {
                            orderBy: { createdAt: "asc" },
                        },
                        doors: {
                            orderBy: { createdAt: "asc" },
                        },
                    },
                },
                zones: {
                    orderBy: { createdAt: "asc" },
                },
                doors: {
                    orderBy: { createdAt: "asc" },
                },
            },
        });

        if (!map) {
            throw new NotFoundException("Không tìm thấy sơ đồ kho.");
        }

        if (!map.floors.length) {
            await this.prisma.warehouseFloor.create({
                data: {
                    mapId,
                    name: "Tầng 1",
                    level: 1,
                },
            });

            return this.getFullMap(mapId);
        }

        return map;
    }

    async createFloor(mapId: string, dto: CreateFloorDto) {
        const map = await this.prisma.warehouseMap.findUnique({
            where: { id: mapId },
        });

        if (!map) {
            throw new NotFoundException("Không tìm thấy sơ đồ kho.");
        }

        return this.prisma.warehouseFloor.create({
            data: {
                mapId,
                name: dto.name || `Tầng ${dto.level || 1}`,
                level: Number(dto.level || 1),
                note: dto.note,
            },
        });
    }

    async createZone(mapId: string, dto: CreateZoneDto) {
        const floor = await this.prisma.warehouseFloor.findUnique({
            where: { id: dto.floorId },
        });

        if (!floor) {
            throw new NotFoundException("Không tìm thấy tầng.");
        }

        return this.prisma.warehouseZone.create({
            data: {
                mapId,
                floorId: dto.floorId,
                name: dto.name,
                type: dto.type || "STORAGE",
                x: Number(dto.x ?? 0),
                y: Number(dto.y ?? 0),
                width: Number(dto.width ?? 500),
                height: Number(dto.height ?? 300),
                color: dto.color,
                note: dto.note,
            },
        });
    }

    async createDoor(mapId: string, dto: CreateDoorDto) {
        const floor = await this.prisma.warehouseFloor.findUnique({
            where: { id: dto.floorId },
        });

        if (!floor) {
            throw new NotFoundException("Không tìm thấy tầng.");
        }

        return this.prisma.warehouseDoor.create({
            data: {
                mapId,
                floorId: dto.floorId,
                name: dto.name || "Cửa kho",
                side: dto.side || "BOTTOM",
                x: Number(dto.x ?? 0),
                y: Number(dto.y ?? 0),
                width: Number(dto.width ?? 180),
            },
        });
    }

    async updateZone(zoneId: string, dto: Partial<CreateZoneDto>) {
        return this.prisma.warehouseZone.update({
            where: { id: zoneId },
            data: {
                ...(dto.name !== undefined ? { name: dto.name } : {}),
                ...(dto.type !== undefined ? { type: dto.type } : {}),
                ...(dto.x !== undefined ? { x: Number(dto.x) } : {}),
                ...(dto.y !== undefined ? { y: Number(dto.y) } : {}),
                ...(dto.width !== undefined ? { width: Number(dto.width) } : {}),
                ...(dto.height !== undefined ? { height: Number(dto.height) } : {}),
                ...(dto.color !== undefined ? { color: dto.color } : {}),
                ...(dto.note !== undefined ? { note: dto.note } : {}),
            },
        });
    }

    async deleteZone(zoneId: string) {
        return this.prisma.warehouseZone.delete({
            where: { id: zoneId },
        });
    }

    async deleteDoor(doorId: string) {
        return this.prisma.warehouseDoor.delete({
            where: { id: doorId },
        });
    }

}