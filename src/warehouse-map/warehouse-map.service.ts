import { BadRequestException, Injectable, NotFoundException } from "@nestjs/common";
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


    private async attachRackOperationMetrics(map: any) {
        if (!map || !Array.isArray(map.racks) || !map.racks.length) return map;

        const variantIds: string[] = Array.from(
            new Set(
                map.racks
                    .flatMap((rack: any) => (rack.variantLocations || []).map((loc: any) => String(loc.variantId || "").trim()))
                    .filter((id: string) => id.length > 0)
            )
        );

        const inventoryItems = variantIds.length
            ? await this.prisma.inventoryItem.findMany({
                  where: { variantId: { in: variantIds }, branchId: map.branchId },
              })
            : [];

        const qtyByVariant = new Map<string, number>();
        for (const item of inventoryItems as any[]) {
            qtyByVariant.set(item.variantId, (qtyByVariant.get(item.variantId) || 0) + Number(item.availableQty || 0));
        }

        map.racks = map.racks.map((rack: any) => {
            const skuCount = (rack.variantLocations || []).length;
            const totalQty = (rack.variantLocations || []).reduce(
                (sum: number, loc: any) => sum + Number(qtyByVariant.get(loc.variantId) || 0),
                0
            );
            const heatLevel = skuCount === 0 ? "EMPTY" : totalQty <= 3 ? "LOW" : totalQty >= 30 ? "HIGH" : "NORMAL";
            const heatColor = heatLevel === "EMPTY" ? "#334155" : heatLevel === "LOW" ? "#f59e0b" : heatLevel === "HIGH" ? "#16a34a" : "#2563eb";
            return {
                ...rack,
                skuCount,
                totalSku: skuCount,
                totalSkus: skuCount,
                totalQty,
                heatLevel,
                heatColor,
            };
        });

        return map;
    }

    async getFullMap(mapId: string) {
        const map = await this.prisma.warehouseMap.findUnique({
            where: { id: mapId },
            include: {
                racks: {
                    where: { isActive: true },
                    include: {
                        shelves: { orderBy: { floorNo: "asc" } },
                        variantLocations: true,
                    },
                    orderBy: [{ zone: "asc" }, { aisle: "asc" }, { rackNo: "asc" }],
                },
                floors: {
                    orderBy: { level: "asc" },
                    include: {
                        zones: { orderBy: { createdAt: "asc" } },
                        doors: { orderBy: { createdAt: "asc" } },
                    },
                },
                zones: { orderBy: { createdAt: "asc" } },
                doors: { orderBy: { createdAt: "asc" } },
            },
        });

        if (!map) {
            throw new NotFoundException("Không tìm thấy sơ đồ kho.");
        }

        if (!map.floors.length) {
            await this.prisma.warehouseFloor.create({
                data: { mapId, name: "Tầng 1", level: 1 },
            });

            return this.getFullMap(mapId);
        }

        return this.attachRackOperationMetrics(map);
    }

    async createFloor(mapId: string, dto: CreateFloorDto) {
        const map = await this.prisma.warehouseMap.findUnique({ where: { id: mapId } });
        if (!map) throw new NotFoundException("Không tìm thấy sơ đồ kho.");

        return this.prisma.warehouseFloor.create({
            data: {
                mapId,
                name: dto.name || `Tầng ${dto.level || 1}`,
                level: Number(dto.level || 1),
                note: dto.note,
            },
        });
    }

    async deleteFloor(floorId: string) {
        return this.prisma.warehouseFloor.delete({ where: { id: floorId } });
    }

    async createZone(mapId: string, dto: CreateZoneDto) {
        const floor = await this.prisma.warehouseFloor.findUnique({ where: { id: dto.floorId } });
        if (!floor) throw new NotFoundException("Không tìm thấy tầng.");

        return this.prisma.warehouseZone.create({
            data: {
                mapId,
                floorId: dto.floorId,
                name: dto.name,
                type: dto.type || "STORAGE",
                x: Number(dto.x ?? 80),
                y: Number(dto.y ?? 80),
                width: Number(dto.width ?? 240),
                height: Number(dto.height ?? 160),
                color: dto.color,
                note: dto.note,
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
        return this.prisma.warehouseZone.delete({ where: { id: zoneId } });
    }

    async createDoor(mapId: string, dto: CreateDoorDto) {
        const floor = await this.prisma.warehouseFloor.findUnique({ where: { id: dto.floorId } });
        if (!floor) throw new NotFoundException("Không tìm thấy tầng.");

        return this.prisma.warehouseDoor.create({
            data: {
                mapId,
                floorId: dto.floorId,
                name: dto.name || "Cửa kho",
                side: dto.side || "BOTTOM",
                x: Number(dto.x ?? 520),
                y: Number(dto.y ?? 720),
                width: Number(dto.width ?? 180),
            },
        });
    }

    async updateDoor(doorId: string, dto: Partial<CreateDoorDto>) {
        return this.prisma.warehouseDoor.update({
            where: { id: doorId },
            data: {
                ...(dto.name !== undefined ? { name: dto.name } : {}),
                ...(dto.side !== undefined ? { side: dto.side } : {}),
                ...(dto.x !== undefined ? { x: Number(dto.x) } : {}),
                ...(dto.y !== undefined ? { y: Number(dto.y) } : {}),
                ...(dto.width !== undefined ? { width: Number(dto.width) } : {}),
            },
        });
    }

    async deleteDoor(doorId: string) {
        return this.prisma.warehouseDoor.delete({ where: { id: doorId } });
    }


    // ===============================
    // PHASE 2 - REAL OPERATION
    // ===============================
    private normalizeLookup(value: any) {
        return String(value || "").trim().toUpperCase();
    }

    async getRackInventory(rackId: string) {
        if (!rackId) throw new BadRequestException("Thiếu rackId.");

        const rack = await this.prisma.warehouseRack.findUnique({
            where: { id: rackId },
            include: {
                shelves: { orderBy: { floorNo: "asc" } },
                variantLocations: true,
            },
        });

        if (!rack) throw new NotFoundException("Không tìm thấy kệ.");

        const variantIds: string[] = Array.from(
            new Set(
                ((rack as any).variantLocations || [])
                    .map((item: any) => String(item.variantId || "").trim())
                    .filter((id: string) => id.length > 0)
            )
        );

        const variants = variantIds.length
            ? await this.prisma.productVariant.findMany({
                  where: { id: { in: variantIds } },
                  include: {
                      product: true,
                      inventoryItems: true,
                  },
              })
            : [];

        const variantById = new Map(variants.map((variant: any) => [variant.id, variant]));

        const items = (rack as any).variantLocations.map((location: any) => {
            const variant: any = variantById.get(location.variantId);
            const branchInventory = variant?.inventoryItems?.find((item: any) => item.branchId === rack.branchId) || null;
            const totalInventory = (variant?.inventoryItems || []).reduce((sum: number, item: any) => sum + Number(item.availableQty || 0), 0);

            return {
                locationId: location.id,
                variantId: location.variantId,
                rackId: location.rackId,
                shelfId: location.shelfId,
                isPrimary: location.isPrimary,
                note: location.note,
                sku: variant?.sku || "",
                productName: variant?.product?.name || "",
                color: variant?.color || "",
                size: variant?.size || "",
                branchId: rack.branchId,
                availableQty: Number(branchInventory?.availableQty || 0),
                reservedQty: Number(branchInventory?.reservedQty || 0),
                incomingQty: Number(branchInventory?.incomingQty || 0),
                totalInventory,
            };
        });

        return {
            rack: {
                id: rack.id,
                code: rack.code,
                name: rack.name,
                branchId: rack.branchId,
                zone: rack.zone,
                aisle: rack.aisle,
                rackNo: rack.rackNo,
                floors: rack.floors,
                status: rack.status,
                shelves: (rack as any).shelves || [],
            },
            totalSkus: items.length,
            totalQty: items.reduce((sum: number, item: any) => sum + Number(item.availableQty || 0), 0),
            items,
        };
    }

    async searchVariants(params: { q?: string; branchId?: string; limit?: number }) {
        const q = String(params?.q || "").trim();
        const branchId = String(params?.branchId || "").trim();
        const limit = Math.min(50, Math.max(1, Number(params?.limit || 20)));

        if (!q) return [];

        const variants = await this.prisma.productVariant.findMany({
            where: {
                OR: [
                    { sku: { contains: q, mode: "insensitive" } },
                    { product: { name: { contains: q, mode: "insensitive" } } },
                ],
            },
            include: {
                product: true,
                inventoryItems: branchId ? { where: { branchId } } : true,
            },
            orderBy: { updatedAt: "desc" },
            take: limit,
        });

        return variants.map((variant: any) => {
            const qty = (variant.inventoryItems || []).reduce((sum: number, item: any) => sum + Number(item.availableQty || 0), 0);
            return {
                id: variant.id,
                sku: variant.sku,
                productName: variant.product?.name || "",
                color: variant.color || "",
                size: variant.size || "",
                price: Number(variant.price || 0),
                costPrice: Number(variant.costPrice || 0),
                availableQty: qty,
            };
        });
    }

    async assignSkuToRack(dto: {
        rackId: string;
        sku?: string;
        variantId?: string;
        shelfId?: string;
        isPrimary?: boolean;
        note?: string;
    }) {
        if (!dto.rackId) throw new BadRequestException("Thiếu rackId.");
        if (!dto.variantId && !dto.sku) throw new BadRequestException("Thiếu SKU hoặc variantId.");

        const rack = await this.prisma.warehouseRack.findUnique({
            where: { id: dto.rackId },
            include: { shelves: true },
        });
        if (!rack) throw new NotFoundException("Không tìm thấy kệ.");

        const variant = dto.variantId
            ? await this.prisma.productVariant.findUnique({ where: { id: dto.variantId } })
            : await this.prisma.productVariant.findUnique({ where: { sku: String(dto.sku || "").trim() } });

        if (!variant) throw new NotFoundException("Không tìm thấy SKU.");

        const shelfId = dto.shelfId || (rack as any).shelves?.[0]?.id || null;

        const existing = await this.prisma.productVariantLocation.findFirst({
            where: {
                variantId: variant.id,
                rackId: rack.id,
                ...(shelfId ? { shelfId } : {}),
            },
        });

        if (existing) {
            return this.prisma.productVariantLocation.update({
                where: { id: existing.id },
                data: {
                    shelfId,
                    isPrimary: Boolean(dto.isPrimary ?? existing.isPrimary),
                    note: dto.note ?? existing.note,
                },
            });
        }

        return this.prisma.productVariantLocation.create({
            data: {
                variantId: variant.id,
                rackId: rack.id,
                shelfId,
                isPrimary: Boolean(dto.isPrimary),
                note: dto.note,
            },
        });
    }

    async removeSkuFromRack(locationId: string) {
        if (!locationId) throw new BadRequestException("Thiếu locationId.");
        return this.prisma.productVariantLocation.delete({ where: { id: locationId } });
    }

    async scanRack(params: { code: string; mapId?: string; branchId?: string }) {
        const code = this.normalizeLookup(params.code);
        if (!code) throw new BadRequestException("Thiếu mã kệ cần scan.");

        const racks = await this.prisma.warehouseRack.findMany({
            where: {
                isActive: true,
                ...(params.mapId ? { mapId: params.mapId } : {}),
                ...(params.branchId ? { branchId: params.branchId } : {}),
            },
            include: { shelves: true },
        });

        const rack = racks.find((item: any) => {
            const compactCode = this.normalizeLookup(item.code);
            const shortCode = this.normalizeLookup(`${item.aisle}-${item.rackNo}`);
            const altShortCode = this.normalizeLookup(`${item.aisle}${item.rackNo}`);
            const name = this.normalizeLookup(item.name);
            return compactCode === code || shortCode === code || altShortCode === code || name === code || compactCode.endsWith(code);
        });

        if (!rack) throw new NotFoundException("Không tìm thấy kệ theo mã scan.");

        return {
            found: true,
            rack,
            inventory: await this.getRackInventory(rack.id),
            scanRackCode: rack.code,
            shortCode: `${rack.aisle}-${rack.rackNo}`,
        };
    }

    async createPickingRoute(mapId: string, dto: { skus: string[]; branchId?: string }) {
        if (!mapId) throw new BadRequestException("Thiếu mapId.");
        const skus = Array.from(new Set((dto.skus || []).map((sku) => String(sku || "").trim()).filter(Boolean)));
        if (!skus.length) return { route: [], missingSkus: [] };

        const variants = await this.prisma.productVariant.findMany({
            where: { sku: { in: skus } },
            include: { product: true },
        });

        const variantBySku = new Map(variants.map((variant: any) => [variant.sku, variant]));
        const variantIds = variants.map((variant: any) => variant.id);

        const locations = variantIds.length
            ? await this.prisma.productVariantLocation.findMany({
                  where: { variantId: { in: variantIds } },
                  include: {
                      rack: true,
                      shelf: true,
                  },
              })
            : [];

        const route = locations
            .filter((location: any) => location.rack?.mapId === mapId && location.rack?.isActive)
            .map((location: any) => {
                const variant: any = variants.find((item: any) => item.id === location.variantId);
                return {
                    sku: variant?.sku || "",
                    productName: variant?.product?.name || "",
                    rackId: location.rackId,
                    rackCode: location.rack?.code,
                    rackName: location.rack?.name,
                    aisle: location.rack?.aisle,
                    rackNo: location.rack?.rackNo,
                    shelfCode: location.shelf?.code,
                    floorNo: location.shelf?.floorNo,
                    x: Number(location.rack?.x || 0),
                    y: Number(location.rack?.y || 0),
                };
            })
            .sort((a, b) => a.x - b.x || a.y - b.y);

        const foundSkus = new Set(route.map((item) => item.sku));
        const missingSkus = skus.filter((sku) => !variantBySku.has(sku) || !foundSkus.has(sku));

        const routeWithSteps = route.map((item, index) => ({
            ...item,
            step: index + 1,
        }));

        return {
            totalRequested: skus.length,
            totalFound: routeWithSteps.length,
            route: routeWithSteps,
            missingSkus,
            path: routeWithSteps.map((item) => ({ rackId: item.rackId, x: item.x, y: item.y, sku: item.sku })),
        };
    }

    async getHeatmap(mapId: string) {
        const map = await this.prisma.warehouseMap.findUnique({
            where: { id: mapId },
            include: {
                racks: {
                    where: { isActive: true },
                    include: { variantLocations: true },
                    orderBy: [{ aisle: "asc" }, { rackNo: "asc" }],
                },
            },
        });

        if (!map) throw new NotFoundException("Không tìm thấy sơ đồ kho.");

        const variantIds: string[] = Array.from(
            new Set(
                map.racks
                    .flatMap((rack: any) => (rack.variantLocations || []).map((loc: any) => String(loc.variantId || "").trim()))
                    .filter((id: string) => id.length > 0)
            )
        );

        const inventoryItems = variantIds.length
            ? await this.prisma.inventoryItem.findMany({
                  where: { variantId: { in: variantIds }, branchId: map.branchId },
              })
            : [];

        const qtyByVariant = new Map<string, number>();
        for (const item of inventoryItems as any[]) {
            qtyByVariant.set(item.variantId, (qtyByVariant.get(item.variantId) || 0) + Number(item.availableQty || 0));
        }

        const racks = map.racks.map((rack: any) => {
            const skuCount = rack.variantLocations.length;
            const qty = rack.variantLocations.reduce((sum: number, loc: any) => sum + Number(qtyByVariant.get(loc.variantId) || 0), 0);
            const heat = skuCount === 0 ? "EMPTY" : qty <= 3 ? "LOW" : qty >= 30 ? "HIGH" : "NORMAL";
            return {
                rackId: rack.id,
                rackCode: rack.code,
                rackName: rack.name,
                aisle: rack.aisle,
                rackNo: rack.rackNo,
                skuCount,
                qty,
                heat,
                color: heat === "EMPTY" ? "#e5e7eb" : heat === "LOW" ? "#f59e0b" : heat === "HIGH" ? "#16a34a" : "#2563eb",
            };
        });

        return {
            mapId,
            totalRacks: racks.length,
            emptyRacks: racks.filter((rack) => rack.heat === "EMPTY").length,
            lowRacks: racks.filter((rack) => rack.heat === "LOW").length,
            highRacks: racks.filter((rack) => rack.heat === "HIGH").length,
            racks,
        };
    }

    async getRebalanceSuggestions(mapId: string) {
        const heatmap = await this.getHeatmap(mapId);
        const emptyRacks = heatmap.racks.filter((rack: any) => rack.heat === "EMPTY");
        const highRacks = heatmap.racks.filter((rack: any) => rack.heat === "HIGH");
        const lowRacks = heatmap.racks.filter((rack: any) => rack.heat === "LOW");

        const suggestions = [
            ...highRacks.slice(0, 10).map((rack: any) => ({
                type: "SPLIT_STOCK",
                priority: "HIGH",
                rackId: rack.rackId,
                fromRackId: rack.rackId,
                fromRackCode: rack.rackCode,
                rackName: rack.rackName,
                message: `${rack.rackName} đang nhiều hàng (${rack.qty}). Nên tách bớt sang kệ trống gần nhất.`,
                targetRackId: emptyRacks[0]?.rackId || null,
                targetRackName: emptyRacks[0]?.rackName || null,
                toRackId: emptyRacks[0]?.rackId || null,
                toRackCode: emptyRacks[0]?.rackCode || null,
            })),
            ...lowRacks.slice(0, 10).map((rack: any) => ({
                type: "LOW_STOCK_REVIEW",
                priority: "MEDIUM",
                rackId: rack.rackId,
                rackName: rack.rackName,
                message: `${rack.rackName} còn ít hàng (${rack.qty}). Cần kiểm tra bổ sung hoặc chuyển vị trí.`,
            })),
        ];

        return {
            totalSuggestions: suggestions.length,
            emptyRacks: emptyRacks.length,
            suggestions,
        };
    }

}