import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  Patch,
  Post,
  Query,
} from "@nestjs/common";
import { WarehouseMapService } from "./warehouse-map.service";
import { CreateWarehouseMapDto } from "./dto/create-warehouse-map.dto";
import { CreateRackDto } from "./dto/create-rack.dto";
import { UpdateRackDto } from "./dto/update-rack.dto";
import { AssignVariantLocationDto } from "./dto/assign-variant-location.dto";
import { CreateCustomLayoutDto } from "./dto/create-custom-layout.dto";
import { CreateFloorDto } from "./dto/create-floor.dto";
import { CreateZoneDto } from "./dto/create-zone.dto";
import { CreateDoorDto } from "./dto/create-door.dto";

@Controller("warehouse-map")
export class WarehouseMapController {
  constructor(private readonly service: WarehouseMapService) {}

  @Post()
  createMap(@Body() dto: CreateWarehouseMapDto) {
    return this.service.createMap(dto);
  }

  @Get()
  listMaps(@Query("branchId") branchId?: string) {
    return this.service.listMaps(branchId);
  }

  @Get("zones")
  getZones(@Query("branchId") branchId: string) {
    return this.service.getZones(branchId);
  }

  @Get(":id")
  getMap(@Param("id") id: string) {
    return this.service.getMap(id);
  }

  @Post(":id/quick-layout")
  createQuickLayout(@Param("id") id: string) {
    return this.service.createQuickLayout(id);
  }

  @Post(":id/reset-layout")
  resetLayout(@Param("id") id: string) {
    return this.service.resetLayout(id);
  }

  @Post("racks")
  createRack(@Body() dto: CreateRackDto) {
    return this.service.createRack(dto);
  }

  @Patch("racks/:id")
  updateRack(@Param("id") id: string, @Body() dto: UpdateRackDto) {
    return this.service.updateRack(id, dto);
  }

  @Delete("racks/:id")
  deleteRack(@Param("id") id: string) {
    return this.service.deleteRack(id);
  }

  @Post("assign")
  assignVariant(@Body() dto: AssignVariantLocationDto) {
    return this.service.assignVariant(dto);
  }
  @Post(":id/custom-layout")
createCustomLayout(
  @Param("id") id: string,
  @Body() dto: CreateCustomLayoutDto
) {
  return this.service.createCustomLayout(id, dto);
}

  @Get(":id/full")
  getFullMap(@Param("id") id: string) {
    return this.service.getFullMap(id);
  }

  @Post(":id/floors")
  createFloor(@Param("id") id: string, @Body() dto: CreateFloorDto) {
    return this.service.createFloor(id, dto);
  }

  @Delete("floors/:floorId")
  deleteFloor(@Param("floorId") floorId: string) {
    return this.service.deleteFloor(floorId);
  }

  @Post(":id/zones")
  createZone(@Param("id") id: string, @Body() dto: CreateZoneDto) {
    return this.service.createZone(id, dto);
  }

  @Patch("zones/:zoneId")
  updateZone(@Param("zoneId") zoneId: string, @Body() dto: Partial<CreateZoneDto>) {
    return this.service.updateZone(zoneId, dto);
  }

  @Delete("zones/:zoneId")
  deleteZone(@Param("zoneId") zoneId: string) {
    return this.service.deleteZone(zoneId);
  }

  @Post(":id/doors")
  createDoor(@Param("id") id: string, @Body() dto: CreateDoorDto) {
    return this.service.createDoor(id, dto);
  }

  @Patch("doors/:doorId")
  updateDoor(@Param("doorId") doorId: string, @Body() dto: Partial<CreateDoorDto>) {
    return this.service.updateDoor(doorId, dto);
  }

  @Delete("doors/:doorId")
  deleteDoor(@Param("doorId") doorId: string) {
    return this.service.deleteDoor(doorId);
  }



  // ===============================
  // PHASE 2 - REAL OPERATION ROUTES
  // ===============================

  @Get("variants/search")
  searchVariants(
    @Query("q") q?: string,
    @Query("branchId") branchId?: string,
    @Query("limit") limit?: string
  ) {
    return this.service.searchVariants({
      q,
      branchId,
      limit: limit ? Number(limit) : undefined,
    });
  }

  @Get("racks/:rackId/inventory")
  getRackInventory(@Param("rackId") rackId: string) {
    return this.service.getRackInventory(rackId);
  }

  @Post("racks/:rackId/assign-sku")
  async assignSkuToRack(
    @Param("rackId") rackId: string,
    @Body() body: { sku?: string; variantId?: string; shelfId?: string; isPrimary?: boolean; note?: string }
  ) {
    await this.service.assignSkuToRack({
      ...body,
      rackId,
    });

    return this.service.getRackInventory(rackId);
  }

  @Delete("locations/:locationId")
  removeSkuFromRack(@Param("locationId") locationId: string) {
    return this.service.removeSkuFromRack(locationId).then(() => ({ ok: true }));
  }

  @Post(":id/scan-rack")
  scanRack(
    @Param("id") mapId: string,
    @Body() body: { code: string; branchId?: string }
  ) {
    return this.service.scanRack({
      code: body.code,
      mapId,
      branchId: body.branchId,
    });
  }

  @Post(":id/picking-route")
  createPickingRoute(
    @Param("id") mapId: string,
    @Body() body: { skus: string[]; branchId?: string }
  ) {
    return this.service.createPickingRoute(mapId, body);
  }

  @Get(":id/heatmap")
  getHeatmap(@Param("id") mapId: string) {
    return this.service.getHeatmap(mapId);
  }

  @Get(":id/rebalance-suggestions")
  getRebalanceSuggestions(@Param("id") mapId: string) {
    return this.service.getRebalanceSuggestions(mapId);
  }

}
