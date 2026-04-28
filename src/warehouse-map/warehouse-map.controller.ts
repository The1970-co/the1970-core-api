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

  @Get(":id/full")
  getFullMap(@Param("id") id: string) {
    return this.service.getFullMap(id);
  }

  @Get(":id")
  getMap(@Param("id") id: string) {
    return this.service.getMap(id);
  }

  @Post(":id/quick-layout")
  createQuickLayout(@Param("id") id: string) {
    return this.service.createQuickLayout(id);
  }

  @Post(":id/custom-layout")
  createCustomLayout(
    @Param("id") id: string,
    @Body() dto: CreateCustomLayoutDto
  ) {
    return this.service.createCustomLayout(id, dto);
  }

  @Post(":id/reset-layout")
  resetLayout(@Param("id") id: string) {
    return this.service.resetLayout(id);
  }

  @Post(":id/floors")
  createFloor(@Param("id") id: string, @Body() dto: CreateFloorDto) {
    return this.service.createFloor(id, dto);
  }

  @Post(":id/zones")
  createZone(@Param("id") id: string, @Body() dto: CreateZoneDto) {
    return this.service.createZone(id, dto);
  }

  @Post(":id/doors")
  createDoor(@Param("id") id: string, @Body() dto: CreateDoorDto) {
    return this.service.createDoor(id, dto);
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

  @Patch("zones/:zoneId")
  updateZone(@Param("zoneId") zoneId: string, @Body() dto: Partial<CreateZoneDto>) {
    return this.service.updateZone(zoneId, dto);
  }

  @Delete("zones/:zoneId")
  deleteZone(@Param("zoneId") zoneId: string) {
    return this.service.deleteZone(zoneId);
  }

  @Delete("doors/:doorId")
  deleteDoor(@Param("doorId") doorId: string) {
    return this.service.deleteDoor(doorId);
  }

  @Post("assign")
  assignVariant(@Body() dto: AssignVariantLocationDto) {
    return this.service.assignVariant(dto);
  }
}
