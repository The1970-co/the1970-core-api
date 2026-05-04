import { Module } from "@nestjs/common";
import { WarehouseMapController } from "./warehouse-map.controller";
import { WarehouseMapService } from "./warehouse-map.service";

@Module({
  controllers: [WarehouseMapController],
  providers: [WarehouseMapService],
})
export class WarehouseMapModule {}