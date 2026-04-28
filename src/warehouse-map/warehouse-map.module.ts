import { Module } from "@nestjs/common";
import { PrismaService } from "src/prisma/prisma.service";
import { WarehouseMapController } from "./warehouse-map.controller";
import { WarehouseMapService } from "./warehouse-map.service";

@Module({
  controllers: [WarehouseMapController],
  providers: [WarehouseMapService, PrismaService],
})
export class WarehouseMapModule {}