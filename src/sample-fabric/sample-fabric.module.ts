import { Module } from "@nestjs/common";
import { PrismaModule } from "../prisma/prisma.module";
import { SampleFabricController } from "./sample-fabric.controller";
import { SampleFabricService } from "./sample-fabric.service";
import { ProductionController } from "./production.controller";
import { ProductionService } from "./production.service";

@Module({
  imports: [PrismaModule],
  controllers: [SampleFabricController, ProductionController],
  providers: [SampleFabricService, ProductionService],
  exports: [SampleFabricService, ProductionService],
})
export class SampleFabricModule {}
