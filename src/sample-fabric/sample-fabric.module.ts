import { Module } from "@nestjs/common";
import { PrismaModule } from "../prisma/prisma.module";
import { SampleFabricController } from "./sample-fabric.controller";
import { SampleFabricService } from "./sample-fabric.service";

@Module({
  imports: [PrismaModule],
  controllers: [SampleFabricController],
  providers: [SampleFabricService],
  exports: [SampleFabricService],
})
export class SampleFabricModule {}
