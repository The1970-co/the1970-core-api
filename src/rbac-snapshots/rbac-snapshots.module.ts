import { Module } from "@nestjs/common";
import { RbacSnapshotsController } from "./rbac-snapshots.controller";
import { RbacSnapshotsService } from "./rbac-snapshots.service";

@Module({
  controllers: [RbacSnapshotsController],
  providers: [RbacSnapshotsService],
  exports: [RbacSnapshotsService],
})
export class RbacSnapshotsModule {}
