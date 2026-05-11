import {
  Body,
  Controller,
  Get,
  Param,
  Post,
  Req,
  UseGuards,
} from "@nestjs/common";
import { Request } from "express";
import { JwtGuard } from "../auth/jwt.guard";
import { PermissionGuard } from "../auth/guards/permission.guard";
import { RequirePermissions } from "../auth/decorators/require-permissions.decorator";
import { RbacSnapshotsService } from "./rbac-snapshots.service";

@UseGuards(JwtGuard, PermissionGuard)
@Controller("rbac-snapshots")
export class RbacSnapshotsController {
  constructor(private readonly service: RbacSnapshotsService) {}

  @Get()
  @RequirePermissions("permissions.manage")
  list() {
    return this.service.list();
  }

  @Get("latest")
  @RequirePermissions("permissions.manage")
  latest() {
    return this.service.latest();
  }

  @Post()
  @RequirePermissions("permissions.manage")
  create(
    @Body() body: { name?: string; description?: string },
    @Req() req: Request & { user?: any },
  ) {
    return this.service.createSnapshot(body, req.user);
  }

  @Post(":id/restore")
  @RequirePermissions("permissions.manage")
  restore(
    @Param("id") id: string,
    @Req() req: Request & { user?: any },
  ) {
    return this.service.restoreSnapshot(id, req.user);
  }
}
