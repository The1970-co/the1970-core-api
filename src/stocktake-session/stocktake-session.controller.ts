import {
  Body,
  Controller,
  Get,
  Header,
  Param,
  Patch,
  Post,
  Query,
  Res,
  UseGuards,
} from "@nestjs/common";
import type { Response } from "express";
import { JwtGuard } from "../auth/jwt.guard";
import { PermissionGuard } from "../auth/guards/permission.guard";
import { RequirePermissions } from "../auth/decorators/require-permissions.decorator";
import { StocktakeSessionService } from "./stocktake-session.service";
import { CreateStocktakeSessionDto } from "./dto/create-stocktake-session.dto";
import { JoinStocktakeSessionDto } from "./dto/join-stocktake-session.dto";
import { ScanStocktakeDto } from "./dto/scan-stocktake.dto";

@UseGuards(JwtGuard, PermissionGuard)
@Controller("stocktake-sessions")
export class StocktakeSessionController {
  constructor(private readonly service: StocktakeSessionService) {}

  @Post()
  @RequirePermissions("stocktake.create")
  createSession(@Body() dto: CreateStocktakeSessionDto) {
    return this.service.createSession(dto);
  }

  @Get()
  @RequirePermissions("stocktake.view")
  listSessions(
    @Query("branchId") branchId?: string,
    @Query("status") status?: string,
    @Query("from") from?: string,
    @Query("to") to?: string,
  ) {
    return this.service.listSessions(branchId, { status, from, to });
  }

  @Get("active/current")
  @RequirePermissions("stocktake.view")
  getActiveSession(@Query("branchId") branchId?: string) {
    return this.service.getActiveSession(branchId);
  }

  @Get(":id")
  @RequirePermissions("stocktake.view")
  getSession(@Param("id") id: string) {
    return this.service.getSession(id);
  }

  @Get(":id/detail")
  @RequirePermissions("stocktake.view")
  getSessionDetail(@Param("id") id: string) {
    return this.service.getSessionDetail(id);
  }

  @Get(":id/items")
  @RequirePermissions("stocktake.view")
  getSessionItems(
    @Param("id") id: string,
    @Query("status") status?: string,
    @Query("q") q?: string,
  ) {
    return this.service.getSessionItems(id, { status, q });
  }

  @Get(":id/unscanned")
  @RequirePermissions("stocktake.view")
  getUnscannedItems(@Param("id") id: string, @Query("q") q?: string) {
    return this.service.getSessionItems(id, { status: "UNCOUNTED", q });
  }

  @Get(":id/discrepancies")
  @RequirePermissions("stocktake.view")
  getDiscrepancyItems(@Param("id") id: string, @Query("q") q?: string) {
    return this.service.getSessionItems(id, { status: "MISMATCH", q });
  }

  @Get(":id/logs")
  @RequirePermissions("stocktake.view")
  getSessionLogs(@Param("id") id: string) {
    return this.service.getSessionLogs(id);
  }

  @Get(":id/export-excel")
  @RequirePermissions("stocktake.excel.export")
  @Header(
    "Content-Type",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  )
  async exportExcel(@Param("id") id: string, @Res() res: Response) {
    const result = await this.service.exportSessionExcel(id);
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${encodeURIComponent(result.fileName)}"`,
    );
    res.send(result.buffer);
  }

  @Patch(":id/start")
  @RequirePermissions("stocktake.edit")
  startSession(@Param("id") id: string) {
    return this.service.startSession(id);
  }

  @Patch(":id/pause")
  @RequirePermissions("stocktake.edit")
  pauseSession(@Param("id") id: string) {
    return this.service.pauseSession(id);
  }

  @Patch(":id/resume")
  @RequirePermissions("stocktake.edit")
  resumeSession(@Param("id") id: string) {
    return this.service.resumeSession(id);
  }

  @Patch(":id/finish")
  @RequirePermissions("stocktake.confirm")
  finishSession(@Param("id") id: string) {
    return this.service.finishSession(id);
  }

  @Patch(":id/apply")
  @RequirePermissions("stocktake.apply")
  applySession(@Param("id") id: string, @Body() body?: any) {
    return this.service.applySession(id, body || {});
  }

  @Post(":id/join")
  @RequirePermissions("stocktake.edit")
  joinSession(@Param("id") id: string, @Body() dto: JoinStocktakeSessionDto) {
    return this.service.joinSession(id, dto);
  }

  @Patch("workers/:workerId/finish")
  @RequirePermissions("stocktake.edit")
  finishWorker(@Param("workerId") workerId: string) {
    return this.service.finishWorker(workerId);
  }

  @Post("scan")
  @RequirePermissions("stocktake.edit")
  scan(@Body() dto: ScanStocktakeDto) {
    return this.service.scan(dto);
  }

  @Get(":id/summary")
  @RequirePermissions("stocktake.view")
  getSummary(@Param("id") id: string) {
    return this.service.getSessionSummary(id);
  }

  @Get(":id/workers/:workerId/summary")
  @RequirePermissions("stocktake.view")
  getWorkerSummary(
    @Param("id") id: string,
    @Param("workerId") workerId: string,
  ) {
    return this.service.getWorkerSummary(id, workerId);
  }

  @Get(":id/zone-summary")
  @RequirePermissions("stocktake.view")
  getZoneSummary(@Param("id") id: string) {
    return this.service.getZoneSummary(id);
  }
}
