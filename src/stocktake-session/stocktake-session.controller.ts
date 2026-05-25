import {
  Body,
  Controller,
  Get,
  Header,
  Param,
  Patch,
  Post,
  Query,
  Req,
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
  createSession(@Body() dto: CreateStocktakeSessionDto, @Req() req: any) {
    return this.service.createSession(dto, req.user);
  }

  @Get()
  @RequirePermissions("stocktake.view")
  listSessions(
    @Query("branchId") branchId?: string,
    @Query("status") status?: string,
    @Query("from") from?: string,
    @Query("to") to?: string,
    @Query("page") page?: string,
    @Query("limit") limit?: string,
    @Req() req?: any,
  ) {
    return this.service.listSessions(
      branchId,
      {
        status,
        from,
        to,
        page: page ? Number(page) : undefined,
        limit: limit ? Number(limit) : undefined,
      },
      req?.user,
    );
  }

  @Get("summary/overview")
  @RequirePermissions("stocktake.view")
  getSessionsOverview(
    @Query("branchId") branchId?: string,
    @Query("status") status?: string,
    @Query("from") from?: string,
    @Query("to") to?: string,
    @Req() req?: any,
  ) {
    return this.service.getSessionsOverview(branchId, { status, from, to }, req?.user);
  }

  @Get("active/current")
  @RequirePermissions("stocktake.view")
  getActiveSession(@Query("branchId") branchId: string | undefined, @Req() req: any) {
    return this.service.getActiveSession(branchId, req.user);
  }

  @Get(":id")
  @RequirePermissions("stocktake.view")
  getSession(@Param("id") id: string, @Req() req: any) {
    return this.service.getSession(id, req.user);
  }

  @Get(":id/detail")
  @RequirePermissions("stocktake.view")
  getSessionDetail(@Param("id") id: string, @Req() req: any) {
    return this.service.getSessionDetail(id, req.user);
  }

  @Get(":id/items")
  @RequirePermissions("stocktake.view")
  getSessionItems(
    @Param("id") id: string,
    @Query("status") status?: string,
    @Query("q") q?: string,
    @Req() req?: any,
  ) {
    return this.service.getSessionItems(id, { status, q }, req.user);
  }

  @Get(":id/unscanned")
  @RequirePermissions("stocktake.view")
  getUnscannedItems(@Param("id") id: string, @Query("q") q: string | undefined, @Req() req: any) {
    return this.service.getSessionItems(id, { status: "UNCOUNTED", q }, req.user);
  }

  @Get(":id/discrepancies")
  @RequirePermissions("stocktake.view")
  getDiscrepancyItems(@Param("id") id: string, @Query("q") q: string | undefined, @Req() req: any) {
    return this.service.getSessionItems(id, { status: "MISMATCH", q }, req.user);
  }

  @Get(":id/logs")
  @RequirePermissions("stocktake.view")
  getSessionLogs(@Param("id") id: string, @Req() req: any) {
    return this.service.getSessionLogs(id, req.user);
  }

  @Get(":id/export-excel")
  @RequirePermissions("stocktake.excel.export")
  @Header(
    "Content-Type",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  )
  async exportExcel(@Param("id") id: string, @Req() req: any, @Res() res: Response) {
    const result = await this.service.exportSessionExcel(id, req.user);
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${encodeURIComponent(result.fileName)}"`,
    );
    res.send(result.buffer);
  }

  @Patch(":id/start")
  @RequirePermissions("stocktake.edit")
  startSession(@Param("id") id: string, @Req() req: any) {
    return this.service.startSession(id, req.user);
  }

  @Patch(":id/pause")
  @RequirePermissions("stocktake.edit")
  pauseSession(@Param("id") id: string, @Req() req: any) {
    return this.service.pauseSession(id, req.user);
  }

  @Patch(":id/resume")
  @RequirePermissions("stocktake.edit")
  resumeSession(@Param("id") id: string, @Req() req: any) {
    return this.service.resumeSession(id, req.user);
  }

  @Patch(":id/finish")
  @RequirePermissions("stocktake.confirm")
  finishSession(@Param("id") id: string, @Req() req: any) {
    return this.service.finishSession(id, req.user);
  }

  @Patch(":id/apply")
  @RequirePermissions("stocktake.apply")
  applySession(@Param("id") id: string, @Body() body: any, @Req() req: any) {
    return this.service.applySession(id, body || {}, req.user);
  }

  @Post(":id/join")
  @RequirePermissions("stocktake.edit")
  joinSession(@Param("id") id: string, @Body() dto: JoinStocktakeSessionDto, @Req() req: any) {
    return this.service.joinSession(id, dto, req.user);
  }

  @Patch("workers/:workerId/finish")
  @RequirePermissions("stocktake.edit")
  finishWorker(@Param("workerId") workerId: string, @Req() req: any) {
    return this.service.finishWorker(workerId, req.user);
  }

  @Post("scan")
  @RequirePermissions("stocktake.edit")
  scan(@Body() dto: ScanStocktakeDto, @Req() req: any) {
    return this.service.scan(dto, req.user);
  }

  @Get(":id/summary")
  @RequirePermissions("stocktake.view")
  getSummary(@Param("id") id: string, @Req() req: any) {
    return this.service.getSessionSummary(id, req.user);
  }

  @Get(":id/workers/:workerId/summary")
  @RequirePermissions("stocktake.view")
  getWorkerSummary(
    @Param("id") id: string,
    @Param("workerId") workerId: string,
    @Req() req: any,
  ) {
    return this.service.getWorkerSummary(id, workerId, req.user);
  }

  @Get(":id/zone-summary")
  @RequirePermissions("stocktake.view")
  getZoneSummary(@Param("id") id: string, @Req() req: any) {
    return this.service.getZoneSummary(id, req.user);
  }
}
