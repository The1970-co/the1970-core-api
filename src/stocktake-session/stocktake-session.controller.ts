import { Body, Controller, Get, Param, Patch, Post, Query } from "@nestjs/common";
import { StocktakeSessionService } from "./stocktake-session.service";
import { CreateStocktakeSessionDto } from "./dto/create-stocktake-session.dto";
import { JoinStocktakeSessionDto } from "./dto/join-stocktake-session.dto";
import { ScanStocktakeDto } from "./dto/scan-stocktake.dto";

@Controller("stocktake-sessions")
export class StocktakeSessionController {
  constructor(private readonly service: StocktakeSessionService) {}

  @Post()
  createSession(@Body() dto: CreateStocktakeSessionDto) {
    return this.service.createSession(dto);
  }

  @Get()
  listSessions(@Query("branchId") branchId?: string) {
    return this.service.listSessions(branchId);
  }

  @Get("active/current")
  getActiveSession(@Query("branchId") branchId?: string) {
    return this.service.getActiveSession(branchId);
  }

  @Get(":id")
  getSession(@Param("id") id: string) {
    return this.service.getSession(id);
  }

  @Patch(":id/start")
  startSession(@Param("id") id: string) {
    return this.service.startSession(id);
  }

  @Patch(":id/pause")
  pauseSession(@Param("id") id: string) {
    return this.service.pauseSession(id);
  }

  @Patch(":id/resume")
  resumeSession(@Param("id") id: string) {
    return this.service.resumeSession(id);
  }

  @Patch(":id/finish")
  finishSession(@Param("id") id: string) {
    return this.service.finishSession(id);
  }

  @Post(":id/join")
  joinSession(
    @Param("id") id: string,
    @Body() dto: JoinStocktakeSessionDto
  ) {
    return this.service.joinSession(id, dto);
  }

  @Patch("workers/:workerId/finish")
  finishWorker(@Param("workerId") workerId: string) {
    return this.service.finishWorker(workerId);
  }

  @Post("scan")
  scan(@Body() dto: ScanStocktakeDto) {
    return this.service.scan(dto);
  }

  @Get(":id/summary")
  getSummary(@Param("id") id: string) {
    return this.service.getSessionSummary(id);
  }

  @Get(":id/workers/:workerId/summary")
  getWorkerSummary(
    @Param("id") id: string,
    @Param("workerId") workerId: string
  ) {
    return this.service.getWorkerSummary(id, workerId);
  }

  @Get(":id/zone-summary")
  getZoneSummary(@Param("id") id: string) {
    return this.service.getZoneSummary(id);
  }
}