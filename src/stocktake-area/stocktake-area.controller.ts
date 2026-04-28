import { Body, Controller, Get, Param, Patch, Post } from "@nestjs/common";
import { StocktakeAreaService } from "./stocktake-area.service";
import { CreateStocktakeAreaDto } from "./dto/create-stocktake-area.dto";

@Controller("stocktake-areas")
export class StocktakeAreaController {
  constructor(private readonly service: StocktakeAreaService) {}

  @Post()
  create(@Body() dto: CreateStocktakeAreaDto) {
    return this.service.create(dto);
  }

  @Get("session/:sessionId")
  listBySession(@Param("sessionId") sessionId: string) {
    return this.service.listBySession(sessionId);
  }

  @Patch(":id/start")
  start(@Param("id") id: string) {
    return this.service.start(id);
  }

  @Patch(":id/finish")
  finish(@Param("id") id: string) {
    return this.service.finish(id);
  }

  @Patch(":id/mismatch")
  markMismatch(@Param("id") id: string) {
    return this.service.markMismatch(id);
  }
}