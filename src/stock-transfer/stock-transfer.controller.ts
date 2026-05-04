import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  Patch,
  Post,
  Query,
  Req,
  UseGuards,
} from "@nestjs/common";

import { JwtGuard } from "../auth/jwt.guard";
import { StockTransferService } from "./stock-transfer.service";
import { CreateStockTransferDto } from "./dto/create-stock-transfer.dto";
import { GenerateOutboundSuggestionsDto } from "./dto/generate-outbound-suggestions.dto";
import { ListStockTransfersDto } from "./dto/list-stock-transfers.dto";
import { UpdateStockTransferStatusDto } from "./dto/update-stock-transfer-status.dto";
import { CreateSelectedOutboundSuggestionsDto } from "./dto/create-selected-outbound-suggestions.dto";
import { UpdateAutoRebalanceConfigDto } from "./dto/update-auto-rebalance-config.dto";

@UseGuards(JwtGuard)
@Controller("stock-transfers")
export class StockTransferController {
  constructor(private readonly stockTransferService: StockTransferService) {}

  @Get()
  async list(@Query() query: ListStockTransfersDto) {
    return this.stockTransferService.list(query);
  }

  @Post()
  async create(@Body() body: CreateStockTransferDto, @Req() req: any) {
    return this.stockTransferService.create(body, req.user);
  }

  @Get("auto-rebalance/config")
  async getAutoRebalanceConfig() {
    return this.stockTransferService.getAutoRebalanceConfig();
  }

  @Patch("auto-rebalance/config")
  async updateAutoRebalanceConfig(@Body() body: UpdateAutoRebalanceConfigDto) {
    return this.stockTransferService.updateAutoRebalanceConfig(body);
  }

  @Post("auto-rebalance/run-now")
  async runAutoRebalanceNow() {
    return this.stockTransferService.runAutoRebalanceNow();
  }

  @Post("suggestions/outbound")
  async previewOutboundSuggestionsLegacy(@Body() body: GenerateOutboundSuggestionsDto) {
    return this.stockTransferService.generateOutboundSuggestions(body);
  }

  @Post("suggestions/outbound/preview")
  async previewOutboundSuggestions(@Body() body: GenerateOutboundSuggestionsDto) {
    return this.stockTransferService.generateOutboundSuggestions(body);
  }

  @Post("suggestions/outbound/create")
  async createOutboundTransfersFromSuggestions(
    @Body() body: GenerateOutboundSuggestionsDto,
  ) {
    return this.stockTransferService.createOutboundTransfersFromSuggestions(body);
  }

  @Post("suggestions/outbound/create-selected")
  async createSelectedOutboundTransfers(
    @Body() body: CreateSelectedOutboundSuggestionsDto,
  ) {
    return this.stockTransferService.createSelectedOutboundTransfers(body);
  }

  @Get(":id")
  async detail(@Param("id") id: string) {
    return this.stockTransferService.detail(id);
  }


  @Patch(":id")
  async updateDraft(
    @Param("id") id: string,
    @Body() body: CreateStockTransferDto,
    @Req() req: any,
  ) {
    return this.stockTransferService.updateDraft(id, body, req.user);
  }

  @Patch(":id/status")
  async updateStatus(
    @Param("id") id: string,
    @Body() body: UpdateStockTransferStatusDto,
  ) {
    return this.stockTransferService.updateStatus(id, body);
  }

  @Delete("bulk-delete")
  async bulkDelete(@Body() body: { ids?: string[] }, @Req() req: any) {
    return this.stockTransferService.bulkDelete(body.ids || [], req.user);
  }

  @Delete(":id")
  async delete(@Param("id") id: string, @Req() req: any) {
    return this.stockTransferService.delete(id, req.user);
  }
}
