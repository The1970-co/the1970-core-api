import {
  Body,
  Controller,
  Get,
  Param,
  Patch,
  Post,
  Query,
} from "@nestjs/common";
import { CreateStockTransferDto } from "./dto/create-stock-transfer.dto";
import { CreateSelectedOutboundSuggestionsDto } from "./dto/create-selected-outbound-suggestions.dto";
import { GenerateOutboundSuggestionsDto } from "./dto/generate-outbound-suggestions.dto";
import { ListStockTransfersDto } from "./dto/list-stock-transfers.dto";
import { UpdateStockTransferStatusDto } from "./dto/update-stock-transfer-status.dto";
import { StockTransferService } from "./stock-transfer.service";
import { UpdateAutoRebalanceConfigDto } from "./dto/update-auto-rebalance-config.dto";

@Controller("stock-transfers")
export class StockTransferController {
  constructor(private readonly stockTransferService: StockTransferService) {}

  @Post()
  async create(@Body() body: CreateStockTransferDto) {
    return this.stockTransferService.create(body);
  }

  @Get()
  async list(@Query() query: ListStockTransfersDto) {
    return this.stockTransferService.list(query);
  }

  @Get("suggestions/outbound")
  async outboundSuggestions(@Query() query: GenerateOutboundSuggestionsDto) {
    return this.stockTransferService.generateOutboundSuggestions({
      ...query,
      minTarget: query.minTarget ? Number(query.minTarget) : undefined,
      maxPerVariant: query.maxPerVariant
        ? Number(query.maxPerVariant)
        : undefined,
      salesVelocityDays: query.salesVelocityDays
        ? Number(query.salesVelocityDays)
        : undefined,
      minSoldQty:
        query.minSoldQty !== undefined ? Number(query.minSoldQty) : undefined,
    });
  }

  @Post("suggestions/outbound/preview")
  async previewOutboundSuggestions(@Body() body: GenerateOutboundSuggestionsDto) {
    return this.stockTransferService.generateOutboundSuggestions(body || {});
  }

  @Post("suggestions/outbound/create")
  async createOutboundFromSuggestions(
    @Body() body: GenerateOutboundSuggestionsDto
  ) {
    return this.stockTransferService.createOutboundTransfersFromSuggestions(
      body || {}
    );
  }

  @Post("suggestions/outbound/create-selected")
  async createSelectedOutboundSuggestions(
    @Body() body: CreateSelectedOutboundSuggestionsDto
  ) {
    return this.stockTransferService.createSelectedOutboundTransfers(body);
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
  @Get(":id")
  async detail(@Param("id") id: string) {
    return this.stockTransferService.detail(id);
  }

  @Patch(":id/status")
  async updateStatus(
    @Param("id") id: string,
    @Body() body: UpdateStockTransferStatusDto
  ) {
    return this.stockTransferService.updateStatus(id, body);
  }
}