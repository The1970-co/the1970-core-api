import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  Patch,
  Post,
} from "@nestjs/common";
import { PromotionsService } from "./promotions.service";
import { CreatePromotionDto } from "./dto/create-promotion.dto";
import { UpdatePromotionDto } from "./dto/update-promotion.dto";

@Controller("promotions")
export class PromotionsController {
  constructor(private readonly promotionsService: PromotionsService) {}

  @Get()
  findAll() {
    return this.promotionsService.findAll();
  }

  @Get(":id")
  findOne(@Param("id") id: string) {
    return this.promotionsService.findOne(id);
  }

  @Post()
  create(@Body() dto: CreatePromotionDto) {
    return this.promotionsService.create(dto);
  }

  @Patch(":id")
  update(@Param("id") id: string, @Body() dto: UpdatePromotionDto) {
    return this.promotionsService.update(id, dto);
  }

  @Delete(":id")
  remove(@Param("id") id: string) {
    return this.promotionsService.remove(id);
  }
}
