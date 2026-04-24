import {
  IsArray,
  IsEnum,
  IsInt,
  IsOptional,
  IsString,
  Min,
  ValidateNested,
} from "class-validator";
import { Type } from "class-transformer";
import {
  StockTransferDirection,
  StockTransferSourceType,
} from "@prisma/client";

export class CreateStockTransferItemDto {
  @IsString()
  variantId: string;

  @IsOptional()
  @IsString()
  sku?: string;

  @IsOptional()
  @IsString()
  productName?: string;

  @IsOptional()
  @IsString()
  color?: string;

  @IsOptional()
  @IsString()
  size?: string;

  @IsInt()
  @Min(1)
  qty: number;
}

export class CreateStockTransferDto {
  @IsEnum(StockTransferDirection)
  direction: StockTransferDirection;

  @IsEnum(StockTransferSourceType)
  sourceType: StockTransferSourceType;

  @IsOptional()
  @IsString()
  sourceRefId?: string;

  @IsString()
  fromBranchId: string;

  @IsString()
  toBranchId: string;

  @IsOptional()
  @IsString()
  note?: string;

  @IsOptional()
  @IsString()
  createdById?: string;

  @IsOptional()
  @IsString()
  createdByName?: string;

  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => CreateStockTransferItemDto)
  items: CreateStockTransferItemDto[];
}