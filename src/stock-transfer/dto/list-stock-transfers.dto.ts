import { IsEnum, IsOptional, IsString } from "class-validator";
import {
  StockTransferDirection,
  StockTransferSourceType,
  TransferStatus,
} from "@prisma/client";

export class ListStockTransfersDto {
  @IsOptional()
  @IsEnum(StockTransferDirection)
  direction?: StockTransferDirection;

  @IsOptional()
  @IsEnum(StockTransferSourceType)
  sourceType?: StockTransferSourceType;

  @IsOptional()
  @IsEnum(TransferStatus)
  status?: TransferStatus;

  @IsOptional()
  @IsString()
  branchId?: string;

  @IsOptional()
  @IsString()
  keyword?: string;
}