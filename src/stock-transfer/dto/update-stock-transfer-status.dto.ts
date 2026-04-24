import { IsEnum, IsOptional, IsString } from "class-validator";
import { TransferStatus } from "@prisma/client";

export class UpdateStockTransferStatusDto {
  @IsEnum(TransferStatus)
  status: TransferStatus;

  @IsOptional()
  @IsString()
  confirmedById?: string;

  @IsOptional()
  @IsString()
  confirmedByName?: string;
}