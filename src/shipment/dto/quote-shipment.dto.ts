import {
  IsArray,
  IsInt,
  IsOptional,
  IsString,
  Min,
  ValidateNested,
} from "class-validator";
import { Type } from "class-transformer";

class QuoteShipmentItemDto {
  @IsString()
  name: string;

  @IsInt()
  @Min(1)
  quantity: number;

  @IsInt()
  @Min(1)
  length: number;

  @IsInt()
  @Min(1)
  width: number;

  @IsInt()
  @Min(1)
  height: number;

  @IsInt()
  @Min(1)
  weight: number;
}

export class QuoteShipmentDto {
  // 🔥 FIX CHÍNH: KHÔNG BẮT BUỘC NỮA
  @IsOptional()
  @IsString()
  toProvinceCode?: string;

  @IsInt()
  toDistrictId: number;

  @IsString()
  toWardCode: string;

  @IsInt()
  @Min(1)
  weight: number;

  @IsInt()
  @Min(1)
  length: number;

  @IsInt()
  @Min(1)
  width: number;

  @IsInt()
  @Min(1)
  height: number;

  @IsOptional()
  @IsInt()
  insuranceValue?: number;

  @IsOptional()
  @IsInt()
  codFailedAmount?: number;

  // 🔥 thêm option GHN
  @IsOptional()
  @IsString()
  requiredNote?: string;

  @IsOptional()
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => QuoteShipmentItemDto)
  items?: QuoteShipmentItemDto[];
}