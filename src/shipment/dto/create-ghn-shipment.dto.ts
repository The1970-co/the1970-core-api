import {
  IsArray,
  IsInt,
  IsOptional,
  IsString,
  Min,
  ValidateNested,
} from "class-validator";
import { Type } from "class-transformer";

class CreateShipmentItemDto {
  @IsString()
  name: string;

  @IsInt()
  @Min(1)
  quantity: number;

  @IsInt()
  price: number;

  @IsInt()
  length: number;

  @IsInt()
  width: number;

  @IsInt()
  height: number;

  @IsInt()
  weight: number;

  @IsOptional()
  @IsString()
  category?: string;
}

export class CreateGhnShipmentDto {
  @IsString()
  toName: string;

  @IsString()
  toPhone: string;

  @IsString()
  toAddress: string;

  @IsInt()
  toDistrictId: number;

  @IsString()
  toWardCode: string;

  @IsInt()
  codAmount: number;

  @IsString()
  clientOrderCode: string;

  @IsOptional()
  @IsString()
  note?: string;

  @IsOptional()
  @IsString()
  content?: string;

  @IsOptional()
  @IsString()
  requiredNote?: string;

  @IsInt()
  weight: number;

  @IsInt()
  length: number;

  @IsInt()
  width: number;

  @IsInt()
  height: number;

  @IsOptional()
  @IsInt()
  insuranceValue?: number;

  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => CreateShipmentItemDto)
  items: CreateShipmentItemDto[];
}