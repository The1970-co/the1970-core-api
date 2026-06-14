import { IsArray, IsInt, IsOptional, IsString, Min, ValidateNested } from "class-validator";
import { Type } from "class-transformer";

class SpxShipmentItemDto {
  @IsString()
  name: string;

  @IsOptional()
  @IsString()
  sku?: string;

  @IsInt()
  @Min(1)
  quantity: number;

  @IsOptional()
  @IsInt()
  price?: number;

  @IsOptional()
  @IsInt()
  weight?: number;
}

export class CreateSpxShipmentDto {
  @IsOptional()
  @IsString()
  fromName?: string;

  @IsOptional()
  @IsString()
  fromPhone?: string;

  @IsOptional()
  @IsString()
  fromAddress?: string;

  @IsString()
  toName: string;

  @IsString()
  toPhone: string;

  @IsString()
  toAddress: string;

  @IsOptional()
  @IsString()
  toProvince?: string;

  @IsOptional()
  @IsString()
  toDistrict?: string;

  @IsOptional()
  @IsString()
  toWard?: string;

  @IsOptional()
  @IsString()
  province?: string;

  @IsOptional()
  @IsString()
  district?: string;

  @IsOptional()
  @IsString()
  ward?: string;

  @IsOptional()
  @IsString()
  clientOrderCode?: string;

  @IsOptional()
  @IsString()
  orderCode?: string;

  @IsOptional()
  @IsString()
  serviceCode?: string;

  @IsOptional()
  @IsString()
  note?: string;

  @IsOptional()
  @IsString()
  shippingNote?: string;

  @IsOptional()
  @IsString()
  deliveryRequirement?: string;

  @IsOptional()
  @IsString()
  requiredNote?: string;

  @IsOptional()
  @IsString()
  required_note?: string;

  @IsOptional()
  @IsInt()
  codAmount?: number;

  @IsOptional()
  @IsInt()
  insuranceValue?: number;

  @IsOptional()
  @IsInt()
  productPrice?: number;

  @IsOptional()
  @IsInt()
  @Min(1)
  weight?: number;

  @IsOptional()
  @IsInt()
  @Min(1)
  length?: number;

  @IsOptional()
  @IsInt()
  @Min(1)
  width?: number;

  @IsOptional()
  @IsInt()
  @Min(1)
  height?: number;

  @IsOptional()
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => SpxShipmentItemDto)
  items?: SpxShipmentItemDto[];
}
