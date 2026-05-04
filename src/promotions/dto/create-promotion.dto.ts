import {
  IsArray,
  IsDateString,
  IsEnum,
  IsNumber,
  IsOptional,
  IsString,
  Min,
} from "class-validator";
import {
  PromotionDiscountType,
  PromotionStatus,
  PromotionType,
  SalesChannel,
} from "@prisma/client";

export class CreatePromotionDto {
  @IsString()
  name: string;

  @IsEnum(PromotionType)
  type: PromotionType;

  @IsOptional()
  @IsEnum(PromotionStatus)
  status?: PromotionStatus;

  @IsEnum(PromotionDiscountType)
  discountType: PromotionDiscountType;

  @IsNumber()
  @Min(0)
  discountValue: number;

  @IsOptional()
  @IsNumber()
  @Min(0)
  minOrderAmount?: number;

  @IsOptional()
  @IsString()
  branchId?: string;

  @IsOptional()
  @IsEnum(SalesChannel)
  salesChannel?: SalesChannel;

  @IsOptional()
  @IsDateString()
  startAt?: string;

  @IsOptional()
  @IsDateString()
  endAt?: string;

  @IsOptional()
  @IsNumber()
  priority?: number;

  @IsOptional()
  @IsString()
  note?: string;

  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  productIds?: string[];
}
