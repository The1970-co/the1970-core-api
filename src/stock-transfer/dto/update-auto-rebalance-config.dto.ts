import { Type } from "class-transformer";
import {
  IsArray,
  IsBoolean,
  IsInt,
  IsObject,
  IsOptional,
  IsString,
  Max,
  Min,
} from "class-validator";

export class UpdateAutoRebalanceConfigDto {
  @IsOptional()
  @IsBoolean()
  isEnabled?: boolean;

  @IsOptional()
  @IsInt()
  @Min(0)
  @Max(23)
  @Type(() => Number)
  runHour?: number;

  @IsOptional()
  @IsInt()
  @Min(0)
  @Max(59)
  @Type(() => Number)
  runMinute?: number;

  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  toBranchIds?: string[];

  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  categoryNames?: string[];

  @IsOptional()
  @IsObject()
  branchMinTargets?: Record<string, number>;

  @IsOptional()
  @IsInt()
  @Min(1)
  @Type(() => Number)
  maxPerVariant?: number;

  @IsOptional()
  @IsInt()
  @Min(1)
  @Type(() => Number)
  salesVelocityDays?: number;

  @IsOptional()
  @IsInt()
  @Min(0)
  @Type(() => Number)
  minSoldQty?: number;
}