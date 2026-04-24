import { Type } from "class-transformer";
import {
  IsArray,
  IsIn,
  IsInt,
  IsObject,
  IsOptional,
  IsString,
  Min,
} from "class-validator";

export class GenerateOutboundSuggestionsDto {
  @IsOptional()
  @IsInt()
  @Min(1)
  @Type(() => Number)
  minTarget?: number;

  @IsOptional()
  @IsInt()
  @Min(1)
  @Type(() => Number)
  maxPerVariant?: number;

  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  toBranchIds?: string[];

  @IsOptional()
  @IsObject()
  branchMinTargets?: Record<string, number>;

  @IsOptional()
  @IsIn(["ALL", "SUMMER", "WINTER"])
  season?: "ALL" | "SUMMER" | "WINTER";

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

  @IsOptional()
  @IsString()
  createdById?: string;

  @IsOptional()
  @IsString()
  createdByName?: string;

  @IsOptional()
@IsArray()
@IsString({ each: true })
categoryNames?: string[];
}