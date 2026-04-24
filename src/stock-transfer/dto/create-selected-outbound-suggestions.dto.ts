import { Type } from "class-transformer";
import {
  IsArray,
  IsInt,
  IsOptional,
  IsString,
  Min,
  ValidateNested,
} from "class-validator";

export class SelectedOutboundSuggestionItemDto {
  @IsString()
  variantId: string;

  @IsString()
  toBranchId: string;

  @IsInt()
  @Min(1)
  @Type(() => Number)
  qty: number;
}

export class CreateSelectedOutboundSuggestionsDto {
  @IsOptional()
  @IsString()
  createdById?: string;

  @IsOptional()
  @IsString()
  createdByName?: string;

  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => SelectedOutboundSuggestionItemDto)
  items: SelectedOutboundSuggestionItemDto[];
}