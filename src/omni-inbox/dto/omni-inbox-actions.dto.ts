import {
  IsArray,
  IsBoolean,
  IsIn,
  IsInt,
  IsOptional,
  IsString,
  MaxLength,
  Min,
  ValidateNested,
} from "class-validator";
import { Type } from "class-transformer";

export class AssignConversationDto {
  @IsString()
  assigneeId!: string;

  @IsString()
  assigneeName!: string;
}

export class UpdateConversationStatusDto {
  @IsString()
  status!: "OPEN" | "PENDING" | "PROCESSING" | "CLOSED" | "SPAM";
}

export class CreateNoteDto {
  @IsString()
  @MaxLength(1000)
  note!: string;

  @IsOptional()
  @IsString()
  templateId?: string;
}

export class CreateNoteTemplateDto {
  @IsString()
  @MaxLength(120)
  name!: string;

  @IsOptional()
  @IsString()
  @MaxLength(30)
  color?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  sortOrder?: number;

  @IsOptional()
  @IsIn(["OPEN", "PENDING", "PROCESSING", "CLOSED", "SPAM"])
  targetStatus?: "OPEN" | "PENDING" | "PROCESSING" | "CLOSED" | "SPAM";
}

export class UpdateNoteTemplateDto extends CreateNoteTemplateDto {
  @IsOptional()
  @IsBoolean()
  isActive?: boolean;
}

export class UpdateTagsDto {
  @IsArray()
  @IsString({ each: true })
  tags!: string[];
}

export class SendMessageDto {
  @IsString()
  @MaxLength(3000)
  text!: string;

  @IsOptional()
  @IsString()
  attachmentUrl?: string;
}

export class CreateQuickOrderItemDto {
  @IsString()
  variantId!: string;

  @Type(() => Number)
  @IsInt()
  @Min(1)
  qty!: number;
}

export class CreateQuickOrderDto {
  @IsOptional()
  @IsString()
  @MaxLength(120)
  customerName?: string;

  @IsString()
  @MaxLength(30)
  phone!: string;

  @IsString()
  @MaxLength(1000)
  address!: string;

  @IsString()
  branchId!: string;

  @IsOptional()
  @IsString()
  @MaxLength(1000)
  note?: string;

  @IsOptional()
  @IsString()
  requestId?: string;

  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => CreateQuickOrderItemDto)
  items!: CreateQuickOrderItemDto[];
}

export class UpdateQuickOrderDto extends CreateQuickOrderDto {}
