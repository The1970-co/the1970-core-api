import {
  IsArray,
  IsBoolean,
  IsInt,
  IsIn,
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


export class OmniHeartbeatDto {
  @IsOptional()
  @IsString()
  activeBranchId?: string;

  @IsOptional()
  @IsBoolean()
  manualAway?: boolean;
}

export class UpdateAssignmentMemberDto {
  @IsString()
  staffId!: string;

  @IsString()
  staffName!: string;

  @IsOptional()
  @IsString()
  branchId?: string;

  @IsOptional()
  @IsString()
  branchName?: string;

  @IsOptional()
  @IsBoolean()
  isActive?: boolean;

  @IsOptional()
  @IsBoolean()
  receiveMessages?: boolean;

  @IsOptional()
  @IsBoolean()
  receiveComments?: boolean;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(0)
  sortOrder?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  weight?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  maxActiveConversations?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  maxUnreadConversations?: number;
}

export class UpdateAssignmentSettingsDto {
  @IsOptional() @IsBoolean() isActive?: boolean;
  @IsOptional() @IsIn(["OFF", "SELF_ASSIGN", "AUTO", "GROUP"]) mode?: string;
  @IsOptional() @IsArray() @IsString({ each: true }) priorityOrder?: string[];
  @IsOptional() @IsBoolean() requireOnline?: boolean;
  @IsOptional() @IsBoolean() branchPriorityEnabled?: boolean;
  @IsOptional() @IsBoolean() lowestLoadEnabled?: boolean;
  @IsOptional() @IsBoolean() draftOwnerPriorityEnabled?: boolean;
  @IsOptional() @IsBoolean() keepPreviousAssignee?: boolean;
  @IsOptional() @Type(() => Number) @IsInt() @Min(0) keepPreviousDays?: number;
  @IsOptional() @IsBoolean() reassignIfAssigneeOffline?: boolean;
  @IsOptional() @IsBoolean() workingHoursOnly?: boolean;
  @IsOptional() @Type(() => Number) @IsInt() @Min(0) workStartMinute?: number;
  @IsOptional() @Type(() => Number) @IsInt() @Min(0) workEndMinute?: number;
  @IsOptional() @IsArray() workDays?: number[];
  @IsOptional() @IsIn(["QUEUE", "ONLINE_ONLY", "ASSIGN_ANYWAY"]) outsideHoursMode?: string;
  @IsOptional() @Type(() => Number) @IsInt() @Min(30) onlineWindowSeconds?: number;
  @IsOptional() @IsBoolean() maxActiveEnabled?: boolean;
  @IsOptional() @Type(() => Number) @IsInt() @Min(1) maxActiveConversations?: number;
  @IsOptional() @IsBoolean() maxUnreadEnabled?: boolean;
  @IsOptional() @Type(() => Number) @IsInt() @Min(1) maxUnreadConversations?: number;
  @IsOptional() @IsBoolean() branchRoutingEnabled?: boolean;
  @IsOptional() @IsString() fallbackBranchId?: string;
  @IsOptional() @IsIn(["UNASSIGNED", "ASSIGN_ANYWAY"]) noCandidateMode?: string;
  @IsOptional() @IsBoolean() onlyAssignedCanView?: boolean;
  @IsOptional() @IsBoolean() managerCanViewBranch?: boolean;
  @IsOptional() @IsBoolean() onlyAssignedCanReply?: boolean;
  @IsOptional() @IsBoolean() shuffleEachRound?: boolean;
  @IsOptional() @IsBoolean() reassignUnreadEnabled?: boolean;
  @IsOptional() @Type(() => Number) @IsInt() @Min(1) reassignAfterMinutes?: number;
  @IsOptional() @IsArray() @ValidateNested({ each: true }) @Type(() => UpdateAssignmentMemberDto) members?: UpdateAssignmentMemberDto[];
}

export class CreateQuickReplyTemplateDto {
  @IsOptional() @IsString() @MaxLength(120) title?: string;
  @IsString() @MaxLength(3000) content!: string;
  @IsOptional() @IsString() @MaxLength(80) category?: string;
  @IsOptional() @Type(() => Number) @IsInt() @Min(0) sortOrder?: number;
}

export class UpdateQuickReplyTemplateDto extends CreateQuickReplyTemplateDto {
  @IsOptional() @IsBoolean() isActive?: boolean;
}
