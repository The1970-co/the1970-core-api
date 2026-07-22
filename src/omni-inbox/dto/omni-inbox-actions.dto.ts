import { IsArray, IsOptional, IsString, MaxLength } from "class-validator";

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
