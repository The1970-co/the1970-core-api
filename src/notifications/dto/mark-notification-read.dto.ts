import { IsOptional, IsString } from "class-validator";

export class MarkNotificationReadDto {
  @IsOptional()
  @IsString()
  notificationId?: string;

  @IsOptional()
  @IsString()
  branchId?: string;
}