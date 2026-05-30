import { IsIn, IsInt, IsOptional, IsString, Max, Min } from "class-validator";
import { Type } from "class-transformer";

export class ListConversationsDto {
  @IsOptional()
  @IsString()
  q?: string;

  @IsOptional()
  @IsIn(["OPEN", "PENDING", "PROCESSING", "CLOSED", "SPAM", "ALL"])
  status?: string;

  @IsOptional()
  @IsString()
  assigneeId?: string;

  @IsOptional()
  @IsString()
  branchId?: string;

  @IsOptional()
  @IsIn(["FACEBOOK", "INSTAGRAM", "SYSTEM", "ALL"])
  channel?: string;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  page?: number = 1;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(10)
  @Max(100)
  limit?: number = 30;
}
