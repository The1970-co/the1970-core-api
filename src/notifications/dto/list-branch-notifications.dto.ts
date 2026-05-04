import { IsBooleanString, IsOptional, IsString } from "class-validator";

export class ListBranchNotificationsDto {
  @IsString()
  branchId: string;

  @IsOptional()
  @IsBooleanString()
  unreadOnly?: string;
}
