import { IsOptional, IsString, MinLength } from "class-validator";

export class CreateStaffDto {
  @IsString()
  code!: string;

  @IsString()
  name!: string;

  @IsString()
  role!: string;

  // ✅ CHUẨN: dùng branchId
  @IsOptional()
  @IsString()
  branchId?: string | null;

  @IsString()
  @MinLength(4)
  password!: string;
}