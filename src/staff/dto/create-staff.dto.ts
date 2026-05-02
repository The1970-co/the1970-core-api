import {
  IsArray,
  IsEmail,
  IsOptional,
  IsString,
  MinLength,
} from "class-validator";

export class CreateStaffDto {
  @IsString()
  code!: string;

  @IsString()
  name!: string;

  @IsOptional()
  @IsString()
  username?: string | null;

  @IsOptional()
  @IsEmail()
  email?: string | null;

  @IsOptional()
  @IsString()
  phone?: string | null;

  @IsOptional()
  @IsString()
  address?: string | null;

  @IsOptional()
  @IsString()
  note?: string | null;

  // legacy role, vẫn giữ để không vỡ code cũ
  @IsOptional()
  @IsString()
  role?: string;

  // multi-role mới
  @IsOptional()
  @IsArray()
  roles?: string[];

  @IsOptional()
  @IsString()
  branchId?: string | null;

  @IsString()
  @MinLength(4)
  password!: string;
}