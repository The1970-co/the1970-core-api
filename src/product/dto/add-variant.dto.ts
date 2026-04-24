import { IsNumber, IsObject, IsString } from "class-validator";

export class AddVariantDto {
  @IsString()
  color: string;

  @IsString()
  size: string;

  @IsNumber()
  price: number;

  @IsNumber()
  costPrice: number;

  @IsObject()
  branchStocks: Record<string, number>;
}