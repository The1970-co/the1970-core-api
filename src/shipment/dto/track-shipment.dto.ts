import { IsOptional, IsString } from "class-validator";

export class TrackShipmentDto {
  @IsOptional()
  @IsString()
  orderCode?: string;

  @IsOptional()
  @IsString()
  clientOrderCode?: string;
}