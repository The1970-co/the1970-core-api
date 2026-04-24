import { IsEnum, IsOptional, IsString } from "class-validator";

export enum OrderStatusDtoEnum {
  NEW = "NEW",
  APPROVED = "APPROVED",
  PACKING = "PACKING",
  SHIPPED = "SHIPPED",
  COMPLETED = "COMPLETED",
  CANCELLED = "CANCELLED",
}

export class UpdateOrderStatusDto {
  @IsEnum(OrderStatusDtoEnum)
  status: OrderStatusDtoEnum;

  @IsOptional()
  @IsString()
  note?: string;
}