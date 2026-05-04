import { InventoryMovementType } from "@prisma/client";

export class InventoryMovementQueryDto {
  q?: string;
  variantId?: string;
  branchId?: string;
  type?: InventoryMovementType;
  refType?: string;
  refId?: string;
  fromDate?: string;
  toDate?: string;
  page?: string;
  limit?: string;
}
