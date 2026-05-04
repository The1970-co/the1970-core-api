export class InventoryMovementQueryDto {
  q?: string;
  variantId?: string;
  branchId?: string;
  type?: string;
  refType?: string;
  refId?: string;
  fromDate?: string;
  toDate?: string;
  page?: string;
  limit?: string;
}