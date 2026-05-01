// assign-variant-location.dto.ts
export class AssignVariantLocationDto {
  variantId: string;
  rackId: string;
  shelfId?: string;
  isPrimary?: boolean;
  note?: string;
}