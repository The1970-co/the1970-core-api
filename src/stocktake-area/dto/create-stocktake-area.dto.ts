export class CreateStocktakeAreaDto {
  sessionId: string;
  branchId: string;
  mapId?: string;

  scopeType: "MAP" | "AISLE" | "RACK";

  aisle?: string;
  rackId?: string;
  rackCode?: string;
  label: string;
}