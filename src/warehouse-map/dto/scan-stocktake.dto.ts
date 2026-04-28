export class ScanStocktakeDto {
  sessionId: string;
  branchId: string;
  code: string;

  workerId?: string;
  qtyDelta?: number;
  zone?: string;
  locationCode?: string;
  note?: string;

  // Map / stocktake-area connection
  areaId?: string;
  rackId?: string;
  rackCode?: string;
  aisle?: string;
}
