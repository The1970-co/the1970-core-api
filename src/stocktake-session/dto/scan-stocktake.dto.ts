export class ScanStocktakeDto {
  sessionId: string;
  workerId?: string;
  branchId: string;

  code: string;
  qtyDelta?: number;

  zone?: string;
  aisle?: string;
  locationCode?: string;

  areaId?: string;
  rackId?: string;
  rackCode?: string;

  note?: string;
}
