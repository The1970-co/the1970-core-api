// create-rack.dto.ts
export class CreateRackDto {
  mapId: string;
  branchId: string;
  floorId?: string;
  zoneId?: string;
  name: string;
  zone: string;
  aisle: string;
  rackNo: string;
  floors?: number;
  x?: number;
  y?: number;
  w?: number;
  h?: number;
  rotation?: number;
  note?: string;
}