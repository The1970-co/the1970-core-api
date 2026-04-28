export class CreateRackDto {
  mapId: string;
  branchId: string;

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