export class UpdateRackDto {
  name?: string;
  zone?: string;
  aisle?: string;
  rackNo?: string;

  floors?: number;

  x?: number;
  y?: number;
  w?: number;
  h?: number;
  rotation?: number;

  status?: string;
  note?: string;
  isActive?: boolean;
}