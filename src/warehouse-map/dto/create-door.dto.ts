export class CreateDoorDto {
  floorId: string;
  name?: string;
  side?: string; // TOP | BOTTOM | LEFT | RIGHT
  x?: number;
  y?: number;
  width?: number;
}
