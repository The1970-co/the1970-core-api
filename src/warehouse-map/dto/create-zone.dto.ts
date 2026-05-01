export class CreateZoneDto {
  floorId: string;
  name: string;
  type: string; // STORAGE | OFFICE | PACKING | RETURN | WALKWAY | OTHER
  x?: number;
  y?: number;
  width?: number;
  height?: number;
  color?: string;
  note?: string;
}
