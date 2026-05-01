// create-custom-layout.dto.ts
export class CreateCustomLayoutDto {
  zone?: string;
  resetBeforeCreate?: boolean;
  floorId?: string;
  aisles: {
    aisle: string;
    rackCount: number;
    floors?: number;
  }[];
}