export type CustomLayoutAisleDto = {
  aisle: string; // A, B, C hoặc D01
  rackCount: number;
  floors?: number;
};

export class CreateCustomLayoutDto {
  zone?: string; // mặc định A
  resetBeforeCreate?: boolean;
  aisles: CustomLayoutAisleDto[];
}