export class PayrollAdjustmentDto {
  adjustmentId?: string;
  type!: string;
  amount!: number;
  reason?: string;
}
