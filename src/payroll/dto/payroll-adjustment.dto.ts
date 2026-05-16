export class PayrollAdjustmentDto {
  type!: "BONUS" | "ALLOWANCE" | "ADVANCE" | "DEDUCTION";
  amount!: number;
  reason?: string;
}
