export class PayrollConfigDto {
  staffId!: string;
  staffCode?: string;
  staffName?: string;
  branchId?: string;
  branchName?: string;

  salaryType?: "MONTHLY" | "DAILY" | "SHIFT" | "NONE";
  baseSalary?: number;
  dailyRate?: number;
  standardWorkingDays?: number;

  commissionPerOrderEnabled?: boolean;
  commissionPerOrderAmount?: number;
  commissionPerItemEnabled?: boolean;
  commissionPerItemAmount?: number;
  commissionPercentEnabled?: boolean;
  commissionRate?: number;

  applyPos?: boolean;
  applyOnline?: boolean;
  applyFacebook?: boolean;
  applyCod?: boolean;

  allowanceDefault?: number;
  effectiveFrom?: string;
  effectiveTo?: string | null;
  isActive?: boolean;
  note?: string;
}
