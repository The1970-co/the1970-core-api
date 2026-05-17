export class PayrollConfigDto {
  staffId!: string;
  staffCode?: string;
  staffName?: string;
  branchId?: string;
  branchName?: string;
  attendanceCode?: string;

  salaryType?: "MONTHLY" | "DAILY" | "SHIFT" | "NONE";
  baseSalary?: number;
  dailyRate?: number;
  standardWorkingDays?: number;

  orderAttributionMode?: "CREATED_BY" | "ASSIGNED_OR_CREATOR" | "ASSIGNED_ONLY";

  commissionPerOrderEnabled?: boolean;
  commissionPerOrderAmount?: number;
  commissionPerItemEnabled?: boolean;
  commissionPerItemAmount?: number;
  commissionPercentEnabled?: boolean;
  commissionRate?: number;

  hourlyEnabled?: boolean;
  hourlyRate?: number;
  standardHoursPerDay?: number;
  overtimeRate?: number;
  holidayRate?: number;

  paidLeaveEnabled?: boolean;
  paidLeaveHoursPerDay?: number;

  mealAllowanceEnabled?: boolean;
  mealHoursPerUnit?: number;
  mealAmountPerUnit?: number;

  insuranceDeductionAmount?: number;

  taggedProductEnabled?: boolean;
  taggedProductRate?: number;

  ghnCodBonusEnabled?: boolean;
  ghnCodBonusPerOrder?: number;

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
