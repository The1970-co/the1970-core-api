export class CreatePayrollPeriodDto {
  code?: string;
  name?: string;
  fromDate!: string;
  toDate!: string;
  branchId?: string;
  branchName?: string;
  note?: string;
}
