import { BadRequestException, ForbiddenException, Injectable, NotFoundException } from "@nestjs/common";
import { OrderStatus, PaymentStatus, Prisma } from "@prisma/client";
import { PrismaService } from "../prisma/prisma.service";
import { FinanceService } from "../finance/finance.service";
import { CreatePayrollPeriodDto } from "./dto/create-payroll-period.dto";
import { PayrollConfigDto } from "./dto/payroll-config.dto";
import { PayrollAdjustmentDto } from "./dto/payroll-adjustment.dto";
import { PayrollFilterDto } from "./dto/payroll-filter.dto";
import { parseAttendanceWorkbook, ParsedAttendanceRow } from "./payroll-attendance-parser";

type AnyUser = any;

type PayrollLikeLine = {
  proratedSalary?: unknown;
  hourlyAmount?: unknown;
  paidLeaveAmount?: unknown;
  taggedProductAmount?: unknown;
  ghnCodBonusAmount?: unknown;
  commissionTotal?: unknown;
  bonus?: unknown;
  allowance?: unknown;
  mealAllowanceAmount?: unknown;
  advance?: unknown;
  deduction?: unknown;
  insuranceDeduction?: unknown;
};

@Injectable()
export class PayrollService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly financeService: FinanceService,
  ) {}

  private toNumber(value: unknown) {
    if (typeof value === "number") return Number.isFinite(value) ? value : 0;
    return Number(value || 0) || 0;
  }

  private money(value: unknown) {
    return new Prisma.Decimal(Math.round(this.toNumber(value)));
  }

  private decimal2(value: unknown) {
    return new Prisma.Decimal(this.toNumber(value));
  }

  private asDate(value?: string | Date | null, fallback?: Date) {
    if (!value) return fallback || new Date();
    const date = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(date.getTime())) {
      throw new BadRequestException("Ngày không hợp lệ.");
    }
    return date;
  }

  private startOfDate(value: string | Date) {
    const text = value instanceof Date ? value.toISOString().slice(0, 10) : String(value).slice(0, 10);
    return new Date(`${text}T00:00:00.000+07:00`);
  }

  private endOfDate(value: string | Date) {
    const text = value instanceof Date ? value.toISOString().slice(0, 10) : String(value).slice(0, 10);
    return new Date(`${text}T23:59:59.999+07:00`);
  }

  private userName(user?: AnyUser) {
    return user?.name || user?.username || user?.email || user?.code || null;
  }

  private isOwner(user?: AnyUser) {
    const roles = [
      ...(Array.isArray(user?.roles) ? user.roles : []),
      user?.role,
    ]
      .map((role) => String(role || "").toLowerCase())
      .filter(Boolean);

    return roles.includes("owner") || roles.includes("admin") ||
      (Array.isArray(user?.permissions) && user.permissions.includes("*"));
  }

  private scopedBranchId(user?: AnyUser, branchId?: string | null) {
    if (this.isOwner(user)) return branchId && branchId !== "ALL" ? branchId : null;
    const userBranch = user?.branchId || null;
    if (!userBranch) throw new ForbiddenException("Tài khoản chưa được gán chi nhánh.");
    if (branchId && branchId !== "ALL" && String(branchId) !== String(userBranch)) {
      throw new ForbiddenException("Không có quyền xem hoặc thao tác dữ liệu chi nhánh khác.");
    }
    return userBranch;
  }

  private async resolveBranchName(branchId?: string | null) {
    if (!branchId || branchId === "ALL") return null;
    const branch = await this.prisma.branch.findUnique({ where: { id: branchId }, select: { name: true } });
    return branch?.name || branchId;
  }

  private buildPeriodCode(from: Date, to: Date, branchId?: string | null) {
    const ym = from.toISOString().slice(0, 7).replace("-", "");
    const toYm = to.toISOString().slice(0, 7).replace("-", "");
    const branch = branchId && branchId !== "ALL" ? `-${branchId}` : "-ALL";
    return `PAY-${ym}${ym === toYm ? "" : `-${toYm}`}${branch}`;
  }

  private calcLineTotals(line: PayrollLikeLine) {
    const grossPay =
      this.toNumber(line.proratedSalary) +
      this.toNumber(line.hourlyAmount) +
      this.toNumber(line.paidLeaveAmount) +
      this.toNumber(line.taggedProductAmount) +
      this.toNumber(line.ghnCodBonusAmount) +
      this.toNumber(line.commissionTotal) +
      this.toNumber(line.bonus) +
      this.toNumber(line.allowance) +
      this.toNumber(line.mealAllowanceAmount);
    const netPay =
      grossPay -
      this.toNumber(line.advance) -
      this.toNumber(line.deduction) -
      this.toNumber(line.insuranceDeduction);
    return {
      grossPay: Math.max(0, Math.round(grossPay)),
      netPay: Math.max(0, Math.round(netPay)),
    };
  }


  private attendanceWarning(row: { lateCount?: number; lateMinutes?: number; earlyCount?: number; earlyMinutes?: number }, settings: any) {
    const lateCount = Number(row.lateCount || 0);
    const lateMinutes = Number(row.lateMinutes || 0);
    const earlyCount = Number(row.earlyCount || 0);
    const earlyMinutes = Number(row.earlyMinutes || 0);
    const critical =
      lateCount >= Number(settings.lateCriticalCount || 5) ||
      lateMinutes >= Number(settings.lateCriticalMinutes || 120) ||
      earlyCount >= Number(settings.earlyCriticalCount || 5) ||
      earlyMinutes >= Number(settings.earlyCriticalMinutes || 120);
    const warning =
      lateCount >= Number(settings.lateWarningCount || 3) ||
      lateMinutes >= Number(settings.lateWarningMinutes || 60) ||
      earlyCount >= Number(settings.earlyWarningCount || 3) ||
      earlyMinutes >= Number(settings.earlyWarningMinutes || 60);

    if (critical) {
      return {
        level: "CRITICAL",
        note: `Đi muộn ${lateCount} lần/${lateMinutes} phút · về sớm ${earlyCount} lần/${earlyMinutes} phút`,
      };
    }
    if (warning) {
      return {
        level: "WARNING",
        note: `Cần nhắc: đi muộn ${lateCount} lần/${lateMinutes} phút · về sớm ${earlyCount} lần/${earlyMinutes} phút`,
      };
    }
    return { level: "OK", note: "Chấm công ổn" };
  }

  private async getOrCreateSettings() {
    const existing = await (this.prisma as any).payrollSettings.findUnique({ where: { id: "default" } }).catch(() => null);
    if (existing) return existing;
    return (this.prisma as any).payrollSettings.create({ data: { id: "default" } });
  }

  private isOrderSuccessForPayroll(order: any) {
    const status = String(order?.status || "").toUpperCase();
    const paymentStatus = String(order?.paymentStatus || "").toUpperCase();
    const channel = String(order?.salesChannel || "").toUpperCase();
    const shipmentStatus = String(order?.shipment?.shippingStatus || order?.shipment?.partnerStatus || "").toUpperCase();

    if (status === "CANCELLED") return false;
    if (status === "COMPLETED") return true;
    if (channel === "POS" && paymentStatus === "PAID") return true;
    if (["DELIVERED", "COMPLETED", "SUCCESS", "FULFILLED"].some((key) => shipmentStatus.includes(key))) return true;
    return false;
  }

  private channelEnabled(config: any, order: any) {
    const channel = String(order?.salesChannel || "").toUpperCase();
    const source = String(order?.source || "").toUpperCase();
    const shipment = order?.shipment;
    const isCod = String(shipment?.carrier || "").trim() || order?.paymentStatus === PaymentStatus.PENDING_COD;

    if (channel === "POS") return config.applyPos !== false;
    if (channel === "FACEBOOK_MANUAL" || source.includes("FACEBOOK")) return config.applyFacebook !== false;
    if (isCod) return config.applyCod !== false;
    return config.applyOnline !== false;
  }

  private orderItemQty(order: any) {
    const items = Array.isArray(order?.items) ? order.items : [];
    return items.reduce((sum: number, item: any) => sum + Math.max(0, Number(item.qty || 0)), 0);
  }

  private normalizeAttributionMode(value?: unknown) {
    const mode = String(value || "ASSIGNED_OR_CREATOR").toUpperCase();
    if (mode === "CREATED_BY" || mode === "ASSIGNED_ONLY") return mode;
    return "ASSIGNED_OR_CREATOR";
  }

  private buildPayrollOrderWhere(staffId: string, period: any, branchId?: string | null, modeValue?: unknown): Prisma.OrderWhereInput {
    const mode = this.normalizeAttributionMode(modeValue);
    const where: Prisma.OrderWhereInput = {
      createdAt: { gte: period.fromDate, lte: period.toDate },
      status: { not: OrderStatus.CANCELLED },
    };
    if (branchId) where.branchId = branchId;

    if (mode === "CREATED_BY") {
      where.createdByStaffId = staffId;
      return where;
    }

    if (mode === "ASSIGNED_ONLY") {
      where.assignedStaffId = staffId;
      return where;
    }

    // Chuẩn vận hành The 1970: nếu đơn đã gán NV phụ trách thì tính cho NV phụ trách.
    // Nếu chưa gán ai thì fallback về nhân viên tạo đơn.
    where.OR = [
      { assignedStaffId: staffId },
      { AND: [{ createdByStaffId: staffId }, { OR: [{ assignedStaffId: null }, { assignedStaffId: "" }] }] },
    ];
    return where;
  }

  private payrollAttributionSource(order: any, staffId: string, modeValue?: unknown) {
    const mode = this.normalizeAttributionMode(modeValue);
    if (mode === "CREATED_BY") return "CREATED_BY";
    if (String(order?.assignedStaffId || "") === String(staffId)) return "ASSIGNED_STAFF";
    return "CREATED_BY_FALLBACK";
  }

  private isGhnCodOrder(order: any) {
    const carrier = String(order?.shipment?.carrier || "").toUpperCase();
    const sourceType = Array.isArray(order?.payments)
      ? order.payments.map((p: any) => String(p?.paymentSource?.type || "").toUpperCase()).join(" ")
      : "";
    const hasCod = String(order?.paymentStatus || "").toUpperCase() === "PENDING_COD" || sourceType.includes("COD") || this.toNumber(order?.shipment?.codAmount) > 0;
    return hasCod && (carrier.includes("GHN") || carrier.includes("GIAOHANGNHANH"));
  }


  async getDashboard(query: PayrollFilterDto, user?: AnyUser) {
    const periods = await this.listPeriods({ ...(query || {}), pageSize: 100 } as any, user);
    const rows = periods.rows || [];
    const branchMap = new Map<string, any>();
    let totalNet = 0;
    let totalPaid = 0;
    let totalStaff = 0;
    let totalWarnings = 0;
    let totalLateMinutes = 0;
    let totalEarlyMinutes = 0;

    for (const row of rows as any[]) {
      totalNet += this.toNumber(row.totalNet);
      totalPaid += this.toNumber(row.totalPaid);
      totalStaff += Number(row.totalStaff || 0);
      totalWarnings += Number(row.totalAttendanceWarnings || 0);
      totalLateMinutes += Number(row.totalLateMinutes || 0);
      totalEarlyMinutes += Number(row.totalEarlyMinutes || 0);
      const key = row.branchId || "ALL";
      if (!branchMap.has(key)) {
        branchMap.set(key, {
          branchId: row.branchId || null,
          branchName: row.branchName || "Tất cả",
          totalPeriods: 0,
          totalStaff: 0,
          totalNet: 0,
          totalPaid: 0,
          totalWarnings: 0,
        });
      }
      const b = branchMap.get(key);
      b.totalPeriods += 1;
      b.totalStaff += Number(row.totalStaff || 0);
      b.totalNet += this.toNumber(row.totalNet);
      b.totalPaid += this.toNumber(row.totalPaid);
      b.totalWarnings += Number(row.totalAttendanceWarnings || 0);
    }

    return {
      summary: {
        totalPeriods: rows.length,
        totalNet,
        totalPaid,
        remaining: Math.max(0, totalNet - totalPaid),
        totalStaff,
        totalWarnings,
        totalLateMinutes,
        totalEarlyMinutes,
      },
      byBranch: Array.from(branchMap.values()).sort((a, b) => b.totalNet - a.totalNet),
      latestPeriods: rows.slice(0, 5),
    };
  }

  async getSettings(user?: AnyUser) {
    this.scopedBranchId(user, null);
    return this.getOrCreateSettings();
  }

  async updateSettings(body: any, user?: AnyUser) {
    this.scopedBranchId(user, null);
    const data: any = {};
    const allowed = [
      "autoCreateEnabled", "autoCreateDay", "cycleMode", "cycleStartDay", "cycleEndDay",
      "autoCalculateMode", "autoLockEnabled", "autoLockAfterDays", "reminderEnabled",
      "lateWarningCount", "lateWarningMinutes", "lateCriticalCount", "lateCriticalMinutes",
      "earlyWarningCount", "earlyWarningMinutes", "earlyCriticalCount", "earlyCriticalMinutes",
    ];
    for (const key of allowed) {
      if (body?.[key] !== undefined) data[key] = body[key];
    }
    await this.getOrCreateSettings();
    return (this.prisma as any).payrollSettings.update({ where: { id: "default" }, data });
  }

  async listPeriods(query: PayrollFilterDto, user?: AnyUser) {
    const page = Math.max(1, Number(query.page || 1));
    const pageSize = Math.min(100, Math.max(10, Number(query.pageSize || 20)));
    const branchId = this.scopedBranchId(user, query.branchId || null);

    const where: Prisma.PayrollPeriodWhereInput = {};
    if (branchId) where.branchId = branchId;
    if (query.status && query.status !== "ALL") where.status = String(query.status).toUpperCase();
    if (query.q?.trim()) {
      const q = query.q.trim();
      where.OR = [
        { code: { contains: q, mode: "insensitive" } },
        { name: { contains: q, mode: "insensitive" } },
        { branchName: { contains: q, mode: "insensitive" } },
      ];
    }

    const [total, rows] = await this.prisma.$transaction([
      this.prisma.payrollPeriod.count({ where }),
      this.prisma.payrollPeriod.findMany({
        where,
        orderBy: { createdAt: "desc" },
        skip: (page - 1) * pageSize,
        take: pageSize,
      }),
    ]);

    return { rows, total, page, pageSize, totalPages: Math.max(1, Math.ceil(total / pageSize)) };
  }

  async createPeriod(body: CreatePayrollPeriodDto, user?: AnyUser) {
    const fromDate = this.startOfDate(body.fromDate);
    const toDate = this.endOfDate(body.toDate);
    if (fromDate > toDate) throw new BadRequestException("Ngày bắt đầu không được lớn hơn ngày kết thúc.");

    const branchId = this.scopedBranchId(user, body.branchId || null);
    const branchName = body.branchName || await this.resolveBranchName(branchId);
    const code = String(body.code || this.buildPeriodCode(fromDate, toDate, branchId)).trim();
    const name = String(body.name || `Kỳ lương ${fromDate.toISOString().slice(0, 10)} → ${toDate.toISOString().slice(0, 10)}${branchName ? ` · ${branchName}` : ""}`).trim();

    return this.prisma.payrollPeriod.create({
      data: {
        code,
        name,
        fromDate,
        toDate,
        branchId,
        branchName,
        note: body.note || null,
      },
    });
  }

  async getPeriod(id: string, user?: AnyUser) {
    const period = await this.prisma.payrollPeriod.findUnique({
      where: { id },
      include: {
        lines: {
          orderBy: [{ branchName: "asc" }, { staffName: "asc" }],
          include: {
            orderLinks: { orderBy: { orderDate: "desc" } },
            adjustments: { orderBy: { createdAt: "desc" } },
          },
        },
      },
    });
    if (!period) throw new NotFoundException("Không tìm thấy kỳ lương.");
    this.scopedBranchId(user, period.branchId || null);
    return period;
  }

  async listConfigs(query: any, user?: AnyUser) {
    const branchId = this.scopedBranchId(user, query.branchId || null);
    const where: Prisma.PayrollConfigWhereInput = {};
    if (branchId) where.OR = [{ branchId }, { branchId: null }];
    if (query.staffId) where.staffId = String(query.staffId);
    if (query.isActive !== undefined && query.isActive !== "ALL") where.isActive = String(query.isActive) !== "false";
    if (query.q?.trim()) {
      const q = query.q.trim();
      where.OR = [
        ...(Array.isArray(where.OR) ? where.OR : []),
        { staffName: { contains: q, mode: "insensitive" } },
        { staffCode: { contains: q, mode: "insensitive" } },
        { branchName: { contains: q, mode: "insensitive" } },
      ];
    }

    return this.prisma.payrollConfig.findMany({ where, orderBy: [{ isActive: "desc" }, { staffName: "asc" }] });
  }

  async createConfig(body: PayrollConfigDto, user?: AnyUser) {
    if (!body.staffId) throw new BadRequestException("Thiếu nhân viên.");
    const branchId = this.scopedBranchId(user, body.branchId || null);
    const staff = await this.prisma.staffUser.findUnique({ where: { id: body.staffId } });
    if (!staff) throw new NotFoundException("Không tìm thấy nhân viên.");
    const branchName = body.branchName || await this.resolveBranchName(branchId);

    return this.prisma.payrollConfig.create({
      data: {
        staffId: staff.id,
        staffCode: body.staffCode || staff.code,
        staffName: body.staffName || staff.name,
        branchId,
        branchName,
        attendanceCode: (body as any).attendanceCode || null,
        salaryType: body.salaryType || "MONTHLY",
        baseSalary: this.money(body.baseSalary),
        dailyRate: this.money(body.dailyRate),
        standardWorkingDays: this.decimal2(body.standardWorkingDays || 26),
        orderAttributionMode: this.normalizeAttributionMode(body.orderAttributionMode),
        commissionPerOrderEnabled: Boolean(body.commissionPerOrderEnabled),
        commissionPerOrderAmount: this.money(body.commissionPerOrderAmount),
        commissionPerItemEnabled: Boolean(body.commissionPerItemEnabled),
        commissionPerItemAmount: this.money(body.commissionPerItemAmount),
        commissionPercentEnabled: Boolean(body.commissionPercentEnabled),
        commissionRate: this.decimal2(body.commissionRate || 0),
        hourlyEnabled: Boolean(body.hourlyEnabled),
        hourlyRate: this.money(body.hourlyRate),
        standardHoursPerDay: this.decimal2(body.standardHoursPerDay || 9.5),
        overtimeRate: this.decimal2(body.overtimeRate || 1),
        holidayRate: this.decimal2(body.holidayRate || 2),
        paidLeaveEnabled: Boolean(body.paidLeaveEnabled),
        paidLeaveHoursPerDay: this.decimal2(body.paidLeaveHoursPerDay || body.standardHoursPerDay || 9.5),
        mealAllowanceEnabled: Boolean(body.mealAllowanceEnabled),
        mealHoursPerUnit: this.decimal2(body.mealHoursPerUnit || body.standardHoursPerDay || 9.5),
        mealAmountPerUnit: this.money(body.mealAmountPerUnit || 30000),
        insuranceDeductionAmount: this.money(body.insuranceDeductionAmount),
        taggedProductEnabled: Boolean(body.taggedProductEnabled),
        taggedProductRate: this.money(body.taggedProductRate),
        ghnCodBonusEnabled: Boolean(body.ghnCodBonusEnabled),
        ghnCodBonusPerOrder: this.money(body.ghnCodBonusPerOrder),
        applyPos: body.applyPos !== false,
        applyOnline: body.applyOnline !== false,
        applyFacebook: body.applyFacebook !== false,
        applyCod: body.applyCod !== false,
        allowanceDefault: this.money(body.allowanceDefault),
        effectiveFrom: this.startOfDate(body.effectiveFrom || new Date()),
        effectiveTo: body.effectiveTo ? this.endOfDate(body.effectiveTo) : null,
        isActive: body.isActive !== false,
        note: body.note || null,
      },
    });
  }

  async updateConfig(id: string, body: PayrollConfigDto, user?: AnyUser) {
    const current = await this.prisma.payrollConfig.findUnique({ where: { id } });
    if (!current) throw new NotFoundException("Không tìm thấy cấu hình lương.");
    this.scopedBranchId(user, current.branchId || null);

    return this.prisma.payrollConfig.update({
      where: { id },
      data: {
        staffCode: body.staffCode ?? undefined,
        staffName: body.staffName ?? undefined,
        branchName: body.branchName ?? undefined,
        attendanceCode: (body as any).attendanceCode ?? undefined,
        salaryType: body.salaryType ?? undefined,
        baseSalary: body.baseSalary === undefined ? undefined : this.money(body.baseSalary),
        dailyRate: body.dailyRate === undefined ? undefined : this.money(body.dailyRate),
        standardWorkingDays: body.standardWorkingDays === undefined ? undefined : this.decimal2(body.standardWorkingDays),
        orderAttributionMode: body.orderAttributionMode === undefined ? undefined : this.normalizeAttributionMode(body.orderAttributionMode),
        commissionPerOrderEnabled: body.commissionPerOrderEnabled ?? undefined,
        commissionPerOrderAmount: body.commissionPerOrderAmount === undefined ? undefined : this.money(body.commissionPerOrderAmount),
        commissionPerItemEnabled: body.commissionPerItemEnabled ?? undefined,
        commissionPerItemAmount: body.commissionPerItemAmount === undefined ? undefined : this.money(body.commissionPerItemAmount),
        commissionPercentEnabled: body.commissionPercentEnabled ?? undefined,
        commissionRate: body.commissionRate === undefined ? undefined : this.decimal2(body.commissionRate),
        hourlyEnabled: body.hourlyEnabled ?? undefined,
        hourlyRate: body.hourlyRate === undefined ? undefined : this.money(body.hourlyRate),
        standardHoursPerDay: body.standardHoursPerDay === undefined ? undefined : this.decimal2(body.standardHoursPerDay),
        overtimeRate: body.overtimeRate === undefined ? undefined : this.decimal2(body.overtimeRate),
        holidayRate: body.holidayRate === undefined ? undefined : this.decimal2(body.holidayRate),
        paidLeaveEnabled: body.paidLeaveEnabled ?? undefined,
        paidLeaveHoursPerDay: body.paidLeaveHoursPerDay === undefined ? undefined : this.decimal2(body.paidLeaveHoursPerDay),
        mealAllowanceEnabled: body.mealAllowanceEnabled ?? undefined,
        mealHoursPerUnit: body.mealHoursPerUnit === undefined ? undefined : this.decimal2(body.mealHoursPerUnit),
        mealAmountPerUnit: body.mealAmountPerUnit === undefined ? undefined : this.money(body.mealAmountPerUnit),
        insuranceDeductionAmount: body.insuranceDeductionAmount === undefined ? undefined : this.money(body.insuranceDeductionAmount),
        taggedProductEnabled: body.taggedProductEnabled ?? undefined,
        taggedProductRate: body.taggedProductRate === undefined ? undefined : this.money(body.taggedProductRate),
        ghnCodBonusEnabled: body.ghnCodBonusEnabled ?? undefined,
        ghnCodBonusPerOrder: body.ghnCodBonusPerOrder === undefined ? undefined : this.money(body.ghnCodBonusPerOrder),
        applyPos: body.applyPos ?? undefined,
        applyOnline: body.applyOnline ?? undefined,
        applyFacebook: body.applyFacebook ?? undefined,
        applyCod: body.applyCod ?? undefined,
        allowanceDefault: body.allowanceDefault === undefined ? undefined : this.money(body.allowanceDefault),
        effectiveFrom: body.effectiveFrom ? this.startOfDate(body.effectiveFrom) : undefined,
        effectiveTo: body.effectiveTo === undefined ? undefined : body.effectiveTo ? this.endOfDate(body.effectiveTo) : null,
        isActive: body.isActive ?? undefined,
        note: body.note ?? undefined,
      },
    });
  }



  private branchTemplateConfigData(body: any) {
    return {
      name: String(body?.name || "Cấu hình mặc định").trim() || "Cấu hình mặc định",
      salaryType: body?.salaryType || "MONTHLY",
      baseSalary: this.money(body?.baseSalary),
      dailyRate: this.money(body?.dailyRate),
      standardWorkingDays: this.decimal2(body?.standardWorkingDays || 26),
      orderAttributionMode: this.normalizeAttributionMode(body?.orderAttributionMode),
      commissionPerOrderEnabled: Boolean(body?.commissionPerOrderEnabled),
      commissionPerOrderAmount: this.money(body?.commissionPerOrderAmount),
      commissionPerItemEnabled: Boolean(body?.commissionPerItemEnabled),
      commissionPerItemAmount: this.money(body?.commissionPerItemAmount),
      commissionPercentEnabled: Boolean(body?.commissionPercentEnabled),
      commissionRate: this.decimal2(body?.commissionRate || 0),
      hourlyEnabled: Boolean(body?.hourlyEnabled),
      hourlyRate: this.money(body?.hourlyRate),
      standardHoursPerDay: this.decimal2(body?.standardHoursPerDay || 9.5),
      overtimeRate: this.decimal2(body?.overtimeRate || 1),
      holidayRate: this.decimal2(body?.holidayRate || 2),
      paidLeaveEnabled: Boolean(body?.paidLeaveEnabled),
      paidLeaveHoursPerDay: this.decimal2(body?.paidLeaveHoursPerDay || body?.standardHoursPerDay || 9.5),
      mealAllowanceEnabled: Boolean(body?.mealAllowanceEnabled),
      mealHoursPerUnit: this.decimal2(body?.mealHoursPerUnit || body?.standardHoursPerDay || 9.5),
      mealAmountPerUnit: this.money(body?.mealAmountPerUnit || 30000),
      insuranceDeductionAmount: this.money(body?.insuranceDeductionAmount),
      taggedProductEnabled: Boolean(body?.taggedProductEnabled),
      taggedProductRate: this.money(body?.taggedProductRate),
      ghnCodBonusEnabled: Boolean(body?.ghnCodBonusEnabled),
      ghnCodBonusPerOrder: this.money(body?.ghnCodBonusPerOrder),
      applyPos: body?.applyPos !== false,
      applyOnline: body?.applyOnline !== false,
      applyFacebook: body?.applyFacebook !== false,
      applyCod: body?.applyCod !== false,
      allowanceDefault: this.money(body?.allowanceDefault),
      note: body?.note || null,
      isActive: body?.isActive !== false,
    };
  }

  private payrollConfigDataFromTemplate(template: any, staff: any, effectiveFrom?: string | Date | null) {
    return {
      staffId: staff.id,
      staffCode: staff.code || null,
      staffName: staff.name || null,
      branchId: template.branchId || staff.branchId || null,
      branchName: template.branchName || staff.branchName || null,
      salaryType: template.salaryType || "MONTHLY",
      baseSalary: template.baseSalary || 0,
      dailyRate: template.dailyRate || 0,
      standardWorkingDays: template.standardWorkingDays || 26,
      orderAttributionMode: template.orderAttributionMode || "ASSIGNED_OR_CREATOR",
      commissionPerOrderEnabled: Boolean(template.commissionPerOrderEnabled),
      commissionPerOrderAmount: template.commissionPerOrderAmount || 0,
      commissionPerItemEnabled: Boolean(template.commissionPerItemEnabled),
      commissionPerItemAmount: template.commissionPerItemAmount || 0,
      commissionPercentEnabled: Boolean(template.commissionPercentEnabled),
      commissionRate: template.commissionRate || 0,
      hourlyEnabled: Boolean(template.hourlyEnabled),
      hourlyRate: template.hourlyRate || 0,
      standardHoursPerDay: template.standardHoursPerDay || 9.5,
      overtimeRate: template.overtimeRate || 1,
      holidayRate: template.holidayRate || 2,
      paidLeaveEnabled: Boolean(template.paidLeaveEnabled),
      paidLeaveHoursPerDay: template.paidLeaveHoursPerDay || template.standardHoursPerDay || 9.5,
      mealAllowanceEnabled: Boolean(template.mealAllowanceEnabled),
      mealHoursPerUnit: template.mealHoursPerUnit || template.standardHoursPerDay || 9.5,
      mealAmountPerUnit: template.mealAmountPerUnit || 30000,
      insuranceDeductionAmount: template.insuranceDeductionAmount || 0,
      taggedProductEnabled: Boolean(template.taggedProductEnabled),
      taggedProductRate: template.taggedProductRate || 0,
      ghnCodBonusEnabled: Boolean(template.ghnCodBonusEnabled),
      ghnCodBonusPerOrder: template.ghnCodBonusPerOrder || 0,
      applyPos: template.applyPos !== false,
      applyOnline: template.applyOnline !== false,
      applyFacebook: template.applyFacebook !== false,
      applyCod: template.applyCod !== false,
      allowanceDefault: template.allowanceDefault || 0,
      effectiveFrom: this.startOfDate(effectiveFrom || new Date()),
      effectiveTo: null,
      isActive: true,
      note: template.note || null,
    };
  }

  async listBranchConfigTemplates(query: any = {}, user?: AnyUser) {
    const branchId = this.scopedBranchId(user, query.branchId || null);
    const where: any = {};
    if (branchId) where.branchId = branchId;
    if (query.isActive !== undefined && query.isActive !== "ALL") where.isActive = String(query.isActive) !== "false";
    if (query.q?.trim()) {
      const q = String(query.q).trim();
      where.OR = [
        { name: { contains: q, mode: "insensitive" } },
        { branchName: { contains: q, mode: "insensitive" } },
        { branchId: { contains: q, mode: "insensitive" } },
      ];
    }
    return (this.prisma as any).payrollBranchConfigTemplate.findMany({
      where,
      orderBy: [{ isActive: "desc" }, { branchName: "asc" }, { name: "asc" }],
    });
  }

  async createBranchConfigTemplate(body: any, user?: AnyUser) {
    const branchId = this.scopedBranchId(user, body?.branchId || null);
    if (!branchId) throw new BadRequestException("Chọn chi nhánh để tạo mẫu cấu hình lương.");
    const branchName = body?.branchName || await this.resolveBranchName(branchId);
    const existing = await (this.prisma as any).payrollBranchConfigTemplate.findFirst({ where: { branchId, name: body?.name || "Cấu hình mặc định" } });
    if (existing) {
      return (this.prisma as any).payrollBranchConfigTemplate.update({
        where: { id: existing.id },
        data: { branchId, branchName, ...this.branchTemplateConfigData(body) },
      });
    }
    return (this.prisma as any).payrollBranchConfigTemplate.create({
      data: { branchId, branchName, ...this.branchTemplateConfigData(body) },
    });
  }

  async updateBranchConfigTemplate(id: string, body: any, user?: AnyUser) {
    const current = await (this.prisma as any).payrollBranchConfigTemplate.findUnique({ where: { id } });
    if (!current) throw new NotFoundException("Không tìm thấy mẫu cấu hình chi nhánh.");
    this.scopedBranchId(user, current.branchId || null);
    const branchId = body?.branchId ? this.scopedBranchId(user, body.branchId) : current.branchId;
    const branchName = body?.branchName || await this.resolveBranchName(branchId);
    return (this.prisma as any).payrollBranchConfigTemplate.update({
      where: { id },
      data: { branchId, branchName, ...this.branchTemplateConfigData(body) },
    });
  }

  async applyBranchConfigTemplate(body: any, user?: AnyUser) {
    const templateId = String(body?.templateId || "").trim();
    const branchId = this.scopedBranchId(user, body?.branchId || null);
    const overwrite = Boolean(body?.overwrite);
    const onlyMissing = body?.onlyMissing !== false && !overwrite;
    const staffIds = Array.isArray(body?.staffIds) ? body.staffIds.map((id: any) => String(id)).filter(Boolean) : [];
    const effectiveFrom = body?.effectiveFrom || new Date();

    const template = templateId
      ? await (this.prisma as any).payrollBranchConfigTemplate.findUnique({ where: { id: templateId } })
      : await (this.prisma as any).payrollBranchConfigTemplate.findFirst({ where: { branchId, isActive: true }, orderBy: { updatedAt: "desc" } });
    if (!template) throw new NotFoundException("Không tìm thấy mẫu cấu hình chi nhánh.");
    this.scopedBranchId(user, template.branchId || null);

    const staffWhere: any = { isActive: true };
    if (staffIds.length) staffWhere.id = { in: staffIds };
    else staffWhere.branchId = branchId || template.branchId;
    const staffRows = await this.prisma.staffUser.findMany({ where: staffWhere, orderBy: { name: "asc" } });
    if (!staffRows.length) throw new BadRequestException("Không có nhân viên phù hợp để áp dụng mẫu.");

    let created = 0;
    let updated = 0;
    let skipped = 0;
    const results: any[] = [];
    for (const staff of staffRows) {
      const configWhere: any = { staffId: staff.id, branchId: template.branchId || staff.branchId || null, isActive: true };
      const existing = await this.prisma.payrollConfig.findFirst({ where: configWhere, orderBy: { updatedAt: "desc" } });
      const data = this.payrollConfigDataFromTemplate(template, staff, effectiveFrom);
      if (existing) {
        if (onlyMissing) {
          skipped += 1;
          results.push({ staffId: staff.id, staffName: staff.name, status: "SKIPPED", configId: existing.id });
          continue;
        }
        const updatedConfig = await this.prisma.payrollConfig.update({ where: { id: existing.id }, data: { ...data, attendanceCode: existing.attendanceCode || undefined } as any });
        updated += 1;
        results.push({ staffId: staff.id, staffName: staff.name, status: "UPDATED", configId: updatedConfig.id });
      } else {
        const createdConfig = await this.prisma.payrollConfig.create({ data: data as any });
        created += 1;
        results.push({ staffId: staff.id, staffName: staff.name, status: "CREATED", configId: createdConfig.id });
      }
    }

    return { templateId: template.id, branchId: template.branchId, created, updated, skipped, total: staffRows.length, results };
  }

  private async activeConfigsForPeriod(period: any) {
    const where: Prisma.PayrollConfigWhereInput = {
      isActive: true,
      effectiveFrom: { lte: period.toDate },
      OR: [{ effectiveTo: null }, { effectiveTo: { gte: period.fromDate } }],
    };
    if (period.branchId) where.AND = [{ OR: [{ branchId: period.branchId }, { branchId: null }] }];
    return this.prisma.payrollConfig.findMany({ where, orderBy: [{ branchName: "asc" }, { staffName: "asc" }] });
  }

  async calculatePeriod(id: string, body: { workingDaysByStaff?: Record<string, number>; force?: boolean } = {}, user?: AnyUser) {
    const period = await this.prisma.payrollPeriod.findUnique({ where: { id } });
    if (!period) throw new NotFoundException("Không tìm thấy kỳ lương.");
    this.scopedBranchId(user, period.branchId || null);

    const status = String(period.status || "DRAFT").toUpperCase();
    if (["LOCKED", "PAID", "PARTIALLY_PAID"].includes(status) && !body.force) {
      throw new BadRequestException("Kỳ lương đã khóa hoặc đã trả, không thể tính lại.");
    }

    const configs = await this.activeConfigsForPeriod(period);
    if (!configs.length) throw new BadRequestException("Chưa có cấu hình lương nào phù hợp kỳ này.");

    // Giữ lại dữ liệu nhập tay theo kỳ lương (giờ thường, CT1, CT2, SP thưởng...)
    // khi admin bấm Tính lại. Nếu không giữ, hệ thống sẽ xóa line cũ rồi tính về 0 giờ/SP.
    const existingLines = await this.prisma.payrollLine.findMany({ where: { periodId: id } });
    const existingInputByStaff = new Map<string, any>(
      existingLines.map((line: any) => [String(line.staffId), line]),
    );
    const manualInputsByStaff = (body as any).manualInputsByStaff || (body as any).inputsByStaff || {};

    const staffIds = Array.from(new Set(configs.map((c) => c.staffId)));
    const staffRows = await this.prisma.staffUser.findMany({
      where: { id: { in: staffIds }, isActive: true },
      select: { id: true, code: true, name: true, branchId: true, branchName: true },
    });
    const staffMap = new Map(staffRows.map((s) => [s.id, s]));

    await this.prisma.$transaction(async (tx) => {
      await tx.payrollLine.deleteMany({ where: { periodId: id } });

      let totalStaff = 0;
      let totalOrders = 0;
      let totalItems = 0;
      let totalRevenue = 0;
      let totalHourlyAmount = 0;
      let totalTaggedProductAmount = 0;
      let totalMealAllowance = 0;
      let totalInsuranceDeduction = 0;
      let totalGhnCodBonus = 0;
      let totalAttendanceWarnings = 0;
      let totalLateMinutes = 0;
      let totalEarlyMinutes = 0;
      let totalGross = 0;
      let totalNet = 0;

      for (const config of configs) {
        const staff = staffMap.get(config.staffId);
        if (!staff) continue;

        const lineBranchId = period.branchId || config.branchId || staff.branchId || null;
        const lineBranchName = period.branchName || config.branchName || staff.branchName || null;
        const input = { ...(existingInputByStaff.get(config.staffId) || {}), ...((manualInputsByStaff || {})[config.staffId] || {}) };
        const workingDays = this.toNumber(body.workingDaysByStaff?.[config.staffId] ?? input.workingDays ?? config.standardWorkingDays ?? 26);
        const attendanceCode = input.attendanceCode || (config as any).attendanceCode || null;
        const attendanceMatchedBy = input.attendanceMatchedBy || null;
        const attendanceRawName = input.attendanceRawName || null;
        const attendanceSourceFile = input.attendanceSourceFile || null;
        const attendanceImportedAt = input.attendanceImportedAt || null;
        const lateCount = Number(input.lateCount || 0);
        const lateMinutes = Number(input.lateMinutes || 0);
        const earlyCount = Number(input.earlyCount || 0);
        const earlyMinutes = Number(input.earlyMinutes || 0);
        const attendanceWarningLevel = input.attendanceWarningLevel || null;
        const attendanceWarningNote = input.attendanceWarningNote || null;
        const standardDays = Math.max(1, this.toNumber(config.standardWorkingDays || 26));
        const salaryType = String(config.salaryType || "MONTHLY").toUpperCase();
        const baseSalary = this.toNumber(config.baseSalary);
        const dailyRate = this.toNumber(config.dailyRate);
        const proratedSalary =
          salaryType === "NONE" ? 0 :
          salaryType === "DAILY" || salaryType === "SHIFT" ? dailyRate * workingDays :
          baseSalary * Math.min(workingDays, standardDays) / standardDays;

        const orderAttributionMode = this.normalizeAttributionMode((config as any).orderAttributionMode);
        const orderWhere = this.buildPayrollOrderWhere(config.staffId, period, lineBranchId, orderAttributionMode);

        const rawOrders = await tx.order.findMany({
          where: orderWhere,
          include: { items: true, shipment: true, payments: { include: { paymentSource: true } } },
          orderBy: { createdAt: "desc" },
        });

        const validOrders = rawOrders.filter((order: any) => this.isOrderSuccessForPayroll(order) && this.channelEnabled(config, order));
        const successOrderCount = validOrders.length;
        const successItemQty = validOrders.reduce((sum: number, order: any) => sum + this.orderItemQty(order), 0);
        const revenueAmount = validOrders.reduce((sum: number, order: any) => sum + this.toNumber(order.finalAmount), 0);

        const normalHours = this.toNumber(input.normalHours || 0);
        const overtimeHours = this.toNumber(input.overtimeHours || 0);
        const holidayHours = this.toNumber(input.holidayHours || 0);
        const overtimeRate = this.toNumber((config as any).overtimeRate || 1);
        const holidayRate = this.toNumber((config as any).holidayRate || 2);
        const hourlyRate = this.toNumber((config as any).hourlyRate || 0);
        const convertedWorkingHours = normalHours + overtimeHours * overtimeRate + holidayHours * holidayRate;
        const hourlyAmount = (config as any).hourlyEnabled ? convertedWorkingHours * hourlyRate : 0;

        const paidLeaveDays = this.toNumber(input.paidLeaveDays || 0);
        const paidLeaveHoursPerDay = this.toNumber((config as any).paidLeaveHoursPerDay || (config as any).standardHoursPerDay || 9.5);
        const paidLeaveAmount = (config as any).paidLeaveEnabled ? paidLeaveDays * paidLeaveHoursPerDay * hourlyRate : 0;

        const rawWorkingHours = normalHours + overtimeHours + holidayHours;
        const mealAllowanceAmount = (config as any).mealAllowanceEnabled
          ? (rawWorkingHours / Math.max(1, this.toNumber((config as any).mealHoursPerUnit || 9.5))) * this.toNumber((config as any).mealAmountPerUnit || 0)
          : 0;

        const insuranceDeduction = this.toNumber((config as any).insuranceDeductionAmount || 0);
        const taggedProductQty = Number(input.taggedProductQty || 0);
        const taggedProductRate = this.toNumber((config as any).taggedProductRate || 0);
        const taggedProductAmount = (config as any).taggedProductEnabled ? taggedProductQty * taggedProductRate : 0;
        const ghnCodOrderCount = (config as any).ghnCodBonusEnabled ? validOrders.filter((order: any) => this.isGhnCodOrder(order)).length : 0;
        const ghnCodBonusPerOrder = this.toNumber((config as any).ghnCodBonusPerOrder || 0);
        const ghnCodBonusAmount = ghnCodOrderCount * ghnCodBonusPerOrder;

        const commissionByOrder = config.commissionPerOrderEnabled
          ? successOrderCount * this.toNumber(config.commissionPerOrderAmount)
          : 0;
        const commissionByItem = config.commissionPerItemEnabled
          ? successItemQty * this.toNumber(config.commissionPerItemAmount)
          : 0;
        const commissionByPercent = config.commissionPercentEnabled
          ? revenueAmount * this.toNumber(config.commissionRate) / 100
          : 0;
        const commissionTotal = commissionByOrder + commissionByItem + commissionByPercent;
        const allowance = this.toNumber(config.allowanceDefault);
        const totals = this.calcLineTotals({
          proratedSalary,
          hourlyAmount,
          paidLeaveAmount,
          taggedProductAmount,
          ghnCodBonusAmount,
          commissionTotal,
          allowance,
          mealAllowanceAmount,
          insuranceDeduction,
        });

        const line = await tx.payrollLine.create({
          data: {
            periodId: id,
            staffId: staff.id,
            staffCode: staff.code,
            staffName: staff.name,
            branchId: lineBranchId,
            branchName: lineBranchName,
            salaryType,
            baseSalary: this.money(baseSalary),
            dailyRate: this.money(dailyRate),
            workingDays: this.decimal2(workingDays),
            standardDays: this.decimal2(standardDays),
            proratedSalary: this.money(proratedSalary),
            orderAttributionMode,
            successOrderCount,
            successItemQty,
            revenueAmount: this.money(revenueAmount),
            attendanceCode,
            attendanceMatchedBy,
            attendanceRawName,
            attendanceSourceFile,
            attendanceImportedAt,
            lateCount,
            lateMinutes,
            earlyCount,
            earlyMinutes,
            attendanceWarningLevel,
            attendanceWarningNote,
            normalHours: this.decimal2(normalHours),
            overtimeHours: this.decimal2(overtimeHours),
            overtimeRate: this.decimal2(overtimeRate),
            holidayHours: this.decimal2(holidayHours),
            holidayRate: this.decimal2(holidayRate),
            convertedWorkingHours: this.decimal2(convertedWorkingHours),
            hourlyRate: this.money(hourlyRate),
            hourlyAmount: this.money(hourlyAmount),
            paidLeaveDays: this.decimal2(paidLeaveDays),
            paidLeaveHoursPerDay: this.decimal2(paidLeaveHoursPerDay),
            paidLeaveAmount: this.money(paidLeaveAmount),
            mealAllowanceAmount: this.money(mealAllowanceAmount),
            insuranceDeduction: this.money(insuranceDeduction),
            taggedProductQty,
            taggedProductRate: this.money(taggedProductRate),
            taggedProductAmount: this.money(taggedProductAmount),
            ghnCodOrderCount,
            ghnCodBonusPerOrder: this.money(ghnCodBonusPerOrder),
            ghnCodBonusAmount: this.money(ghnCodBonusAmount),
            commissionByOrder: this.money(commissionByOrder),
            commissionByItem: this.money(commissionByItem),
            commissionByPercent: this.money(commissionByPercent),
            commissionTotal: this.money(commissionTotal),
            allowance: this.money(allowance),
            grossPay: this.money(totals.grossPay),
            netPay: this.money(totals.netPay),
            status: "CALCULATED",
          },
        });

        if (validOrders.length) {
          await tx.payrollOrderLink.createMany({
            data: validOrders.map((order: any) => {
              const itemQty = this.orderItemQty(order);
              const revenue = this.toNumber(order.finalAmount);
              const byOrder = config.commissionPerOrderEnabled ? this.toNumber(config.commissionPerOrderAmount) : 0;
              const byItem = config.commissionPerItemEnabled ? itemQty * this.toNumber(config.commissionPerItemAmount) : 0;
              const byPercent = config.commissionPercentEnabled ? revenue * this.toNumber(config.commissionRate) / 100 : 0;
              return {
                payrollLineId: line.id,
                orderId: order.id,
                orderCode: order.orderCode,
                branchId: order.branchId || null,
                salesChannel: String(order.salesChannel || ""),
                orderDate: order.createdAt,
                completedAt: order.updatedAt,
                revenueAmount: this.money(revenue),
                itemQty,
                commissionByOrder: this.money(byOrder),
                commissionByItem: this.money(byItem),
                commissionByPercent: this.money(byPercent),
                commissionTotal: this.money(byOrder + byItem + byPercent),
                attributedStaffId: config.staffId,
                attributedStaffName: staff.name,
                attributionSource: this.payrollAttributionSource(order, config.staffId, orderAttributionMode),
                reason: this.payrollAttributionSource(order, config.staffId, orderAttributionMode) === "ASSIGNED_STAFF"
                  ? "Đơn tính theo NV phụ trách"
                  : "Đơn tính theo nhân viên tạo",
              };
            }),
          });
        }

        totalStaff += 1;
        totalOrders += successOrderCount;
        totalItems += successItemQty;
        totalRevenue += revenueAmount;
        totalHourlyAmount += hourlyAmount;
        totalTaggedProductAmount += taggedProductAmount;
        totalMealAllowance += mealAllowanceAmount;
        totalInsuranceDeduction += insuranceDeduction;
        totalGhnCodBonus += ghnCodBonusAmount;
        if (["WARNING", "CRITICAL"].includes(String(attendanceWarningLevel || "").toUpperCase())) totalAttendanceWarnings += 1;
        totalLateMinutes += lateMinutes;
        totalEarlyMinutes += earlyMinutes;
        totalGross += totals.grossPay;
        totalNet += totals.netPay;
      }

      await tx.payrollPeriod.update({
        where: { id },
        data: {
          status: "CALCULATED",
          totalStaff,
          totalOrders,
          totalItems,
          totalRevenue: this.money(totalRevenue),
          totalHourlyAmount: this.money(totalHourlyAmount),
          totalTaggedProductAmount: this.money(totalTaggedProductAmount),
          totalMealAllowance: this.money(totalMealAllowance),
          totalInsuranceDeduction: this.money(totalInsuranceDeduction),
          totalGhnCodBonus: this.money(totalGhnCodBonus),
          totalAttendanceWarnings,
          totalLateMinutes,
          totalEarlyMinutes,
          totalGross: this.money(totalGross),
          totalNet: this.money(totalNet),
          totalPaid: this.money(0),
        },
      });
    }, { maxWait: 10000, timeout: 60000 });

    return this.getPeriod(id, user);
  }


  private normalizeMatchText(value: unknown) {
    return String(value || "")
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
  }

  private async buildAttendancePreviewRows(period: any, parsedRows: ParsedAttendanceRow[], fileName?: string) {
    const settings = await this.getOrCreateSettings();
    const configs = await this.activeConfigsForPeriod(period);
    const staffIds = Array.from(new Set(configs.map((c) => c.staffId)));
    const staffRows = await this.prisma.staffUser.findMany({
      where: { id: { in: staffIds } },
      select: { id: true, code: true, name: true, branchId: true, branchName: true, isActive: true },
    });

    const staffById = new Map(staffRows.map((s) => [s.id, s]));
    const configByStaff = new Map(configs.map((c: any) => [c.staffId, c]));
    const byAttendanceCode = new Map<string, any>();
    const byStaffCode = new Map<string, any>();
    const byName = new Map<string, any>();

    for (const config of configs as any[]) {
      const staff = staffById.get(config.staffId);
      if (!staff) continue;
      if (config.attendanceCode) byAttendanceCode.set(String(config.attendanceCode).trim(), { staff, config, matchedBy: "ATTENDANCE_CODE" });
      if (staff.code) byStaffCode.set(String(staff.code).trim(), { staff, config, matchedBy: "STAFF_CODE" });
      const normalizedName = this.normalizeMatchText(staff.name || config.staffName);
      if (normalizedName) byName.set(normalizedName, { staff, config, matchedBy: "NAME" });
    }

    return parsedRows.map((row) => {
      let match = byAttendanceCode.get(String(row.attendanceCode || "").trim()) || byStaffCode.get(String(row.attendanceCode || "").trim()) || null;
      if (!match) {
        const rowName = this.normalizeMatchText(row.staffName);
        match = byName.get(rowName) || null;
        if (!match && rowName) {
          const candidate = Array.from(byName.entries()).find(([key]) => key && (key.includes(rowName) || rowName.includes(key)));
          if (candidate) match = candidate[1];
        }
      }

      const warning = this.attendanceWarning(row, settings);
      return {
        ...row,
        fileName,
        matched: Boolean(match?.staff?.id),
        matchedBy: match?.matchedBy || null,
        staffId: match?.staff?.id || null,
        staffCode: match?.staff?.code || null,
        systemStaffName: match?.staff?.name || null,
        systemBranchId: match?.staff?.branchId || match?.config?.branchId || null,
        systemBranchName: match?.staff?.branchName || match?.config?.branchName || null,
        hourlyRate: match?.config ? this.toNumber(match.config.hourlyRate) : 0,
        overtimeRate: match?.config ? this.toNumber(match.config.overtimeRate || 1) : 1,
        holidayRate: match?.config ? this.toNumber(match.config.holidayRate || 2) : 2,
        taggedProductRate: match?.config ? this.toNumber(match.config.taggedProductRate || 0) : 0,
        warningLevel: warning.level,
        warningNote: warning.note,
      };
    });
  }

  async previewAttendanceImport(id: string, fileBuffer: Buffer, fileName: string, user?: AnyUser) {
    const period = await this.prisma.payrollPeriod.findUnique({ where: { id } });
    if (!period) throw new NotFoundException("Không tìm thấy kỳ lương.");
    this.scopedBranchId(user, period.branchId || null);
    const parsed = parseAttendanceWorkbook(fileBuffer);
    const rows = await this.buildAttendancePreviewRows(period, parsed, fileName);
    const summary = rows.reduce((acc: any, row: any) => {
      acc.totalRows += 1;
      if (row.matched) acc.matchedRows += 1;
      else acc.unmatchedRows += 1;
      acc.totalHours += this.toNumber(row.normalHours) + this.toNumber(row.overtimeHours) + this.toNumber(row.holidayHours);
      acc.totalLateMinutes += Number(row.lateMinutes || 0);
      acc.totalEarlyMinutes += Number(row.earlyMinutes || 0);
      if (["WARNING", "CRITICAL"].includes(String(row.warningLevel || "").toUpperCase())) acc.warningRows += 1;
      return acc;
    }, { totalRows: 0, matchedRows: 0, unmatchedRows: 0, totalHours: 0, totalLateMinutes: 0, totalEarlyMinutes: 0, warningRows: 0 });
    return { fileName, summary, rows };
  }

  async applyAttendanceImport(id: string, body: { rows?: any[]; fileName?: string; autoCalculate?: boolean; saveMappings?: boolean } = {}, user?: AnyUser) {
    const period = await this.prisma.payrollPeriod.findUnique({ where: { id }, include: { lines: true } });
    if (!period) throw new NotFoundException("Không tìm thấy kỳ lương.");
    this.scopedBranchId(user, period.branchId || null);
    const status = String(period.status || "").toUpperCase();
    if (["LOCKED", "PAID", "PARTIALLY_PAID"].includes(status)) throw new BadRequestException("Kỳ lương đã khóa hoặc đã trả, không thể import chấm công.");

    if (!period.lines?.length) {
      await this.calculatePeriod(id, { force: true }, user);
    }

    const rows = Array.isArray(body.rows) ? body.rows : [];
    if (!rows.length) throw new BadRequestException("Không có dữ liệu chấm công để áp dụng.");
    const settings = await this.getOrCreateSettings();
    const latest = await this.prisma.payrollPeriod.findUnique({ where: { id }, include: { lines: true } });
    const lineByStaff = new Map((latest?.lines || []).map((line: any) => [String(line.staffId), line]));
    const now = new Date();
    let matchedRows = 0;
    let unmatchedRows = 0;
    let totalHours = 0;
    let totalLateMinutes = 0;
    let totalEarlyMinutes = 0;

    for (const row of rows) {
      const staffId = String(row.staffId || "").trim();
      const line = staffId ? lineByStaff.get(staffId) : null;
      if (!line) { unmatchedRows += 1; continue; }
      const warning = this.attendanceWarning(row, settings);
      matchedRows += 1;
      totalHours += this.toNumber(row.normalHours) + this.toNumber(row.overtimeHours) + this.toNumber(row.holidayHours);
      totalLateMinutes += Number(row.lateMinutes || 0);
      totalEarlyMinutes += Number(row.earlyMinutes || 0);
      if (body?.saveMappings !== false && row.attendanceCode) {
        const existingConfig = await this.prisma.payrollConfig.findFirst({
          where: { staffId, isActive: true, OR: [{ branchId: line.branchId }, { branchId: null }] },
          orderBy: { updatedAt: "desc" },
        });
        if (existingConfig && (!existingConfig.attendanceCode || existingConfig.attendanceCode !== row.attendanceCode)) {
          await this.prisma.payrollConfig.update({ where: { id: existingConfig.id }, data: { attendanceCode: row.attendanceCode } });
        }
      }

      await this.updateLine(line.id, {
        normalHours: row.normalHours,
        overtimeHours: row.overtimeHours,
        overtimeRate: row.overtimeRate || line.overtimeRate || 1,
        holidayHours: row.holidayHours,
        holidayRate: row.holidayRate || line.holidayRate || 2,
        hourlyRate: row.hourlyRate || line.hourlyRate || 0,
        attendanceCode: row.attendanceCode,
        attendanceMatchedBy: row.matchedBy || null,
        attendanceRawName: row.staffName || row.attendanceRawName || null,
        attendanceSourceFile: body.fileName || row.fileName || null,
        attendanceImportedAt: now,
        lateCount: row.lateCount || 0,
        lateMinutes: row.lateMinutes || 0,
        earlyCount: row.earlyCount || 0,
        earlyMinutes: row.earlyMinutes || 0,
        attendanceWarningLevel: warning.level,
        attendanceWarningNote: warning.note,
      }, user);
    }

    await (this.prisma as any).payrollAttendanceImport.create({
      data: {
        periodId: id,
        fileName: body.fileName || null,
        totalRows: rows.length,
        matchedRows,
        unmatchedRows,
        totalHours: this.decimal2(totalHours),
        totalLateMinutes,
        totalEarlyMinutes,
        createdById: user?.id || null,
        createdByName: this.userName(user),
      },
    }).catch(() => null);

    await this.prisma.payrollPeriod.update({
      where: { id },
      data: {
        attendanceImportedAt: now,
        attendanceImportFileName: body.fileName || null,
      } as any,
    }).catch(() => null);

    if (body.autoCalculate) await this.calculatePeriod(id, { force: true }, user);
    return this.getPeriod(id, user);
  }

  async exportPeriodCsv(id: string, user?: AnyUser) {
    const period = await this.getPeriod(id, user) as any;
    const header = [
      "Nhân viên", "Chi nhánh", "Giờ thường", "CT1", "CT2", "Giờ quy đổi", "Lương giờ",
      "Số SP", "Giá/SP", "Lương SP", "Ăn trưa", "Bảo hiểm", "Hoa hồng", "Thưởng", "Phụ cấp", "Tạm ứng", "Khấu trừ", "Đi muộn", "Về sớm", "Cảnh báo", "Thực nhận",
    ];
    const lines = [header.join(",")];
    for (const line of period.lines || []) {
      lines.push([
        line.staffName, line.branchName, line.normalHours, line.overtimeHours, line.holidayHours, line.convertedWorkingHours, line.hourlyAmount,
        line.taggedProductQty, line.taggedProductRate, line.taggedProductAmount, line.mealAllowanceAmount, line.insuranceDeduction,
        line.commissionTotal, line.bonus, line.allowance, line.advance, line.deduction,
        `${line.lateCount || 0}/${line.lateMinutes || 0}`, `${line.earlyCount || 0}/${line.earlyMinutes || 0}`, line.attendanceWarningLevel || "", line.netPay,
      ].map((v: any) => `"${String(v ?? "").replace(/"/g, '""')}"`).join(","));
    }
    return "\ufeff" + lines.join("\n");
  }

  async lockPeriod(id: string, user?: AnyUser) {
    const period = await this.prisma.payrollPeriod.findUnique({ where: { id } });
    if (!period) throw new NotFoundException("Không tìm thấy kỳ lương.");
    this.scopedBranchId(user, period.branchId || null);
    if (!Number(period.totalStaff || 0)) throw new BadRequestException("Kỳ lương chưa có dữ liệu tính lương.");
    return this.prisma.payrollPeriod.update({
      where: { id },
      data: { status: "LOCKED", lockedAt: new Date(), lockedById: user?.id || null, lockedByName: this.userName(user) },
    });
  }

  async unlockPeriod(id: string, user?: AnyUser) {
    const period = await this.prisma.payrollPeriod.findUnique({ where: { id } });
    if (!period) throw new NotFoundException("Không tìm thấy kỳ lương.");
    this.scopedBranchId(user, period.branchId || null);
    if (String(period.status).toUpperCase() === "PAID") throw new BadRequestException("Kỳ lương đã trả không thể mở khóa.");
    return this.prisma.payrollPeriod.update({
      where: { id },
      data: { status: "CALCULATED", lockedAt: null, lockedById: null, lockedByName: null },
    });
  }

  async updateLine(id: string, body: any, user?: AnyUser) {
    const line = await this.prisma.payrollLine.findUnique({ where: { id }, include: { period: true } });
    if (!line) throw new NotFoundException("Không tìm thấy dòng lương.");
    this.scopedBranchId(user, line.branchId || line.period.branchId || null);
    if (["PAID"].includes(String(line.status || "").toUpperCase())) throw new BadRequestException("Dòng lương đã trả không thể sửa.");

    const normalHours = body.normalHours === undefined ? this.toNumber((line as any).normalHours) : this.toNumber(body.normalHours);
    const overtimeHours = body.overtimeHours === undefined ? this.toNumber((line as any).overtimeHours) : this.toNumber(body.overtimeHours);
    const overtimeRate = body.overtimeRate === undefined ? this.toNumber((line as any).overtimeRate || 1) : this.toNumber(body.overtimeRate || 1);
    const holidayHours = body.holidayHours === undefined ? this.toNumber((line as any).holidayHours) : this.toNumber(body.holidayHours);
    const holidayRate = body.holidayRate === undefined ? this.toNumber((line as any).holidayRate || 2) : this.toNumber(body.holidayRate || 2);
    const hourlyRate = body.hourlyRate === undefined ? this.toNumber((line as any).hourlyRate) : this.toNumber(body.hourlyRate);
    const convertedWorkingHours = normalHours + overtimeHours * overtimeRate + holidayHours * holidayRate;
    const hourlyAmount = convertedWorkingHours * hourlyRate;

    const paidLeaveDays = body.paidLeaveDays === undefined ? this.toNumber((line as any).paidLeaveDays) : this.toNumber(body.paidLeaveDays);
    const paidLeaveHoursPerDay = body.paidLeaveHoursPerDay === undefined ? this.toNumber((line as any).paidLeaveHoursPerDay) : this.toNumber(body.paidLeaveHoursPerDay);
    const paidLeaveAmount = paidLeaveDays * paidLeaveHoursPerDay * hourlyRate;

    const mealHoursPerUnit = Math.max(1, this.toNumber(body.mealHoursPerUnit || 9.5));
    const mealAmountPerUnit = this.toNumber(body.mealAmountPerUnit || 0);
    const mealAllowanceAmount = body.mealAllowanceAmount === undefined
      ? this.toNumber((line as any).mealAllowanceAmount)
      : this.toNumber(body.mealAllowanceAmount);
    const autoMealAllowanceAmount = body.autoMealAllowance === true
      ? ((normalHours + overtimeHours + holidayHours) / mealHoursPerUnit) * mealAmountPerUnit
      : mealAllowanceAmount;

    const taggedProductQty = body.taggedProductQty === undefined ? Number((line as any).taggedProductQty || 0) : Number(body.taggedProductQty || 0);
    const taggedProductRate = body.taggedProductRate === undefined ? this.toNumber((line as any).taggedProductRate) : this.toNumber(body.taggedProductRate);
    const taggedProductAmount = taggedProductQty * taggedProductRate;

    const ghnCodOrderCount = body.ghnCodOrderCount === undefined ? Number((line as any).ghnCodOrderCount || 0) : Number(body.ghnCodOrderCount || 0);
    const ghnCodBonusPerOrder = body.ghnCodBonusPerOrder === undefined ? this.toNumber((line as any).ghnCodBonusPerOrder) : this.toNumber(body.ghnCodBonusPerOrder);
    const ghnCodBonusAmount = ghnCodOrderCount * ghnCodBonusPerOrder;

    const next: any = {
      workingDays: body.workingDays === undefined ? line.workingDays : this.decimal2(body.workingDays),
      normalHours: this.decimal2(normalHours),
      overtimeHours: this.decimal2(overtimeHours),
      overtimeRate: this.decimal2(overtimeRate),
      holidayHours: this.decimal2(holidayHours),
      holidayRate: this.decimal2(holidayRate),
      convertedWorkingHours: this.decimal2(convertedWorkingHours),
      hourlyRate: this.money(hourlyRate),
      hourlyAmount: this.money(hourlyAmount),
      paidLeaveDays: this.decimal2(paidLeaveDays),
      paidLeaveHoursPerDay: this.decimal2(paidLeaveHoursPerDay),
      paidLeaveAmount: this.money(paidLeaveAmount),
      mealAllowanceAmount: this.money(autoMealAllowanceAmount),
      insuranceDeduction: body.insuranceDeduction === undefined ? (line as any).insuranceDeduction : this.money(body.insuranceDeduction),
      taggedProductQty,
      taggedProductRate: this.money(taggedProductRate),
      taggedProductAmount: this.money(taggedProductAmount),
      ghnCodOrderCount,
      ghnCodBonusPerOrder: this.money(ghnCodBonusPerOrder),
      ghnCodBonusAmount: this.money(ghnCodBonusAmount),
      bonus: body.bonus === undefined ? line.bonus : this.money(body.bonus),
      allowance: body.allowance === undefined ? line.allowance : this.money(body.allowance),
      advance: body.advance === undefined ? line.advance : this.money(body.advance),
      deduction: body.deduction === undefined ? line.deduction : this.money(body.deduction),
      attendanceCode: body.attendanceCode === undefined ? (line as any).attendanceCode : body.attendanceCode || null,
      attendanceMatchedBy: body.attendanceMatchedBy === undefined ? (line as any).attendanceMatchedBy : body.attendanceMatchedBy || null,
      attendanceRawName: body.attendanceRawName === undefined ? (line as any).attendanceRawName : body.attendanceRawName || null,
      attendanceSourceFile: body.attendanceSourceFile === undefined ? (line as any).attendanceSourceFile : body.attendanceSourceFile || null,
      attendanceImportedAt: body.attendanceImportedAt === undefined ? (line as any).attendanceImportedAt : body.attendanceImportedAt ? this.asDate(body.attendanceImportedAt) : null,
      lateCount: body.lateCount === undefined ? (line as any).lateCount : Number(body.lateCount || 0),
      lateMinutes: body.lateMinutes === undefined ? (line as any).lateMinutes : Number(body.lateMinutes || 0),
      earlyCount: body.earlyCount === undefined ? (line as any).earlyCount : Number(body.earlyCount || 0),
      earlyMinutes: body.earlyMinutes === undefined ? (line as any).earlyMinutes : Number(body.earlyMinutes || 0),
      attendanceWarningLevel: body.attendanceWarningLevel === undefined ? (line as any).attendanceWarningLevel : body.attendanceWarningLevel || null,
      attendanceWarningNote: body.attendanceWarningNote === undefined ? (line as any).attendanceWarningNote : body.attendanceWarningNote || null,
      note: body.note ?? line.note,
    };

    const totals = this.calcLineTotals({ ...line, ...next });
    const updated = await this.prisma.payrollLine.update({
      where: { id },
      data: { ...next, grossPay: this.money(totals.grossPay), netPay: this.money(totals.netPay), status: "CALCULATED" },
    });
    await this.recalculatePeriodTotals(line.periodId);
    return updated;
  }

  async addAdjustment(id: string, body: PayrollAdjustmentDto, user?: AnyUser) {
    const line = await this.prisma.payrollLine.findUnique({ where: { id }, include: { period: true } });
    if (!line) throw new NotFoundException("Không tìm thấy dòng lương.");
    this.scopedBranchId(user, line.branchId || line.period.branchId || null);
    const type = String(body.type || "").toUpperCase();
    if (!["BONUS", "ALLOWANCE", "ADVANCE", "DEDUCTION"].includes(type)) {
      throw new BadRequestException("Loại điều chỉnh không hợp lệ.");
    }
    const amount = Math.max(0, this.toNumber(body.amount));
    if (amount <= 0) throw new BadRequestException("Số tiền điều chỉnh phải lớn hơn 0.");

    await this.prisma.payrollAdjustment.create({
      data: {
        payrollLineId: id,
        type,
        amount: this.money(amount),
        reason: body.reason || null,
        createdById: user?.id || null,
        createdByName: this.userName(user),
      },
    });

    const data: any = {};
    if (type === "BONUS") data.bonus = this.money(this.toNumber(line.bonus) + amount);
    if (type === "ALLOWANCE") data.allowance = this.money(this.toNumber(line.allowance) + amount);
    if (type === "ADVANCE") data.advance = this.money(this.toNumber(line.advance) + amount);
    if (type === "DEDUCTION") data.deduction = this.money(this.toNumber(line.deduction) + amount);

    const totals = this.calcLineTotals({ ...line, ...data });
    const updated = await this.prisma.payrollLine.update({
      where: { id },
      data: { ...data, grossPay: this.money(totals.grossPay), netPay: this.money(totals.netPay) },
      include: { adjustments: { orderBy: { createdAt: "desc" } } },
    });
    await this.recalculatePeriodTotals(line.periodId);
    return updated;
  }

  private async recalculatePeriodTotals(periodId: string) {
    const lines = await this.prisma.payrollLine.findMany({ where: { periodId } });
    const totalStaff = lines.length;
    const totalOrders = lines.reduce((s, l) => s + Number(l.successOrderCount || 0), 0);
    const totalItems = lines.reduce((s, l) => s + Number(l.successItemQty || 0), 0);
    const totalRevenue = lines.reduce((s, l) => s + this.toNumber(l.revenueAmount), 0);
    const totalHourlyAmount = lines.reduce((s, l: any) => s + this.toNumber(l.hourlyAmount), 0);
    const totalTaggedProductAmount = lines.reduce((s, l: any) => s + this.toNumber(l.taggedProductAmount), 0);
    const totalMealAllowance = lines.reduce((s, l: any) => s + this.toNumber(l.mealAllowanceAmount), 0);
    const totalInsuranceDeduction = lines.reduce((s, l: any) => s + this.toNumber(l.insuranceDeduction), 0);
    const totalGhnCodBonus = lines.reduce((s, l: any) => s + this.toNumber(l.ghnCodBonusAmount), 0);
    const totalAttendanceWarnings = lines.filter((l: any) => ["WARNING", "CRITICAL"].includes(String(l.attendanceWarningLevel || "").toUpperCase())).length;
    const totalLateMinutes = lines.reduce((s, l: any) => s + Number(l.lateMinutes || 0), 0);
    const totalEarlyMinutes = lines.reduce((s, l: any) => s + Number(l.earlyMinutes || 0), 0);
    const totalGross = lines.reduce((s, l) => s + this.toNumber(l.grossPay), 0);
    const totalNet = lines.reduce((s, l) => s + this.toNumber(l.netPay), 0);
    const totalPaid = lines.reduce((s, l) => s + this.toNumber(l.paidAmount), 0);
    await this.prisma.payrollPeriod.update({
      where: { id: periodId },
      data: {
        totalStaff,
        totalOrders,
        totalItems,
        totalRevenue: this.money(totalRevenue),
        totalHourlyAmount: this.money(totalHourlyAmount),
        totalTaggedProductAmount: this.money(totalTaggedProductAmount),
        totalMealAllowance: this.money(totalMealAllowance),
        totalInsuranceDeduction: this.money(totalInsuranceDeduction),
        totalGhnCodBonus: this.money(totalGhnCodBonus),
        totalGross: this.money(totalGross),
        totalNet: this.money(totalNet),
        totalPaid: this.money(totalPaid),
        status: totalPaid >= totalNet && totalNet > 0 ? "PAID" : totalPaid > 0 ? "PARTIALLY_PAID" : undefined,
      },
    });
  }

  async markLinePaid(id: string, body: { paymentSourceId?: string; paidAmount?: number; note?: string }, user?: AnyUser) {
    const line = await this.prisma.payrollLine.findUnique({ where: { id }, include: { period: true } });
    if (!line) throw new NotFoundException("Không tìm thấy dòng lương.");
    this.scopedBranchId(user, line.branchId || line.period.branchId || null);

    const paidAmount = Math.min(this.toNumber(body.paidAmount || line.netPay), this.toNumber(line.netPay));
    if (paidAmount <= 0) throw new BadRequestException("Số tiền trả phải lớn hơn 0.");

    let voucher: any = null;
    if (body.paymentSourceId) {
      voucher = await this.financeService.createCashVoucher({
        type: "PAYMENT",
        branchId: line.branchId || line.period.branchId || undefined,
        paymentSourceId: body.paymentSourceId,
        amount: paidAmount,
        category: "Chi lương nhân viên",
        title: `Trả lương ${line.staffName} - ${line.period.name}`,
        partnerName: line.staffName,
        note: body.note || `Trả lương kỳ ${line.period.code}`,
      }, user);
      voucher = await this.financeService.confirmCashVoucher(voucher.id, {}, user);
    }

    const updated = await this.prisma.payrollLine.update({
      where: { id },
      data: {
        status: paidAmount >= this.toNumber(line.netPay) ? "PAID" : "PARTIALLY_PAID",
        paidAmount: this.money(paidAmount),
        paidAt: new Date(),
        paidById: user?.id || null,
        paidByName: this.userName(user),
        paymentSourceId: body.paymentSourceId || null,
        paymentVoucherId: voucher?.id || null,
      },
    });
    await this.recalculatePeriodTotals(line.periodId);
    return updated;
  }

  async markPeriodPaid(id: string, body: { paymentSourceId?: string; note?: string }, user?: AnyUser) {
    const period = await this.prisma.payrollPeriod.findUnique({ where: { id }, include: { lines: true } });
    if (!period) throw new NotFoundException("Không tìm thấy kỳ lương.");
    this.scopedBranchId(user, period.branchId || null);
    const unpaidLines = period.lines.filter((line) => String(line.status || "").toUpperCase() !== "PAID");
    if (!unpaidLines.length) return period;

    const totalNet = unpaidLines.reduce((sum, line) => sum + this.toNumber(line.netPay), 0);
    if (totalNet <= 0) throw new BadRequestException("Kỳ lương không có số tiền cần trả.");

    let voucher: any = null;
    if (body.paymentSourceId) {
      voucher = await this.financeService.createCashVoucher({
        type: "PAYMENT",
        branchId: period.branchId || undefined,
        paymentSourceId: body.paymentSourceId,
        amount: totalNet,
        category: "Chi lương nhân viên",
        title: `Trả lương ${period.name}`,
        partnerName: "Nhân viên The 1970",
        note: body.note || `Trả lương kỳ ${period.code}`,
      }, user);
      voucher = await this.financeService.confirmCashVoucher(voucher.id, {}, user);
    }

    await this.prisma.payrollLine.updateMany({
      where: { periodId: id, status: { not: "PAID" } },
      data: {
        status: "PAID",
        paidAmount: this.money(0),
        paidAt: new Date(),
        paidById: user?.id || null,
        paidByName: this.userName(user),
        paymentSourceId: body.paymentSourceId || null,
        paymentVoucherId: voucher?.id || null,
      },
    });

    // updateMany không set paidAmount theo từng netPay được, nên dùng vòng update nhỏ để giữ đúng sổ lương.
    for (const line of unpaidLines) {
      await this.prisma.payrollLine.update({ where: { id: line.id }, data: { paidAmount: line.netPay } });
    }

    await this.prisma.payrollPeriod.update({
      where: { id },
      data: {
        status: "PAID",
        totalPaid: this.money(this.toNumber(period.totalNet)),
        paidAt: new Date(),
        paidById: user?.id || null,
        paidByName: this.userName(user),
        paymentSourceId: body.paymentSourceId || null,
        paymentVoucherId: voucher?.id || null,
      },
    });

    return this.getPeriod(id, user);
  }
}
