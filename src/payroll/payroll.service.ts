import { BadRequestException, ForbiddenException, Injectable, NotFoundException } from "@nestjs/common";
import { OrderStatus, PaymentStatus, Prisma } from "@prisma/client";
import { PrismaService } from "../prisma/prisma.service";
import { FinanceService } from "../finance/finance.service";
import { CreatePayrollPeriodDto } from "./dto/create-payroll-period.dto";
import { PayrollConfigDto } from "./dto/payroll-config.dto";
import { PayrollAdjustmentDto } from "./dto/payroll-adjustment.dto";
import { PayrollFilterDto } from "./dto/payroll-filter.dto";

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
      let totalGross = 0;
      let totalNet = 0;

      for (const config of configs) {
        const staff = staffMap.get(config.staffId);
        if (!staff) continue;

        const lineBranchId = period.branchId || config.branchId || staff.branchId || null;
        const lineBranchName = period.branchName || config.branchName || staff.branchName || null;
        const workingDays = this.toNumber(body.workingDaysByStaff?.[config.staffId] ?? config.standardWorkingDays ?? 26);
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

        const normalHours = 0;
        const overtimeHours = 0;
        const holidayHours = 0;
        const overtimeRate = this.toNumber((config as any).overtimeRate || 1);
        const holidayRate = this.toNumber((config as any).holidayRate || 2);
        const hourlyRate = this.toNumber((config as any).hourlyRate || 0);
        const convertedWorkingHours = normalHours + overtimeHours * overtimeRate + holidayHours * holidayRate;
        const hourlyAmount = (config as any).hourlyEnabled ? convertedWorkingHours * hourlyRate : 0;

        const paidLeaveDays = 0;
        const paidLeaveHoursPerDay = this.toNumber((config as any).paidLeaveHoursPerDay || (config as any).standardHoursPerDay || 9.5);
        const paidLeaveAmount = (config as any).paidLeaveEnabled ? paidLeaveDays * paidLeaveHoursPerDay * hourlyRate : 0;

        const rawWorkingHours = normalHours + overtimeHours + holidayHours;
        const mealAllowanceAmount = (config as any).mealAllowanceEnabled
          ? (rawWorkingHours / Math.max(1, this.toNumber((config as any).mealHoursPerUnit || 9.5))) * this.toNumber((config as any).mealAmountPerUnit || 0)
          : 0;

        const insuranceDeduction = this.toNumber((config as any).insuranceDeductionAmount || 0);
        const taggedProductQty = 0;
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
          totalGross: this.money(totalGross),
          totalNet: this.money(totalNet),
          totalPaid: this.money(0),
        },
      });
    }, { maxWait: 10000, timeout: 60000 });

    return this.getPeriod(id, user);
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
