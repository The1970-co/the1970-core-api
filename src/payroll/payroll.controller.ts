import { Body, Controller, Get, Param, Patch, Post, Query, Req, Res, UploadedFile, UseGuards, UseInterceptors } from "@nestjs/common";
import type { Request, Response } from "express";
import { FileInterceptor } from "@nestjs/platform-express";
import { JwtGuard } from "../auth/jwt.guard";
import { PermissionGuard } from "../auth/guards/permission.guard";
import { RequirePermissions } from "../auth/decorators/require-permissions.decorator";
import { RolesGuard } from "../auth/roles.guard";
import { Roles } from "../auth/roles.decorator";
import { PayrollService } from "./payroll.service";
import { CreatePayrollPeriodDto } from "./dto/create-payroll-period.dto";
import { PayrollConfigDto } from "./dto/payroll-config.dto";
import { PayrollAdjustmentDto } from "./dto/payroll-adjustment.dto";
import { PayrollFilterDto } from "./dto/payroll-filter.dto";

@UseGuards(JwtGuard, RolesGuard, PermissionGuard)
@Roles("owner", "admin")
@Controller("payroll")
export class PayrollController {
  constructor(private readonly payrollService: PayrollService) {}

  @Get("dashboard")
  @RequirePermissions("payroll.view")
  getDashboard(@Query() query: PayrollFilterDto, @Req() req: Request & { user?: any }) {
    return this.payrollService.getDashboard(query || {}, req.user);
  }

  @Get("settings")
  @RequirePermissions("payroll.config")
  getSettings(@Req() req: Request & { user?: any }) {
    return this.payrollService.getSettings(req.user);
  }

  @Patch("settings")
  @RequirePermissions("payroll.config")
  updateSettings(@Body() body: any, @Req() req: Request & { user?: any }) {
    return this.payrollService.updateSettings(body || {}, req.user);
  }

  @Get("periods")
  @RequirePermissions("payroll.view")
  listPeriods(@Query() query: PayrollFilterDto, @Req() req: Request & { user?: any }) {
    return this.payrollService.listPeriods(query, req.user);
  }

  @Post("periods")
  @RequirePermissions("payroll.create")
  createPeriod(@Body() body: CreatePayrollPeriodDto, @Req() req: Request & { user?: any }) {
    return this.payrollService.createPeriod(body, req.user);
  }

  @Get("periods/:id")
  @RequirePermissions("payroll.view")
  getPeriod(@Param("id") id: string, @Req() req: Request & { user?: any }) {
    return this.payrollService.getPeriod(id, req.user);
  }

  @Get("periods/:id/export")
  @RequirePermissions("payroll.export")
  async exportPeriod(@Param("id") id: string, @Req() req: Request & { user?: any }, @Res() res: Response) {
    const csv = await this.payrollService.exportPeriodCsv(id, req.user);
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="payroll-${id}.csv"`);
    return res.send(csv);
  }

  @Post("periods/:id/calculate")
  @RequirePermissions("payroll.calculate")
  calculate(@Param("id") id: string, @Body() body: any, @Req() req: Request & { user?: any }) {
    return this.payrollService.calculatePeriod(id, body || {}, req.user);
  }

  @Post("periods/:id/import-attendance/preview")
  @RequirePermissions("payroll.edit")
  @UseInterceptors(FileInterceptor("file"))
  previewAttendance(@Param("id") id: string, @UploadedFile() file: any, @Req() req: Request & { user?: any }) {
    if (!file?.buffer) throw new Error("Thiếu file chấm công.");
    return this.payrollService.previewAttendanceImport(id, file.buffer, file.originalname || "attendance.xlsm", req.user);
  }

  @Post("periods/:id/import-attendance/apply")
  @RequirePermissions("payroll.edit")
  applyAttendance(@Param("id") id: string, @Body() body: any, @Req() req: Request & { user?: any }) {
    return this.payrollService.applyAttendanceImport(id, body || {}, req.user);
  }

  @Post("periods/:id/lock")
  @RequirePermissions("payroll.lock")
  lock(@Param("id") id: string, @Req() req: Request & { user?: any }) {
    return this.payrollService.lockPeriod(id, req.user);
  }

  @Post("periods/:id/unlock")
  @RequirePermissions("payroll.lock")
  unlock(@Param("id") id: string, @Req() req: Request & { user?: any }) {
    return this.payrollService.unlockPeriod(id, req.user);
  }

  @Post("periods/:id/mark-paid")
  @RequirePermissions("payroll.mark_paid")
  markPeriodPaid(@Param("id") id: string, @Body() body: any, @Req() req: Request & { user?: any }) {
    return this.payrollService.markPeriodPaid(id, body || {}, req.user);
  }

  @Patch("lines/:id")
  @RequirePermissions("payroll.edit")
  updateLine(@Param("id") id: string, @Body() body: any, @Req() req: Request & { user?: any }) {
    return this.payrollService.updateLine(id, body || {}, req.user);
  }

  @Post("lines/:id/adjustments")
  @RequirePermissions("payroll.edit")
  addAdjustment(@Param("id") id: string, @Body() body: PayrollAdjustmentDto, @Req() req: Request & { user?: any }) {
    return this.payrollService.addAdjustment(id, body, req.user);
  }

  @Post("lines/:id/mark-paid")
  @RequirePermissions("payroll.mark_paid")
  markLinePaid(@Param("id") id: string, @Body() body: any, @Req() req: Request & { user?: any }) {
    return this.payrollService.markLinePaid(id, body || {}, req.user);
  }

  @Get("configs")
  @RequirePermissions("payroll.config")
  listConfigs(@Query() query: any, @Req() req: Request & { user?: any }) {
    return this.payrollService.listConfigs(query || {}, req.user);
  }

  @Post("configs")
  @RequirePermissions("payroll.config")
  createConfig(@Body() body: PayrollConfigDto, @Req() req: Request & { user?: any }) {
    return this.payrollService.createConfig(body, req.user);
  }

  @Patch("configs/:id")
  @RequirePermissions("payroll.config")
  updateConfig(@Param("id") id: string, @Body() body: PayrollConfigDto, @Req() req: Request & { user?: any }) {
    return this.payrollService.updateConfig(id, body, req.user);
  }
}
