import { Body, Controller, Get, Param, Patch, Post, Query, Req, UseGuards } from "@nestjs/common";
import type { Request } from "express";
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

  @Post("periods/:id/calculate")
  @RequirePermissions("payroll.calculate")
  calculate(@Param("id") id: string, @Body() body: any, @Req() req: Request & { user?: any }) {
    return this.payrollService.calculatePeriod(id, body || {}, req.user);
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
