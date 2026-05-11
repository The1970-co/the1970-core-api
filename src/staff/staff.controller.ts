import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  Patch,
  Post,
  Request,
  UseGuards,
} from "@nestjs/common";
import { StaffService } from "./staff.service";
import { CreateStaffDto } from "./dto/create-staff.dto";
import { UpdateStaffStatusDto } from "./dto/update-staff-status.dto";
import { UpdateStaffPasswordDto } from "./dto/update-staff-password.dto";
import { JwtGuard } from "../auth/jwt.guard";
import { RolesGuard } from "../auth/roles.guard";
import { Roles } from "../auth/roles.decorator";

// ─── Admin-only controller ─────────────────────────────────────────────────────

@UseGuards(JwtGuard, RolesGuard)
@Roles("owner", "admin")
@Controller("staff")
export class StaffController {
  constructor(private readonly staffService: StaffService) {}

  // ─── Staff CRUD ──────────────────────────────────────────────────────────

  @Get()
  async findAll() {
    return this.staffService.findAll();
  }

  @Post()
  async create(@Body() dto: CreateStaffDto) {
    return this.staffService.create(dto);
  }

  @Patch(":id")
  async update(@Param("id") id: string, @Body() dto: any) {
    return this.staffService.update(id, dto);
  }

  @Delete(":id")
  async deleteStaff(@Param("id") id: string) {
    return this.staffService.deleteStaff(id);
  }

  @Patch(":id/status")
  async updateStatus(
    @Param("id") id: string,
    @Body() dto: UpdateStaffStatusDto
  ) {
    return this.staffService.updateStatus(id, dto);
  }

  // ─── Permissions & Roles ─────────────────────────────────────────────────

  @Get("role-templates")
  async getRoleTemplates() {
    return this.staffService.getRoleTemplates();
  }

  @Patch("role-templates")
  async saveRoleTemplates(@Body() dto: any) {
    return this.staffService.saveRoleTemplates(dto);
  }

  @Patch(":id/permissions")
  async updatePermissions(@Param("id") id: string, @Body() dto: any) {
    return this.staffService.updatePermissions(id, dto);
  }

  @Patch(":id/branch-roles")
  async updateBranchRoles(@Param("id") id: string, @Body() dto: any) {
    return this.staffService.updateBranchRoles(id, dto);
  }

  @Post("sync-permissions")
  async syncPermissions(@Body() dto: { force?: boolean }) {
    return this.staffService.syncAllPermissionsFromRoleTemplates({
      force: dto?.force !== false,
    });
  }

  @Post(":id/sync-permissions")
  async syncStaffPermissions(
    @Param("id") id: string,
    @Body() dto: { force?: boolean }
  ) {
    return this.staffService.syncPermissionsForStaff(id, {
      force: dto?.force !== false,
    });
  }

  // ─── Security (admin reset) ───────────────────────────────────────────────

  @Patch(":id/password")
  async updatePassword(
    @Param("id") id: string,
    @Body() dto: UpdateStaffPasswordDto
  ) {
    return this.staffService.updatePassword(id, dto.password);
  }

  @Patch(":id/second-password")
  async updateSecondPassword(
    @Param("id") id: string,
    @Body() body: { secondPassword: string }
  ) {
    return this.staffService.updateSecondPassword(id, body.secondPassword);
  }

  // ─── Departments ──────────────────────────────────────────────────────────

  @Get("departments")
  async getDepartments() {
    return this.staffService.getDepartments();
  }

  @Post("departments")
  async createDepartment(@Body() dto: any) {
    return this.staffService.createDepartment(dto);
  }

  @Patch("departments/:id")
  async updateDepartment(@Param("id") id: string, @Body() dto: any) {
    return this.staffService.updateDepartment(id, dto);
  }

  @Delete("departments/:id")
  async deleteDepartment(@Param("id") id: string) {
    return this.staffService.deleteDepartment(id);
  }

  @Patch(":id/departments")
  async updateStaffDepartments(
    @Param("id") id: string,
    @Body() body: { departmentIds: string[]; headOfDepartmentId?: string }
  ) {
    return this.staffService.updateStaffDepartments(id, body);
  }
}

// ─── Self-change controller (nhân viên tự đổi mật khẩu/PIN của mình) ─────────

@UseGuards(JwtGuard)
@Controller("staff/me")
export class StaffMeController {
  constructor(private readonly staffService: StaffService) {}

  @Patch("password")
  async changeMyPassword(
    @Request() req: any,
    @Body() body: { currentPassword: string; newPassword: string }
  ) {
    return this.staffService.changeOwnPassword(
      req.user?.id || req.user?.sub,
      body.currentPassword,
      body.newPassword
    );
  }

  @Patch("security-pin")
  async changeMyPin(
    @Request() req: any,
    @Body() body: { currentPassword: string; newPin: string }
  ) {
    return this.staffService.changeOwnSecurityPin(
      req.user?.id || req.user?.sub,
      body.currentPassword,
      body.newPin
    );
  }
}
