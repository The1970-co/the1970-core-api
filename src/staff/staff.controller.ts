import {
  Body,
  Controller,
  Get,
  Param,
  Patch,
  Post,
  UseGuards,
} from "@nestjs/common";
import { StaffService } from "./staff.service";
import { CreateStaffDto } from "./dto/create-staff.dto";
import { UpdateStaffStatusDto } from "./dto/update-staff-status.dto";
import { UpdateStaffPasswordDto } from "./dto/update-staff-password.dto";
import { JwtGuard } from "../auth/jwt.guard";
import { RolesGuard } from "../auth/roles.guard";
import { Roles } from "../auth/roles.decorator";

@UseGuards(JwtGuard, RolesGuard)
@Roles("owner", "admin")
@Controller("staff")
export class StaffController {
  constructor(private readonly staffService: StaffService) {}

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

  @Patch(":id/permissions")
  async updatePermissions(@Param("id") id: string, @Body() dto: any) {
    return this.staffService.updatePermissions(id, dto);
  }



  @Patch(":id/branch-roles")
  async updateBranchRoles(@Param("id") id: string, @Body() dto: any) {
    return this.staffService.updateBranchRoles(id, dto);
  }

  @Patch(":id/status")
  async updateStatus(
    @Param("id") id: string,
    @Body() dto: UpdateStaffStatusDto
  ) {
    return this.staffService.updateStatus(id, dto);
  }

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
}