import { Injectable, BadRequestException } from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";
import { CreateStaffDto } from "./dto/create-staff.dto";
import { UpdateStaffStatusDto } from "./dto/update-staff-status.dto";
import * as bcrypt from "bcrypt";

@Injectable()
export class StaffService {
  constructor(private readonly prisma: PrismaService) {}

  async create(dto: CreateStaffDto) {
    if (!dto.password || dto.password.length < 4) {
      throw new BadRequestException("Mật khẩu tối thiểu 4 ký tự");
    }

    if (!dto.code?.trim()) {
      throw new BadRequestException("Thiếu mã nhân viên");
    }

    if (!dto.name?.trim()) {
      throw new BadRequestException("Thiếu tên nhân viên");
    }

    if (!dto.role?.trim()) {
      throw new BadRequestException("Thiếu role");
    }

    const existing = await this.prisma.staffUser.findUnique({
      where: { code: dto.code.trim() },
    });

    if (existing) {
      throw new BadRequestException("Mã nhân viên đã tồn tại");
    }

    const hash = await bcrypt.hash(dto.password, 10);

    let branchId: string | null = null;
    let branchName: string | null = null;

    if (dto.branchId) {
      const branch = await this.prisma.branch.findUnique({
        where: { id: String(dto.branchId).trim() },
        select: {
          id: true,
          name: true,
        },
      });

      if (!branch) {
        throw new BadRequestException("Chi nhánh không tồn tại");
      }

      branchId = branch.id;
      branchName = branch.name;
    }

    return this.prisma.staffUser.create({
      data: {
        code: dto.code.trim(),
        name: dto.name.trim(),
        role: dto.role,
        branchId,
        branchName,
        passwordHash: hash,
        isActive: true,
      },
    });
  }

  async findAll() {
    return this.prisma.staffUser.findMany({
      orderBy: [{ createdAt: "desc" }],
      select: {
        id: true,
        code: true,
        name: true,
        role: true,
        branchId: true,
        branchName: true,
        isActive: true,
        createdAt: true,
        lastLoginAt: true,
      },
    });
  }

  async updateStatus(id: string, dto: UpdateStaffStatusDto) {
    return this.prisma.staffUser.update({
      where: { id },
      data: {
        isActive: dto.status === "ACTIVE",
      },
    });
  }

  async updatePassword(id: string, newPassword: string) {
    if (!newPassword || newPassword.length < 4) {
      throw new BadRequestException("Mật khẩu tối thiểu 4 ký tự");
    }

    const hash = await bcrypt.hash(newPassword, 10);

    return this.prisma.staffUser.update({
      where: { id },
      data: {
        passwordHash: hash,
      },
    });
  }

  async update(id: string, dto: any) {
    if (!dto.role?.trim()) {
      throw new BadRequestException("Thiếu role");
    }

    let branchId: string | null = null;
    let branchName: string | null = null;

    if (dto.branchId) {
      const branch = await this.prisma.branch.findUnique({
        where: { id: String(dto.branchId).trim() },
        select: {
          id: true,
          name: true,
        },
      });

      if (!branch) {
        throw new BadRequestException("Chi nhánh không tồn tại");
      }

      branchId = branch.id;
      branchName = branch.name;
    }

    return this.prisma.staffUser.update({
      where: { id },
      data: {
        role: dto.role,
        branchId,
        branchName,
      },
    });
  }
}