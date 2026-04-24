import {
  Body,
  Controller,
  Get,
  Post,
  Patch,
  Param,
  UseGuards,
  BadRequestException,
} from "@nestjs/common";
import { JwtGuard } from "../auth/jwt.guard";
import { PrismaService } from "../prisma/prisma.service";

@Controller("branches")
@UseGuards(JwtGuard)
export class BranchesController {
  constructor(private prisma: PrismaService) {}

  // ======================
  // GET: danh sách kho
  // ======================
  @Get()
  async findAll() {
    return this.prisma.branch.findMany({
      where: { isActive: true },
      orderBy: { createdAt: "asc" },
      select: {
        id: true,
        name: true,
        address: true,
        isActive: true,
      },
    });
  }

  // ======================
  // POST: tạo kho mới
  // ======================
  @Post()
  async create(
    @Body()
    body: {
      id: string;
      name: string;
      address?: string;
    }
  ) {
    const id = String(body.id || "").trim();
    const name = String(body.name || "").trim();

    if (!id) throw new BadRequestException("Thiếu mã kho");
    if (!name) throw new BadRequestException("Thiếu tên kho");

    const existing = await this.prisma.branch.findUnique({
      where: { id },
    });

    if (existing) {
      throw new BadRequestException("Kho đã tồn tại");
    }

    return this.prisma.branch.create({
      data: {
        id,
        name,
        address: body.address || null,
        isActive: true,
      },
    });
  }

  // ======================
  // PATCH: update kho + cascade mã kho
  // ======================
  @Patch(":id")
  async updateBranch(
    @Param("id") oldId: string,
    @Body()
    body: {
      newId?: string;
      name?: string;
      address?: string;
    }
  ) {
    const oldBranchId = String(oldId || "").trim();
    const newBranchId = String(body.newId || oldId || "").trim();
    const name = String(body.name || "").trim();
    const address =
      body.address === undefined ? undefined : String(body.address || "").trim();

    if (!oldBranchId) {
      throw new BadRequestException("Thiếu mã kho hiện tại");
    }

    if (!newBranchId) {
      throw new BadRequestException("Thiếu mã kho mới");
    }

    if (!name) {
      throw new BadRequestException("Thiếu tên kho");
    }

    const existingBranch = await this.prisma.branch.findUnique({
      where: { id: oldBranchId },
    });

    if (!existingBranch) {
      throw new BadRequestException("Kho không tồn tại");
    }

    if (newBranchId !== oldBranchId) {
      const duplicated = await this.prisma.branch.findUnique({
        where: { id: newBranchId },
      });

      if (duplicated) {
        throw new BadRequestException("Mã kho mới đã tồn tại");
      }
    }

    return this.prisma.$transaction(async (tx) => {
      if (newBranchId !== oldBranchId) {
        await tx.staffUser.updateMany({
          where: { branchId: oldBranchId },
          data: {
            branchId: newBranchId,
            branchName: name,
          },
        });

        await tx.order.updateMany({
          where: { branchId: oldBranchId },
          data: { branchId: newBranchId },
        });

        await tx.inventoryItem.updateMany({
          where: { branchId: oldBranchId },
          data: { branchId: newBranchId },
        });

        await tx.inventoryMovement.updateMany({
          where: { branchId: oldBranchId },
          data: { branchId: newBranchId },
        });
      } else {
        await tx.staffUser.updateMany({
          where: { branchId: oldBranchId },
          data: {
            branchName: name,
          },
        });
      }

      const updated = await tx.branch.update({
        where: { id: oldBranchId },
        data: {
          id: newBranchId,
          name,
          address: address === undefined ? existingBranch.address : address || null,
        },
      });

      return updated;
    });
  }

  // ======================
  // PATCH: tắt kho
  // ======================
  @Patch(":id/deactivate")
  async deactivate(@Param("id") id: string) {
    return this.prisma.branch.update({
      where: { id },
      data: { isActive: false },
    });
  }
}