import { Injectable } from "@nestjs/common";
import { PrismaService } from "../../prisma/prisma.service";

@Injectable()
export class MobileBranchesService {
  constructor(private prisma: PrismaService) {}

  async getBranches() {
    const branches = await this.prisma.branch.findMany({
      where: { isActive: true },
      select: {
        id: true,
        name: true,
      },
      orderBy: {
        name: "asc",
      },
    });

    return [
      { id: "all", name: "Tất cả chi nhánh" },
      ...branches,
    ];
  }
}