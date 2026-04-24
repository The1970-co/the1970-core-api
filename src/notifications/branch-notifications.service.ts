import { Injectable, BadRequestException } from "@nestjs/common";
import {
  BranchNotificationType,
  Prisma,
} from "@prisma/client";
import { PrismaService } from "../prisma/prisma.service";
import { ListBranchNotificationsDto } from "./dto/list-branch-notifications.dto";
import { MarkNotificationReadDto } from "./dto/mark-notification-read.dto";

@Injectable()
export class BranchNotificationsService {
  constructor(private readonly prisma: PrismaService) {}

  async createNotification(input: {
    branchId: string;
    branchName?: string;
    title: string;
    message: string;
    type: BranchNotificationType;
    transferId?: string;
    transferCode?: string;
  }) {
    return this.prisma.branchNotification.create({
      data: {
        branchId: input.branchId,
        branchName: input.branchName,
        title: input.title,
        message: input.message,
        type: input.type,
        transferId: input.transferId,
        transferCode: input.transferCode,
      },
    });
  }

  async list(dto: ListBranchNotificationsDto) {
    const where: Prisma.BranchNotificationWhereInput = {
      branchId: dto.branchId,
    };

    if (dto.unreadOnly === "true") {
      where.isRead = false;
    }

    return this.prisma.branchNotification.findMany({
      where,
      orderBy: { createdAt: "desc" },
      take: 30,
    });
  }

  async unreadCount(branchId: string) {
    const count = await this.prisma.branchNotification.count({
      where: {
        branchId,
        isRead: false,
      },
    });

    return { branchId, unread: count };
  }

  async markRead(dto: MarkNotificationReadDto) {
    if (dto.notificationId) {
      return this.prisma.branchNotification.update({
        where: { id: dto.notificationId },
        data: {
          isRead: true,
          readAt: new Date(),
        },
      });
    }

    if (dto.branchId) {
      const result = await this.prisma.branchNotification.updateMany({
        where: {
          branchId: dto.branchId,
          isRead: false,
        },
        data: {
          isRead: true,
          readAt: new Date(),
        },
      });

      return {
        success: true,
        updatedCount: result.count,
      };
    }

    throw new BadRequestException("Thiếu notificationId hoặc branchId");
  }
}