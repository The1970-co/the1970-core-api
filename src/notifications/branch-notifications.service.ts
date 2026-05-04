import { Injectable, BadRequestException } from "@nestjs/common";
import { BranchNotificationType, Prisma } from "@prisma/client";
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
    const now = new Date();

    const branchId = String(dto.branchId || "").trim();
    const transferCode = String(dto.transferCode || "").trim();
    const notificationId = String(dto.notificationId || "").trim();

    // CASE 1: FE gửi đủ branchId + transferCode.
    // Mark toàn bộ notification cùng phiếu ở chi nhánh đó.
    if (branchId && transferCode) {
      const result = await this.prisma.branchNotification.updateMany({
        where: {
          branchId,
          transferCode,
          isRead: false,
        },
        data: {
          isRead: true,
          readAt: now,
        },
      });

      return {
        success: true,
        mode: "BRANCH_TRANSFER_CODE",
        branchId,
        transferCode,
        updatedCount: result.count,
      };
    }

    // CASE 2: FE chỉ gửi notificationId.
    // HARD FIX: tự tìm notification, nếu có transferCode thì cũng mark toàn bộ notification cùng phiếu.
    // Đây là chỗ chặn vòng lặp refresh/poll kéo lại thông báo cũ.
    if (notificationId) {
      const notification = await this.prisma.branchNotification.findUnique({
        where: { id: notificationId },
      });

      if (!notification) {
        return {
          success: true,
          mode: "NOT_FOUND_BUT_IGNORED",
          updatedCount: 0,
        };
      }

      if (notification.branchId && notification.transferCode) {
        const result = await this.prisma.branchNotification.updateMany({
          where: {
            branchId: notification.branchId,
            transferCode: notification.transferCode,
            isRead: false,
          },
          data: {
            isRead: true,
            readAt: now,
          },
        });

        return {
          success: true,
          mode: "NOTIFICATION_ID_RESOLVED_TO_TRANSFER_CODE",
          branchId: notification.branchId,
          transferCode: notification.transferCode,
          updatedCount: result.count,
        };
      }

      const row = await this.prisma.branchNotification.update({
        where: { id: notificationId },
        data: {
          isRead: true,
          readAt: now,
        },
      });

      return {
        success: true,
        mode: "NOTIFICATION_ID_ONLY",
        branchId: row.branchId,
        transferCode: row.transferCode,
        updatedCount: 1,
      };
    }

    // CASE 3: FE gửi branchId không có transferCode.
    // Mark toàn bộ unread của branch.
    if (branchId) {
      const result = await this.prisma.branchNotification.updateMany({
        where: {
          branchId,
          isRead: false,
        },
        data: {
          isRead: true,
          readAt: now,
        },
      });

      return {
        success: true,
        mode: "BRANCH_ALL",
        branchId,
        updatedCount: result.count,
      };
    }

    throw new BadRequestException("Thiếu notificationId hoặc branchId");
  }
}
