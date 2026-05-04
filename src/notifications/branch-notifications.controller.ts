import { Body, Controller, Get, Patch, Query } from "@nestjs/common";
import { BranchNotificationsService } from "./branch-notifications.service";
import { ListBranchNotificationsDto } from "./dto/list-branch-notifications.dto";
import { MarkNotificationReadDto } from "./dto/mark-notification-read.dto";

@Controller("branch-notifications")
export class BranchNotificationsController {
  constructor(
    private readonly branchNotificationsService: BranchNotificationsService
  ) {}

  @Get()
  async list(@Query() query: ListBranchNotificationsDto) {
    return this.branchNotificationsService.list(query);
  }

  @Get("unread-count")
  async unreadCount(@Query("branchId") branchId: string) {
    return this.branchNotificationsService.unreadCount(branchId);
  }

  @Patch("mark-read")
  async markRead(@Body() body: MarkNotificationReadDto) {
    return this.branchNotificationsService.markRead(body);
  }
}
