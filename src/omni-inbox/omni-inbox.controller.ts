import {
  Body,
  Controller,
  Delete,
  ForbiddenException,
  Get,
  Param,
  Patch,
  Post,
  Query,
  Request,
  Sse,
  UseGuards,
} from "@nestjs/common";
import { JwtGuard } from "../auth/jwt.guard";
import { PermissionGuard } from "../auth/guards/permission.guard";
import {
  RequireAnyPermissions,
  RequirePermissions,
} from "../auth/decorators/require-permissions.decorator";
import { PERMISSIONS } from "../auth/constants/permissions";
import { OmniInboxService } from "./omni-inbox.service";
import { OmniInboxRealtimeService } from "./omni-inbox.realtime";
import { ListConversationsDto } from "./dto/list-conversations.dto";
import {
  AssignConversationDto,
  CreateNoteDto,
  CreateNoteTemplateDto,
  UpdateNoteTemplateDto,
  CreateQuickOrderDto,
  SendMessageDto,
  UpdateConversationStatusDto,
  UpdateTagsDto,
} from "./dto/omni-inbox-actions.dto";

@Controller("omni-inbox")
export class OmniInboxController {
  constructor(
    private readonly service: OmniInboxService,
    private readonly realtime: OmniInboxRealtimeService,
  ) {}

  private assertAdmin(user?: any) {
    const roles = [
      user?.role,
      user?.roleName,
      user?.activeRole,
      ...(Array.isArray(user?.roles) ? user.roles : []),
    ]
      .map((value) => String(value || "").trim().toUpperCase())
      .filter(Boolean);

    if (!roles.includes("OWNER") && !roles.includes("ADMIN")) {
      throw new ForbiddenException(
        "Chỉ Admin hoặc Owner được thay đổi cài đặt ghi chú.",
      );
    }
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Get("conversations")
  @RequireAnyPermissions(
    "omni_inbox.view",
    PERMISSIONS.MENU_OMNI_MESSAGES,
  )
  listConversations(@Query() query: ListConversationsDto) {
    return this.service.listConversations(query);
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Get("conversations/:id")
  @RequireAnyPermissions(
    "omni_inbox.view",
    PERMISSIONS.MENU_OMNI_MESSAGES,
  )
  getConversation(@Param("id") id: string) {
    return this.service.getConversation(id);
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Post("conversations/:id/messages")
  @RequirePermissions("omni_inbox.reply")
  sendMessage(
    @Param("id") id: string,
    @Body() dto: SendMessageDto,
    @Request() req: any,
  ) {
    return this.service.sendMessage(id, dto, req.user);
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Patch("conversations/:id/assign")
  @RequirePermissions("omni_inbox.assign")
  assign(@Param("id") id: string, @Body() dto: AssignConversationDto) {
    return this.service.assignConversation(id, dto);
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Patch("conversations/:id/status")
  @RequirePermissions("omni_inbox.close")
  updateStatus(
    @Param("id") id: string,
    @Body() dto: UpdateConversationStatusDto,
  ) {
    return this.service.updateStatus(id, dto.status);
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Patch("conversations/:id/tags")
  @RequirePermissions("omni_inbox.tags.manage")
  updateTags(@Param("id") id: string, @Body() dto: UpdateTagsDto) {
    return this.service.updateTags(id, dto.tags);
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Post("conversations/:id/notes")
  @RequirePermissions("omni_inbox.notes.manage")
  createNote(
    @Param("id") id: string,
    @Body() dto: CreateNoteDto,
    @Request() req: any,
  ) {
    return this.service.createNote(id, dto, req.user);
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Patch("conversations/:id/read")
  @RequireAnyPermissions(
    "omni_inbox.view",
    PERMISSIONS.MENU_OMNI_MESSAGES,
  )
  markRead(@Param("id") id: string) {
    return this.service.markRead(id);
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Post("conversations/:id/refresh-profile")
  @RequireAnyPermissions(
    "omni_inbox.view",
    PERMISSIONS.MENU_OMNI_MESSAGES,
  )
  refreshConversationProfile(@Param("id") id: string) {
    return this.service.refreshConversationProfile(id);
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Post("customers/refresh-profiles")
  @RequireAnyPermissions(
    "omni_inbox.view",
    PERMISSIONS.MENU_OMNI_MESSAGES,
  )
  refreshMissingCustomerProfiles(@Query("limit") limit?: string) {
    return this.service.refreshMissingCustomerProfiles(Number(limit || 50));
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Get("meta/connection")
  @RequireAnyPermissions(
    "omni_inbox.view",
    PERMISSIONS.MENU_OMNI_MESSAGES,
  )
  getMetaConnection() {
    return this.service.getMetaConnectionStatus();
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Post("meta/subscribe-page")
  @RequireAnyPermissions(
    "omni_inbox.close",
    PERMISSIONS.MENU_OMNI_MESSAGES,
  )
  subscribeMetaPage() {
    return this.service.subscribeConfiguredPage();
  }


  @UseGuards(JwtGuard, PermissionGuard)
  @Get("note-templates")
  @RequireAnyPermissions("omni_inbox.notes.manage", "omni_inbox.settings")
  listNoteTemplates(@Query("includeInactive") includeInactive?: string) {
    return this.service.listNoteTemplates(["1", "true", "yes"].includes(String(includeInactive || "").toLowerCase()));
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Post("note-templates")
  @RequirePermissions("omni_inbox.settings")
  createNoteTemplate(@Body() dto: CreateNoteTemplateDto, @Request() req: any) {
    this.assertAdmin(req.user);
    return this.service.createNoteTemplate(dto, req.user);
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Patch("note-templates/:id")
  @RequirePermissions("omni_inbox.settings")
  updateNoteTemplate(
    @Param("id") id: string,
    @Body() dto: UpdateNoteTemplateDto,
    @Request() req: any,
  ) {
    this.assertAdmin(req.user);
    return this.service.updateNoteTemplate(id, dto);
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Delete("note-templates/:id")
  @RequirePermissions("omni_inbox.settings")
  deleteNoteTemplate(@Param("id") id: string, @Request() req: any) {
    this.assertAdmin(req.user);
    return this.service.deleteNoteTemplate(id);
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Post("conversations/:id/quick-orders")
  @RequireAnyPermissions("omni_inbox.create_order", PERMISSIONS.ORDERS_CREATE)
  createQuickOrder(@Param("id") id: string, @Body() dto: CreateQuickOrderDto, @Request() req: any) {
    return this.service.createQuickOrder(id, dto, req.user);
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Post("conversations/:id/quick-orders/:orderId/cancel")
  @RequireAnyPermissions("omni_inbox.create_order", PERMISSIONS.ORDERS_CANCEL)
  cancelQuickOrder(@Param("id") id: string, @Param("orderId") orderId: string, @Request() req: any) {
    return this.service.cancelQuickOrder(id, orderId, req.user);
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Delete("conversations/:id/quick-orders/:orderId")
  @RequireAnyPermissions("omni_inbox.create_order", PERMISSIONS.ORDERS_DELETE)
  deleteQuickOrder(@Param("id") id: string, @Param("orderId") orderId: string, @Request() req: any) {
    return this.service.deleteQuickOrder(id, orderId, req.user);
  }

  /**
   * 1 kết nối SSE duy nhất cho cả màn Inbox.
   * Không poll liên tục → giảm request và egress Railway.
   */
  @Sse("events")
  events() {
    return this.realtime.stream();
  }
}
