import {
  Body,
  Controller,
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

  @UseGuards(JwtGuard, PermissionGuard)
  @Get("conversations")
  @RequireAnyPermissions(
    PERMISSIONS.OMNI_MESSAGES_VIEW,
    PERMISSIONS.MENU_OMNI_MESSAGES,
  )
  listConversations(@Query() query: ListConversationsDto) {
    return this.service.listConversations(query);
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Get("conversations/:id")
  @RequireAnyPermissions(
    PERMISSIONS.OMNI_MESSAGES_VIEW,
    PERMISSIONS.MENU_OMNI_MESSAGES,
  )
  getConversation(@Param("id") id: string) {
    return this.service.getConversation(id);
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Post("conversations/:id/messages")
  @RequirePermissions(PERMISSIONS.OMNI_MESSAGES_REPLY)
  sendMessage(
    @Param("id") id: string,
    @Body() dto: SendMessageDto,
    @Request() req: any,
  ) {
    return this.service.sendMessage(id, dto, req.user);
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Patch("conversations/:id/assign")
  @RequirePermissions(PERMISSIONS.OMNI_MESSAGES_ASSIGN)
  assign(@Param("id") id: string, @Body() dto: AssignConversationDto) {
    return this.service.assignConversation(id, dto);
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Patch("conversations/:id/status")
  @RequirePermissions(PERMISSIONS.OMNI_MESSAGES_MANAGE)
  updateStatus(
    @Param("id") id: string,
    @Body() dto: UpdateConversationStatusDto,
  ) {
    return this.service.updateStatus(id, dto.status);
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Patch("conversations/:id/tags")
  @RequirePermissions(PERMISSIONS.OMNI_MESSAGES_TAGS)
  updateTags(@Param("id") id: string, @Body() dto: UpdateTagsDto) {
    return this.service.updateTags(id, dto.tags);
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Post("conversations/:id/notes")
  @RequirePermissions(PERMISSIONS.OMNI_MESSAGES_NOTES)
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
    PERMISSIONS.OMNI_MESSAGES_VIEW,
    PERMISSIONS.MENU_OMNI_MESSAGES,
  )
  markRead(@Param("id") id: string) {
    return this.service.markRead(id);
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Get("meta/connection")
  @RequireAnyPermissions(
    PERMISSIONS.OMNI_MESSAGES_VIEW,
    PERMISSIONS.MENU_OMNI_MESSAGES,
  )
  getMetaConnection() {
    return this.service.getMetaConnectionStatus();
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Post("meta/subscribe-page")
  @RequireAnyPermissions(
    PERMISSIONS.OMNI_MESSAGES_MANAGE,
    PERMISSIONS.MENU_OMNI_MESSAGES,
  )
  subscribeMetaPage() {
    return this.service.subscribeConfiguredPage();
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
