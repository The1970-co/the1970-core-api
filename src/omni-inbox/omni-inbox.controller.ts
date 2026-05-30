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
import { RequireAnyPermissions, RequirePermissions } from "../auth/decorators/require-permissions.decorator";
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

@UseGuards(JwtGuard, PermissionGuard)
@Controller("omni-inbox")
export class OmniInboxController {
  constructor(
    private readonly service: OmniInboxService,
    private readonly realtime: OmniInboxRealtimeService,
  ) {}

  @Get("conversations")
  @RequireAnyPermissions(PERMISSIONS.OMNI_MESSAGES_VIEW, PERMISSIONS.MENU_OMNI_MESSAGES)
  listConversations(@Query() query: ListConversationsDto) {
    return this.service.listConversations(query);
  }

  @Get("conversations/:id")
  @RequireAnyPermissions(PERMISSIONS.OMNI_MESSAGES_VIEW, PERMISSIONS.MENU_OMNI_MESSAGES)
  getConversation(@Param("id") id: string) {
    return this.service.getConversation(id);
  }

  @Post("conversations/:id/messages")
  @RequirePermissions(PERMISSIONS.OMNI_MESSAGES_REPLY)
  sendMessage(@Param("id") id: string, @Body() dto: SendMessageDto, @Request() req: any) {
    return this.service.sendMessage(id, dto, req.user);
  }

  @Patch("conversations/:id/assign")
  @RequirePermissions(PERMISSIONS.OMNI_MESSAGES_ASSIGN)
  assign(@Param("id") id: string, @Body() dto: AssignConversationDto) {
    return this.service.assignConversation(id, dto);
  }

  @Patch("conversations/:id/status")
  @RequirePermissions(PERMISSIONS.OMNI_MESSAGES_MANAGE)
  updateStatus(@Param("id") id: string, @Body() dto: UpdateConversationStatusDto) {
    return this.service.updateStatus(id, dto.status);
  }

  @Patch("conversations/:id/tags")
  @RequirePermissions(PERMISSIONS.OMNI_MESSAGES_TAGS)
  updateTags(@Param("id") id: string, @Body() dto: UpdateTagsDto) {
    return this.service.updateTags(id, dto.tags);
  }

  @Post("conversations/:id/notes")
  @RequirePermissions(PERMISSIONS.OMNI_MESSAGES_NOTES)
  createNote(@Param("id") id: string, @Body() dto: CreateNoteDto, @Request() req: any) {
    return this.service.createNote(id, dto, req.user);
  }

  @Patch("conversations/:id/read")
  @RequireAnyPermissions(PERMISSIONS.OMNI_MESSAGES_VIEW, PERMISSIONS.MENU_OMNI_MESSAGES)
  markRead(@Param("id") id: string) {
    return this.service.markRead(id);
  }

  /**
   * 1 kết nối SSE duy nhất cho cả màn Inbox.
   * Không poll liên tục → giảm request và egress Railway.
   */
  @Sse("events")
  @RequireAnyPermissions(PERMISSIONS.OMNI_MESSAGES_VIEW, PERMISSIONS.MENU_OMNI_MESSAGES)
  events() {
    return this.realtime.stream();
  }
}
