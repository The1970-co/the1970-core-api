import {
  Body,
  ForbiddenException,
  Controller,
  Get,
  Param,
  Post,
  Patch,
  Query,
  Req,
  UploadedFile,
  UseGuards,
  UseInterceptors,
} from "@nestjs/common";
import { FileInterceptor } from "@nestjs/platform-express";
import { JwtGuard } from "../auth/jwt.guard";
import { GhnCodReconciliationService } from "./ghn-cod-reconciliation.service";

@Controller("finance/ghn-cod-reconciliation")
@UseGuards(JwtGuard)
export class GhnCodReconciliationController {
  constructor(private readonly service: GhnCodReconciliationService) {}

  private requireAdmin(user: any) {
    const roles = [user?.role, user?.userRole, user?.primaryRole,
      ...(Array.isArray(user?.roles) ? user.roles : [])]
      .map((role) => String(role || "").toLowerCase());
    if (!roles.some((role) => role === "owner" || role === "admin")) {
      throw new ForbiddenException("Chỉ Admin/Owner được truy cập đối soát COD GHN.");
    }
  }


  @Post("upload")
  @UseInterceptors(FileInterceptor("file"))
  upload(@UploadedFile() file: Express.Multer.File, @Body() body: any, @Req() req: any) {
    this.requireAdmin(req.user);
    return this.service.parseExcel(file, this.withActor(body, req));
  }

  @Post("manual")
  manual(@Body() body: any, @Req() req: any) {
    this.requireAdmin(req.user);
    return this.service.parseManual(this.withActor(body, req));
  }

  @Patch("rows/:rowId/partner-cod")
  updatePartnerCod(@Param("rowId") rowId: string, @Body() body: any, @Req() req: any) {
    this.requireAdmin(req.user);
    return this.service.updatePartnerCod(rowId, body);
  }

  @Post("rows/delete")
  deleteRows(@Body() body: any, @Req() req: any) {
    this.requireAdmin(req.user);
    return this.service.deleteRows(body?.rowIds || [], body?.batchId);
  }

  @Get("history")
  history(@Query() query: any, @Req() req: any) {
    this.requireAdmin(req.user);
    return this.service.listHistory(query);
  }

  @Get("history/:batchId")
  historyDetail(@Param("batchId") batchId: string, @Req() req: any) {
    this.requireAdmin(req.user);
    return this.service.getHistoryBatch(batchId);
  }

  @Post(":batchId/save")
  save(@Param("batchId") batchId: string, @Body() body: any, @Req() req: any) {
    this.requireAdmin(req.user);
    return this.service.saveBatch(batchId, this.withActor(body, req));
  }

  @Post(":batchId/confirm")
  confirm(@Param("batchId") batchId: string, @Body() body: any, @Req() req: any) {
    this.requireAdmin(req.user);
    return this.service.confirmBatch(batchId, this.withActor(body, req));
  }

  @Post(":batchId/payment")
  payment(@Param("batchId") batchId: string, @Body() body: any, @Req() req: any) {
    this.requireAdmin(req.user);
    return this.service.markBatchPaid(batchId, this.withActor(body, req));
  }

  @Post(":batchId/delete")
  deleteBatch(@Param("batchId") batchId: string, @Req() req: any) {
    this.requireAdmin(req.user);
    return this.service.deleteBatch(batchId);
  }

  private withActor(body: any, req: any) {
    const actorId = req?.user?.id || req?.user?.staffId || req?.user?.sub || null;
    return { ...(body || {}), actorId };
  }
}
