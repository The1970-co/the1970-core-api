import {
  Body,
  Controller,
  Param,
  Post,
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

  @Post("upload")
  @UseInterceptors(FileInterceptor("file"))
  upload(@UploadedFile() file: Express.Multer.File, @Body() body: any, @Req() req: any) {
    return this.service.parseExcel(file, this.withActor(body, req));
  }

  @Post("manual")
  manual(@Body() body: any, @Req() req: any) {
    return this.service.parseManual(this.withActor(body, req));
  }

  @Post("rows/delete")
  deleteRows(@Body() body: any) {
    return this.service.deleteRows(body?.rowIds || [], body?.batchId);
  }

  @Post(":batchId/save")
  save(@Param("batchId") batchId: string, @Body() body: any, @Req() req: any) {
    return this.service.saveBatch(batchId, this.withActor(body, req));
  }

  @Post(":batchId/confirm")
  confirm(@Param("batchId") batchId: string, @Body() body: any, @Req() req: any) {
    return this.service.confirmBatch(batchId, this.withActor(body, req));
  }

  @Post(":batchId/payment")
  payment(@Param("batchId") batchId: string, @Body() body: any, @Req() req: any) {
    return this.service.markBatchPaid(batchId, this.withActor(body, req));
  }

  @Post(":batchId/delete")
  deleteBatch(@Param("batchId") batchId: string) {
    return this.service.deleteBatch(batchId);
  }

  private withActor(body: any, req: any) {
    const actorId = req?.user?.id || req?.user?.staffId || req?.user?.sub || null;
    return { ...(body || {}), actorId };
  }
}
