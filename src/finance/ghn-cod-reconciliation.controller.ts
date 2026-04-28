import {
  Body,
  Controller,
  Post,
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
  upload(@UploadedFile() file: Express.Multer.File, @Body() body: any) {
    return this.service.parseExcel(file, body);
  }
}