import {
  Body,
  Controller,
  Get,
  Param,
  Post,
  Query,
  UploadedFiles,
  UseGuards,
  UseInterceptors,
} from "@nestjs/common";
import { FilesInterceptor } from "@nestjs/platform-express";
import { JwtGuard } from "../auth/jwt.guard";
import { ImportsService } from "./imports.service";

@Controller("imports")
@UseGuards(JwtGuard)
export class ImportsController {
  constructor(private readonly importsService: ImportsService) {}

  @Post("customers")
  @UseInterceptors(FilesInterceptor("files", 20))
  async importCustomers(
    @UploadedFiles() files: any[],
    @Body("defaultBranchId") defaultBranchId?: string,
    @Body("overwrite") overwrite?: string
  ) {
    if (!files || files.length === 0) {
      return {
        message: "No files uploaded",
      };
    }

    return this.importsService.importCustomers(files, {
      defaultBranchId: defaultBranchId || null,
      overwrite: overwrite !== "false",
    });
  }

  @Get("jobs")
  async getJobs(@Query("type") type?: string) {
    return this.importsService.getJobs(type);
  }

  @Get("jobs/:id/errors")
  async getJobErrors(@Param("id") id: string) {
    return this.importsService.getJobErrors(id);
  }
}