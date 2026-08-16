import {
  BadRequestException,
  Body,
  Controller,
  Delete,
  Get,
  Param,
  Patch,
  Post,
  Query,
  Req,
  UploadedFile,
  UseGuards,
  UseInterceptors,
} from "@nestjs/common";
import { FileInterceptor } from "@nestjs/platform-express";
import { Readable } from "stream";
import cloudinary from "../utils/cloudinary";
import { JwtGuard } from "../auth/jwt.guard";
import { PermissionGuard } from "../auth/guards/permission.guard";
import { RequirePermissions } from "../auth/decorators/require-permissions.decorator";
import { SampleFabricService } from "./sample-fabric.service";

async function uploadSampleFabricImage(file: Express.Multer.File, folder: string) {
  if (!file) throw new BadRequestException("Thiếu file ảnh");
  if (!String(file.mimetype || "").startsWith("image/")) {
    throw new BadRequestException("Chỉ chấp nhận file ảnh");
  }

  const result: any = await new Promise((resolve, reject) => {
    const stream = cloudinary.uploader.upload_stream(
      {
        folder,
        resource_type: "image",
      },
      (error, uploaded) => {
        if (uploaded) resolve(uploaded);
        else reject(error);
      },
    );
    Readable.from(file.buffer).pipe(stream);
  });

  return {
    success: true,
    filename: file.originalname,
    url: result?.secure_url || result?.url || "",
    public_id: result?.public_id,
  };
}

@UseGuards(JwtGuard, PermissionGuard)
@Controller("sample-fabric")
export class SampleFabricController {
  constructor(private readonly service: SampleFabricService) {}

  @Get("samples/meta")
  @RequirePermissions("design_sample.view")
  sampleMeta() { return this.service.sampleMeta(); }

  @Get("samples")
  @RequirePermissions("design_sample.view")
  listSamples(@Query() query: any) { return this.service.listSamples(query); }

  @Post("samples")
  @RequirePermissions("design_sample.create")
  createSample(@Body() body: any, @Req() req: any) { return this.service.createSample(body, req.user); }

  @Patch("samples/:id")
  @RequirePermissions("design_sample.edit")
  updateSample(@Param("id") id: string, @Body() body: any, @Req() req: any) { return this.service.updateSample(id, body, req.user); }

  @Delete("samples/:id")
  @RequirePermissions("design_sample.delete")
  deleteSample(@Param("id") id: string) { return this.service.deleteSample(id); }

  @Post("samples/upload")
  @RequirePermissions("design_sample.upload_images")
  @UseInterceptors(FileInterceptor("file", { limits: { fileSize: 10 * 1024 * 1024 } }))
  uploadSampleImage(@UploadedFile() file: Express.Multer.File) {
    return uploadSampleFabricImage(file, "the1970/sample-fabric/samples");
  }

  @Get("fabric-receipts/meta")
  @RequirePermissions("fabric_receipt.view")
  fabricMeta() { return this.service.fabricMeta(); }

  @Get("fabric-receipts")
  @RequirePermissions("fabric_receipt.view")
  listFabricReceipts(@Query() query: any, @Req() req: any) { return this.service.listFabricReceipts(query, req.user); }

  @Post("fabric-receipts")
  @RequirePermissions("fabric_receipt.create")
  createFabricReceipt(@Body() body: any, @Req() req: any) { return this.service.createFabricReceipt(body, req.user); }

  @Patch("fabric-receipts/:id")
  @RequirePermissions("fabric_receipt.edit")
  updateFabricReceipt(@Param("id") id: string, @Body() body: any) { return this.service.updateFabricReceipt(id, body); }

  @Patch("fabric-receipts/:id/cost")
  @RequirePermissions("fabric_receipt.cost.edit")
  setCost(@Param("id") id: string, @Body() body: any) { return this.service.setFabricReceiptCost(id, body); }

  @Post("fabric-receipts/:id/measurements")
  @RequirePermissions("fabric_receipt.measure")
  addMeasurement(@Param("id") id: string, @Body() body: any, @Req() req: any) { return this.service.addMeasurement(id, body, req.user); }

  @Post("fabric-receipts/:id/images")
  @RequirePermissions("fabric_receipt.upload_images")
  addFabricImage(@Param("id") id: string, @Body() body: any) { return this.service.addFabricImage(id, body); }

  @Post("fabric-receipts/upload")
  @RequirePermissions("fabric_receipt.upload_images")
  @UseInterceptors(FileInterceptor("file", { limits: { fileSize: 10 * 1024 * 1024 } }))
  uploadFabricImage(@UploadedFile() file: Express.Multer.File) {
    return uploadSampleFabricImage(file, "the1970/sample-fabric/fabric-receipts");
  }

  @Post("fabric-receipts/:id/complete")
  @RequirePermissions("fabric_receipt.complete")
  complete(@Param("id") id: string) { return this.service.completeFabricReceipt(id); }

  @Post("fabric-receipts/:id/approve-variance")
  @RequirePermissions("fabric_receipt.approve_variance")
  approveVariance(@Param("id") id: string, @Req() req: any) { return this.service.approveVariance(id, req.user); }

  @Delete("fabric-receipts/:id")
  @RequirePermissions("fabric_receipt.delete")
  deleteFabricReceipt(@Param("id") id: string) { return this.service.deleteFabricReceipt(id); }
}
