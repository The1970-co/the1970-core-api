
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

async function uploadImage(file: Express.Multer.File, folder: string) {
  if (!file) throw new BadRequestException("Thiếu file ảnh");
  if (!String(file.mimetype || "").startsWith("image/")) throw new BadRequestException("Chỉ chấp nhận file ảnh");
  const result: any = await new Promise((resolve, reject) => {
    const stream = cloudinary.uploader.upload_stream(
      { folder, resource_type: "image" },
      (error, uploaded) => uploaded ? resolve(uploaded) : reject(error),
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

  @Get("fabric-suppliers")
  @RequirePermissions("fabric_library.view")
  listFabricSuppliers(@Req() req: any) {
    return this.service.listFabricSuppliers(req.user);
  }

  @Post("fabric-suppliers")
  @RequirePermissions("fabric_library.create")
  createFabricSupplier(@Body() body: any) {
    return this.service.createFabricSupplier(body);
  }

  @Patch("fabric-suppliers/:id")
  @RequirePermissions("fabric_library.edit")
  updateFabricSupplier(@Param("id") id: string, @Body() body: any) {
    return this.service.updateFabricSupplier(id, body);
  }

  @Delete("fabric-suppliers/:id")
  @RequirePermissions("fabric_library.delete")
  deactivateFabricSupplier(@Param("id") id: string) {
    return this.service.deactivateFabricSupplier(id);
  }

  // Thư viện / bảng vải
  @Get("library/meta")
  @RequirePermissions("fabric_library.view")
  fabricLibraryMeta() {
    return this.service.fabricLibraryMeta();
  }

  @Get("library")
  @RequirePermissions("fabric_library.view")
  listFabricBoards(@Query() query: any) {
    return this.service.listFabricBoards(query);
  }

  @Get("library/:id")
  @RequirePermissions("fabric_library.view")
  getFabricBoard(@Param("id") id: string) {
    return this.service.getFabricBoard(id);
  }

  @Post("library")
  @RequirePermissions("fabric_library.create")
  createFabricBoard(@Body() body: any, @Req() req: any) {
    return this.service.createFabricBoard(body, req.user);
  }

  @Patch("library/:id")
  @RequirePermissions("fabric_library.edit")
  updateFabricBoard(@Param("id") id: string, @Body() body: any) {
    return this.service.updateFabricBoard(id, body);
  }

  @Delete("library/:id")
  @RequirePermissions("fabric_library.delete")
  deleteFabricBoard(@Param("id") id: string) {
    return this.service.deleteFabricBoard(id);
  }

  @Post("library/upload")
  @RequirePermissions("fabric_library.upload_images")
  @UseInterceptors(FileInterceptor("file", { limits: { fileSize: 10 * 1024 * 1024 } }))
  uploadFabricBoardImage(@UploadedFile() file: Express.Multer.File) {
    return uploadImage(file, "the1970/sample-fabric/library");
  }

  // Mẫu triển khai
  @Get("samples/meta")
  @RequirePermissions("design_sample.view")
  sampleMeta() {
    return this.service.sampleMeta();
  }

  @Get("samples/check-code")
  @RequirePermissions("design_sample.view")
  checkSampleCode(@Query("code") code?: string, @Query("excludeId") excludeId?: string) {
    return this.service.checkSampleCode(code, excludeId);
  }

  @Get("samples")
  @RequirePermissions("design_sample.view")
  listSamples(@Query() query: any) {
    return this.service.listSamples(query);
  }

  @Post("samples")
  @RequirePermissions("design_sample.create")
  createSample(@Body() body: any, @Req() req: any) {
    return this.service.createSample(body, req.user);
  }

  @Post("samples/quick")
  @RequirePermissions("design_sample.create")
  createQuickSample(@Body() body: any, @Req() req: any) {
    return this.service.createQuickSample(body, req.user);
  }


  @Patch("samples/:id")
  @RequirePermissions("design_sample.edit")
  updateSample(@Param("id") id: string, @Body() body: any, @Req() req: any) {
    return this.service.updateSample(id, body, req.user);
  }

  @Delete("samples/:id")
  @RequirePermissions("design_sample.delete")
  deleteSample(@Param("id") id: string) {
    return this.service.deleteSample(id);
  }

  @Post("samples/:id/images")
  @RequirePermissions("design_sample.upload_images")
  addSampleImage(@Param("id") id: string, @Body() body: any) {
    return this.service.addSampleImage(id, body);
  }

  @Post("samples/upload")
  @RequirePermissions("design_sample.upload_images")
  @UseInterceptors(FileInterceptor("file", { limits: { fileSize: 10 * 1024 * 1024 } }))
  uploadSampleImage(@UploadedFile() file: Express.Multer.File) {
    return uploadImage(file, "the1970/sample-fabric/samples");
  }

  // Gửi công ty / xưởng làm mẫu
  @Get("sample-dispatches")
  @RequirePermissions("sample_dispatch.view")
  listSampleDispatches(@Query() query: any) {
    return this.service.listSampleDispatches(query);
  }

  @Post("sample-dispatches")
  @RequirePermissions("sample_dispatch.create")
  createSampleDispatch(@Body() body: any, @Req() req: any) {
    return this.service.createSampleDispatch(body, req.user);
  }

  @Patch("sample-dispatches/:id")
  @RequirePermissions("sample_dispatch.edit")
  updateSampleDispatch(@Param("id") id: string, @Body() body: any) {
    return this.service.updateSampleDispatch(id, body);
  }

  @Delete("sample-dispatches/:id")
  @RequirePermissions("sample_dispatch.delete")
  deleteSampleDispatch(@Param("id") id: string) {
    return this.service.deleteSampleDispatch(id);
  }

  // Vải về / kiểm nhận
  @Get("fabric-receipts/next-code")
  @RequirePermissions("fabric_receipt.view")
  nextFabricReceiptCode(@Query("receivedAt") receivedAt?: string) {
    return this.service.previewFabricReceiptCode(receivedAt);
  }

  @Get("fabric-receipts/meta")
  @RequirePermissions("fabric_receipt.view")
  fabricMeta(@Req() req: any) {
    return this.service.fabricMeta(req.user);
  }

  @Get("fabric-receipts")
  @RequirePermissions("fabric_receipt.view")
  listFabricReceipts(@Query() query: any, @Req() req: any) {
    return this.service.listFabricReceipts(query, req.user);
  }

  @Post("fabric-receipts")
  @RequirePermissions("fabric_receipt.create")
  createFabricReceipt(@Body() body: any, @Req() req: any) {
    return this.service.createFabricReceipt(body, req.user);
  }

  @Patch("fabric-receipts/:id")
  @RequirePermissions("fabric_receipt.edit")
  updateFabricReceipt(@Param("id") id: string, @Body() body: any, @Req() req: any) {
    return this.service.updateFabricReceipt(id, body, req.user);
  }

  @Patch("fabric-receipts/:id/cost")
  @RequirePermissions("fabric_receipt.cost.edit")
  setCost(@Param("id") id: string, @Body() body: any, @Req() req: any) {
    return this.service.setFabricReceiptCost(id, body, req.user);
  }

  @Patch("fabric-receipts/:id/rolls/:rollId")
  @RequirePermissions("fabric_receipt.edit")
  updateFabricReceiptRoll(@Param("id") id: string, @Param("rollId") rollId: string, @Body() body: any) {
    return this.service.updateFabricReceiptRoll(id, rollId, body);
  }

  @Post("fabric-receipts/:id/measurements")
  @RequirePermissions("fabric_receipt.measure")
  addMeasurement(@Param("id") id: string, @Body() body: any, @Req() req: any) {
    return this.service.addMeasurement(id, body, req.user);
  }

  @Post("fabric-receipts/:id/images")
  @RequirePermissions("fabric_receipt.upload_images")
  addFabricImage(@Param("id") id: string, @Body() body: any) {
    return this.service.addFabricImage(id, body);
  }

  @Post("fabric-receipts/upload")
  @RequirePermissions("fabric_receipt.upload_images")
  @UseInterceptors(FileInterceptor("file", { limits: { fileSize: 10 * 1024 * 1024 } }))
  uploadFabricReceiptImage(@UploadedFile() file: Express.Multer.File) {
    return uploadImage(file, "the1970/sample-fabric/fabric-receipts");
  }

  @Post("fabric-receipts/:id/complete")
  @RequirePermissions("fabric_receipt.complete")
  complete(@Param("id") id: string) {
    return this.service.completeFabricReceipt(id);
  }

  @Post("fabric-receipts/:id/approve-variance")
  @RequirePermissions("fabric_receipt.approve_variance")
  approveVariance(@Param("id") id: string, @Req() req: any) {
    return this.service.approveVariance(id, req.user);
  }

  @Delete("fabric-receipts/:id")
  @RequirePermissions("fabric_receipt.delete")
  deleteFabricReceipt(@Param("id") id: string) {
    return this.service.deleteFabricReceipt(id);
  }
}
