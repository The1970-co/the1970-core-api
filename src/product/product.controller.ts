import {
  BadRequestException,
  Body,
  Controller,
  Delete,
  Get,
  Param,
  Patch,
  Post,
  UploadedFile,
  UploadedFiles,
  UseInterceptors,
} from "@nestjs/common";
import {
  FileInterceptor,
  FilesInterceptor,
} from "@nestjs/platform-express";
import { Readable } from "stream";
import cloudinary from "../utils/cloudinary";
import { ProductService } from "./product.service";

@Controller("products")
export class ProductController {
  constructor(private readonly productService: ProductService) {}

  @Get()
  getProducts() {
    return this.productService.getProducts();
  }

  @Get(":id")
  getProductById(@Param("id") id: string) {
    return this.productService.getProductById(id);
  }

  @Post()
  createProduct(@Body() body: any) {
    return this.productService.createProduct(body);
  }

  @Patch(":id")
  updateProduct(@Param("id") id: string, @Body() body: any) {
    return this.productService.updateProduct(id, body);
  }

  @Delete(":id")
  deleteProduct(@Param("id") id: string) {
    return this.productService.deleteProduct(id);
  }

  @Post("import")
  @UseInterceptors(FilesInterceptor("files"))
  importProducts(
    @UploadedFiles() files: Express.Multer.File[],
    @Body("overwrite") overwrite?: string
  ) {
    return this.productService.importProducts(files, overwrite === "true");
  }

  @Post("upload-image")
  @UseInterceptors(FileInterceptor("file"))
  async uploadProductImage(@UploadedFile() file: Express.Multer.File) {
    if (!file) {
      throw new BadRequestException("Thiếu file ảnh");
    }

    const streamUpload = (fileBuffer: Buffer) => {
      return new Promise((resolve, reject) => {
        const stream = cloudinary.uploader.upload_stream(
          {
            folder: "the1970/products",
            resource_type: "image",
          },
          (error, result) => {
            if (result) resolve(result);
            else reject(error);
          }
        );

        Readable.from(fileBuffer).pipe(stream);
      });
    };

    const result: any = await streamUpload(file.buffer);

    return {
      success: true,
      url: result.secure_url,
      public_id: result.public_id,
    };
  }

  @Post(":id/variants")
  addVariant(@Param("id") id: string, @Body() body: any) {
    return this.productService.addVariant(id, body);
  }

  @Patch(":id/status")
  toggleStatus(@Param("id") id: string) {
    return this.productService.toggleProductStatus(id);
  }
}