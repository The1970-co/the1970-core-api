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
  constructor(private readonly productService: ProductService) { }

  @Get()
  getProducts(
    @Query("page") page?: string,
    @Query("limit") limit?: string,
    @Query("q") q?: string,
    @Query("category") category?: string,
    @Query("status") status?: string
  ) {
    return this.productService.getProducts({
      page: Number(page || 1),
      limit: Number(limit || 20),
      q,
      category,
      status,
    });
  }

  @Post("sync-categories")
  syncCategoriesFromProducts() {
    return this.productService.syncCategoriesFromProducts();
  }

  @Get("category-options")
  getProductCategoryOptions() {
    return this.productService.getProductCategoryOptions();
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

  @Post("rename-category")
  renameCategory(@Body() body: { oldName: string; newName: string }) {
    return this.productService.renameCategory(body.oldName, body.newName);
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
