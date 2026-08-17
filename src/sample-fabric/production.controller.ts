import { BadRequestException, Body, Controller, Delete, Get, Param, Patch, Post, Query, Req, UploadedFile, UseGuards, UseInterceptors } from "@nestjs/common";
import { FileInterceptor } from "@nestjs/platform-express";
import { Readable } from "stream";
import cloudinary from "../utils/cloudinary";
import { JwtGuard } from "../auth/jwt.guard";
import { PermissionGuard } from "../auth/guards/permission.guard";
import { RequirePermissions } from "../auth/decorators/require-permissions.decorator";
import { ProductionService } from "./production.service";

async function uploadImage(file: Express.Multer.File) {
  if (!file) throw new BadRequestException("Thiếu file ảnh");
  if (!String(file.mimetype || "").startsWith("image/")) throw new BadRequestException("Chỉ chấp nhận file ảnh");
  const result:any = await new Promise((resolve,reject)=>{
    const stream=cloudinary.uploader.upload_stream({folder:"the1970/production/accessories",resource_type:"image"},(error,uploaded)=>uploaded?resolve(uploaded):reject(error));
    Readable.from(file.buffer).pipe(stream);
  });
  return {success:true,url:result?.secure_url||result?.url||"",public_id:result?.public_id};
}

@UseGuards(JwtGuard,PermissionGuard)
@Controller("production")
export class ProductionController {
  constructor(private readonly service:ProductionService){}

  @Get("meta") @RequirePermissions("production.view") meta(){return this.service.meta()}
  @Get("fabric-rolls") @RequirePermissions("production.view") rolls(){return this.service.availableFabricRolls()}

  @Get("factories") @RequirePermissions("production.view") factories(){return this.service.listFactories()}
  @Post("factories") @RequirePermissions("production.manage") createFactory(@Body() body:any){return this.service.createFactory(body)}
  @Patch("factories/:id") @RequirePermissions("production.manage") updateFactory(@Param("id") id:string,@Body() body:any){return this.service.updateFactory(id,body)}
  @Delete("factories/:id") @RequirePermissions("production.manage") deleteFactory(@Param("id") id:string){return this.service.deactivateFactory(id)}

  @Get("accessory-suppliers") @RequirePermissions("accessories.view") suppliers(){return this.service.listAccessorySuppliers()}
  @Post("accessory-suppliers") @RequirePermissions("accessories.manage") createSupplier(@Body() body:any){return this.service.createAccessorySupplier(body)}
  @Patch("accessory-suppliers/:id") @RequirePermissions("accessories.manage") updateSupplier(@Param("id") id:string,@Body() body:any){return this.service.updateAccessorySupplier(id,body)}
  @Delete("accessory-suppliers/:id") @RequirePermissions("accessories.manage") deleteSupplier(@Param("id") id:string){return this.service.deactivateAccessorySupplier(id)}

  @Get("accessories") @RequirePermissions("accessories.view") accessories(@Query() query:any){return this.service.listAccessories(query)}
  @Post("accessories") @RequirePermissions("accessories.manage") createAccessory(@Body() body:any){return this.service.createAccessory(body)}
  @Patch("accessories/:id") @RequirePermissions("accessories.manage") updateAccessory(@Param("id") id:string,@Body() body:any){return this.service.updateAccessory(id,body)}
  @Post("accessories/:id/stock") @RequirePermissions("accessories.stock") adjustStock(@Param("id") id:string,@Body() body:any){return this.service.adjustAccessoryStock(id,body)}
  @Post("accessories/upload") @RequirePermissions("accessories.manage")
  @UseInterceptors(FileInterceptor("file",{limits:{fileSize:10*1024*1024}}))
  upload(@UploadedFile() file:Express.Multer.File){return uploadImage(file)}

  @Get("sample-spec/:sampleId") @RequirePermissions("production.view") sampleSpec(@Param("sampleId") id:string){return this.service.getSampleSpec(id)}
  @Patch("sample-spec/:sampleId") @RequirePermissions("production.manage") saveSampleSpec(@Param("sampleId") id:string,@Body() body:any){return this.service.saveSampleSpec(id,body)}

  @Get("orders") @RequirePermissions("production.view") orders(@Query() query:any){return this.service.listOrders(query)}
  @Get("orders/:id") @RequirePermissions("production.view") order(@Param("id") id:string){return this.service.getOrder(id)}
  @Post("orders") @RequirePermissions("production.create") createOrder(@Body() body:any,@Req() req:any){return this.service.createOrder(body,req.user)}
  @Patch("orders/:id") @RequirePermissions("production.edit") updateOrder(@Param("id") id:string,@Body() body:any){return this.service.updateOrder(id,body)}
  @Patch("orders/:id/rolls") @RequirePermissions("production.edit") setRolls(@Param("id") id:string,@Body() body:any){return this.service.setOrderRolls(id,body)}
  @Post("orders/:id/calculate") @RequirePermissions("production.calculate") calculate(@Param("id") id:string){return this.service.calculateOrder(id)}
  @Get("orders/:id/print") @RequirePermissions("production.view") print(@Param("id") id:string){return this.service.printPayload(id)}
}
