import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  Patch,
  Post,
  Query,
  UseGuards,
} from "@nestjs/common";
import { JwtGuard } from "../auth/jwt.guard";
import { PermissionGuard } from "../auth/guards/permission.guard";
import { RequirePermissions } from "../auth/decorators/require-permissions.decorator";
import { WebsiteCatalogService } from "./website-catalog.service";

@UseGuards(JwtGuard, PermissionGuard)
@Controller("website/products")
export class WebsiteCatalogController {
  constructor(private readonly service: WebsiteCatalogService) {}

  @Get()
  @RequirePermissions("products.view")
  list(@Query("q") q?: string, @Query("status") status?: string) {
    return this.service.list({ q, status });
  }

  @Get(":id")
  @RequirePermissions("products.view")
  get(@Param("id") id: string) {
    return this.service.get(id);
  }

  @Post()
  @RequirePermissions("products.edit")
  create(@Body() body: any) {
    return this.service.create(body);
  }

  @Patch(":id")
  @RequirePermissions("products.edit")
  update(@Param("id") id: string, @Body() body: any) {
    return this.service.update(id, body);
  }

  @Delete(":id")
  @RequirePermissions("products.edit")
  remove(@Param("id") id: string) {
    return this.service.remove(id);
  }
}
