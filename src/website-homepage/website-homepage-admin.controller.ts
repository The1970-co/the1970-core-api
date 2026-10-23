import { Body, Controller, Get, Patch, Post, Query, UseGuards } from "@nestjs/common";
import { JwtGuard } from "../auth/jwt.guard";
import { PermissionGuard } from "../auth/guards/permission.guard";
import { RequirePermissions } from "../auth/decorators/require-permissions.decorator";
import { WebsiteHomepageService } from "./website-homepage.service";

@UseGuards(JwtGuard, PermissionGuard)
@Controller("website/homepage")
export class WebsiteHomepageAdminController {
  constructor(private readonly service: WebsiteHomepageService) {}

  @Get()
  @RequirePermissions("products.view")
  get(@Query("market") market?: string) {
    return this.service.getAdmin(market);
  }

  @Patch()
  @RequirePermissions("products.edit")
  save(@Query("market") market: string | undefined, @Body() body: any) {
    return this.service.saveDraft(market, body?.config ?? body);
  }

  @Post("publish")
  @RequirePermissions("products.edit")
  publish(@Query("market") market?: string) {
    return this.service.publish(market);
  }
}
