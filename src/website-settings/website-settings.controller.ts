import { Body, Controller, Get, Patch, Query, UseGuards } from "@nestjs/common";
import { JwtGuard } from "../auth/jwt.guard";
import { PermissionGuard } from "../auth/guards/permission.guard";
import { RequirePermissions } from "../auth/decorators/require-permissions.decorator";
import { WebsiteSettingsService } from "./website-settings.service";

@Controller()
export class WebsiteSettingsController {
  constructor(private readonly service: WebsiteSettingsService) {}

  @UseGuards(JwtGuard, PermissionGuard)
  @Get("website/settings")
  @RequirePermissions("website.view")
  adminGet(@Query("market") market?: string) {
    return this.service.get(market);
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Patch("website/settings")
  @RequirePermissions("website.edit")
  adminUpdate(@Query("market") market: string | undefined, @Body() body: any) {
    return this.service.update(market, body);
  }

  @Get("storefront/settings")
  publicGet(@Query("market") market?: string) {
    return this.service.getPublic(market);
  }
}
