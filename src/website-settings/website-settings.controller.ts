import {
  Body,
  Controller,
  Get,
  Patch,
  Query,
  UseGuards,
} from "@nestjs/common";
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
  getAdmin(@Query("market") market?: string) {
    return this.service.getAdmin(market);
  }

  @UseGuards(JwtGuard, PermissionGuard)
  @Patch("website/settings")
  @RequirePermissions("website.edit")
  updateAdmin(
    @Query("market") market: string | undefined,
    @Body() body: any,
  ) {
    return this.service.update(market, body);
  }

  @Get("storefront/settings")
  getPublic(@Query("market") market?: string) {
    return this.service.publicSettings(market);
  }
}
