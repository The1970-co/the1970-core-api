import { Body, Controller, Post, Req, UseGuards } from "@nestjs/common";
import { JwtGuard } from "../auth/jwt.guard";
import { PermissionGuard } from "../auth/guards/permission.guard";
import { RequirePermissions } from "../auth/decorators/require-permissions.decorator";
import { StocktakeService } from "./stocktake.service";

@UseGuards(JwtGuard, PermissionGuard)
@Controller("stocktake")
export class StocktakeController {
  constructor(private readonly stocktakeService: StocktakeService) {}

  @Post("apply")
  @RequirePermissions("stocktake.apply")
  applyStocktake(@Body() body: any, @Req() req: any) {
    return this.stocktakeService.applyStocktake(body, req.user);
  }
}
