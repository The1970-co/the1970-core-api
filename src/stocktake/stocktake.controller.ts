import { Body, Controller, Delete, Param, Patch, Post, Req, UseGuards } from "@nestjs/common";
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

@UseGuards(JwtGuard, PermissionGuard)
@Controller("stocktake-sessions")
export class StocktakeSessionMaintenanceController {
  constructor(private readonly stocktakeService: StocktakeService) {}

  @Patch(":id/cancel")
  @RequirePermissions("stocktake.apply")
  cancelStocktakeSession(@Param("id") id: string, @Req() req: any) {
    return this.stocktakeService.cancelStocktakeSession(id, req.user);
  }

  @Delete(":id")
  @RequirePermissions("stocktake.apply")
  deleteStocktakeSession(@Param("id") id: string, @Req() req: any) {
    return this.stocktakeService.deleteStocktakeSession(id, req.user);
  }
}
