import { Controller, Get, UseGuards } from "@nestjs/common";
import { JwtGuard } from "../../auth/jwt.guard";
import { MobileBranchesService } from "./mobile-branches.service";

@Controller("mobile/branches")
@UseGuards(JwtGuard)
export class MobileBranchesController {
  constructor(private readonly service: MobileBranchesService) {}

  @Get()
  getBranches() {
    return this.service.getBranches();
  }
}