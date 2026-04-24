import { Controller, Get, Query, UseGuards } from "@nestjs/common";
import { JwtGuard } from "../auth/jwt.guard";
import { AddressService } from "./address.service";

@Controller("address")
@UseGuards(JwtGuard)
export class AddressController {
  constructor(private readonly addressService: AddressService) {}

  @Get("provinces")
  async provinces() {
    return this.addressService.getProvinces();
  }

  @Get("districts")
  async districts(@Query("provinceId") provinceId?: string) {
    return this.addressService.getDistricts(Number(provinceId || 0));
  }

  @Get("wards")
  async wards(@Query("districtId") districtId?: string) {
    return this.addressService.getWards(Number(districtId || 0));
  }
}