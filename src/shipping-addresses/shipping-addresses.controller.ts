import { Controller, Get, Query, UseGuards } from "@nestjs/common";
import { JwtGuard } from "../auth/jwt.guard";
import { GhnClient } from "../shipment/ghn.client";

@UseGuards(JwtGuard)
@Controller("shipping-addresses")
export class ShippingAddressesController {
  constructor(private readonly ghnClient: GhnClient) {}

  @Get("provinces")
  async getProvinces() {
    const rows = await this.ghnClient.getProvinces();

    return Array.isArray(rows)
      ? rows.map((item: any) => ({
          id: Number(item.ProvinceID || item.province_id || 0),
          name: String(item.ProvinceName || item.province_name || ""),
        }))
      : [];
  }

  @Get("districts")
  async getDistricts(@Query("provinceId") provinceId?: string) {
    const pid = Number(provinceId || 0);
    if (!pid) return [];

    const rows = await this.ghnClient.getDistricts(pid);

    return Array.isArray(rows)
      ? rows.map((item: any) => ({
          id: Number(item.DistrictID || item.district_id || 0),
          name: String(item.DistrictName || item.district_name || ""),
          provinceId: pid,
        }))
      : [];
  }

  @Get("wards")
  async getWards(@Query("districtId") districtId?: string) {
    const did = Number(districtId || 0);
    if (!did) return [];

    const rows = await this.ghnClient.getWards(did);

    return Array.isArray(rows)
      ? rows.map((item: any) => ({
          code: String(item.WardCode || item.ward_code || ""),
          name: String(item.WardName || item.ward_name || ""),
          districtId: did,
        }))
      : [];
  }
}