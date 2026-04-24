import { BadRequestException, Injectable } from "@nestjs/common";
import { ShipmentService } from "../shipment/shipment.service";

@Injectable()
export class AddressService {
  constructor(private readonly shipmentService: ShipmentService) {}

  async getProvinces() {
    const rows: any[] = await this.shipmentService.ghnProvinces();

    return (Array.isArray(rows) ? rows : [])
      .map((item) => ({
        id: Number(item.ProvinceID ?? item.province_id ?? item.id ?? 0),
        name: String(item.ProvinceName ?? item.name ?? ""),
        code: String(item.Code ?? item.code ?? ""),
      }))
      .filter((item) => item.id > 0 && item.name);
  }

  async getDistricts(provinceId?: number) {
    if (!provinceId || Number(provinceId) <= 0) {
      throw new BadRequestException("Thiếu provinceId");
    }

    const rows: any[] = await this.shipmentService.ghnDistricts(Number(provinceId));

    return (Array.isArray(rows) ? rows : [])
      .map((item) => ({
        id: Number(item.DistrictID ?? item.district_id ?? item.id ?? 0),
        name: String(item.DistrictName ?? item.name ?? ""),
      }))
      .filter((item) => item.id > 0 && item.name);
  }

  async getWards(districtId?: number) {
    if (!districtId || Number(districtId) <= 0) {
      throw new BadRequestException("Thiếu districtId");
    }

    const rows: any[] = await this.shipmentService.ghnWards(Number(districtId));

    return (Array.isArray(rows) ? rows : [])
      .map((item) => ({
        code: String(item.WardCode ?? item.ward_code ?? item.code ?? ""),
        name: String(item.WardName ?? item.name ?? ""),
      }))
      .filter((item) => item.code && item.name);
  }
}