import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Param,
  Post,
  Query,
  Req,
  UseGuards,
} from "@nestjs/common";
import { JwtGuard } from "../auth/jwt.guard";
import { ShipmentService } from "./shipment.service";
import { QuoteShipmentDto } from "./dto/quote-shipment.dto";
import { CreateGhnShipmentDto } from "./dto/create-ghn-shipment.dto";
import { TrackShipmentDto } from "./dto/track-shipment.dto";

@Controller("shipments")
@UseGuards(JwtGuard)
export class ShipmentController {
  constructor(private readonly shipmentService: ShipmentService) {}

  private normalizeName(input?: string) {
    return String(input || "")
      .trim()
      .toLowerCase()
      .replace(/^tỉnh\s+/i, "")
      .replace(/^thành phố\s+/i, "")
      .replace(/^tp\.\s*/i, "")
      .replace(/^tp\s+/i, "")
      .replace(/^quận\s+/i, "")
      .replace(/^huyện\s+/i, "")
      .replace(/^phường\s+/i, "")
      .replace(/^xã\s+/i, "");
  }

  @Get(":id")
  getShipmentDetail(@Param("id") id: string) {
    return this.shipmentService.getShipmentDetail(id);
  }

  @Get(":id/tracking")
  getShipmentTracking(
    @Param("id") id: string,
    @Query("force") force?: string
  ) {
    return this.shipmentService.getShipmentTracking(id, force === "1");
  }

  @Post(":id/tracking/refresh")
  refreshShipmentTracking(@Param("id") id: string) {
    return this.shipmentService.getShipmentTracking(id, true);
  }

  @Post("ghn/resolve-address")
  async resolveAddress(@Body() body: any) {
    const provinceName = this.normalizeName(body?.province);
    const districtName = this.normalizeName(body?.district);
    const wardName = this.normalizeName(body?.ward);

    const provinces: any[] = await this.shipmentService.ghnProvinces();
    const province =
      provinces.find((p) => this.normalizeName(p.ProvinceName) === provinceName) ||
      provinces.find((p) =>
        this.normalizeName(p.ProvinceName).includes(provinceName)
      );

    if (!province) return {};

    const districts: any[] = await this.shipmentService.ghnDistricts(
      Number(province.ProvinceID)
    );

    const district =
      districts.find((d) => this.normalizeName(d.DistrictName) === districtName) ||
      districts.find((d) =>
        this.normalizeName(d.DistrictName).includes(districtName)
      );

    if (!district) {
      return {
        provinceId: Number(province.ProvinceID),
        provinceName: province.ProvinceName,
      };
    }

    const wards: any[] = await this.shipmentService.ghnWards(
      Number(district.DistrictID)
    );

    const ward =
      wards.find((w) => this.normalizeName(w.WardName) === wardName) ||
      wards.find((w) => this.normalizeName(w.WardName).includes(wardName));

    return {
      provinceId: Number(province.ProvinceID),
      districtId: Number(district.DistrictID),
      wardCode: ward?.WardCode,
      provinceName: province?.ProvinceName,
      districtName: district?.DistrictName,
      wardName: ward?.WardName,
    };
  }

  @Post("ghn/quote")
  quote(@Body() dto: QuoteShipmentDto) {
    return this.shipmentService.quote(dto);
  }

  @Post(":orderId/ghn/create")
  createGhnShipment(
    @Param("orderId") orderId: string,
    @Body() dto: CreateGhnShipmentDto
  ) {
    return this.shipmentService.createGhnShipment(orderId, dto);
  }

  @Post(":orderId/create")
  createShipmentFromOrder(
    @Param("orderId") orderId: string,
    @Req() req: any
  ) {
    return this.shipmentService.createShipmentFromOrder(orderId, req.user);
  }

  @Post(":orderId/cancel")
  cancelShipmentByOrderId(
    @Param("orderId") orderId: string,
    @Req() req: any
  ) {
    return this.shipmentService.cancelShipmentByOrderId(orderId, req.user);
  }

  @Post(":orderId/cod/verify-and-update")
  verifyAndUpdateCod(
    @Param("orderId") orderId: string,
    @Body() body: { codAmount: number; code: string },
    @Req() req: any
  ) {
    if (typeof body?.codAmount !== "number" || Number.isNaN(body.codAmount)) {
      throw new BadRequestException("codAmount không hợp lệ.");
    }

    if (!body?.code || !String(body.code).trim()) {
      throw new BadRequestException("Thiếu mã authen.");
    }

    return this.shipmentService.verifyAndUpdateCod(
      orderId,
      body.codAmount,
      String(body.code).trim(),
      req.user
    );
  }

  @Post("ghn/track")
  track(@Body() dto: TrackShipmentDto) {
    return this.shipmentService.track(dto);
  }

  @Get("ghn/provinces")
  ghnProvinces() {
    return this.shipmentService.ghnProvinces();
  }

  @Get("ghn/districts")
  ghnDistricts(@Query("provinceId") provinceId?: string) {
    return this.shipmentService.ghnDistricts(
      provinceId ? Number(provinceId) : undefined
    );
  }

  @Get("ghn/wards")
  ghnWards(@Query("districtId") districtId: string) {
    return this.shipmentService.ghnWards(Number(districtId));
  }
}