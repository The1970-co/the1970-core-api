import { Module } from "@nestjs/common";
import { ShippingAddressesController } from "./shipping-addresses.controller";
import { GhnClient } from "../shipment/ghn.client";

@Module({
  controllers: [ShippingAddressesController],
  providers: [GhnClient],
})
export class ShippingAddressesModule {}