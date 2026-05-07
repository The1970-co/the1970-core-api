import { Body, Controller, Headers, Post } from "@nestjs/common";
import { ShipmentService } from "./shipment.service";

@Controller("shipments/ahamove/webhook")
export class AhamoveWebhookController {
  constructor(private readonly shipmentService: ShipmentService) {}

  @Post()
  handleWebhook(@Body() body: any, @Headers() headers: Record<string, string>) {
    return this.shipmentService.handleAhamoveWebhook(body, headers);
  }
}
