import { BadRequestException, Controller, Headers, Post, Body, UnauthorizedException } from "@nestjs/common";
import { timingSafeEqual } from "crypto";
import { ShipmentService } from "./shipment.service";

/** Register this controller in ShipmentModule.controllers (public callback; no JWT guard). */
@Controller("shipments/viettelpost/webhook")
export class ViettelPostWebhookController {
  constructor(private readonly shipments: ShipmentService) {}

  @Post()
  async receive(@Body() body: any, @Headers("authorization") authorization?: string) {
    const secret = String(process.env.VIETTELPOST_WEBHOOK_SECRET || "").trim();
    if (secret.length < 24) throw new UnauthorizedException("Webhook not configured");
    const compare = (value: unknown) => {
      const a = Buffer.from(String(value || "").trim(), "utf8");
      const b = Buffer.from(secret, "utf8");
      return a.length === b.length && timingSafeEqual(a, b);
    };
    const fromHeader = String(authorization || "").replace(/^Bearer\s+/i, "").trim();
    // Official samples include both Authorization and TOKEN. Require both.
    if (!compare(fromHeader) || !compare(body?.TOKEN)) throw new UnauthorizedException("Invalid webhook secret");
    if (!body?.DATA || typeof body.DATA !== "object") throw new BadRequestException("Missing DATA");
    return this.shipments.receiveViettelPostWebhook(body);
  }
}
