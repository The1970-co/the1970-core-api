import { Body, Controller, Get, Param, Patch, Post } from '@nestjs/common';
import { ShipmentService } from './shipment.service';

@Controller('shipments')
export class ShipmentController {
  constructor(private readonly shipmentService: ShipmentService) {}

  @Post()
  create(@Body() body: any) {
    return this.shipmentService.createShipment(body);
  }

  @Get()
  findAll() {
    return this.shipmentService.getShipments();
  }

  @Get(':id')
  findOne(@Param('id') id: string) {
    return this.shipmentService.getShipmentById(id);
  }

  @Patch(':id/status')
  updateStatus(
    @Param('id') id: string,
    @Body() body: { shippingStatus: string },
  ) {
    return this.shipmentService.updateShipmentStatus(id, body.shippingStatus);
  }
}