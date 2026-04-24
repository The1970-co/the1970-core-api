import { Body, Controller, Get, Param, Patch, Post } from '@nestjs/common';
import { SuppliersService } from './suppliers.service';

@Controller('suppliers')
export class SuppliersController {
  constructor(private readonly suppliersService: SuppliersService) {}

  @Get()
  findAll() {
    return this.suppliersService.findAll();
  }

  @Post()
  create(
    @Body()
    body: {
      name: string;
      code: string;
      phone?: string;
      email?: string;
      address?: string;
      note?: string;
    },
  ) {
    return this.suppliersService.create(body);
  }

  @Patch(':id')
  update(
    @Param('id') id: string,
    @Body()
    body: {
      name?: string;
      code?: string;
      phone?: string;
      email?: string;
      address?: string;
      note?: string;
      isActive?: boolean;
    },
  ) {
    return this.suppliersService.update(id, body);
  }

  @Patch(':id/toggle')
  toggle(@Param('id') id: string) {
    return this.suppliersService.toggle(id);
  }
}