import {
  Body,
  Controller,
  Get,
  Patch,
  Post,
  Query,
  Req,
  UseGuards,
} from '@nestjs/common';
import { Request } from 'express';
import { JwtGuard } from '../auth/jwt.guard';
import { InventoryService } from './inventory.service';

@UseGuards(JwtGuard)
@Controller('inventory')
export class InventoryController {
  constructor(private readonly inventoryService: InventoryService) {}

  @Get()
  async getInventory(
    @Req() req: Request & { user?: any },
    @Query('branchId') branchId?: string
  ) {
    return this.inventoryService.getInventory(req.user, branchId);
  }

  @Post('adjust')
  async adjustInventory(
    @Body()
    body: {
      variantId: string;
      qty: number;
      type: 'IN' | 'OUT' | 'SET';
      note?: string;
      branchId?: string;
    },
    @Req() req: Request & { user?: any }
  ) {
    return this.inventoryService.adjustInventory(body, req.user);
  }

  @Patch('transfer')
  async transferInventory(
    @Body()
    body: {
      variantId: string;
      qty: number;
      fromBranchId: string;
      toBranchId: string;
      note?: string;
    },
    @Req() req: Request & { user?: any }
  ) {
    return this.inventoryService.transferInventory(body, req.user);
  }

  @Get('movements/history')
  async getInventoryMovements(
    @Query('limit') limit?: string,
    @Req() req?: Request & { user?: any }
  ) {
    const parsedLimit = Number(limit || 100);
    return this.inventoryService.getInventoryMovements(parsedLimit, req?.user);
  }
}