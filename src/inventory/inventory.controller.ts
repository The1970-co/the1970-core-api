import {
  Body,
  Controller,
  Get,
  Patch,
  Post,
  Query,
  Req,
  UploadedFile,
  UseGuards,
  UseInterceptors,
  UploadedFiles,
  BadRequestException,
} from '@nestjs/common';
import {
  FileInterceptor,
  FileFieldsInterceptor,
} from '@nestjs/platform-express';
import { Request } from 'express';
import { JwtGuard } from '../auth/jwt.guard';
import { InventoryService } from './inventory.service';

@UseGuards(JwtGuard)
@Controller('inventory')
export class InventoryController {
  constructor(private readonly inventoryService: InventoryService) {}

  @Get('summary')
  async getInventorySummary(
    @Req() req: Request & { user?: any },
    @Query('branchId') branchId?: string,
  ) {
    return this.inventoryService.getInventorySummary(req.user, branchId);
  }

  @Post('import-stock-report')
  @UseInterceptors(FileInterceptor('file'))
  async importStockReport(
    @UploadedFile() file: Express.Multer.File,
    @Req() req: Request & { user?: any },
  ) {
    return this.inventoryService.importStockReport(file, req.user);
  }

  @Post('audit-sapo-file')
  @UseInterceptors(FileInterceptor('file'))
  async auditSapoFile(
    @UploadedFile() file: Express.Multer.File,
    @Req() req: Request & { user?: any },
  ) {
    return this.inventoryService.auditSapoFile(file, req.user);
  }

  @Get()
  async getInventory(
    @Req() req: Request & { user?: any },
    @Query('branchId') branchId?: string,
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
    @Req() req: Request & { user?: any },
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
    @Req() req: Request & { user?: any },
  ) {
    return this.inventoryService.transferInventory(body, req.user);
  }

  @Get('movements/history')
  async getInventoryMovements(
    @Query('limit') limit?: string,
    @Req() req?: Request & { user?: any },
  ) {
    const parsedLimit = Number(limit || 100);
    return this.inventoryService.getInventoryMovements(parsedLimit, req?.user);
  }

  // 🔥 FIX CHÍNH Ở ĐÂY
  @Post('audit-two-sapo-files')
  @UseInterceptors(
    FileFieldsInterceptor([
      { name: 'stockReportFile', maxCount: 1 }, // ⚠️ đổi tên
      { name: 'productFile', maxCount: 1 },
    ]),
  )
  async auditTwoSapoFiles(
    @UploadedFiles()
    files: {
      stockReportFile?: Express.Multer.File[];
      productFile?: Express.Multer.File[];
    },
    @Req() req: any,
  ) {
    const stockReportFile = files?.stockReportFile?.[0];
    const productFile = files?.productFile?.[0];

    if (!stockReportFile || !productFile) {
      throw new BadRequestException(
        'Thiếu file tồn kho hoặc file sản phẩm.',
      );
    }

    return this.inventoryService.auditTwoSapoFiles(
      stockReportFile,
      productFile,
      req.user,
    );
  }
}