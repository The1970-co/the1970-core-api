import {
  BadRequestException,
  ForbiddenException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { InventoryMovementType, Prisma } from '@prisma/client';
import * as XLSX from 'xlsx';
import { PrismaService } from '../prisma/prisma.service';

type StockReportBranchRow = {
  branchId: string;
  stock: number;
  value: number;
  cost: number;
};

type StockReportRow = {
  rowNumber: number;
  sku: string;
  barcode: string;
  productName: string;
  variantName: string;
  category: string;
  branchRows: StockReportBranchRow[];
};

@Injectable()
export class InventoryService {
  constructor(private readonly prisma: PrismaService) {}

  private isOwner(user?: any) {
    return user?.role === 'owner' || user?.role === 'admin';
  }

  private resolveBranchIdFromUser(user?: any) {
    return user?.branchId || user?.branchName || null;
  }

  private ensureBranchAccess(user: any, branchId?: string | null) {
    if (this.isOwner(user)) return;

    const userBranch = this.resolveBranchIdFromUser(user);

    if (!userBranch) {
      throw new ForbiddenException('Tài khoản chưa được gán chi nhánh.');
    }

    if (branchId && userBranch !== branchId) {
      throw new ForbiddenException('Bạn không có quyền truy cập chi nhánh này.');
    }
  }

  private normalizeText(value: any) {
    return String(value ?? '')
      .trim()
      .toLowerCase()
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/đ/g, 'd')
      .replace(/Đ/g, 'D')
      .replace(/[*:]/g, '')
      .replace(/[_-]+/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
  }

  private compactKey(value: any) {
    return this.normalizeText(value).replace(/[^a-z0-9]/g, '');
  }

  private buildProductVariantKey(productName: string, variantName: string) {
    return `${this.compactKey(productName)}__${this.compactKey(variantName)}`;
  }

  private buildDbVariantNames(
    productName: string,
    color?: string | null,
    size?: string | null,
    sku?: string | null,
  ) {
    const cleanProductName = String(productName || '').trim();
    const cleanColor = String(color || '').trim();
    const cleanSize = String(size || '').trim();
    const cleanSku = String(sku || '').trim();

    const names = new Set<string>();

    if (cleanColor && cleanSize) {
      names.add(`${cleanProductName} - ${cleanColor} - ${cleanSize}`);
      names.add(`${cleanProductName} - ${cleanSize} - ${cleanColor}`);
      names.add(`${cleanProductName} - ${cleanColor} / ${cleanSize}`);
      names.add(`${cleanProductName} - ${cleanSize} / ${cleanColor}`);
      names.add(`${cleanProductName} - ${cleanColor}-${cleanSize}`);
      names.add(`${cleanProductName} - ${cleanSize}-${cleanColor}`);
      names.add(`${cleanColor} - ${cleanSize}`);
      names.add(`${cleanSize} - ${cleanColor}`);
      names.add(`${cleanColor} / ${cleanSize}`);
      names.add(`${cleanSize} / ${cleanColor}`);
      names.add(`${cleanColor}-${cleanSize}`);
      names.add(`${cleanSize}-${cleanColor}`);
    }

    if (cleanSku) {
      names.add(`${cleanProductName} - ${cleanSku}`);
      names.add(cleanSku);
    }

    return Array.from(names);
  }

private toNumber(value: any): number {
  if (value === null || value === undefined || value === '') return 0;

  if (typeof value === 'number') {
    return Number.isFinite(value) ? Math.round(value) : 0;
  }

  let raw = String(value).trim().replace(/\s/g, '');
  if (!raw || raw === '-') return 0;

  raw = raw.replace(/[^\d.,-]/g, '');

  // Số lượng: 1, 2, 10, 1.000, 1,000
  if (raw.includes('.') && raw.includes(',')) {
    // VN: 20.327.999,999 => 20327999.999
    raw = raw.replace(/\./g, '').replace(',', '.');
  } else if (raw.includes('.')) {
    // 550.000 => 550000
    raw = raw.replace(/\./g, '');
  } else if (raw.includes(',')) {
    // 451733,333 => 451733.333
    raw = raw.replace(',', '.');
  }

  const n = Number(raw);
  return Number.isFinite(n) ? Math.round(n) : 0;
}

private toMoney(value: any): number {
  if (value === null || value === undefined || value === '') return 0;

  // Quan trọng: khi đọc file bằng raw:true, XLSX trả số thật của ô Excel
  // VD: 462.000đ => 462000, 27.720.000đ => 27720000.
  // Không tự nhân x1000 ở đây nữa, vì sẽ làm đội số.
  if (typeof value === 'number') {
    return Number.isFinite(value) ? Math.round(value) : 0;
  }

  const rawText = String(value).trim();
  if (!rawText || rawText === '-') return 0;

  return this.toNumber(rawText);
}
private chunkArray<T>(items: T[], size = 1000) {
  const chunks: T[][] = [];

  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }

  return chunks;
}
  private buildStockReportRows(sheetRows: any[][]): StockReportRow[] {
    const rows: StockReportRow[] = [];

    // File Báo cáo tồn kho SAPO hiện tại:
    // A STT, B Tên sản phẩm, C Tên phiên bản, D Mã SKU, E Mã Barcode, F Loại SP, G Đơn vị tính
    // H/I/J = Chùa Láng tồn / giá trị tồn / giá vốn
    // L/M/N = Xã Đàn tồn / giá trị tồn / giá vốn
    // P/Q/R = KHO(QO) tồn / giá trị tồn / giá vốn
    // T/U/V = Thái Hà tồn / giá trị tồn / giá vốn
    for (let rowIndex = 2; rowIndex < sheetRows.length; rowIndex++) {
      const row = sheetRows[rowIndex] || [];

      const stt = this.toNumber(row[0]);
      const productName = String(row[1] ?? '').trim();
      const variantName = String(row[2] ?? '').trim();
      const sku = String(row[3] ?? '').trim();
      const barcode = String(row[4] ?? '').trim();
      const category = String(row[5] ?? '').trim();

      if (!stt || !sku || !productName) continue;
      if (this.normalizeText(productName).startsWith('tong')) continue;

      rows.push({
        rowNumber: rowIndex + 1,
        sku,
        barcode,
        productName,
        variantName,
        category,
        branchRows: [
          {
            branchId: 'CL',
            stock: this.toNumber(row[7]),
            value: this.toMoney(row[8]),
            cost: this.toMoney(row[9]),
          },
          {
            branchId: 'XD',
            stock: this.toNumber(row[11]),
            value: this.toMoney(row[12]),
            cost: this.toMoney(row[13]),
          },
          {
            branchId: 'QO',
            stock: this.toNumber(row[15]),
            value: this.toMoney(row[16]),
            cost: this.toMoney(row[17]),
          },
          {
            branchId: 'TH',
            stock: this.toNumber(row[19]),
            value: this.toMoney(row[20]),
            cost: this.toMoney(row[21]),
          },
        ],
      });
    }

    return rows;
  }

  async getInventory(user?: any, branchId?: string) {
    const requestedBranchId = branchId?.trim() || null;

    if (requestedBranchId) {
      this.ensureBranchAccess(user, requestedBranchId);
    }

    const effectiveBranchId = this.isOwner(user)
      ? requestedBranchId
      : this.resolveBranchIdFromUser(user);

    const rows = await this.prisma.inventoryItem.findMany({
      where: effectiveBranchId ? { branchId: effectiveBranchId } : {},
      include: {
        variant: {
          include: {
            product: true,
          },
        },
      },
      orderBy: [{ updatedAt: 'desc' }],
    });

    return rows.map((row) => {
      const variant = (row as any).variant;
      const availableQty = Number((row as any).availableQty || 0);
      const costPrice = Number(variant?.costPrice || 0);
      const price = Number(variant?.price || 0);

      return {
        id: row.id,
        branchId: row.branchId,
        availableQty,
        reservedQty: Number((row as any).reservedQty || 0),
        incomingQty: Number((row as any).incomingQty || 0),
        updatedAt: new Date(row.updatedAt).toLocaleString('vi-VN'),
        variantId: row.variantId,
        sku: variant?.sku || '—',
        color: variant?.color || '',
        size: variant?.size || '',
        price,
        costPrice,
        inventoryValue: availableQty * costPrice,
        productName: variant?.product?.name || '—',
        productSlug: variant?.product?.slug || '',
        category: variant?.product?.category || '',
      };
    });
  }

  async getInventorySummary(user?: any, branchId?: string) {
    const requestedBranchId = branchId?.trim() || null;

    if (requestedBranchId) {
      this.ensureBranchAccess(user, requestedBranchId);
    }

    const effectiveBranchId = this.isOwner(user)
      ? requestedBranchId
      : this.resolveBranchIdFromUser(user);

    const inventoryRows = await this.prisma.inventoryItem.findMany({
      where: effectiveBranchId ? { branchId: effectiveBranchId } : {},
      include: {
        variant: {
          include: {
            product: {
              select: {
                id: true,
                status: true,
              },
            },
          },
        },
      },
    });

    let totalInventoryValue = 0;
    let totalQty = 0;
    let lowStockSkus = 0;

    const productIds = new Set<string>();
    const variantIds = new Set<string>();
    const branchValues = new Map<string, number>();
    const variantQtyMap = new Map<string, number>();

    for (const item of inventoryRows) {
      const variant = (item as any).variant;
      const product = variant?.product;

      if (!variant || product?.status === 'INACTIVE') continue;

      const qty = Number((item as any).availableQty || 0);
      const costPrice = Number(variant.costPrice || 0);
      const value = qty * costPrice;

      totalQty += qty;
      totalInventoryValue += value;

      if (product?.id) productIds.add(product.id);
      if (variant?.id) {
        variantIds.add(variant.id);
        variantQtyMap.set(variant.id, (variantQtyMap.get(variant.id) || 0) + qty);
      }

      branchValues.set(item.branchId, (branchValues.get(item.branchId) || 0) + value);
    }

    for (const qty of variantQtyMap.values()) {
      if (qty <= 3) lowStockSkus += 1;
    }

    const highestBranch =
      Array.from(branchValues.entries()).sort((a, b) => b[1] - a[1])[0]?.[0] || '';

    return {
      totalInventoryValue,
      totalQty,
      totalProducts: productIds.size,
      totalSkus: variantIds.size,
      lowStockSkus,
      highestBranch,
      branchValues: Object.fromEntries(branchValues.entries()),
    };
  }

  async importStockReport(file: Express.Multer.File, user?: any) {
    if (!this.isOwner(user)) {
      throw new ForbiddenException('Chỉ admin/owner được import báo cáo tồn kho.');
    }

    if (!file?.buffer) {
      throw new BadRequestException('Thiếu file báo cáo tồn kho.');
    }

    const startedAt = Date.now();

    const workbook = XLSX.read(file.buffer, { type: 'buffer' });
    const sheetName = workbook.SheetNames[0];

    if (!sheetName) {
      throw new BadRequestException('File Excel không có sheet dữ liệu.');
    }

    const worksheet = workbook.Sheets[sheetName];
    const sheetRows = XLSX.utils.sheet_to_json<any[]>(worksheet, {
      header: 1,
      defval: '',
      raw: false,
    });

    const reportRows = this.buildStockReportRows(sheetRows);

    if (!reportRows.length) {
      throw new BadRequestException('Không tìm thấy dòng tồn kho hợp lệ trong file.');
    }

    const products = await this.prisma.product.findMany({
      include: {
        variants: {
          select: {
            id: true,
            sku: true,
            color: true,
            size: true,
            productId: true,
          },
        },
      },
    });

    const variantByName = new Map<string, { id: string; sku: string | null; productId: string }>();
    const variantBySku = new Map<string, { id: string; sku: string | null; productId: string }>();

    for (const product of products) {
      for (const variant of product.variants) {
        const record = {
          id: variant.id,
          sku: variant.sku,
          productId: variant.productId,
        };

        for (const dbVariantName of this.buildDbVariantNames(
          product.name,
          variant.color,
          variant.size,
          variant.sku,
        )) {
          variantByName.set(this.buildProductVariantKey(product.name, dbVariantName), record);
        }

        if (variant.sku) {
          variantBySku.set(this.compactKey(variant.sku), record);
        }
      }
    }

    let matchedRows = 0;
    let updatedInventoryRows = 0;
    let updatedVariantCosts = 0;
    let totalImportedQty = 0;
    let totalImportedValue = 0;

    const missingSkus: Array<{ rowNumber: number; sku: string; productName: string }> = [];
    const zeroCostSkus: Array<{ rowNumber: number; sku: string; productName: string; totalQty: number }> = [];
    const skuWarnings: Array<{ rowNumber: number; fileSku: string; dbSku: string; productName: string }> = [];

    const inventoryRows: Prisma.InventoryItemCreateManyInput[] = [];
    const variantCostMap = new Map<string, number>();

    for (const reportRow of reportRows) {
      const skuKey = this.compactKey(reportRow.sku);
      const nameKey = this.buildProductVariantKey(reportRow.productName, reportRow.variantName);

      const variant =
        (skuKey ? variantBySku.get(skuKey) : undefined) ||
        variantByName.get(nameKey);

      if (!variant) {
        missingSkus.push({
          rowNumber: reportRow.rowNumber,
          sku: reportRow.sku || reportRow.variantName,
          productName: `${reportRow.productName} / ${reportRow.variantName}`,
        });
        continue;
      }

      if (
        reportRow.sku &&
        variant.sku &&
        this.compactKey(reportRow.sku) !== this.compactKey(variant.sku)
      ) {
        skuWarnings.push({
          rowNumber: reportRow.rowNumber,
          fileSku: reportRow.sku,
          dbSku: variant.sku,
          productName: `${reportRow.productName} / ${reportRow.variantName}`,
        });
      }

      matchedRows += 1;

      let rowTotalQty = 0;
      let rowTotalValue = 0;
      let weightedCostSum = 0;
      let weightedCostQty = 0;
      let fallbackCostPrice = 0;

      for (const branchRow of reportRow.branchRows) {
        const qty = Math.round(Number(branchRow.stock || 0));
        const cost = Math.round(Number(branchRow.cost || 0));
        const value = qty > 0 && cost > 0 ? qty * cost : 0;

        rowTotalQty += qty;
        rowTotalValue += value;

        if (qty > 0 && cost > 0) {
          weightedCostSum += qty * cost;
          weightedCostQty += qty;
        }

        if (fallbackCostPrice <= 0 && cost > 0) {
          fallbackCostPrice = cost;
        }

        inventoryRows.push({
          variantId: variant.id,
          branchId: branchRow.branchId,
          availableQty: qty,
          reservedQty: 0,
          incomingQty: 0,
        });
      }

      totalImportedQty += rowTotalQty;
      totalImportedValue += rowTotalValue;

      const costPrice =
        weightedCostQty > 0
          ? Math.round(weightedCostSum / weightedCostQty)
          : fallbackCostPrice > 0
            ? Math.round(fallbackCostPrice)
            : 0;

      if (costPrice > 0) {
        variantCostMap.set(variant.id, costPrice);
      } else {
        zeroCostSkus.push({
          rowNumber: reportRow.rowNumber,
          sku: reportRow.sku || reportRow.variantName,
          productName: `${reportRow.productName} / ${reportRow.variantName}`,
          totalQty: rowTotalQty,
        });
      }
    }

    const variantCostUpdates = Array.from(variantCostMap.entries()).map(
      ([id, costPrice]) => ({ id, costPrice }),
    );

    // Snapshot SAPO full: xoá sạch tồn cũ rồi ghi lại theo file.
    await this.prisma.inventoryItem.deleteMany({});

    await this.prisma.productVariant.updateMany({
      data: {
        costPrice: new Prisma.Decimal(0),
      },
    });

    for (const chunk of this.chunkArray(inventoryRows, 3000)) {
      if (!chunk.length) continue;

      await this.prisma.inventoryItem.createMany({
        data: chunk,
        skipDuplicates: true,
      });

      updatedInventoryRows += chunk.length;
    }

    for (const chunk of this.chunkArray(variantCostUpdates, 1000)) {
      if (!chunk.length) continue;

      const valuesSql = Prisma.join(
        chunk.map((item) => Prisma.sql`(${item.id}, ${item.costPrice})`),
      );

      await this.prisma.$executeRaw`
        UPDATE "ProductVariant" AS pv
        SET "costPrice" = data.cost_price::numeric
        FROM (VALUES ${valuesSql}) AS data(id, cost_price)
        WHERE pv.id = data.id::text
      `;

      updatedVariantCosts += chunk.length;
    }

    const summary = await this.getInventorySummary(user);
    const durationMs = Date.now() - startedAt;

    return {
      success: true,
      fileName: file.originalname,
      reportRows: reportRows.length,
      matchedRows,
      missingSkuCount: missingSkus.length,
      missingSkus: missingSkus.slice(0, 200),
      zeroCostSkuCount: zeroCostSkus.length,
      zeroCostSkus: zeroCostSkus.slice(0, 200),
      skuWarningCount: skuWarnings.length,
      skuWarnings: skuWarnings.slice(0, 200),
      updatedInventoryRows,
      updatedVariantCosts,
      totalImportedQty,
      totalImportedValue,
      durationMs,
      summary,
    };
  }


  async auditSapoFile(file: Express.Multer.File, user?: any) {
    if (!this.isOwner(user)) {
      throw new ForbiddenException('Chỉ admin/owner được đối chiếu SAPO.');
    }

    if (!file?.buffer) {
      throw new BadRequestException('Thiếu file SAPO.');
    }

    const workbook = XLSX.read(file.buffer, { type: 'buffer' });
    const sheetName = workbook.SheetNames[0];

    if (!sheetName) {
      throw new BadRequestException('File Excel không có sheet dữ liệu.');
    }

    const worksheet = workbook.Sheets[sheetName];
    const rows = XLSX.utils.sheet_to_json<any[]>(worksheet, {
      header: 1,
      defval: '',
      raw: false,
    });

    // File DS sản phẩm SAPO:
    // A = tên sản phẩm, M = tên phiên bản, N = SKU, BB = giá nhập/giá vốn.
    // AB = CL tồn, AL = TH tồn, AQ = QO/KHO tồn, AV = XD tồn.
    const COL = {
      productName: 0,
      variantName: 12,
      sku: 13,
      costPrice: 53,
      CL: 27,
      TH: 37,
      QO: 42,
      XD: 47,
    };

    type FileSkuRow = {
      sku: string;
      productName: string;
      variantName: string;
      costPrice: number;
      branches: Record<string, number>;
    };

    const fileBySku = new Map<string, FileSkuRow>();

    for (let rowIndex = 1; rowIndex < rows.length; rowIndex++) {
      const row = rows[rowIndex] || [];
      const sku = String(row[COL.sku] || '').trim();

      if (!sku || this.normalizeText(sku).includes('ma sku')) continue;

      fileBySku.set(sku, {
        sku,
        productName: String(row[COL.productName] || '').trim(),
        variantName: String(row[COL.variantName] || '').trim(),
        costPrice: this.toNumber(row[COL.costPrice]),
        branches: {
          CL: this.toNumber(row[COL.CL]),
          XD: this.toNumber(row[COL.XD]),
          QO: this.toNumber(row[COL.QO]),
          TH: this.toNumber(row[COL.TH]),
        },
      });
    }

    const skus = Array.from(fileBySku.keys());
    const variants = skus.length
      ? await this.prisma.productVariant.findMany({
          where: { sku: { in: skus } },
          include: {
            product: true,
            inventoryItems: true,
          },
        })
      : [];

    const variantBySku = new Map(variants.map((variant) => [variant.sku, variant]));
    const branchIds = ['CL', 'XD', 'QO', 'TH'];

    const branchTotals: Record<
      string,
      {
        fileQty: number;
        systemQty: number;
        qtyDiff: number;
        fileValue: number;
        systemValue: number;
        valueDiff: number;
      }
    > = Object.fromEntries(
      branchIds.map((branchId) => [
        branchId,
        {
          fileQty: 0,
          systemQty: 0,
          qtyDiff: 0,
          fileValue: 0,
          systemValue: 0,
          valueDiff: 0,
        },
      ]),
    ) as any;

    const diffRows: Array<{
      sku: string;
      productName: string;
      branchId: string;
      fileQty: number;
      systemQty: number;
      qtyDiff: number;
      fileCostPrice: number;
      systemCostPrice: number;
      fileValue: number;
      systemValue: number;
      valueDiff: number;
    }> = [];

    const missingSkus: FileSkuRow[] = [];

    for (const fileRow of fileBySku.values()) {
      const variant = variantBySku.get(fileRow.sku);

      if (!variant) {
        missingSkus.push(fileRow);

        for (const branchId of branchIds) {
          const fileQty = Number(fileRow.branches[branchId] || 0);
          const fileValue = fileQty * Number(fileRow.costPrice || 0);
          branchTotals[branchId].fileQty += fileQty;
          branchTotals[branchId].fileValue += fileValue;
        }

        continue;
      }

      const systemBranchQty = Object.fromEntries(
        variant.inventoryItems.map((item) => [
          item.branchId,
          Number(item.availableQty || 0),
        ]),
      );

      const systemCostPrice = Number(variant.costPrice || 0);
      const fileCostPrice = Number(fileRow.costPrice || 0);

      for (const branchId of branchIds) {
        const fileQty = Number(fileRow.branches[branchId] || 0);
        const systemQty = Number(systemBranchQty[branchId] || 0);
        const fileValue = fileQty * fileCostPrice;
        const systemValue = systemQty * systemCostPrice;

        branchTotals[branchId].fileQty += fileQty;
        branchTotals[branchId].systemQty += systemQty;
        branchTotals[branchId].fileValue += fileValue;
        branchTotals[branchId].systemValue += systemValue;

        const qtyDiff = systemQty - fileQty;
        const valueDiff = systemValue - fileValue;

        if (qtyDiff !== 0 || Math.abs(valueDiff) >= 1 || systemCostPrice !== fileCostPrice) {
          diffRows.push({
            sku: fileRow.sku,
            productName: variant.product?.name || fileRow.productName,
            branchId,
            fileQty,
            systemQty,
            qtyDiff,
            fileCostPrice,
            systemCostPrice,
            fileValue,
            systemValue,
            valueDiff,
          });
        }
      }
    }

    for (const branchId of branchIds) {
      const row = branchTotals[branchId];
      row.qtyDiff = row.systemQty - row.fileQty;
      row.valueDiff = row.systemValue - row.fileValue;
    }

    const totalFileValue = Object.values(branchTotals).reduce(
      (sum, row) => sum + row.fileValue,
      0,
    );
    const totalSystemValue = Object.values(branchTotals).reduce(
      (sum, row) => sum + row.systemValue,
      0,
    );
    const totalFileQty = Object.values(branchTotals).reduce(
      (sum, row) => sum + row.fileQty,
      0,
    );
    const totalSystemQty = Object.values(branchTotals).reduce(
      (sum, row) => sum + row.systemQty,
      0,
    );

    return {
      success: true,
      fileRows: fileBySku.size,
      matchedSkus: variants.length,
      missingSkuCount: missingSkus.length,
      missingSkus: missingSkus.slice(0, 200),
      total: {
        fileQty: totalFileQty,
        systemQty: totalSystemQty,
        qtyDiff: totalSystemQty - totalFileQty,
        fileValue: totalFileValue,
        systemValue: totalSystemValue,
        valueDiff: totalSystemValue - totalFileValue,
      },
      branchTotals,
      diffCount: diffRows.length,
      diffRows: diffRows
        .sort((a, b) => Math.abs(b.valueDiff) - Math.abs(a.valueDiff))
        .slice(0, 500),
    };
  }


  private getProductCodeFromSku(sku: string) {
    const cleanSku = String(sku || '').trim();
    if (!cleanSku) return '';
    return cleanSku.split('-')[0]?.trim() || cleanSku;
  }

  async auditTwoSapoFiles(
    stockReportFile: Express.Multer.File,
    productFile: Express.Multer.File,
    user?: any,
  ) {
    if (!this.isOwner(user)) {
      throw new ForbiddenException('Chỉ admin/owner được đối chiếu 2 file SAPO.');
    }

    if (!stockReportFile?.buffer || !productFile?.buffer) {
      throw new BadRequestException('Cần upload đủ 2 file: Báo cáo tồn kho và Danh sách sản phẩm.');
    }

    const branchIds = ['CL', 'XD', 'QO', 'TH'];

    const emptyBranchRows = () =>
      Object.fromEntries(
        branchIds.map((branchId) => [
          branchId,
          {
            qty: 0,
            value: 0,
            costPrice: 0,
          },
        ]),
      ) as Record<string, { qty: number; value: number; costPrice: number }>;

    const readFirstSheetRows = (file: Express.Multer.File) => {
      const workbook = XLSX.read(file.buffer, { type: 'buffer' });
      const sheetName = workbook.SheetNames[0];

      if (!sheetName) {
        throw new BadRequestException(`File ${file.originalname || ''} không có sheet dữ liệu.`);
      }

      return XLSX.utils.sheet_to_json<any[]>(workbook.Sheets[sheetName], {
        header: 1,
        defval: '',
        raw: true,
      });
    };

    const stockReportRows = this.buildStockReportRows(readFirstSheetRows(stockReportFile));

    // File Danh sách sản phẩm SAPO:
    // A = tên sản phẩm, M = tên phiên bản, N = SKU, BB = giá nhập/giá vốn.
    // AB = CL tồn, AL = TH tồn, AQ = QO/KHO tồn, AV = XD tồn.
    const productSheetRows = readFirstSheetRows(productFile);
    const COL = {
      productName: 0,
      variantName: 12,
      sku: 13,
      costPrice: 53,
      CL: 27,
      TH: 37,
      QO: 42,
      XD: 47,
    };

    type CompareBranchRow = {
      qty: number;
      value: number;
      costPrice: number;
    };

    type CompareRow = {
      sku: string;
      productCode: string;
      productName: string;
      qty: number;
      value: number;
      costPrice: number;
      branches: Record<string, CompareBranchRow>;
    };

    const stockBySku = new Map<string, CompareRow>();
    const productBySku = new Map<string, CompareRow>();

    for (const row of stockReportRows) {
      const sku = String(row.sku || '').trim();
      if (!sku) continue;

      const branches = emptyBranchRows();

      for (const branch of row.branchRows) {
        const branchId = branch.branchId;
        const qty = Number(branch.stock || 0);
        const value = Number(branch.value || 0);
        const costPrice = Number(branch.cost || 0);

        branches[branchId] = {
          qty,
          value,
          costPrice,
        };
      }

const qty = Object.values(branches).reduce(
  (sum, branch) => sum + this.toNumber(branch.qty),
  0,
);

const value = Object.values(branches).reduce(
  (sum, branch) => sum + this.toNumber(branch.value),
  0,
);
      const costPrice = qty > 0 ? Math.round(value / qty) : 0;

      stockBySku.set(sku, {
        sku,
        productCode: this.getProductCodeFromSku(sku),
        productName: row.productName || row.variantName || sku,
        qty,
        value,
        costPrice,
        branches,
      });
    }

    for (let rowIndex = 1; rowIndex < productSheetRows.length; rowIndex++) {
      const row = productSheetRows[rowIndex] || [];
      const sku = String(row[COL.sku] || '').trim();

      if (!sku || this.normalizeText(sku).includes('ma sku')) continue;

      const costPrice = this.toMoney(row[COL.costPrice]);
      const branches = emptyBranchRows();

      const branchQty: Record<string, number> = {
        CL: this.toNumber(row[COL.CL]),
        XD: this.toNumber(row[COL.XD]),
        QO: this.toNumber(row[COL.QO]),
        TH: this.toNumber(row[COL.TH]),
      };

      for (const branchId of branchIds) {
        const qty = Number(branchQty[branchId] || 0);
        branches[branchId] = {
          qty,
          value: qty * costPrice,
          costPrice,
        };
      }

      const qty = Object.values(branches).reduce((sum, branch) => sum + Number(branch.qty || 0), 0);
      const value = Object.values(branches).reduce((sum, branch) => sum + Number(branch.value || 0), 0);

      productBySku.set(sku, {
        sku,
        productCode: this.getProductCodeFromSku(sku),
        productName: String(row[COL.productName] || row[COL.variantName] || sku).trim(),
        qty,
        value,
        costPrice,
        branches,
      });
    }

    const buildBranchDiffRows = (stockRow?: CompareRow, productRow?: CompareRow) => {
      return branchIds.map((branchId) => {
        const stockBranch = stockRow?.branches?.[branchId];
        const productBranch = productRow?.branches?.[branchId];
        const stockReportQty = Number(stockBranch?.qty || 0);
        const productFileQty = Number(productBranch?.qty || 0);
        const stockReportValue = Number(stockBranch?.value || 0);
        const productFileValue = Number(productBranch?.value || 0);
        const stockCostPrice = Number(stockBranch?.costPrice || 0);
        const productCostPrice = Number(productBranch?.costPrice || 0);

        return {
          branchId,
          stockReportQty,
          productFileQty,
          qtyDiff: stockReportQty - productFileQty,
          stockReportValue,
          productFileValue,
          diff: stockReportValue - productFileValue,
          stockCostPrice,
          productCostPrice,
          costPriceDiff: stockCostPrice - productCostPrice,
        };
      });
    };

    const allSkus = new Set([
      ...Array.from(stockBySku.keys()),
      ...Array.from(productBySku.keys()),
    ]);

    const allSkuRows = Array.from(allSkus).map((sku) => {
      const stockRow = stockBySku.get(sku);
      const productRow = productBySku.get(sku);
      const stockReportValue = Number(stockRow?.value || 0);
      const productFileValue = Number(productRow?.value || 0);
      const stockReportQty = Number(stockRow?.qty || 0);
      const productFileQty = Number(productRow?.qty || 0);
      const stockCostPrice = Number(stockRow?.costPrice || 0);
      const productCostPrice = Number(productRow?.costPrice || 0);
      const branchDiffRows = buildBranchDiffRows(stockRow, productRow);

      const hasCostPriceDiff = branchDiffRows.some(
        (branch) =>
          branch.stockReportQty === branch.productFileQty &&
          branch.stockReportQty > 0 &&
          Math.abs(branch.diff) >= 1,
      );

      return {
        sku,
        productCode: stockRow?.productCode || productRow?.productCode || this.getProductCodeFromSku(sku),
        productName: stockRow?.productName || productRow?.productName || sku,
        stockReportQty,
        productFileQty,
        qtyDiff: stockReportQty - productFileQty,
        stockReportValue,
        productFileValue,
        diff: stockReportValue - productFileValue,
        stockCostPrice,
        productCostPrice,
        costPriceDiff: stockCostPrice - productCostPrice,
        hasCostPriceDiff,
        branchDiffRows,
      };
    });

    const groupByProduct = (skuRows: typeof allSkuRows) => {
      const map = new Map<
        string,
        {
          productCode: string;
          stockReportQty: number;
          productFileQty: number;
          stockReportValue: number;
          productFileValue: number;
          skuRows: typeof allSkuRows;
        }
      >();

      for (const row of skuRows) {
        const productCode = row.productCode || row.sku;
        const current = map.get(productCode) || {
          productCode,
          stockReportQty: 0,
          productFileQty: 0,
          stockReportValue: 0,
          productFileValue: 0,
          skuRows: [],
        };

        current.stockReportQty += Number(row.stockReportQty || 0);
        current.productFileQty += Number(row.productFileQty || 0);
        current.stockReportValue += Number(row.stockReportValue || 0);
        current.productFileValue += Number(row.productFileValue || 0);
        current.skuRows.push(row);

        map.set(productCode, current);
      }

      return Array.from(map.values()).map((row) => {
        const branchDiffRows = branchIds.map((branchId) => {
          const stockReportQty = row.skuRows.reduce(
            (sum, skuRow) => sum + Number(skuRow.branchDiffRows.find((branch) => branch.branchId === branchId)?.stockReportQty || 0),
            0,
          );
          const productFileQty = row.skuRows.reduce(
            (sum, skuRow) => sum + Number(skuRow.branchDiffRows.find((branch) => branch.branchId === branchId)?.productFileQty || 0),
            0,
          );
          const stockReportValue = row.skuRows.reduce(
            (sum, skuRow) => sum + Number(skuRow.branchDiffRows.find((branch) => branch.branchId === branchId)?.stockReportValue || 0),
            0,
          );
          const productFileValue = row.skuRows.reduce(
            (sum, skuRow) => sum + Number(skuRow.branchDiffRows.find((branch) => branch.branchId === branchId)?.productFileValue || 0),
            0,
          );

          return {
            branchId,
            stockReportQty,
            productFileQty,
            qtyDiff: stockReportQty - productFileQty,
            stockReportValue,
            productFileValue,
            diff: stockReportValue - productFileValue,
          };
        });

        const diff = row.stockReportValue - row.productFileValue;
        const qtyDiff = row.stockReportQty - row.productFileQty;
        const skuRows = row.skuRows
          .filter((item) => Math.abs(item.diff) >= 1 || item.qtyDiff !== 0 || item.hasCostPriceDiff)
          .sort((a, b) => Math.abs(b.diff) - Math.abs(a.diff));

        return {
          productCode: row.productCode,
          stockReportValue: row.stockReportValue,
          productFileValue: row.productFileValue,
          diff,
          stockReportQty: row.stockReportQty,
          productFileQty: row.productFileQty,
          qtyDiff,
          skuCount: row.skuRows.length,
          costDiffSkuCount: row.skuRows.filter((item) => item.hasCostPriceDiff).length,
          branchDiffRows,
          skuRows,
        };
      });
    };

    const productDiffRows = groupByProduct(allSkuRows)
      .filter((row) => Math.abs(row.diff) >= 1 || row.qtyDiff !== 0 || row.costDiffSkuCount > 0)
      .sort((a, b) => Math.abs(b.diff) - Math.abs(a.diff));

    const skuDiffRows = allSkuRows
      .filter((row) => Math.abs(row.diff) >= 1 || row.qtyDiff !== 0 || row.hasCostPriceDiff)
      .sort((a, b) => Math.abs(b.diff) - Math.abs(a.diff));

    const stockReportValue = Array.from(stockBySku.values()).reduce(
      (sum, row) => sum + Number(row.value || 0),
      0,
    );
    const productFileValue = Array.from(productBySku.values()).reduce(
      (sum, row) => sum + Number(row.value || 0),
      0,
    );
    const stockReportQty = Array.from(stockBySku.values()).reduce(
      (sum, row) => sum + Number(row.qty || 0),
      0,
    );
    const productFileQty = Array.from(productBySku.values()).reduce(
      (sum, row) => sum + Number(row.qty || 0),
      0,
    );

    return {
      success: true,
      stockReportRows: stockBySku.size,
      productFileRows: productBySku.size,
      total: {
        stockReportQty,
        productFileQty,
        qtyDiff: stockReportQty - productFileQty,
        stockReportValue,
        productFileValue,
        diff: stockReportValue - productFileValue,
      },
      productDiffRows: productDiffRows.slice(0, 500),
      skuDiffRows: skuDiffRows.slice(0, 1000),
    };
  }

  async adjustInventory(
    body: {
      variantId: string;
      qty: number;
      type: 'IN' | 'OUT' | 'SET';
      note?: string;
      branchId?: string;
    },
    user?: any,
  ) {
    const branchId = this.isOwner(user)
      ? body.branchId?.trim() || this.resolveBranchIdFromUser(user)
      : this.resolveBranchIdFromUser(user);

    this.ensureBranchAccess(user, branchId);

    if (!branchId) {
      throw new BadRequestException('Thiếu branchId');
    }

    if (!body.variantId) {
      throw new BadRequestException('Thiếu variantId');
    }

    const qty = Number(body.qty || 0);
    if (!Number.isFinite(qty) || qty <= 0) {
      throw new BadRequestException('Số lượng không hợp lệ');
    }

    const inventory = await this.prisma.inventoryItem.findUnique({
      where: {
        variantId_branchId: {
          variantId: body.variantId,
          branchId,
        },
      },
      include: {
        variant: {
          include: {
            product: true,
          },
        },
      },
    });

    if (!inventory) {
      throw new NotFoundException('Không tìm thấy tồn kho của variant ở chi nhánh này');
    }

    const costPrice = Number((inventory as any).variant?.costPrice || 0);
    if (costPrice <= 0) {
      throw new BadRequestException('SKU chưa có giá vốn. Vui lòng cập nhật giá vốn trước khi điều chỉnh kho.');
    }

    const currentQty = Number((inventory as any).availableQty || 0);
    let nextQty = currentQty;

    if (body.type === 'SET') {
      nextQty = qty;
    } else if (body.type === 'IN') {
      nextQty = currentQty + qty;
    } else if (body.type === 'OUT') {
      nextQty = currentQty - qty;
      if (nextQty < 0) {
        throw new BadRequestException('Tồn kho không đủ');
      }
    } else {
      throw new BadRequestException('Loại điều chỉnh không hợp lệ');
    }

    const movementQty =
      body.type === 'OUT'
        ? -qty
        : body.type === 'SET'
          ? nextQty - currentQty
          : qty;

    const updated = await this.prisma.$transaction(async (tx) => {
      const row = await tx.inventoryItem.update({
        where: {
          variantId_branchId: {
            variantId: body.variantId,
            branchId,
          },
        },
        data: {
          availableQty: nextQty,
        },
        include: {
          variant: {
            include: {
              product: true,
            },
          },
        },
      });

      await tx.inventoryMovement.create({
        data: {
          variantId: body.variantId,
          type: InventoryMovementType.ADJUSTMENT,
          qty: movementQty,
          note: body.note || `Điều chỉnh tồn kho (${body.type})`,
          refType: 'INVENTORY',
          branchId,
        },
      });

      return row;
    });

    const updatedVariant = (updated as any).variant;

    return {
      id: updated.id,
      branchId: updated.branchId,
      availableQty: Number((updated as any).availableQty || 0),
      reservedQty: Number((updated as any).reservedQty || 0),
      incomingQty: Number((updated as any).incomingQty || 0),
      updatedAt: new Date(updated.updatedAt).toLocaleString('vi-VN'),
      variantId: updated.variantId,
      sku: updatedVariant?.sku || '—',
      color: updatedVariant?.color || '',
      size: updatedVariant?.size || '',
      productName: updatedVariant?.product?.name || '—',
    };
  }

  async transferInventory(
    body: {
      variantId: string;
      qty: number;
      fromBranchId: string;
      toBranchId: string;
      note?: string;
    },
    user?: any,
  ) {
    if (!body.variantId || !body.fromBranchId || !body.toBranchId) {
      throw new BadRequestException('Thiếu dữ liệu chuyển kho');
    }

    if (body.fromBranchId === body.toBranchId) {
      throw new BadRequestException('Chi nhánh chuyển và nhận không được trùng nhau');
    }

    const qty = Number(body.qty || 0);
    if (!Number.isFinite(qty) || qty <= 0) {
      throw new BadRequestException('Số lượng chuyển không hợp lệ');
    }

    this.ensureBranchAccess(user, body.fromBranchId);

    const fromInventory = await this.prisma.inventoryItem.findUnique({
      where: {
        variantId_branchId: {
          variantId: body.variantId,
          branchId: body.fromBranchId,
        },
      },
      include: {
        variant: {
          include: {
            product: true,
          },
        },
      },
    });

    if (!fromInventory) {
      throw new NotFoundException('Không tìm thấy tồn kho ở chi nhánh chuyển');
    }

    const costPrice = Number((fromInventory as any).variant?.costPrice || 0);
    if (costPrice <= 0) {
      throw new BadRequestException('SKU chưa có giá vốn. Vui lòng cập nhật giá vốn trước khi chuyển kho.');
    }

    const currentQty = Number((fromInventory as any).availableQty || 0);
    if (currentQty < qty) {
      throw new BadRequestException('Tồn kho không đủ để chuyển');
    }

    const updated = await this.prisma.$transaction(async (tx) => {
      const deductedRow = await tx.inventoryItem.update({
        where: {
          variantId_branchId: {
            variantId: body.variantId,
            branchId: body.fromBranchId,
          },
        },
        data: {
          availableQty: currentQty - qty,
        },
        include: {
          variant: {
            include: {
              product: true,
            },
          },
        },
      });

      await tx.inventoryItem.upsert({
        where: {
          variantId_branchId: {
            variantId: body.variantId,
            branchId: body.toBranchId,
          },
        },
        update: {
          availableQty: {
            increment: qty,
          },
        },
        create: {
          variantId: body.variantId,
          branchId: body.toBranchId,
          availableQty: qty,
          reservedQty: 0,
          incomingQty: 0,
        },
      });

      await tx.inventoryMovement.create({
        data: {
          variantId: body.variantId,
          type: InventoryMovementType.ADJUSTMENT,
          qty: -qty,
          note:
            body.note ||
            `Chuyển kho từ ${body.fromBranchId} sang ${body.toBranchId}`,
          refType: 'INVENTORY_TRANSFER',
          branchId: body.fromBranchId,
        },
      });

      await tx.inventoryMovement.create({
        data: {
          variantId: body.variantId,
          type: InventoryMovementType.ADJUSTMENT,
          qty,
          note:
            body.note ||
            `Nhận kho từ ${body.fromBranchId} sang ${body.toBranchId}`,
          refType: 'INVENTORY_TRANSFER',
          branchId: body.toBranchId,
        },
      });

      return deductedRow;
    });

    const updatedVariant = (updated as any).variant;

    return {
      id: updated.id,
      branchId: updated.branchId,
      availableQty: Number((updated as any).availableQty || 0),
      reservedQty: Number((updated as any).reservedQty || 0),
      incomingQty: Number((updated as any).incomingQty || 0),
      updatedAt: new Date(updated.updatedAt).toLocaleString('vi-VN'),
      variantId: updated.variantId,
      sku: updatedVariant?.sku || '—',
      color: updatedVariant?.color || '',
      size: updatedVariant?.size || '',
      productName: updatedVariant?.product?.name || '—',
    };
  }

  async getInventoryMovements(limit = 100, user?: any) {
    const where = this.isOwner(user)
      ? {}
      : {
          branchId: this.resolveBranchIdFromUser(user) || '__NO_BRANCH__',
        };

    const rows = await this.prisma.inventoryMovement.findMany({
      where,
      orderBy: { createdAt: 'desc' },
      take: limit,
      include: {
        variant: {
          include: {
            product: true,
          },
        },
      },
    });

    return rows.map((row) => {
      const variant = (row as any).variant;

      return {
        id: row.id,
        type: row.type,
        qty: row.qty,
        note: row.note,
        refType: row.refType,
        refId: row.refId,
        branchId: row.branchId,
        createdAt: new Date(row.createdAt).toLocaleString('vi-VN'),
        sku: variant?.sku || '—',
        productName: variant?.product?.name || '—',
        color: variant?.color || '',
        size: variant?.size || '',
      };
    });
  }

  async getInventoryByRack(rackId: string, user?: any) {
    if (!rackId) {
      throw new BadRequestException('Thiếu rackId');
    }

    const locations = await this.prisma.productVariantLocation.findMany({
      where: { rackId },
    });

    if (!locations.length) return [];

    const variantIds = locations.map((location) => location.variantId);

    const [variants, inventoryItems] = await Promise.all([
      this.prisma.productVariant.findMany({
        where: { id: { in: variantIds } },
        include: {
          product: true,
        },
      }),
      this.prisma.inventoryItem.findMany({
        where: {
          variantId: { in: variantIds },
        },
      }),
    ]);

    const variantMap = new Map(variants.map((variant) => [variant.id, variant]));
    const qtyMap = new Map<string, number>();
    const branchQtyMap = new Map<string, Record<string, number>>();

    for (const item of inventoryItems) {
      const availableQty = Number((item as any).availableQty || 0);
      qtyMap.set(item.variantId, (qtyMap.get(item.variantId) || 0) + availableQty);

      const currentBranchQty = branchQtyMap.get(item.variantId) || {};
      currentBranchQty[item.branchId] =
        Number(currentBranchQty[item.branchId] || 0) + availableQty;
      branchQtyMap.set(item.variantId, currentBranchQty);
    }

    return locations.map((location) => {
      const variant = variantMap.get(location.variantId);
      const costPrice = Number((variant as any)?.costPrice || 0);
      const totalQty = qtyMap.get(location.variantId) || 0;

      return {
        id: location.id,
        rackId: location.rackId,
        variantId: location.variantId,
        sku: variant?.sku || '—',
        productName: variant?.product?.name || '—',
        productSlug: variant?.product?.slug || '',
        color: variant?.color || '',
        size: variant?.size || '',
        qty: totalQty,
        costPrice,
        inventoryValue: totalQty * costPrice,
        branchQty: branchQtyMap.get(location.variantId) || {},
      };
    });
  }
}
