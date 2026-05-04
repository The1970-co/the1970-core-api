import { BadRequestException, Injectable } from '@nestjs/common';
import { InventoryMovementType, PrismaClient } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';

type TxClient = Omit<
  PrismaClient,
  '$connect' | '$disconnect' | '$on' | '$transaction' | '$use' | '$extends'
>;

@Injectable()
export class StocktakeService {
  constructor(private readonly prisma: PrismaService) {}

  private toNumber(value: unknown) {
    if (typeof value === 'number') return value;
    return Number(value || 0);
  }

  private async logInventoryMovement(
    tx: TxClient,
    input: {
      variantId: string;
      qty: number;
      note?: string;
      refType?: string;
      refId?: string;
      branchId: string;
      beforeQty?: number;
      afterQty?: number;
    }
  ) {
    const branchId = String(input.branchId || '').trim();

    if (!branchId) {
      throw new BadRequestException('Thiếu branchId khi ghi lịch sử kho.');
    }

    let beforeQty =
      typeof input.beforeQty === 'number' ? input.beforeQty : undefined;
    let afterQty = typeof input.afterQty === 'number' ? input.afterQty : undefined;

    if (beforeQty === undefined || afterQty === undefined) {
      const inventory = await tx.inventoryItem.findUnique({
        where: {
          variantId_branchId: {
            variantId: input.variantId,
            branchId,
          },
        },
        select: {
          availableQty: true,
        },
      });

      beforeQty = Number(inventory?.availableQty || 0);
      afterQty = beforeQty + Number(input.qty || 0);
    }

    await tx.inventoryMovement.create({
      data: {
        variantId: input.variantId,
        type: InventoryMovementType.ADJUSTMENT,
        qty: input.qty,
        beforeQty,
        afterQty,
        note: input.note || null,
        refType: input.refType || 'STOCKTAKE',
        refId: input.refId || null,
        branchId,
      },
    });
  }

  async applyStocktake(body: any) {
    const sessionName = String(body.sessionName || 'Stocktake Session').trim();
    const sessionNote = String(body.sessionNote || '').trim();
    const branchId = String(body.branchId || '').trim();
    const rows = Array.isArray(body.rows) ? body.rows : [];

    if (!branchId) {
      throw new BadRequestException('Thiếu branchId');
    }

    if (!rows.length) {
      throw new BadRequestException('Không có dòng kiểm kho để apply');
    }

    return this.prisma.$transaction(
      async (tx) => {
        const refId = `stocktake-${Date.now()}`;

        let adjustedCount = 0;
        let totalDelta = 0;

        for (const row of rows) {
          const variantId = String(row.variantId || '').trim();
          const counted = this.toNumber(row.counted);
          const system = this.toNumber(row.system);
          const diff = counted - system;
          const reason = String(row.reason || '').trim();
          const note = String(row.note || '').trim();

          if (!variantId) continue;
          if (diff === 0) continue;

          const inventoryItem = await tx.inventoryItem.findUnique({
            where: {
              variantId_branchId: {
                variantId,
                branchId,
              },
            },
          });

          if (!inventoryItem) continue;

          const beforeQty = Number(inventoryItem.availableQty || 0);
          const afterQty = counted;

          await tx.inventoryItem.update({
            where: {
              variantId_branchId: {
                variantId,
                branchId,
              },
            },
            data: {
              availableQty: afterQty,
            },
          });

          await this.logInventoryMovement(tx, {
            variantId,
            qty: diff,
            branchId,
            beforeQty,
            afterQty,
            refType: 'STOCKTAKE',
            refId,
            note: [
              `Phiên: ${sessionName}`,
              `Chi nhánh: ${branchId}`,
              reason ? `Lý do: ${reason}` : '',
              note ? `Ghi chú: ${note}` : '',
            ]
              .filter(Boolean)
              .join(' | '),
          });

          adjustedCount += 1;
          totalDelta += diff;
        }

        return {
          ok: true,
          refId,
          sessionName,
          branchId,
          adjustedCount,
          totalDelta,
          sessionNote,
        };
      },
      {
        maxWait: 10000,
        timeout: 20000,
      }
    );
  }
}