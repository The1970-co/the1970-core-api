import { BadRequestException, ForbiddenException, Injectable, NotFoundException } from '@nestjs/common';
import { InventoryMovementType, PrismaClient } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';

type TxClient = Omit<
  PrismaClient,
  '$connect' | '$disconnect' | '$on' | '$transaction' | '$use' | '$extends'
>;

@Injectable()
export class StocktakeService {
  constructor(private readonly prisma: PrismaService) {}

  private isOwner(user?: any) {
    const roles = [
      ...(Array.isArray(user?.roles) ? user.roles : []),
      user?.role,
    ]
      .map((role) => String(role || '').toLowerCase())
      .filter(Boolean);

    return roles.includes('owner') || roles.includes('admin') ||
      (Array.isArray(user?.permissions) && user.permissions.includes('*'));
  }

  private assertOwner(user?: any) {
    if (!this.isOwner(user)) {
      throw new ForbiddenException('Chỉ admin/owner được xử lý phiên kiểm kho.');
    }
  }

  private scopedBranchId(user?: any, requestedBranchId?: string | null) {
    if (this.isOwner(user)) return String(requestedBranchId || '').trim();

    const branchId = String(user?.branchId || '').trim();
    if (!branchId) throw new ForbiddenException('Tài khoản chưa được gán chi nhánh.');
    if (requestedBranchId && String(requestedBranchId) !== branchId) {
      throw new ForbiddenException('Không có quyền kiểm kho chi nhánh khác.');
    }
    return branchId;
  }

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

  async applyStocktake(body: any, user?: any) {
    const sessionName = String(body.sessionName || 'Stocktake Session').trim();
    const sessionNote = String(body.sessionNote || '').trim();
    const branchId = this.scopedBranchId(user, body.branchId);
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

  async cancelStocktakeSession(id: string, user?: any) {
    this.assertOwner(user);

    const sessionId = String(id || '').trim();
    if (!sessionId) throw new BadRequestException('Thiếu sessionId.');

    const session = await (this.prisma as any).stocktakeSession.findUnique({
      where: { id: sessionId },
    });

    if (!session) throw new NotFoundException('Không tìm thấy phiên kiểm kho.');

    const status = String(session.status || '').toUpperCase();
    if (status === 'APPLIED') {
      throw new BadRequestException('Phiên đã chốt tồn thật, không được huỷ.');
    }

    return (this.prisma as any).stocktakeSession.update({
      where: { id: sessionId },
      data: {
        status: 'CANCELLED' as any,
        finishedAt: session.finishedAt || new Date(),
      },
    });
  }

  async deleteStocktakeSession(id: string, user?: any) {
    this.assertOwner(user);

    const sessionId = String(id || '').trim();
    if (!sessionId) throw new BadRequestException('Thiếu sessionId.');

    const session = await (this.prisma as any).stocktakeSession.findUnique({
      where: { id: sessionId },
    });

    if (!session) throw new NotFoundException('Không tìm thấy phiên kiểm kho.');

    const status = String(session.status || '').toUpperCase();
    if (status === 'APPLIED') {
      throw new BadRequestException('Phiên đã chốt tồn thật, không được xoá.');
    }

    return this.prisma.$transaction(async (tx) => {
      const db = tx as any;

      // Xoá theo thứ tự con -> cha để tránh lỗi khoá ngoại.
      // Có try/catch để an toàn nếu project đang dùng tên model khác ở vài bản schema.
      const safeDeleteMany = async (modelName: string, where: any) => {
        if (!db[modelName]?.deleteMany) return;
        try {
          await db[modelName].deleteMany({ where });
        } catch {
          // model tồn tại nhưng không có field tương ứng trong schema hiện tại thì bỏ qua
        }
      };

      await safeDeleteMany('stocktakeScanEvent', { sessionId });
      await safeDeleteMany('stocktakeLog', { sessionId });
      await safeDeleteMany('stocktakeSnapshotItem', { sessionId });
      await safeDeleteMany('stocktakeSnapshot', { sessionId });
      await safeDeleteMany('stocktakeSessionItem', { sessionId });
      await safeDeleteMany('stocktakeArea', { sessionId });
      await safeDeleteMany('stocktakeWorker', { sessionId });

      await db.stocktakeSession.delete({ where: { id: sessionId } });

      return {
        ok: true,
        deleted: true,
        id: sessionId,
      };
    });
  }
}
