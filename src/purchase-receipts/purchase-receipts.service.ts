import { BadRequestException, ForbiddenException, Injectable, NotFoundException } from "@nestjs/common";
import {
  InventoryMovementType,
  Prisma,
  PurchaseReceiptStatus,
} from "@prisma/client";
import { PrismaService } from "../prisma/prisma.service";

type CreateReceiptItemInput = {
  variantId: string;
  qty: number;
  unitCost?: number;
};

type CreateReceiptInput = {
  supplierId?: string;
  branchId: string;
  note?: string;
  createdById?: string;
  items: CreateReceiptItemInput[];
};

type UpdateReceiptInput = {
  supplierId?: string | null;
  branchId?: string;
  note?: string;
  items?: CreateReceiptItemInput[];
};

type PayReceiptInput = {
  paymentSourceId?: string | null;
  amount?: number;
  note?: string;
  paidById?: string;
  paidByName?: string;
};

@Injectable()
export class PurchaseReceiptsService {
  constructor(private readonly prisma: PrismaService) {}

  private isOwner(user?: any) {
    const roles = [
      ...(Array.isArray(user?.roles) ? user.roles : []),
      user?.role,
    ]
      .map((role) => String(role || "").toLowerCase())
      .filter(Boolean);

    return roles.includes("owner") || roles.includes("admin") ||
      (Array.isArray(user?.permissions) && user.permissions.includes("*"));
  }

  private userBranch(user?: any) {
    return user?.branchId || null;
  }

  private ensureBranchAccess(user?: any, branchId?: string | null) {
    if (this.isOwner(user)) return;

    const currentBranchId = this.userBranch(user);
    if (!currentBranchId) {
      throw new ForbiddenException("Tài khoản chưa được gán chi nhánh.");
    }

    if (branchId && String(branchId) !== String(currentBranchId)) {
      throw new ForbiddenException("Không có quyền xem hoặc thao tác phiếu nhập chi nhánh khác.");
    }
  }

  private scopeWhereByUser(user?: any) {
    if (this.isOwner(user)) return {};
    const currentBranchId = this.userBranch(user);
    if (!currentBranchId) return { id: "__NO_ACCESS__" };
    return { branchId: currentBranchId };
  }

  private toNumber(value: unknown) {
    if (typeof value === "number") return value;
    return Number(value || 0);
  }

  private async generateReceiptCode() {
    const count = await this.prisma.purchaseReceipt.count();
    return `PN${String(count + 1).padStart(6, "0")}`;
  }

  private async resolveCreatedById(createdById?: string | null) {
    if (!createdById) return null;

    const staff = await this.prisma.staffUser.findUnique({
      where: { id: createdById },
      select: {
        id: true,
        isActive: true,
      },
    });

    // PurchaseReceipt.createdById liên kết StaffUser.
    // Admin/Owner thường không nằm trong StaffUser nên không được làm crash phiếu nhập.
    if (!staff || !staff.isActive) return null;

    return staff.id;
  }

  private async resolveInventoryMovementCreatedById(createdById?: string | null) {
    if (!createdById) return null;

    const admin = await this.prisma.adminUser.findUnique({
      where: { id: createdById },
      select: {
        id: true,
        isActive: true,
      },
    });

    // InventoryMovement.createdById liên kết AdminUser.
    // Nếu user hiện tại là StaffUser thì để null để không vỡ FK.
    if (!admin || !admin.isActive) return null;

    return admin.id;
  }

  private getReceiptInclude() {
    return {
      supplier: true,
      branch: true,
      createdBy: true,
      items: {
        include: {
          variant: {
            select: {
              id: true,
              costPrice: true,
            },
          },
        },
        orderBy: { createdAt: "asc" as const },
      },
      purchaseReceiptPayments: {
        include: {
          paymentSource: true,
        },
        orderBy: { paidAt: "desc" as const },
      },
    };
  }

  private normalizeItems(items: CreateReceiptItemInput[]) {
    if (!Array.isArray(items) || items.length === 0) {
      throw new BadRequestException("Phiếu nhập phải có ít nhất 1 dòng hàng");
    }

    const normalizedItems = items.map((item) => {
      const qty = this.toNumber(item.qty);
      const unitCost = this.toNumber(item.unitCost);

      if (!item.variantId) {
        throw new BadRequestException("Có dòng hàng thiếu variantId");
      }

      if (!Number.isFinite(qty) || qty <= 0) {
        throw new BadRequestException("Số lượng nhập phải lớn hơn 0");
      }

      if (!Number.isFinite(unitCost) || unitCost < 0) {
        throw new BadRequestException("Giá nhập không hợp lệ");
      }

      return {
        variantId: item.variantId,
        qty,
        unitCost,
      };
    });

    const seen = new Set<string>();

    for (const item of normalizedItems) {
      if (seen.has(item.variantId)) {
        throw new BadRequestException("Một variant đang bị thêm trùng trong phiếu nhập");
      }
      seen.add(item.variantId);
    }

    return normalizedItems;
  }


  private async getVariantMapForReceiptItems(
    items: { variantId: string }[],
    prisma: Prisma.TransactionClient | PrismaService = this.prisma,
  ) {
    const variantIds = Array.from(new Set(items.map((item) => item.variantId)));

    const variants = await prisma.productVariant.findMany({
      where: {
        id: {
          in: variantIds,
        },
      },
      include: {
        product: true,
      },
    });

    const variantMap = new Map(variants.map((variant) => [variant.id, variant]));

    for (const variantId of variantIds) {
      const variant = variantMap.get(variantId);

      if (!variant) {
        throw new BadRequestException("Có variant không tồn tại");
      }

      if (!variant.product) {
        throw new BadRequestException(`Variant ${variant.sku} chưa có sản phẩm cha`);
      }
    }

    return variantMap;
  }

  private getReceiptTotal(receipt: { items: { lineTotal: Prisma.Decimal | number | string }[] }) {
    return receipt.items.reduce((sum, item) => sum + this.toNumber(item.lineTotal), 0);
  }

  private getPaidTotal(receipt: { purchaseReceiptPayments?: { amount: Prisma.Decimal | number | string }[] }) {
    return (receipt.purchaseReceiptPayments || []).reduce(
      (sum, payment) => sum + this.toNumber(payment.amount),
      0,
    );
  }

  private getEffectiveUnitCost(item: any) {
    const receiptCost = this.toNumber(item?.unitCost);
    if (receiptCost > 0) return receiptCost;

    const productCost = this.toNumber(item?.variant?.costPrice);
    return productCost > 0 ? productCost : 0;
  }

  /**
   * Với phiếu cũ có unitCost = 0, trả về giá nhập hiện tại của variant nếu sản phẩm đã có giá nhập.
   * Chỉ hydrate response, không tự ghi DB trong GET.
   */
  private hydrateReceiptCosts<T>(receipt: T): T {
    const raw = receipt as any;
    if (!raw || !Array.isArray(raw.items)) return receipt;

    return {
      ...raw,
      items: raw.items.map((item: any) => {
        const unitCost = this.getEffectiveUnitCost(item);
        const qty = this.toNumber(item?.qty);

        return {
          ...item,
          unitCost,
          lineTotal: qty * unitCost,
        };
      }),
    } as T;
  }

  /**
   * Trước các thao tác tài chính, lấp giá nhập còn thiếu từ ProductVariant.costPrice.
   * Giá đã nhập trực tiếp trên phiếu luôn được giữ nguyên.
   */
  private async fillMissingItemCostsFromProduct(
    receiptId: string,
    prisma: Prisma.TransactionClient | PrismaService = this.prisma,
  ) {
    const items = await prisma.purchaseReceiptItem.findMany({
      where: { receiptId },
      include: {
        variant: {
          select: { costPrice: true },
        },
      },
    });

    for (const item of items) {
      const currentCost = this.toNumber(item.unitCost);
      const productCost = this.toNumber(item.variant?.costPrice);

      if (currentCost > 0 || productCost <= 0) continue;

      await prisma.purchaseReceiptItem.update({
        where: { id: item.id },
        data: {
          unitCost: new Prisma.Decimal(productCost),
          lineTotal: new Prisma.Decimal(this.toNumber(item.qty) * productCost),
        },
      });
    }
  }

  async findAll(user?: any) {
    const receipts = await this.prisma.purchaseReceipt.findMany({
      where: this.scopeWhereByUser(user),
      orderBy: { createdAt: "desc" },
      include: this.getReceiptInclude(),
    });

    return receipts.map((receipt) => this.hydrateReceiptCosts(receipt));
  }

  async getById(id: string, user?: any) {
    const receipt = await this.prisma.purchaseReceipt.findUnique({
      where: { id },
      include: {
        supplier: true,
        branch: true,
        createdBy: true,
        items: {
          include: {
            variant: true,
            product: true,
          },
          orderBy: { createdAt: "asc" },
        },
        purchaseReceiptPayments: {
          include: {
            paymentSource: true,
          },
          orderBy: { paidAt: "desc" },
        },
      },
    });

    if (!receipt) {
      throw new NotFoundException("Không tìm thấy phiếu nhập");
    }

    this.ensureBranchAccess(user, receipt.branchId);

    return this.hydrateReceiptCosts(receipt);
  }

  async create(data: CreateReceiptInput, user?: any) {
    if (!data.branchId) {
      throw new BadRequestException("Thiếu kho nhập");
    }

    if (!data.supplierId) {
      throw new BadRequestException("Thiếu nhà cung cấp");
    }

    this.ensureBranchAccess(user, data.branchId);

    const [branch, supplier, validCreatedById] = await Promise.all([
      this.prisma.branch.findUnique({
        where: { id: data.branchId },
      }),
      this.prisma.supplier.findUnique({
        where: { id: data.supplierId },
      }),
      this.resolveCreatedById(data.createdById),
    ]);

    if (!branch) {
      throw new BadRequestException("Kho nhập không tồn tại");
    }

    if (!supplier) {
      throw new BadRequestException("Nhà cung cấp không tồn tại");
    }

    if (!branch.isActive) {
      throw new BadRequestException("Kho nhập đã ngừng hoạt động");
    }

    if (!supplier.isActive) {
      throw new BadRequestException("Nhà cung cấp đã ngừng hoạt động");
    }

    const normalizedItems = this.normalizeItems(data.items);
    const variantMap = await this.getVariantMapForReceiptItems(normalizedItems);
    const receiptCode = await this.generateReceiptCode();

    return this.prisma.$transaction(
      async (tx) => {
        const receipt = await tx.purchaseReceipt.create({
          data: {
            receiptCode,
            supplierId: data.supplierId,
            branchId: data.branchId,
            note: data.note?.trim() || null,
            createdById: validCreatedById,
            status: PurchaseReceiptStatus.DRAFT,
          },
        });

        await tx.purchaseReceiptItem.createMany({
          data: normalizedItems.map((item) => {
            const variant = variantMap.get(item.variantId);

            if (!variant) {
              throw new BadRequestException("Có variant không tồn tại");
            }

            const productCost = this.toNumber(variant.costPrice);
            const unitCost = item.unitCost > 0 ? item.unitCost : productCost > 0 ? productCost : 0;

            return {
              receiptId: receipt.id,
              productId: variant.productId,
              variantId: variant.id,
              sku: variant.sku,
              productName: variant.product.name,
              color: variant.color,
              size: variant.size,
              qty: item.qty,
              unitCost: new Prisma.Decimal(unitCost),
              lineTotal: new Prisma.Decimal(item.qty * unitCost),
            };
          }),
        });

        return tx.purchaseReceipt.findUnique({
          where: { id: receipt.id },
          include: this.getReceiptInclude(),
        });
      },
      {
        timeout: 30000,
        maxWait: 10000,
      },
    );
  }

  async updateDraft(id: string, data: UpdateReceiptInput, user?: any) {
    const receipt = await this.prisma.purchaseReceipt.findUnique({
      where: { id },
      include: {
        items: true,
        purchaseReceiptPayments: true,
      },
    });

    if (!receipt) {
      throw new NotFoundException("Không tìm thấy phiếu nhập");
    }

    this.ensureBranchAccess(user, receipt.branchId);

    if (receipt.status !== PurchaseReceiptStatus.DRAFT) {
      throw new BadRequestException("Chỉ sửa được phiếu đang nháp");
    }

    if (this.getPaidTotal(receipt) > 0) {
      throw new BadRequestException("Phiếu đã thanh toán, không được sửa trực tiếp");
    }

    if (data.branchId) {
      const branch = await this.prisma.branch.findUnique({
        where: { id: data.branchId },
      });

      if (!branch) {
        throw new BadRequestException("Kho nhập không tồn tại");
      }

      if (!branch.isActive) {
        throw new BadRequestException("Kho nhập đã ngừng hoạt động");
      }
    }

    if (data.supplierId) {
      const supplier = await this.prisma.supplier.findUnique({
        where: { id: data.supplierId },
      });

      if (!supplier) {
        throw new BadRequestException("Nhà cung cấp không tồn tại");
      }

      if (!supplier.isActive) {
        throw new BadRequestException("Nhà cung cấp đã ngừng hoạt động");
      }
    }

    const normalizedItems = Array.isArray(data.items)
      ? this.normalizeItems(data.items)
      : undefined;

    const variantMap = normalizedItems
      ? await this.getVariantMapForReceiptItems(normalizedItems)
      : undefined;

    return this.prisma.$transaction(
      async (tx) => {
        await tx.purchaseReceipt.update({
          where: { id },
          data: {
            ...(data.branchId !== undefined ? { branchId: data.branchId } : {}),
            ...(data.supplierId !== undefined
              ? { supplierId: data.supplierId || null }
              : {}),
            ...(data.note !== undefined ? { note: data.note?.trim() || null } : {}),
          },
        });

        if (normalizedItems && variantMap) {
          await tx.purchaseReceiptItem.deleteMany({
            where: { receiptId: id },
          });

          await tx.purchaseReceiptItem.createMany({
            data: normalizedItems.map((item) => {
              const variant = variantMap.get(item.variantId);

              if (!variant) {
                throw new BadRequestException("Có variant không tồn tại");
              }

              const productCost = this.toNumber(variant.costPrice);
              const unitCost = item.unitCost > 0 ? item.unitCost : productCost > 0 ? productCost : 0;

              return {
                receiptId: id,
                productId: variant.productId,
                variantId: variant.id,
                sku: variant.sku,
                productName: variant.product.name,
                color: variant.color,
                size: variant.size,
                qty: item.qty,
                unitCost: new Prisma.Decimal(unitCost),
                lineTotal: new Prisma.Decimal(item.qty * unitCost),
              };
            }),
          });
        }

        return tx.purchaseReceipt.findUnique({
          where: { id },
          include: this.getReceiptInclude(),
        });
      },
      {
        timeout: 30000,
        maxWait: 10000,
      },
    );
  }


  /**
   * Cập nhật giá nhập tại màn thanh toán NCC theo "SKU chính".
   * Một nhóm = cùng productId + cùng color, nên nhập giá ở 1 size sẽ áp dụng
   * cho mọi size cùng màu trên phiếu và đồng thời cập nhật ProductVariant.costPrice
   * của toàn bộ size cùng màu trong chi tiết sản phẩm.
   */
  async updateItemCostsAndSyncProduct(
    id: string,
    items: { itemId: string; unitCost: number }[],
    user?: any,
  ) {
    const receipt = await this.prisma.purchaseReceipt.findUnique({
      where: { id },
      include: {
        items: {
          include: {
            variant: {
              select: {
                productId: true,
                color: true,
              },
            },
          },
        },
        purchaseReceiptPayments: true,
      },
    });

    if (!receipt) {
      throw new NotFoundException("Không tìm thấy phiếu nhập");
    }

    this.ensureBranchAccess(user, receipt.branchId);

    if (!Array.isArray(items) || items.length === 0) {
      throw new BadRequestException("Chưa có giá nhập cần cập nhật");
    }

    // Giá vốn của phiếu chỉ được chốt/sửa trước lần thanh toán đầu tiên.
    if (this.getPaidTotal(receipt) > 0) {
      throw new BadRequestException(
        "Phiếu đã phát sinh thanh toán, không được sửa lại giá nhập",
      );
    }

    if (
      receipt.status === PurchaseReceiptStatus.CANCELLED ||
      receipt.status === PurchaseReceiptStatus.COMPLETED
    ) {
      throw new BadRequestException("Phiếu không còn cho phép sửa giá nhập");
    }

    const receiptItemById = new Map(
      receipt.items.map((item) => [String(item.id), item]),
    );

    const groupCosts = new Map<
      string,
      { productId: string; color: string | null; unitCost: number }
    >();

    for (const input of items) {
      const receiptItem = receiptItemById.get(String(input.itemId));
      if (!receiptItem) {
        throw new BadRequestException(
          `Có dòng giá nhập không thuộc phiếu ${receipt.receiptCode}`,
        );
      }

      const unitCost = this.toNumber(input.unitCost);
      if (!Number.isFinite(unitCost) || unitCost < 0) {
        throw new BadRequestException(
          `Giá nhập SKU ${receiptItem.sku} không hợp lệ`,
        );
      }

      // PurchaseReceiptItem.productId là nullable trong schema (phiếu cũ có thể bị null).
      // Variant luôn bắt buộc, nên dùng variant.productId làm fallback an toàn.
      const productId = receiptItem.productId ?? receiptItem.variant?.productId;
      if (!productId) {
        throw new BadRequestException(
          `Không xác định được sản phẩm cha của SKU ${receiptItem.sku}`,
        );
      }

      const color = receiptItem.color ?? receiptItem.variant?.color ?? null;
      const groupKey = `${productId}::${String(color ?? "").trim().toUpperCase()}`;
      groupCosts.set(groupKey, {
        productId,
        color,
        unitCost,
      });
    }

    const result = await this.prisma.$transaction(
      async (tx) => {
        for (const group of groupCosts.values()) {
          const groupedReceiptItems = receipt.items.filter((item) => {
            const itemProductId = item.productId ?? item.variant?.productId;
            const itemColor = item.color ?? item.variant?.color ?? null;

            return (
              String(itemProductId ?? "") === String(group.productId) &&
              String(itemColor ?? "").trim().toUpperCase() ===
                String(group.color ?? "").trim().toUpperCase()
            );
          });

          for (const item of groupedReceiptItems) {
            await tx.purchaseReceiptItem.update({
              where: { id: item.id },
              data: {
                unitCost: new Prisma.Decimal(group.unitCost),
                lineTotal: new Prisma.Decimal(
                  this.toNumber(item.qty) * group.unitCost,
                ),
              },
            });
          }

          // Chỉ ghi giá dương vào chi tiết sản phẩm để không vô tình xóa giá vốn cũ.
          if (group.unitCost > 0) {
            await tx.productVariant.updateMany({
              where: {
                productId: group.productId,
                color: group.color,
              },
              data: {
                costPrice: new Prisma.Decimal(group.unitCost),
              },
            });
          }
        }

        return tx.purchaseReceipt.findUnique({
          where: { id },
          include: this.getReceiptInclude(),
        });
      },
      {
        timeout: 30000,
        maxWait: 10000,
      },
    );

    if (!result) {
      throw new NotFoundException("Không tìm thấy phiếu nhập sau khi cập nhật giá");
    }

    return this.hydrateReceiptCosts(result);
  }


  async requestPayment(id: string, user?: any) {
    const receipt = await this.prisma.purchaseReceipt.findUnique({
      where: { id },
      include: {
        items: true,
        purchaseReceiptPayments: true,
      },
    });

    if (!receipt) {
      throw new NotFoundException("Không tìm thấy phiếu nhập");
    }

    this.ensureBranchAccess(user, receipt.branchId);

    if (receipt.status !== PurchaseReceiptStatus.DRAFT) {
      throw new BadRequestException("Chỉ xác nhận đủ hàng được từ phiếu nháp");
    }

    if (!receipt.items.length) {
      throw new BadRequestException("Phiếu nhập chưa có dòng hàng");
    }

    // Phiếu cũ chưa ghi giá nhập: lấy từ chi tiết sản phẩm nếu variant đã có costPrice.
    await this.fillMissingItemCostsFromProduct(id);

    const updated = await this.prisma.purchaseReceipt.update({
      where: { id },
      data: {
        status: PurchaseReceiptStatus.PAYMENT_REQUESTED,
      },
      include: this.getReceiptInclude(),
    });

    return this.hydrateReceiptCosts(updated);
  }

  async pay(id: string, data: PayReceiptInput = {}, user?: any) {
    let receipt = await this.prisma.purchaseReceipt.findUnique({
      where: { id },
      include: {
        supplier: true,
        items: true,
        purchaseReceiptPayments: true,
      },
    });

    if (!receipt) {
      throw new NotFoundException("Không tìm thấy phiếu nhập");
    }

    this.ensureBranchAccess(user, receipt.branchId);

    if (
      receipt.status !== PurchaseReceiptStatus.PAYMENT_REQUESTED &&
      receipt.status !== PurchaseReceiptStatus.PARTIALLY_PAID &&
      receipt.status !== PurchaseReceiptStatus.STOCK_IMPORTED
    ) {
      throw new BadRequestException("Phiếu chưa ở trạng thái chờ thanh toán");
    }

    // Khi thanh toán, ưu tiên giá trên phiếu; nếu phiếu còn 0 thì lấy giá nhập trong chi tiết sản phẩm.
    await this.fillMissingItemCostsFromProduct(id);

    receipt = await this.prisma.purchaseReceipt.findUnique({
      where: { id },
      include: {
        supplier: true,
        items: true,
        purchaseReceiptPayments: true,
      },
    });

    if (!receipt) {
      throw new NotFoundException("Không tìm thấy phiếu nhập");
    }

    const paymentReceipt = receipt;

    if (!paymentReceipt.items.length) {
      throw new BadRequestException("Phiếu nhập chưa có dòng hàng");
    }

    for (const item of paymentReceipt.items) {
      if (this.toNumber(item.unitCost) <= 0) {
        throw new BadRequestException(
          `SKU ${item.sku} chưa có giá nhập. Hãy nhập giá trước khi thanh toán`,
        );
      }
    }

    const totalAmount = this.getReceiptTotal(paymentReceipt);
    const paidBefore = this.getPaidTotal(paymentReceipt);
    const remainingAmount = Math.max(totalAmount - paidBefore, 0);
    const amount =
      data.amount === undefined || data.amount === null
        ? remainingAmount
        : this.toNumber(data.amount);

    if (!Number.isFinite(amount) || amount <= 0) {
      throw new BadRequestException("Số tiền thanh toán phải lớn hơn 0");
    }

    if (amount > remainingAmount) {
      throw new BadRequestException("Số tiền thanh toán vượt quá số tiền còn phải trả");
    }

    if (!data.paymentSourceId) {
      throw new BadRequestException("Vui lòng chọn nguồn tiền thanh toán");
    }

    const paymentSource = await this.prisma.paymentSource.findUnique({
      where: { id: data.paymentSourceId },
    });

    if (!paymentSource || !paymentSource.isActive) {
      throw new BadRequestException("Nguồn tiền không tồn tại hoặc đã ngừng hoạt động");
    }

    const result = await this.prisma.$transaction(async (tx) => {
      await tx.purchaseReceiptPayment.create({
        data: {
          receiptId: id,
          paymentSourceId: data.paymentSourceId || null,
          amount: new Prisma.Decimal(amount),
          note:
            data.note?.trim() ||
            `Thanh toán nhà cung cấp ${paymentReceipt.supplier?.name || ""} cho phiếu ${paymentReceipt.receiptCode}`.trim(),
          paidById: data.paidById || null,
          paidByName: data.paidByName || null,
          paidAt: new Date(),
        },
      });

      const paidAfter = paidBefore + amount;
      const nextStatus =
        paidAfter >= totalAmount
          ? PurchaseReceiptStatus.PAID
          : PurchaseReceiptStatus.PARTIALLY_PAID;

      return tx.purchaseReceipt.update({
        where: { id },
        data: {
          status: nextStatus,
        },
        include: this.getReceiptInclude(),
      });
    });

    return this.hydrateReceiptCosts(result);
  }

  async importStock(id: string, createdById?: string, user?: any) {
    let receipt = await this.prisma.purchaseReceipt.findUnique({
      where: { id },
      include: {
        items: true,
        purchaseReceiptPayments: true,
      },
    });

    if (!receipt) {
      throw new NotFoundException("Không tìm thấy phiếu nhập");
    }

    this.ensureBranchAccess(user, receipt.branchId);

    if (
      receipt.confirmedAt ||
      receipt.status === PurchaseReceiptStatus.STOCK_IMPORTED ||
      receipt.status === PurchaseReceiptStatus.COMPLETED
    ) {
      throw new BadRequestException("Phiếu này đã được nhập kho");
    }

    if (
      receipt.status !== PurchaseReceiptStatus.PAYMENT_REQUESTED &&
      receipt.status !== PurchaseReceiptStatus.PARTIALLY_PAID &&
      receipt.status !== PurchaseReceiptStatus.PAID
    ) {
      throw new BadRequestException("Phải xác nhận đủ hàng trước khi nhập kho");
    }

    if (!receipt.items.length) {
      throw new BadRequestException("Phiếu nhập chưa có dòng hàng");
    }

    // Có giá nhập trong chi tiết sản phẩm thì dùng luôn, nhưng thiếu giá vẫn được nhập kho.
    await this.fillMissingItemCostsFromProduct(id);

    receipt = await this.prisma.purchaseReceipt.findUnique({
      where: { id },
      include: {
        items: true,
        purchaseReceiptPayments: true,
      },
    });

    if (!receipt) {
      throw new NotFoundException("Không tìm thấy phiếu nhập");
    }

    const stockReceipt = receipt;

    const validCreatedById = await this.resolveInventoryMovementCreatedById(
      createdById || stockReceipt.createdById,
    );

    const result = await this.prisma.$transaction(
      async (tx) => {
        for (const item of stockReceipt.items) {
          await tx.inventoryItem.upsert({
            where: {
              variantId_branchId: {
                variantId: item.variantId,
                branchId: stockReceipt.branchId,
              },
            },
            update: {
              availableQty: {
                increment: item.qty,
              },
            },
            create: {
              variantId: item.variantId,
              branchId: stockReceipt.branchId,
              availableQty: item.qty,
              reservedQty: 0,
              incomingQty: 0,
            },
          });

          await tx.inventoryMovement.create({
            data: {
              variantId: item.variantId,
              type: InventoryMovementType.IMPORT,
              qty: item.qty,
              note: `Nhập kho từ phiếu ${stockReceipt.receiptCode}`,
              refType: "PURCHASE_RECEIPT",
              refId: stockReceipt.id,
              createdById: validCreatedById,
              branchId: stockReceipt.branchId,
            },
          });

          // Không được lấy giá 0 của phiếu ghi đè giá nhập đang có trong sản phẩm.
          const unitCost = this.toNumber(item.unitCost);
          if (unitCost > 0) {
            await tx.productVariant.update({
              where: { id: item.variantId },
              data: {
                costPrice: item.unitCost,
              },
            });
          }
        }

        // confirmedAt là mốc kho độc lập. Không đổi trạng thái tài chính ở đây.
        return tx.purchaseReceipt.update({
          where: { id: stockReceipt.id },
          data: {
            confirmedAt: new Date(),
          },
          include: this.getReceiptInclude(),
        });
      },
      {
        timeout: 30000,
        maxWait: 10000,
      },
    );

    return this.hydrateReceiptCosts(result);
  }

  async complete(id: string, user?: any) {
    const receipt = await this.prisma.purchaseReceipt.findUnique({
      where: { id },
      include: {
        items: true,
        purchaseReceiptPayments: true,
      },
    });

    if (!receipt) {
      throw new NotFoundException("Không tìm thấy phiếu nhập");
    }

    this.ensureBranchAccess(user, receipt.branchId);

    const stockImported =
      Boolean(receipt.confirmedAt) ||
      receipt.status === PurchaseReceiptStatus.STOCK_IMPORTED;

    if (!stockImported) {
      throw new BadRequestException("Chỉ hoàn tất được phiếu đã nhập kho");
    }

    // Bước hoàn tất vẫn là điểm kết thúc tài chính: phải thanh toán đủ.
    await this.fillMissingItemCostsFromProduct(id);

    const freshReceipt = await this.prisma.purchaseReceipt.findUnique({
      where: { id },
      include: {
        items: true,
        purchaseReceiptPayments: true,
      },
    });

    if (!freshReceipt) {
      throw new NotFoundException("Không tìm thấy phiếu nhập");
    }

    const totalAmount = this.getReceiptTotal(freshReceipt);
    const paidAmount = this.getPaidTotal(freshReceipt);

    if (totalAmount <= 0 || paidAmount < totalAmount) {
      throw new BadRequestException("Phiếu chưa thanh toán đủ, không thể hoàn tất");
    }

    const updated = await this.prisma.purchaseReceipt.update({
      where: { id },
      data: {
        status: PurchaseReceiptStatus.COMPLETED,
      },
      include: this.getReceiptInclude(),
    });

    return this.hydrateReceiptCosts(updated);
  }

  async cancel(id: string, user?: any) {
    const receipt = await this.prisma.purchaseReceipt.findUnique({
      where: { id },
      include: {
        purchaseReceiptPayments: true,
      },
    });

    if (!receipt) {
      throw new NotFoundException("Không tìm thấy phiếu nhập");
    }

    this.ensureBranchAccess(user, receipt.branchId);

    if (receipt.status !== PurchaseReceiptStatus.DRAFT) {
      throw new BadRequestException("Chỉ hủy được phiếu đang nháp/chưa nhập kho");
    }

    if (this.getPaidTotal(receipt) > 0) {
      throw new BadRequestException("Phiếu đã thanh toán, cần xử lý hoàn tiền trước khi hủy");
    }

    const updated = await this.prisma.purchaseReceipt.update({
      where: { id },
      data: { status: PurchaseReceiptStatus.CANCELLED },
      include: this.getReceiptInclude(),
    });

    return this.hydrateReceiptCosts(updated);
  }
}
