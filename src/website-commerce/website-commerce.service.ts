import { BadRequestException, Injectable } from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";
import { WebsiteSettingsService } from "../website-settings/website-settings.service";

type CheckoutItem = { variantId: string; qty: number };

@Injectable()
export class WebsiteCommerceService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly settings: WebsiteSettingsService,
  ) {}

  private phoneVariants(input: string) {
    const digits = String(input || "").replace(/\D/g, "");
    if (!digits) return [];
    const local = digits.startsWith("84") ? `0${digits.slice(2)}` : digits;
    const intl = local.startsWith("0") ? `84${local.slice(1)}` : local;
    return [...new Set([digits, local, `+${intl}`, intl])];
  }

  private async customerByPhone(phone: string) {
    const variants = this.phoneVariants(phone);
    if (!variants.length) return null;
    return this.prisma.customer.findFirst({
      where: {
        OR: variants.map((x) => ({ phone: x })),
      },
      orderBy: { updatedAt: "desc" },
    });
  }

  async customerStatus(phone: string, market = "VN") {
    const customer = await this.customerByPhone(phone);
    const cfg = await this.settings.get(market);
    const isNewCustomer = !customer;
    const discountPercent =
      isNewCustomer && cfg.newCustomerDiscountEnabled
        ? Number(cfg.newCustomerDiscountPercent || 0)
        : 0;

    return {
      exists: !!customer,
      isNewCustomer,
      customer: customer
        ? {
            id: customer.id,
            fullName: customer.fullName,
            phone: customer.phone,
            totalOrders: customer.totalOrders,
            totalSpent: Number(customer.totalSpent || 0),
            lastOrderAt: customer.lastOrderAt,
          }
        : null,
      discountPercent,
    };
  }

  private async priceItems(items: CheckoutItem[]) {
    if (!Array.isArray(items) || !items.length) {
      throw new BadRequestException("Giỏ hàng đang trống.");
    }

    const ids = [...new Set(items.map((x) => String(x.variantId || "")).filter(Boolean))];
    const variants = await this.prisma.productVariant.findMany({
      where: { id: { in: ids }, status: "ACTIVE" },
      include: {
        product: true,
        inventoryItems: true,
      },
    });

    const byId = new Map(variants.map((x) => [x.id, x]));
    return items.map((item) => {
      const variant = byId.get(String(item.variantId || ""));
      const qty = Math.max(1, Math.floor(Number(item.qty || 1)));
      if (!variant) throw new BadRequestException("Có sản phẩm trong giỏ không còn hợp lệ.");

      const available = variant.inventoryItems.reduce(
        (sum, x) =>
          sum + Math.max(0, Number(x.availableQty || 0) - Number(x.reservedQty || 0)),
        0,
      );
      if (available < qty) {
        throw new BadRequestException(
          `${variant.product.name} ${variant.color || ""} ${variant.size || ""} không đủ tồn.`,
        );
      }

      const unitPrice = Number(variant.price || 0);
      return {
        variant,
        qty,
        unitPrice,
        lineTotal: unitPrice * qty,
      };
    });
  }

  async quote(body: any) {
    const market = String(body?.market || "VN").toUpperCase();
    const cfg = await this.settings.get(market);
    if (!cfg.cartEnabled) throw new BadRequestException("Giỏ hàng website đang tắt.");

    const priced = await this.priceItems(body?.items || []);
    const customerStatus = await this.customerStatus(body?.phone || "", market);
    const subtotal = priced.reduce((sum, x) => sum + x.lineTotal, 0);
    const discountPercent = customerStatus.discountPercent;
    const discountAmount = Math.round((subtotal * discountPercent) / 100);
    const finalAmount = Math.max(0, subtotal - discountAmount);

    return {
      subtotal,
      discountPercent,
      discountAmount,
      shippingFee: 0,
      finalAmount,
      isNewCustomer: customerStatus.isNewCustomer,
      customer: customerStatus.customer,
    };
  }

  async createOrder(body: any) {
    const market = String(body?.market || "VN").toUpperCase();
    const cfg = await this.settings.get(market);
    if (!cfg.cartEnabled || !cfg.checkoutEnabled) {
      throw new BadRequestException("Checkout website đang tắt.");
    }

    const phone = String(body?.phone || "").trim();
    const fullName = String(body?.fullName || "").trim();
    const addressLine1 = String(body?.addressLine1 || "").trim();
    if (!phone || !fullName || !addressLine1) {
      throw new BadRequestException("Thiếu họ tên, số điện thoại hoặc địa chỉ.");
    }

    const priced = await this.priceItems(body?.items || []);
    const status = await this.customerStatus(phone, market);
    const subtotal = priced.reduce((sum, x) => sum + x.lineTotal, 0);
    const discountPercent = status.discountPercent;
    const discountAmount = Math.round((subtotal * discountPercent) / 100);
    const finalAmount = Math.max(0, subtotal - discountAmount);

    const customer =
      (status.customer &&
        (await this.prisma.customer.findUnique({ where: { id: status.customer.id } }))) ||
      (await this.prisma.customer.create({
        data: {
          fullName,
          phone,
          source: "WEBSITE",
        },
      }));

    const orderCode = `WEB-${Date.now().toString(36).toUpperCase()}-${Math.random()
      .toString(36)
      .slice(2, 6)
      .toUpperCase()}`;

    const order = await this.prisma.order.create({
      data: {
        orderCode,
        salesChannel: market === "INTERNATIONAL" ? "INTL_WEB" : "VN_WEB",
        customerId: customer.id,
        customerName: fullName,
        customerPhone: phone,
        branchId: cfg.defaultBranchId || null,
        currency: market === "INTERNATIONAL" ? "USD" : "VND",
        totalAmount: subtotal,
        discountAmount,
        shippingFee: 0,
        finalAmount,
        paymentStatus: "UNPAID",
        fulfillmentStatus: "UNFULFILLED",
        status: "NEW",
        source: "WEBSITE",
        note: String(body?.note || "").trim() || null,
        shippingRecipientName: fullName,
        shippingPhone: phone,
        shippingEmail: String(body?.email || "").trim() || null,
        shippingAddressLine1: addressLine1,
        shippingAddressLine2: String(body?.addressLine2 || "").trim() || null,
        shippingWard: String(body?.ward || "").trim() || null,
        shippingDistrict: String(body?.district || "").trim() || null,
        shippingProvince: String(body?.province || "").trim() || null,
        shippingCountry: "Vietnam",
        items: {
          create: priced.map(({ variant, qty, unitPrice, lineTotal }) => ({
            variantId: variant.id,
            sku: variant.sku,
            productName: variant.product.name,
            color: variant.color,
            size: variant.size,
            qty,
            unitPrice,
            lineTotal,
          })),
        },
      },
      include: { items: true },
    });

    return {
      id: order.id,
      orderCode: order.orderCode,
      subtotal,
      discountPercent,
      discountAmount,
      finalAmount,
      messengerUrl: cfg.messengerEnabled ? cfg.messengerUrl : null,
      zaloUrl: cfg.zaloEnabled ? cfg.zaloUrl : null,
    };
  }
}
