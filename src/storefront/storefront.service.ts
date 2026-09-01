import { BadRequestException, Injectable, UnauthorizedException } from "@nestjs/common";
import { FulfillmentStatus, OrderStatus, PaymentStatus, ProductStatus, SalesChannel, VariantStatus, Prisma } from "@prisma/client";
import { PrismaService } from "../prisma/prisma.service";
import { WebsiteCatalogService } from "../website-catalog/website-catalog.service";
import * as bcrypt from "bcrypt";
import * as jwt from "jsonwebtoken";
import { createHash, randomInt } from "crypto";

@Injectable()
export class StorefrontService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly websiteCatalog: WebsiteCatalogService,
  ) {}

  private accessSecret = process.env.CUSTOMER_JWT_SECRET || process.env.JWT_SECRET || "dev-customer-secret";
  private refreshSecret = process.env.CUSTOMER_JWT_REFRESH_SECRET || process.env.CUSTOMER_JWT_SECRET || process.env.JWT_REFRESH_SECRET || this.accessSecret;

  private normalizePhone(value: unknown) {
    let digits = String(value || "").replace(/\D/g, "");
    if (digits.startsWith("84") && digits.length >= 11) digits = `0${digits.slice(2)}`;
    return digits;
  }
  private normalizeEmail(value: unknown) { return String(value || "").trim().toLowerCase(); }
  private hashToken(value: string) { return createHash("sha256").update(value).digest("hex"); }
  private money(value: unknown) { const n = Number(value || 0); return Number.isFinite(n) ? Math.round(n) : 0; }

  async listProducts() {
    return this.websiteCatalog.publicList("VN");
  }

  async getProduct(slug: string) {
    return this.websiteCatalog.publicGet(slug, "VN");
  }

  private async issueTokens(account: any, meta?: { userAgent?: string; ipAddress?: string }) {
    const session = await this.prisma.customerSession.create({
      data: {
        accountId: account.id,
        refreshTokenHash: "pending",
        deviceInfo: String(meta?.userAgent || "").slice(0, 500) || null,
        ipAddress: String(meta?.ipAddress || "").slice(0, 80) || null,
        sessionVersion: account.sessionVersion || 1,
        expiresAt: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000),
      },
    });
    const accessToken = jwt.sign({ sub: account.id, sid: session.id, sv: account.sessionVersion || 1, customerId: account.customerId, type: "customer" }, this.accessSecret, { expiresIn: "15m" });
    const refreshToken = jwt.sign({ sub: account.id, sid: session.id, sv: account.sessionVersion || 1, type: "customer-refresh" }, this.refreshSecret, { expiresIn: "30d" });
    await this.prisma.customerSession.update({ where: { id: session.id }, data: { refreshTokenHash: this.hashToken(refreshToken) } });
    return { accessToken, refreshToken };
  }

  private publicAccount(account: any) {
    return { id: account.id, customerId: account.customerId, phone: account.phone, email: account.email, fullName: account.customer?.fullName || "" };
  }

  async register(body: any, meta?: { userAgent?: string; ipAddress?: string }) {
    const fullName = String(body?.fullName || "").trim();
    const phone = this.normalizePhone(body?.phone);
    const email = this.normalizeEmail(body?.email) || null;
    const password = String(body?.password || "");
    if (!fullName || !phone || password.length < 8) throw new BadRequestException("Thiếu tên, số điện thoại hoặc mật khẩu tối thiểu 8 ký tự.");

    const duplicate = await this.prisma.customerAccount.findFirst({ where: { OR: [{ phone }, ...(email ? [{ email }] : [])] } });
    if (duplicate) throw new BadRequestException("Số điện thoại hoặc email đã có tài khoản.");

    const existingCustomer = await this.prisma.customer.findFirst({ where: { OR: [{ phone }, ...(email ? [{ email }] : [])] } });
    if (existingCustomer && existingCustomer.totalOrders > 0) {
      throw new BadRequestException("CLAIM_VERIFICATION_REQUIRED");
    }

    const passwordHash = await bcrypt.hash(password, 10);
    const account = await this.prisma.$transaction(async (tx) => {
      const customer = existingCustomer
        ? await tx.customer.update({ where: { id: existingCustomer.id }, data: { fullName, phone, email: email || existingCustomer.email } })
        : await tx.customer.create({ data: { fullName, phone, email, source: "VN_WEB" } });
      return tx.customerAccount.create({
        data: { customerId: customer.id, phone, email, passwordHash },
        include: { customer: true },
      });
    });
    return { ...(await this.issueTokens(account, meta)), user: this.publicAccount(account) };
  }

  async login(body: any, meta?: { userAgent?: string; ipAddress?: string }) {
    const identifier = String(body?.identifier || body?.phone || body?.email || "").trim();
    const password = String(body?.password || "");
    if (!identifier || !password) throw new UnauthorizedException("Thiếu thông tin đăng nhập.");
    const phone = this.normalizePhone(identifier);
    const email = this.normalizeEmail(identifier);
    const account = await this.prisma.customerAccount.findFirst({
      where: { OR: [{ phone }, { email }] }, include: { customer: true },
    });
    if (!account?.isActive || !account.passwordHash || !(await bcrypt.compare(password, account.passwordHash))) {
      throw new UnauthorizedException("Số điện thoại/email hoặc mật khẩu không đúng.");
    }
    await this.prisma.customerAccount.update({ where: { id: account.id }, data: { lastLoginAt: new Date() } });
    return { ...(await this.issueTokens(account, meta)), user: this.publicAccount(account) };
  }

  async refresh(refreshToken: string) {
    if (!refreshToken) throw new UnauthorizedException("Missing refresh token");
    let payload: any;
    try { payload = jwt.verify(refreshToken, this.refreshSecret); } catch { throw new UnauthorizedException("Phiên đăng nhập đã hết hạn."); }
    if (payload?.type !== "customer-refresh" || !payload.sid) throw new UnauthorizedException("Refresh token không hợp lệ.");
    const session = await this.prisma.customerSession.findUnique({ where: { id: String(payload.sid) }, include: { account: { include: { customer: true } } } });
    if (!session || session.revokedAt || session.expiresAt <= new Date() || session.refreshTokenHash !== this.hashToken(refreshToken) || !session.account?.isActive) {
      throw new UnauthorizedException("Phiên đăng nhập không hợp lệ.");
    }
    await this.prisma.customerSession.update({ where: { id: session.id }, data: { revokedAt: new Date() } });
    const tokens = await this.issueTokens(session.account);
    return { ...tokens, user: this.publicAccount(session.account) };
  }

  async logout(refreshToken?: string) {
    if (!refreshToken) return { success: true };
    try {
      const payload: any = jwt.verify(refreshToken, this.refreshSecret);
      if (payload?.sid) await this.prisma.customerSession.update({ where: { id: String(payload.sid) }, data: { revokedAt: new Date() } }).catch(() => null);
    } catch {}
    return { success: true };
  }

  async me(customerId: string) {
    const account = await this.prisma.customerAccount.findUnique({ where: { customerId }, include: { customer: true } });
    if (!account) throw new UnauthorizedException();
    return this.publicAccount(account);
  }

  async createOrder(body: any, customerId?: string | null) {
    const branchId = String(process.env.STOREFRONT_VN_BRANCH_ID || "").trim();
    if (!branchId) throw new BadRequestException("STOREFRONT_VN_BRANCH_ID chưa được cấu hình.");
    const items = Array.isArray(body?.items) ? body.items : [];
    if (!items.length) throw new BadRequestException("Giỏ hàng trống.");

    const ids: string[] = Array.from(
      new Set<string>(
        items
          .map((i: any) => String(i?.variantId || "").trim())
          .filter((id: string) => id.length > 0),
      ),
    );

    const variants = await this.prisma.productVariant.findMany({
      where: {
        id: { in: ids },
        status: VariantStatus.ACTIVE,
        product: { status: ProductStatus.ACTIVE },
      },
      include: {
        product: true,
        inventoryItems: { where: { branchId } },
      },
    });

    type CheckoutVariant = (typeof variants)[number];
    const map = new Map<string, CheckoutVariant>(
      variants.map((v) => [v.id, v]),
    );
    const prepared = items.map((i: any) => {
      const v = map.get(String(i.variantId || ""));
      const qty = Math.max(1, Math.floor(Number(i.qty ?? i.quantity ?? 1)));
      if (!v) throw new BadRequestException("Có sản phẩm không còn bán.");
      const stock = v.inventoryItems.reduce((s, x) => s + Math.max(0, Number(x.availableQty || 0) - Number(x.reservedQty || 0)), 0);
      if (stock < qty) throw new BadRequestException(`${v.product.name} không đủ tồn kho.`);
      const unitPrice = this.money(v.price);
      return { variant: v, qty, unitPrice, lineTotal: unitPrice * qty };
    });

    const customerName = String(body?.customerName || body?.shippingRecipientName || "").trim();
    const phone = this.normalizePhone(body?.customerPhone || body?.shippingPhone);
    if (!customerName || !phone) throw new BadRequestException("Thiếu tên hoặc số điện thoại nhận hàng.");

    const totalAmount = prepared.reduce((s, i) => s + i.lineTotal, 0);
    const shippingFee = Math.max(0, this.money(body?.shippingFee));
    const discountAmount = 0;
    const finalAmount = totalAmount + shippingFee - discountAmount;

    return this.prisma.$transaction(async (tx) => {
      let resolvedCustomerId = customerId || null;
      if (!resolvedCustomerId) {
        const existing = await tx.customer.findFirst({ where: { phone } });
        const customer = existing
          ? await tx.customer.update({ where: { id: existing.id }, data: { fullName: customerName, email: this.normalizeEmail(body?.shippingEmail) || existing.email, source: "VN_WEB" } })
          : await tx.customer.create({ data: { fullName: customerName, phone, email: this.normalizeEmail(body?.shippingEmail) || null, source: "VN_WEB" } });
        resolvedCustomerId = customer.id;
      }

      const order = await tx.order.create({
        data: {
          orderCode: `WEB-${Date.now()}-${randomInt(100, 999)}`,
          salesChannel: SalesChannel.VN_WEB,
          customerId: resolvedCustomerId,
          customerName, customerPhone: phone, branchId, currency: "VND",
          totalAmount: new Prisma.Decimal(totalAmount), discountAmount: new Prisma.Decimal(discountAmount), shippingFee: new Prisma.Decimal(shippingFee), finalAmount: new Prisma.Decimal(finalAmount),
          paymentStatus: PaymentStatus.UNPAID, fulfillmentStatus: FulfillmentStatus.UNFULFILLED, status: OrderStatus.NEW,
          source: "STOREFRONT_VN",
          shippingRecipientName: customerName,
          shippingPhone: phone,
          shippingEmail: this.normalizeEmail(body?.shippingEmail) || null,
          shippingAddressLine1: String(body?.shippingAddressLine1 || "").trim() || null,
          shippingAddressLine2: String(body?.shippingAddressLine2 || "").trim() || null,
          shippingWard: String(body?.shippingWard || "").trim() || null,
          shippingDistrict: String(body?.shippingDistrict || "").trim() || null,
          shippingProvince: String(body?.shippingProvince || "").trim() || null,
          shippingCountry: "Vietnam",
          shippingGhnDistrictId: Number(body?.shippingGhnDistrictId || 0) || null,
          shippingGhnWardCode: String(body?.shippingGhnWardCode || "").trim() || null,
          note: String(body?.note || "").trim() || null,
          items: { create: prepared.map((i) => ({ variantId: i.variant.id, sku: i.variant.sku, productName: i.variant.product.name, color: i.variant.color, size: i.variant.size, qty: i.qty, unitPrice: new Prisma.Decimal(i.unitPrice), lineTotal: new Prisma.Decimal(i.lineTotal) })) },
        },
        include: { items: true, shipment: true },
      });
      await tx.customer.update({ where: { id: resolvedCustomerId! }, data: { totalOrders: { increment: 1 }, totalSpent: { increment: new Prisma.Decimal(finalAmount) }, lastOrderAt: new Date() } });
      return this.publicOrder(order);
    });
  }

  async listMyOrders(customerId: string) {
    const rows = await this.prisma.order.findMany({ where: { customerId }, orderBy: { createdAt: "desc" }, include: { items: true, shipment: true } });
    return rows.map((x) => this.publicOrder(x));
  }
  async getMyOrder(customerId: string, idOrCode: string) {
    const row = await this.prisma.order.findFirst({ where: { customerId, OR: [{ id: idOrCode }, { orderCode: idOrCode }] }, include: { items: true, shipment: { include: { trackingEvents: { orderBy: { eventTime: "desc" }, take: 30 } } } } });
    return row ? this.publicOrder(row) : null;
  }

  private publicOrder(order: any) {
    const carrier = String(order.shipment?.carrier || "").toUpperCase();
    const trackingCode = String(order.shipment?.trackingCode || "").trim();
    const trackingUrl = carrier.includes("GHN") && trackingCode
      ? `https://donhang.ghn.vn/?order_code=${encodeURIComponent(trackingCode)}`
      : carrier.includes("AHAMOVE") ? String(order.shipment?.ahamoveTrackingUrl || "") : "";
    return {
      id: order.id, orderCode: order.orderCode, createdAt: order.createdAt, status: order.status,
      paymentStatus: order.paymentStatus, fulfillmentStatus: order.fulfillmentStatus,
      totalAmount: this.money(order.totalAmount), discountAmount: this.money(order.discountAmount), shippingFee: this.money(order.shippingFee), finalAmount: this.money(order.finalAmount),
      shippingRecipientName: order.shippingRecipientName, shippingPhone: order.shippingPhone,
      shippingAddressLine1: order.shippingAddressLine1, shippingWard: order.shippingWard, shippingDistrict: order.shippingDistrict, shippingProvince: order.shippingProvince,
      items: (order.items || []).map((i: any) => ({ id: i.id, variantId: i.variantId, sku: i.sku, productName: i.productName, color: i.color, size: i.size, qty: i.qty, unitPrice: this.money(i.unitPrice), lineTotal: this.money(i.lineTotal) })),
      shipment: order.shipment ? { carrier: order.shipment.carrier, trackingCode, shippingStatus: order.shipment.shippingStatus, partnerStatus: order.shipment.partnerStatus, trackingUrl, trackingEvents: order.shipment.trackingEvents || [] } : null,
    };
  }
}
