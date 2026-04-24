import { BadRequestException, Injectable } from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";

type CustomerAddressInput = {
  addressLine1?: string;
  addressLine2?: string;
  ward?: string;
  district?: string;
  city?: string;
  province?: string;
  country?: string;
  postalCode?: string;
  label?: string;
  recipientName?: string;
  phone?: string;
  email?: string;
  isDefaultAddress?: boolean;
};

type CreateCustomerInput = {
  legacyCode?: string;
  fullName: string;
  phone?: string;
  email?: string;
  source?: string;
  customerGroup?: string;
  gender?: string;
  birthDate?: string;
  points?: number;

  totalOrders?: number;
  totalSpent?: number;
  lastOrderAt?: string;

  defaultDiscountPercent?: number;
  pricePolicyName?: string;
  customerNote?: string;

  lastImportedSource?: string;

  addressLine1?: string;
  addressLine2?: string;
  ward?: string;
  district?: string;
  city?: string;
  province?: string;
  country?: string;
  postalCode?: string;
  label?: string;
  recipientName?: string;
  isDefaultAddress?: boolean;
};

type UpdateCustomerInput = {
  legacyCode?: string;
  fullName?: string;
  phone?: string;
  email?: string;
  source?: string;
  customerGroup?: string;
  gender?: string;
  birthDate?: string;
  points?: number;

  totalOrders?: number;
  totalSpent?: number;
  lastOrderAt?: string;

  defaultDiscountPercent?: number;
  pricePolicyName?: string;
  customerNote?: string;

  lastImportedSource?: string;

  addressLine1?: string;
  addressLine2?: string;
  ward?: string;
  district?: string;
  city?: string;
  province?: string;
  country?: string;
  postalCode?: string;
  label?: string;
  recipientName?: string;
  isDefaultAddress?: boolean;
};

type ExistingAddress = {
  id: string;
  label: string | null;
  recipientName: string | null;
  phone: string | null;
  email?: string | null;
  addressLine1: string;
  addressLine2: string | null;
  ward: string | null;
  district: string | null;
  city: string | null;
  province: string | null;
  country: string | null;
  postalCode: string | null;
  isDefault: boolean;
  createdAt: Date;
  updatedAt: Date;
  customerId: string;
};

type CreateAddressInput = {
  label?: string;
  recipientName?: string;
  phone?: string;
  email?: string;
  addressLine1: string;
  addressLine2?: string;
  ward?: string;
  district?: string;
  city?: string;
  province?: string;
  country?: string;
  postalCode?: string;
  isDefault?: boolean;
};

type UpdateAddressInput = {
  label?: string;
  recipientName?: string;
  phone?: string;
  email?: string;
  addressLine1?: string;
  addressLine2?: string;
  ward?: string;
  district?: string;
  city?: string;
  province?: string;
  country?: string;
  postalCode?: string;
  isDefault?: boolean;
};

@Injectable()
export class CustomerService {
  constructor(private prisma: PrismaService) {}

  private normalizePhone(phone?: string | null) {
    if (!phone) return null;

    let cleaned = String(phone).replace(/[^\d+]/g, "").trim();

    if (cleaned.startsWith("+84")) {
      cleaned = "0" + cleaned.slice(3);
    }

    return cleaned || null;
  }

  private normalizeEmail(email?: string | null) {
    if (!email) return null;
    const cleaned = String(email).trim().toLowerCase();
    return cleaned || null;
  }

  private normalizeString(value?: string | null) {
    if (value === undefined || value === null) return null;
    const cleaned = String(value).trim();
    return cleaned || null;
  }

  private parseBirthDate(value?: string | null) {
    if (!value) return null;

    const direct = new Date(value);
    if (!Number.isNaN(direct.getTime())) return direct;

    const parts = String(value).split(/[\/\-]/);
    if (parts.length === 3) {
      const [d, m, y] = parts.map(Number);
      if (d && m && y) {
        const parsed = new Date(y, m - 1, d);
        if (!Number.isNaN(parsed.getTime())) return parsed;
      }
    }

    return null;
  }

  private parseDate(value?: string | null) {
    if (!value) return null;

    const direct = new Date(value);
    if (!Number.isNaN(direct.getTime())) return direct;

    const parts = String(value).split(/[\/\-]/);
    if (parts.length === 3) {
      const [d, m, y] = parts.map(Number);
      if (d && m && y) {
        const parsed = new Date(y, m - 1, d);
        if (!Number.isNaN(parsed.getTime())) return parsed;
      }
    }

    return null;
  }

  private parseNumber(value: unknown) {
    if (value === undefined || value === null || value === "") return null;
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  private serializeAddress(item: any) {
    return {
      id: item.id,
      label: item.label,
      recipientName: item.recipientName,
      phone: item.phone,
      email: item.email ?? null,
      addressLine1: item.addressLine1,
      addressLine2: item.addressLine2,
      ward: item.ward,
      district: item.district,
      city: item.city,
      province: item.province,
      country: item.country,
      postalCode: item.postalCode,
      isDefault: item.isDefault,
      createdAt: item.createdAt.toISOString(),
      updatedAt: item.updatedAt.toISOString(),
    };
  }

  private serializeCustomer(item: any) {
    return {
      id: item.id,
      legacyCode: item.legacyCode,
      fullName: item.fullName,
      phone: item.phone,
      email: item.email,
      source: item.source,
      customerGroup: item.customerGroup,
      gender: item.gender,
      birthDate: item.birthDate ? item.birthDate.toISOString() : null,
      points: item.points,
      totalSpent: Number(item.totalSpent || 0),
      totalOrders: item.totalOrders,
      lastOrderAt: item.lastOrderAt ? item.lastOrderAt.toISOString() : null,
      defaultDiscountPercent:
        item.defaultDiscountPercent !== null &&
        item.defaultDiscountPercent !== undefined
          ? Number(item.defaultDiscountPercent)
          : null,
      pricePolicyName: item.pricePolicyName,
      customerNote: item.customerNote,
      lastImportedAt: item.lastImportedAt
        ? item.lastImportedAt.toISOString()
        : null,
      lastImportedSource: item.lastImportedSource,
      createdAt: item.createdAt.toISOString(),
      updatedAt: item.updatedAt.toISOString(),
      addresses: Array.isArray(item.addresses)
        ? item.addresses.map((address: any) => this.serializeAddress(address))
        : [],
    };
  }

  private buildCustomerUpdateData(
    existing: any,
    data: Partial<CreateCustomerInput | UpdateCustomerInput>,
    mode: "create_merge" | "manual_update"
  ) {
    const normalizedLegacyCode = this.normalizeString(data.legacyCode);
    const normalizedFullName = this.normalizeString(data.fullName);
    const normalizedPhone = this.normalizePhone(data.phone);
    const normalizedEmail = this.normalizeEmail(data.email);
    const normalizedSource = this.normalizeString(data.source);
    const normalizedCustomerGroup = this.normalizeString(data.customerGroup);
    const normalizedGender = this.normalizeString(data.gender);
    const parsedBirthDate = this.parseBirthDate(data.birthDate);

    const parsedPoints =
      data.points !== undefined ? Number(data.points) || 0 : undefined;
    const parsedTotalOrders =
      data.totalOrders !== undefined ? Number(data.totalOrders) || 0 : undefined;
    const parsedTotalSpent =
      data.totalSpent !== undefined
        ? this.parseNumber(data.totalSpent) ?? 0
        : undefined;
    const parsedLastOrderAt =
      data.lastOrderAt !== undefined ? this.parseDate(data.lastOrderAt) : undefined;

    const parsedDefaultDiscountPercent =
      data.defaultDiscountPercent !== undefined
        ? this.parseNumber(data.defaultDiscountPercent)
        : undefined;

    const normalizedPricePolicyName =
      data.pricePolicyName !== undefined
        ? this.normalizeString(data.pricePolicyName)
        : undefined;

    const normalizedCustomerNote =
      data.customerNote !== undefined
        ? this.normalizeString(data.customerNote)
        : undefined;

    const normalizedLastImportedSource =
      data.lastImportedSource !== undefined
        ? this.normalizeString(data.lastImportedSource)
        : undefined;

    return {
      legacyCode:
        normalizedLegacyCode !== null ? normalizedLegacyCode : existing.legacyCode,

      fullName:
        normalizedFullName !== null ? normalizedFullName : existing.fullName,

      phone: normalizedPhone !== null ? normalizedPhone : existing.phone,

      email: normalizedEmail !== null ? normalizedEmail : existing.email,

      source: normalizedSource !== null ? normalizedSource : existing.source,

      customerGroup:
        normalizedCustomerGroup !== null
          ? normalizedCustomerGroup
          : existing.customerGroup,

      gender:
        normalizedGender !== null ? normalizedGender : existing.gender,

      birthDate:
        parsedBirthDate !== null ? parsedBirthDate : existing.birthDate,

      points: parsedPoints !== undefined ? parsedPoints : existing.points,

      totalOrders:
        parsedTotalOrders !== undefined ? parsedTotalOrders : existing.totalOrders,

      totalSpent:
        parsedTotalSpent !== undefined ? parsedTotalSpent : existing.totalSpent,

      lastOrderAt:
        parsedLastOrderAt !== undefined ? parsedLastOrderAt : existing.lastOrderAt,

      defaultDiscountPercent:
        parsedDefaultDiscountPercent !== undefined
          ? parsedDefaultDiscountPercent
          : existing.defaultDiscountPercent,

      pricePolicyName:
        normalizedPricePolicyName !== undefined
          ? normalizedPricePolicyName
          : existing.pricePolicyName,

      customerNote:
        normalizedCustomerNote !== undefined
          ? normalizedCustomerNote
          : existing.customerNote,

      lastImportedAt: new Date(),

      lastImportedSource:
        normalizedLastImportedSource !== undefined
          ? normalizedLastImportedSource
          : mode === "manual_update"
          ? "MANUAL_UPDATE"
          : existing.lastImportedSource,
    };
  }

  private async ensureCustomerExists(id: string) {
    const customer = await this.prisma.customer.findUnique({
      where: { id },
      include: {
        addresses: {
          orderBy: [{ isDefault: "desc" }, { updatedAt: "desc" }],
        },
      },
    });

    if (!customer) {
      throw new BadRequestException("Không tìm thấy khách hàng");
    }

    return customer;
  }

  private async findExistingCustomer(data: {
    legacyCode?: string | null;
    phone?: string | null;
    email?: string | null;
  }) {
    const orConditions: any[] = [];
    if (data.legacyCode) orConditions.push({ legacyCode: data.legacyCode });
    if (data.phone) orConditions.push({ phone: data.phone });
    if (data.email) orConditions.push({ email: data.email });

    if (!orConditions.length) return null;

    return this.prisma.customer.findFirst({
      where: { OR: orConditions },
      include: { addresses: true },
      orderBy: { updatedAt: "desc" },
    });
  }

  async findAll() {
    const rows = await this.prisma.customer.findMany({
      orderBy: { updatedAt: "desc" },
      include: {
        addresses: {
          orderBy: [{ isDefault: "desc" }, { updatedAt: "desc" }],
        },
      },
    });

    return rows.map((item) => this.serializeCustomer(item));
  }

  async findOne(id: string) {
    const item = await this.prisma.customer.findUnique({
      where: { id },
      include: {
        addresses: {
          orderBy: [{ isDefault: "desc" }, { updatedAt: "desc" }],
        },
      },
    });

    if (!item) {
      throw new BadRequestException("Không tìm thấy khách hàng");
    }

    return this.serializeCustomer(item);
  }

  async findByPhone(phone: string) {
    const cleaned = this.normalizePhone(phone);
    if (!cleaned) return null;

    const item = await this.prisma.customer.findFirst({
      where: { phone: cleaned },
      orderBy: { updatedAt: "desc" },
      include: {
        addresses: {
          orderBy: [{ isDefault: "desc" }, { updatedAt: "desc" }],
        },
      },
    });

    if (!item) return null;

    return this.serializeCustomer(item);
  }

  async getAddresses(customerId: string) {
    await this.ensureCustomerExists(customerId);

    const rows = await this.prisma.customerAddress.findMany({
      where: { customerId },
      orderBy: [{ isDefault: "desc" }, { updatedAt: "desc" }],
    });

    return rows.map((item) => this.serializeAddress(item));
  }

  async createAddress(customerId: string, data: CreateAddressInput) {
    const customer = await this.ensureCustomerExists(customerId);

    const addressLine1 = this.normalizeString(data.addressLine1);
    if (!addressLine1) {
      throw new BadRequestException("Thiếu địa chỉ cụ thể");
    }

    const recipientName =
      this.normalizeString(data.recipientName) || customer.fullName;
    const phone = this.normalizePhone(data.phone) || customer.phone || null;
    const email = this.normalizeEmail(data.email) || customer.email || null;
    const label = this.normalizeString(data.label) || "Địa chỉ mới";
    const addressLine2 = this.normalizeString(data.addressLine2);
    const ward = this.normalizeString(data.ward);
    const district = this.normalizeString(data.district);
    const city = this.normalizeString(data.city);
    const province = this.normalizeString(data.province);
    const country = this.normalizeString(data.country) || "Vietnam";
    const postalCode = this.normalizeString(data.postalCode);
    const isDefault = data.isDefault ?? !customer.addresses.length;

    return this.prisma.$transaction(async (tx) => {
      if (isDefault) {
        await tx.customerAddress.updateMany({
          where: { customerId, isDefault: true },
          data: { isDefault: false },
        });
      }

      const created = await tx.customerAddress.create({
        data: {
          customerId,
          label,
          recipientName,
          phone,
          email,
          addressLine1,
          addressLine2,
          ward,
          district,
          city,
          province,
          country,
          postalCode,
          isDefault,
        },
      });

      return this.serializeAddress(created);
    });
  }

  async updateAddress(
    customerId: string,
    addressId: string,
    data: UpdateAddressInput
  ) {
    await this.ensureCustomerExists(customerId);

    const existing = await this.prisma.customerAddress.findFirst({
      where: { id: addressId, customerId },
    });

    if (!existing) {
      throw new BadRequestException("Không tìm thấy địa chỉ");
    }

    const nextAddressLine1 =
      data.addressLine1 !== undefined
        ? this.normalizeString(data.addressLine1)
        : existing.addressLine1;

    if (!nextAddressLine1) {
      throw new BadRequestException("Thiếu địa chỉ cụ thể");
    }

    const nextData = {
      label:
        data.label !== undefined
          ? this.normalizeString(data.label) || "Địa chỉ"
          : existing.label,
      recipientName:
        data.recipientName !== undefined
          ? this.normalizeString(data.recipientName)
          : existing.recipientName,
      phone:
        data.phone !== undefined
          ? this.normalizePhone(data.phone)
          : existing.phone,
      email:
        data.email !== undefined
          ? this.normalizeEmail(data.email)
          : (existing as any).email ?? null,
      addressLine1: nextAddressLine1,
      addressLine2:
        data.addressLine2 !== undefined
          ? this.normalizeString(data.addressLine2)
          : existing.addressLine2,
      ward:
        data.ward !== undefined ? this.normalizeString(data.ward) : existing.ward,
      district:
        data.district !== undefined
          ? this.normalizeString(data.district)
          : existing.district,
      city:
        data.city !== undefined ? this.normalizeString(data.city) : existing.city,
      province:
        data.province !== undefined
          ? this.normalizeString(data.province)
          : existing.province,
      country:
        data.country !== undefined
          ? this.normalizeString(data.country) || "Vietnam"
          : existing.country,
      postalCode:
        data.postalCode !== undefined
          ? this.normalizeString(data.postalCode)
          : existing.postalCode,
      isDefault:
        data.isDefault !== undefined ? data.isDefault : existing.isDefault,
    };

    return this.prisma.$transaction(async (tx) => {
      if (nextData.isDefault) {
        await tx.customerAddress.updateMany({
          where: {
            customerId,
            isDefault: true,
            NOT: { id: addressId },
          },
          data: { isDefault: false },
        });
      }

      const updated = await tx.customerAddress.update({
        where: { id: addressId },
        data: nextData,
      });

      return this.serializeAddress(updated);
    });
  }

  async setDefaultAddress(customerId: string, addressId: string) {
    await this.ensureCustomerExists(customerId);

    const existing = await this.prisma.customerAddress.findFirst({
      where: { id: addressId, customerId },
    });

    if (!existing) {
      throw new BadRequestException("Không tìm thấy địa chỉ");
    }

    await this.prisma.$transaction(async (tx) => {
      await tx.customerAddress.updateMany({
        where: { customerId, isDefault: true },
        data: { isDefault: false },
      });

      await tx.customerAddress.update({
        where: { id: addressId },
        data: { isDefault: true },
      });
    });

    const refreshed = await this.prisma.customerAddress.findMany({
      where: { customerId },
      orderBy: [{ isDefault: "desc" }, { updatedAt: "desc" }],
    });

    return refreshed.map((item) => this.serializeAddress(item));
  }

  async createCustomer(data: CreateCustomerInput) {
    const legacyCode = this.normalizeString(data.legacyCode);
    const fullName = this.normalizeString(data.fullName);
    const phone = this.normalizePhone(data.phone);
    const email = this.normalizeEmail(data.email);
    const source = this.normalizeString(data.source) || "ADMIN";
    const customerGroup = this.normalizeString(data.customerGroup);
    const gender = this.normalizeString(data.gender);
    const birthDate = this.parseBirthDate(data.birthDate);
    const points =
      data.points !== undefined && data.points !== null
        ? Number(data.points) || 0
        : 0;

    const defaultDiscountPercent = this.parseNumber(data.defaultDiscountPercent);
    const totalOrders = this.parseNumber(data.totalOrders);
    const totalSpent = this.parseNumber(data.totalSpent);
    const lastOrderAt = this.parseDate(data.lastOrderAt);
    const pricePolicyName = this.normalizeString(data.pricePolicyName);
    const customerNote = this.normalizeString(data.customerNote);
    const lastImportedSource =
      this.normalizeString(data.lastImportedSource) || "MANUAL_CREATE";

    if (!fullName) {
      throw new BadRequestException("Thiếu tên khách hàng");
    }

    if (!legacyCode && !phone && !email) {
      throw new BadRequestException(
        "Cần ít nhất mã khách hàng, số điện thoại hoặc email"
      );
    }

    const existing = await this.findExistingCustomer({
      legacyCode,
      phone,
      email,
    });

    if (existing) {
      const updated = await this.prisma.customer.update({
        where: { id: existing.id },
        data: {
          legacyCode: legacyCode ?? existing.legacyCode,
          fullName: fullName ?? existing.fullName,
          phone: phone ?? existing.phone,
          email: email ?? existing.email,
          source: source ?? existing.source,
          customerGroup: customerGroup ?? existing.customerGroup,
          gender: gender ?? existing.gender,
          birthDate: birthDate ?? existing.birthDate,
          points,
          totalOrders: totalOrders ?? existing.totalOrders,
          totalSpent: totalSpent ?? existing.totalSpent,
          lastOrderAt: lastOrderAt ?? existing.lastOrderAt,
          defaultDiscountPercent:
            defaultDiscountPercent ?? existing.defaultDiscountPercent,
          pricePolicyName: pricePolicyName ?? existing.pricePolicyName,
          customerNote: customerNote ?? existing.customerNote,
          lastImportedAt: new Date(),
          lastImportedSource,
        },
        include: { addresses: true },
      });

      await this.upsertDefaultAddress(
        updated.id,
        updated.addresses,
        data,
        fullName,
        phone,
        email
      );

      const refreshed = await this.prisma.customer.findUnique({
        where: { id: updated.id },
        include: {
          addresses: {
            orderBy: [{ isDefault: "desc" }, { updatedAt: "desc" }],
          },
        },
      });

      if (!refreshed) {
        throw new BadRequestException("Không cập nhật được khách hàng");
      }

      return this.serializeCustomer(refreshed);
    }

    const created = await this.prisma.customer.create({
      data: {
        legacyCode,
        fullName,
        phone,
        email,
        source,
        customerGroup,
        gender,
        birthDate,
        points,
        totalOrders: totalOrders ?? 0,
        totalSpent: totalSpent ?? 0,
        lastOrderAt,
        defaultDiscountPercent,
        pricePolicyName,
        customerNote,
        lastImportedAt: new Date(),
        lastImportedSource,
      },
      include: { addresses: true },
    });

    await this.upsertDefaultAddress(created.id, [], data, fullName, phone, email);

    const finalCustomer = await this.prisma.customer.findUnique({
      where: { id: created.id },
      include: {
        addresses: {
          orderBy: [{ isDefault: "desc" }, { updatedAt: "desc" }],
        },
      },
    });

    if (!finalCustomer) {
      throw new BadRequestException("Không tạo được khách hàng");
    }

    return this.serializeCustomer(finalCustomer);
  }

  async updateCustomer(id: string, data: UpdateCustomerInput) {
    const existing = await this.prisma.customer.findUnique({
      where: { id },
      include: {
        addresses: {
          orderBy: [{ isDefault: "desc" }, { updatedAt: "desc" }],
        },
      },
    });

    if (!existing) {
      throw new BadRequestException("Không tìm thấy khách hàng");
    }

    const nextLegacyCode =
      data.legacyCode !== undefined
        ? this.normalizeString(data.legacyCode)
        : existing.legacyCode;

    const nextPhone =
      data.phone !== undefined ? this.normalizePhone(data.phone) : existing.phone;

    const nextEmail =
      data.email !== undefined
        ? this.normalizeEmail(data.email)
        : existing.email;

    const duplicate = await this.findExistingCustomer({
      legacyCode: nextLegacyCode,
      phone: nextPhone,
      email: nextEmail,
    });

    if (duplicate && duplicate.id !== id) {
      throw new BadRequestException(
        "Thông tin khách hàng trùng với một khách hàng khác"
      );
    }

    const updated = await this.prisma.customer.update({
      where: { id },
      data: this.buildCustomerUpdateData(existing, data, "manual_update"),
      include: {
        addresses: {
          orderBy: [{ isDefault: "desc" }, { updatedAt: "desc" }],
        },
      },
    });

    const fullNameForAddress =
      this.normalizeString(data.fullName) ?? updated.fullName;

    const phoneForAddress =
      data.phone !== undefined ? this.normalizePhone(data.phone) : updated.phone;

    const emailForAddress =
      data.email !== undefined ? this.normalizeEmail(data.email) : updated.email;

    await this.upsertDefaultAddress(
      updated.id,
      updated.addresses,
      data,
      fullNameForAddress,
      phoneForAddress,
      emailForAddress
    );

    const refreshed = await this.prisma.customer.findUnique({
      where: { id: updated.id },
      include: {
        addresses: {
          orderBy: [{ isDefault: "desc" }, { updatedAt: "desc" }],
        },
      },
    });

    if (!refreshed) {
      throw new BadRequestException("Không cập nhật được khách hàng");
    }

    return this.serializeCustomer(refreshed);
  }

  async getImportHistory(id: string) {
    const customer = await this.prisma.customer.findUnique({
      where: { id },
      select: {
        id: true,
        legacyCode: true,
        phone: true,
        email: true,
      },
    });

    if (!customer) {
      throw new BadRequestException("Không tìm thấy khách hàng");
    }

    const jobs = await this.prisma.importJob.findMany({
      where: { type: "customer" },
      orderBy: { createdAt: "desc" },
      take: 20,
      include: {
        errors: {
          take: 20,
          orderBy: [{ rowNumber: "asc" }, { createdAt: "asc" }],
        },
        defaultBranch: true,
      },
    });

    return {
      customer,
      jobs,
    };
  }

  private async upsertDefaultAddress(
    customerId: string,
    existingAddresses: ExistingAddress[],
    data: CustomerAddressInput,
    fullName: string,
    phone: string | null,
    email: string | null
  ) {
    const addressLine1 = this.normalizeString(data.addressLine1);
    const addressLine2 = this.normalizeString(data.addressLine2);
    const ward = this.normalizeString(data.ward);
    const district = this.normalizeString(data.district);
    const city = this.normalizeString(data.city);
    const province = this.normalizeString(data.province);
    const country = this.normalizeString(data.country) || "Vietnam";
    const postalCode = this.normalizeString(data.postalCode);
    const label = this.normalizeString(data.label) || "Mặc định";
    const recipientName =
      this.normalizeString(data.recipientName) || fullName;

    const hasAddress =
      !!addressLine1 ||
      !!addressLine2 ||
      !!ward ||
      !!district ||
      !!city ||
      !!province ||
      !!postalCode;

    if (!hasAddress) return;

    const requestedDefault = data.isDefaultAddress ?? true;
    const defaultAddress =
      existingAddresses.find((a) => a.isDefault) || existingAddresses[0] || null;

    if (defaultAddress) {
      await this.prisma.customerAddress.update({
        where: { id: defaultAddress.id },
        data: {
          label,
          recipientName,
          phone: phone ?? defaultAddress.phone,
          email: email ?? (defaultAddress as any).email ?? null,
          addressLine1: addressLine1 || defaultAddress.addressLine1,
          addressLine2,
          ward,
          district,
          city,
          province,
          country,
          postalCode,
          isDefault: requestedDefault,
        },
      });
    } else {
      await this.prisma.customerAddress.create({
        data: {
          customerId,
          label,
          recipientName,
          phone,
          email,
          addressLine1: addressLine1 || "",
          addressLine2,
          ward,
          district,
          city,
          province,
          country,
          postalCode,
          isDefault: requestedDefault,
        },
      });
    }
  }
}