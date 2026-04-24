import { BadRequestException, Injectable } from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";
import * as XLSX from "xlsx";

type UploadedFile = {
  originalname: string;
  buffer: Buffer;
};

type ImportCustomersOptions = {
  defaultBranchId?: string | null;
  overwrite?: boolean;
};

type ImportCustomerResult = {
  jobId: string;
  fileName: string;
  totalRows: number;
  successRows: number;
  failedRows: number;
};

type NormalizedCustomerRow = {
  legacyCode?: string | null;
  fullName: string;
  phone?: string | null;
  email?: string | null;
  source?: string | null;
  customerGroup?: string | null;
  gender?: string | null;
  birthDate?: Date | null;
  points?: number | null;
  totalSpent?: number | null;
  totalOrders?: number | null;
  lastOrderAt?: Date | null;

  recipientName?: string | null;
  addressLine1?: string | null;
  addressLine2?: string | null;
  ward?: string | null;
  district?: string | null;
  city?: string | null;
  province?: string | null;
  country?: string | null;
  postalCode?: string | null;
  addressLabel?: string | null;
};

@Injectable()
export class ImportsService {
  constructor(private readonly prisma: PrismaService) {}

  async importCustomers(
    files: UploadedFile[],
    options: ImportCustomersOptions
  ) {
    if (!files || files.length === 0) {
      throw new BadRequestException("Không có file nào được upload");
    }

    if (options.defaultBranchId) {
      const branch = await this.prisma.branch.findUnique({
        where: { id: options.defaultBranchId },
      });

      if (!branch) {
        throw new BadRequestException("defaultBranchId không tồn tại");
      }
    }

    const results: ImportCustomerResult[] = [];

    for (const file of files) {
      const result = await this.processCustomerFile(file, options);
      results.push(result);
    }

    return {
      success: true,
      results,
    };
  }

  async getJobs(type?: string) {
    return this.prisma.importJob.findMany({
      where: type ? { type } : {},
      orderBy: { createdAt: "desc" },
      include: {
        defaultBranch: true,
      },
      take: 100,
    });
  }

  async getJobErrors(jobId: string) {
    return this.prisma.importErrorLog.findMany({
      where: { jobId },
      orderBy: [{ rowNumber: "asc" }, { createdAt: "asc" }],
      take: 1000,
    });
  }

  private detectHeaderRowIndex(sheetData: any[][]) {
    return sheetData.findIndex((row) => {
      if (!Array.isArray(row)) return false;

      const joined = row
        .map((cell) => this.normalizeHeader(String(cell ?? "")))
        .join(" | ");

      return (
        joined.includes("ten khach hang") ||
        joined.includes("dien thoai") ||
        joined.includes("ma khach hang")
      );
    });
  }

  private buildRowsFromSheetData(sheetData: any[][], headerRowIndex: number) {
    const headerRow = (sheetData[headerRowIndex] || []).map((cell) =>
      String(cell ?? "").trim()
    );

    const rows: any[] = [];

    for (let i = headerRowIndex + 1; i < sheetData.length; i++) {
      const rowArray = sheetData[i];
      if (!Array.isArray(rowArray)) continue;

      const rowObject: Record<string, any> = {};

      for (let col = 0; col < headerRow.length; col++) {
        const header = headerRow[col];
        if (!header) continue;
        rowObject[header] = rowArray[col] ?? "";
      }

      rows.push(rowObject);
    }

    return rows;
  }

  private async processCustomerFile(
    file: UploadedFile,
    options: ImportCustomersOptions
  ): Promise<ImportCustomerResult> {
    const job = await this.prisma.importJob.create({
      data: {
        type: "customer",
        fileName: file.originalname,
        status: "processing",
        defaultBranchId: options.defaultBranchId || null,
      },
    });

    try {
      const workbook = XLSX.read(file.buffer, { type: "buffer" });
      const allRows: any[] = [];

      for (const sheetName of workbook.SheetNames) {
        const sheet = workbook.Sheets[sheetName];

        const sheetData = XLSX.utils.sheet_to_json<any[]>(sheet, {
          header: 1,
          defval: "",
          raw: false,
        });

        const headerRowIndex = this.detectHeaderRowIndex(sheetData);

        if (headerRowIndex === -1) {
          continue;
        }

        const rows = this.buildRowsFromSheetData(
          sheetData,
          headerRowIndex
        ).filter((row) =>
          Object.values(row).some(
            (value) => String(value ?? "").trim() !== ""
          )
        );

        allRows.push(...rows);
      }

      const nonEmptyRows = allRows.filter((row) => this.hasAnyValue(row));

      await this.prisma.importJob.update({
        where: { id: job.id },
        data: {
          totalRows: nonEmptyRows.length,
        },
      });

      let successRows = 0;
      let failedRows = 0;

      const normalizedRows: Array<{
        rowNumber: number;
        data?: NormalizedCustomerRow;
        error?: string;
        raw: any;
      }> = [];

      for (let i = 0; i < nonEmptyRows.length; i++) {
        const raw = nonEmptyRows[i];
        const rowNumber = i + 2;

        try {
          const normalized = this.normalizeCustomerRow(raw);
          const validationError =
            this.validateNormalizedCustomerRow(normalized);

          if (validationError) {
            normalizedRows.push({
              rowNumber,
              error: validationError,
              raw,
            });
            failedRows++;
            continue;
          }

          normalizedRows.push({
            rowNumber,
            data: normalized,
            raw,
          });
        } catch (error: any) {
          normalizedRows.push({
            rowNumber,
            error: error?.message || "Không thể chuẩn hoá dòng dữ liệu",
            raw,
          });
          failedRows++;
        }
      }

      const validRows = normalizedRows
        .filter(
          (
            x
          ): x is {
            rowNumber: number;
            data: NormalizedCustomerRow;
            raw: any;
          } => !!x.data
        )
        .map((x) => ({
          rowNumber: x.rowNumber,
          data: x.data,
          raw: x.raw,
        }));

      const chunkSize = 300;

      for (let i = 0; i < validRows.length; i += chunkSize) {
        const chunk = validRows.slice(i, i + chunkSize);
        const chunkResult = await this.processCustomerChunk(
          chunk,
          job.id,
          options.overwrite ?? true
        );

        successRows += chunkResult.successRows;
        failedRows += chunkResult.failedRows;
      }

      const errorRows = normalizedRows.filter((x) => x.error);

      if (errorRows.length > 0) {
        await this.prisma.importErrorLog.createMany({
          data: errorRows.map((row) => ({
            jobId: job.id,
            rowNumber: row.rowNumber,
            message: row.error!,
            rawData: row.raw,
          })),
        });
      }

      await this.prisma.importJob.update({
        where: { id: job.id },
        data: {
          status: "done",
          successRows,
          failedRows,
        },
      });

      return {
        jobId: job.id,
        fileName: file.originalname,
        totalRows: nonEmptyRows.length,
        successRows,
        failedRows,
      };
    } catch (error: any) {
      await this.prisma.importJob.update({
        where: { id: job.id },
        data: {
          status: "failed",
        },
      });

      throw error;
    }
  }

  private async processCustomerChunk(
    rows: Array<{
      rowNumber: number;
      data: NormalizedCustomerRow;
      raw: any;
    }>,
    jobId: string,
    overwrite: boolean
  ) {
    let successRows = 0;
    let failedRows = 0;

    const legacyCodes = rows
      .map((r) => r.data.legacyCode)
      .filter((v): v is string => !!v);

    const phones = rows
      .map((r) => r.data.phone)
      .filter((v): v is string => !!v);

    const emails = rows
      .map((r) => r.data.email)
      .filter((v): v is string => !!v);

    const orConditions: any[] = [];
    if (legacyCodes.length) orConditions.push({ legacyCode: { in: legacyCodes } });
    if (phones.length) orConditions.push({ phone: { in: phones } });
    if (emails.length) orConditions.push({ email: { in: emails } });

    const existingCustomers =
      orConditions.length > 0
        ? await this.prisma.customer.findMany({
            where: { OR: orConditions },
            include: { addresses: true },
          })
        : [];

    const legacyCodeMap = new Map<string, (typeof existingCustomers)[number]>();
    const phoneMap = new Map<string, (typeof existingCustomers)[number]>();
    const emailMap = new Map<string, (typeof existingCustomers)[number]>();

    for (const customer of existingCustomers) {
      if (customer.legacyCode) legacyCodeMap.set(customer.legacyCode, customer);
      if (customer.phone) phoneMap.set(customer.phone, customer);
      if (customer.email) emailMap.set(customer.email, customer);
    }

    for (const row of rows) {
      try {
        const matched =
          (row.data.legacyCode && legacyCodeMap.get(row.data.legacyCode)) ||
          (row.data.phone && phoneMap.get(row.data.phone)) ||
          (row.data.email && emailMap.get(row.data.email)) ||
          null;

        if (matched) {
          if (overwrite) {
            const updated = await this.prisma.customer.update({
              where: { id: matched.id },
              data: {
                legacyCode: row.data.legacyCode ?? matched.legacyCode,
                fullName: row.data.fullName || matched.fullName,
                phone: row.data.phone ?? matched.phone,
                email: row.data.email ?? matched.email,
                source: row.data.source ?? matched.source,
                customerGroup: row.data.customerGroup ?? matched.customerGroup,
                gender: row.data.gender ?? matched.gender,
                birthDate: row.data.birthDate ?? matched.birthDate,
                points:
                  row.data.points !== null && row.data.points !== undefined
                    ? row.data.points
                    : matched.points,
                totalSpent:
                  row.data.totalSpent !== null && row.data.totalSpent !== undefined
                    ? row.data.totalSpent
                    : Number(matched.totalSpent || 0),
                totalOrders:
                  row.data.totalOrders !== null && row.data.totalOrders !== undefined
                    ? row.data.totalOrders
                    : matched.totalOrders,
                lastOrderAt: row.data.lastOrderAt ?? matched.lastOrderAt,
              },
            });

            await this.upsertCustomerAddress(
              updated.id,
              matched.addresses,
              row.data
            );
          }

          successRows++;
        } else {
          const created = await this.prisma.customer.create({
            data: {
              legacyCode: row.data.legacyCode ?? null,
              fullName: row.data.fullName,
              phone: row.data.phone ?? null,
              email: row.data.email ?? null,
              source: row.data.source ?? null,
              customerGroup: row.data.customerGroup ?? null,
              gender: row.data.gender ?? null,
              birthDate: row.data.birthDate ?? null,
              points:
                row.data.points !== null && row.data.points !== undefined
                  ? row.data.points
                  : 0,
              totalSpent:
                row.data.totalSpent !== null && row.data.totalSpent !== undefined
                  ? row.data.totalSpent
                  : 0,
              totalOrders:
                row.data.totalOrders !== null && row.data.totalOrders !== undefined
                  ? row.data.totalOrders
                  : 0,
              lastOrderAt: row.data.lastOrderAt ?? null,
            },
          });

          await this.upsertCustomerAddress(created.id, [], row.data);

          successRows++;
        }
      } catch (error: any) {
        failedRows++;

        await this.prisma.importErrorLog.create({
          data: {
            jobId,
            rowNumber: row.rowNumber,
            message: error?.message || "Lỗi khi ghi dữ liệu vào database",
            rawData: row.raw,
          },
        });
      }
    }

    return {
      successRows,
      failedRows,
    };
  }

  private async upsertCustomerAddress(
    customerId: string,
    existingAddresses: Array<{
      id: string;
      label: string | null;
      recipientName: string | null;
      phone: string | null;
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
    }>,
    data: NormalizedCustomerRow
  ) {
    const hasAddress =
      data.addressLine1 ||
      data.addressLine2 ||
      data.ward ||
      data.district ||
      data.city ||
      data.province ||
      data.postalCode;

    if (!hasAddress) return;

    const defaultAddress =
      existingAddresses.find((a) => a.isDefault) || existingAddresses[0] || null;

    if (defaultAddress) {
      await this.prisma.customerAddress.update({
        where: { id: defaultAddress.id },
        data: {
          label: data.addressLabel ?? defaultAddress.label,
          recipientName: data.recipientName ?? defaultAddress.recipientName,
          phone: data.phone ?? defaultAddress.phone,
          addressLine1: data.addressLine1 ?? defaultAddress.addressLine1,
          addressLine2: data.addressLine2 ?? defaultAddress.addressLine2,
          ward: data.ward ?? defaultAddress.ward,
          district: data.district ?? defaultAddress.district,
          city: data.city ?? defaultAddress.city,
          province: data.province ?? defaultAddress.province,
          country: data.country ?? defaultAddress.country ?? "Vietnam",
          postalCode: data.postalCode ?? defaultAddress.postalCode,
          isDefault: true,
        },
      });
    } else {
      await this.prisma.customerAddress.create({
        data: {
          customerId,
          label: data.addressLabel ?? "Mặc định",
          recipientName: data.recipientName ?? data.fullName,
          phone: data.phone ?? null,
          addressLine1: data.addressLine1 ?? "",
          addressLine2: data.addressLine2 ?? null,
          ward: data.ward ?? null,
          district: data.district ?? null,
          city: data.city ?? null,
          province: data.province ?? null,
          country: data.country ?? "Vietnam",
          postalCode: data.postalCode ?? null,
          isDefault: true,
        },
      });
    }
  }

  private normalizeCustomerRow(raw: any): NormalizedCustomerRow {
    const get = (...keys: string[]) => {
      for (const key of keys) {
        const matchedKey = Object.keys(raw).find(
          (k) => this.normalizeHeader(k) === this.normalizeHeader(key)
        );

        if (
          matchedKey &&
          raw[matchedKey] !== undefined &&
          raw[matchedKey] !== null &&
          String(raw[matchedKey]).trim() !== ""
        ) {
          return String(raw[matchedKey]).trim();
        }
      }
      return "";
    };

    const legacyCode = get(
      "Mã khách hàng",
      "customer code",
      "code",
      "ma khach hang",
      "mã khách hàng",
      "customer id",
      "ma kh",
      "mã kh"
    );

    const fullName = get(
      "Tên khách hàng",
      "Tên khách hàng *",
      "full name",
      "customer name",
      "name",
      "ho ten",
      "họ tên",
      "ten"
    );

    const phoneRaw = get(
      "Điện thoại",
      "phone",
      "mobile",
      "sdt",
      "so dien thoai",
      "số điện thoại",
      "dien thoai"
    );

    const emailRaw = get("Email", "email", "e-mail");
    const source = get("Nguồn", "source", "nguon");

    const customerGroup = get(
      "Mã nhóm khách hàng",
      "Nhóm khách hàng",
      "customer group",
      "group",
      "nhom khach hang"
    );

    const gender = get("Giới tính", "gender", "gioi tinh");
    const birthDateRaw = get(
      "Ngày sinh",
      "birthdate",
      "birthday",
      "ngay sinh",
      "dob"
    );
    const pointsRaw = get("Điểm hiện tại", "points", "diem", "point");
    const totalSpentRaw = get("Tổng chi tiêu", "total spent", "tong chi tieu");
    const totalOrdersRaw = get(
      "SL đơn hàng",
      "total orders",
      "so don",
      "sl don hang"
    );
    const lastOrderAtRaw = get(
      "Ngày mua cuối cùng",
      "last order at",
      "ngay mua cuoi cung"
    );

    const recipientName = get(
      "Người nhận",
      "recipient name",
      "ten nguoi nhan",
      "tên người nhận",
      "nguoi nhan"
    );

    const addressLine1 = get(
      "Địa chỉ",
      "address",
      "dia chi",
      "địa chỉ",
      "so dia chi",
      "address line 1",
      "address1"
    );

    const addressLine2 = get(
      "address line 2",
      "address2",
      "dia chi 2",
      "địa chỉ 2"
    );

    const ward = get(
      "Phường xã",
      "ward",
      "phuong xa",
      "phuong",
      "xã",
      "xa",
      "phường"
    );

    const district = get(
      "Quận huyện",
      "district",
      "quan huyen",
      "quan",
      "quận",
      "huyen",
      "huyện"
    );

    const city = get("Thành phố", "city", "thanh pho", "thành phố");
    const province = get("Tỉnh thành", "province", "tinh thanh", "tỉnh", "tinh");
    const country = get("country", "quoc gia", "quốc gia");
    const postalCode = get("postal code", "zip", "zipcode", "mã bưu điện");
    const addressLabel = get(
      "label",
      "nhan dia chi",
      "nhãn địa chỉ",
      "loai dia chi"
    );

    const phone = this.normalizePhone(phoneRaw);
    const email = this.normalizeEmail(emailRaw);
    const birthDate = this.parseDate(birthDateRaw);
    const points = this.parseInteger(pointsRaw);
    const totalSpent = this.parseNumber(totalSpentRaw);
    const totalOrders = this.parseInteger(totalOrdersRaw);
    const lastOrderAt = this.parseDate(lastOrderAtRaw);

    return {
      legacyCode: legacyCode || null,
      fullName,
      phone: phone || null,
      email: email || null,
      source: source || null,
      customerGroup: customerGroup || null,
      gender: gender || null,
      birthDate: birthDate || null,
      points,
      totalSpent,
      totalOrders,
      lastOrderAt: lastOrderAt || null,
      recipientName: recipientName || null,
      addressLine1: addressLine1 || null,
      addressLine2: addressLine2 || null,
      ward: ward || null,
      district: district || null,
      city: city || null,
      province: province || null,
      country: country || "Vietnam",
      postalCode: postalCode || null,
      addressLabel: addressLabel || null,
    };
  }

  private validateNormalizedCustomerRow(row: NormalizedCustomerRow): string | null {
    if (!row.fullName) {
      return "Thiếu họ tên khách hàng";
    }

    if (!row.legacyCode && !row.phone && !row.email) {
      return "Thiếu mã khách hàng, số điện thoại và email";
    }

    if (row.email && !this.isValidEmail(row.email)) {
      return "Email không hợp lệ";
    }

    if (row.phone && row.phone.length < 8) {
      return "Số điện thoại không hợp lệ";
    }

    return null;
  }

  private hasAnyValue(row: Record<string, any>) {
    return Object.values(row).some((value) => {
      if (value === null || value === undefined) return false;
      return String(value).trim() !== "";
    });
  }

  private normalizeHeader(value: string) {
    return String(value)
      .trim()
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/[*:]/g, "")
      .replace(/\s+/g, " ")
      .replace(/[_-]+/g, " ")
      .trim();
  }

  private normalizePhone(phone: string) {
    if (!phone) return "";

    let cleaned = phone.replace(/[^\d+]/g, "");

    if (cleaned.startsWith("+84")) {
      cleaned = "0" + cleaned.slice(3);
    }

    return cleaned;
  }

  private normalizeEmail(email: string) {
    if (!email) return "";
    return email.trim().toLowerCase();
  }

  private isValidEmail(email: string) {
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
  }

  private parseInteger(value: string): number | null {
    if (!value) return null;
    const cleaned = value.replace(/[^\d-]/g, "");
    if (!cleaned) return null;
    const parsed = parseInt(cleaned, 10);
    return Number.isNaN(parsed) ? null : parsed;
  }

  private parseNumber(value: string): number | null {
    if (!value) return null;
    const cleaned = value.replace(/[^\d.,-]/g, "").replace(/,/g, "");
    if (!cleaned) return null;
    const parsed = Number(cleaned);
    return Number.isNaN(parsed) ? null : parsed;
  }

  private parseDate(value: string): Date | null {
    if (!value) return null;

    const direct = new Date(value);
    if (!Number.isNaN(direct.getTime())) return direct;

    const parts = value.split(/[\/\-]/);
    if (parts.length === 3) {
      const [d, m, y] = parts.map(Number);
      if (d && m && y) {
        const parsed = new Date(y, m - 1, d);
        if (!Number.isNaN(parsed.getTime())) return parsed;
      }
    }

    return null;
  }
}