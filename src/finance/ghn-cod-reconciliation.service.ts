import { BadRequestException, Injectable } from "@nestjs/common";
import * as XLSX from "xlsx";
import { PrismaService } from "../prisma/prisma.service";

type HeaderMap = Record<string, number>;

type ReconciliationStatus =
  | "MATCHED"
  | "MISMATCH"
  | "NOT_FOUND"
  | "MATCHED_BY_PARTIAL_DELIVERY";

@Injectable()
export class GhnCodReconciliationService {
  constructor(private readonly prisma: PrismaService) {}

  async parseExcel(file: Express.Multer.File, body: any) {
    if (!file?.buffer) {
      throw new BadRequestException("Không nhận được file Excel.");
    }

    const workbook = XLSX.read(file.buffer, { type: "buffer", cellDates: true });
    const sheetName = workbook.SheetNames[0];

    if (!sheetName) {
      throw new BadRequestException("File không có sheet dữ liệu.");
    }

    const sheet = workbook.Sheets[sheetName];
    const range = XLSX.utils.decode_range(sheet["!ref"] || "A1:S1");
    const matrix = this.sheetToMatrix(sheet, range);

    const summary = this.extractSummarySmart(matrix, sheet);
    const headerResult = this.findHeaderRowSmart(matrix);
    const finalHeader = headerResult || this.getFallbackHeader();
    const parserMode = headerResult ? "SMART" : "FALLBACK_GHN_FIXED";

    const dataStartRowIndex = finalHeader.headerRowIndex + 1;
    const rows: any[] = [];

    for (let r = dataStartRowIndex; r <= range.e.r; r++) {
      const row = matrix[r] || [];
      const parsed = this.parseRow(row, finalHeader.headerMap, r + 1);

      if (!parsed.ghnOrderCode) continue;

      const code = normalize(parsed.ghnOrderCode);
      if (code.includes("ma don")) continue;
      if (code.includes("tong")) continue;
      if (code.includes("khach hang")) continue;

      rows.push(parsed);
    }

    const transferCode =
      body?.transferCode || this.detectTransferCode(matrix) || null;
    const transferDate =
      body?.transferDate || this.detectTransferDate(matrix) || null;

    const batch = await this.prisma.ghnCodReconciliationBatch.create({
      data: {
        fileName: file.originalname,
        transferCode,
        transferDate,
        totalRows: rows.length,
        totalAmount: summary.totalReconcile,
        transferFee: summary.transferFee,
        netAmount: summary.netAmount,
        parserMode,
      },
    });

    const partialReturnBaseCodes = new Set(
      rows
        .filter((r) => /_PR$/i.test(r.ghnOrderCode))
        .map((r) => r.ghnOrderCode.replace(/_PR$/i, ""))
    );

    const enrichedRows = await Promise.all(
      rows.map(async (row) => {
        const issueTypes: string[] = [];

        const isPartialReturn = /_PR$/i.test(row.ghnOrderCode);
        const baseGhnCode = isPartialReturn
          ? row.ghnOrderCode.replace(/_PR$/i, "")
          : row.ghnOrderCode;

        const hasPrInFile = partialReturnBaseCodes.has(baseGhnCode);

        const shipment = await this.prisma.shipment.findFirst({
          where: {
            OR: [{ trackingCode: row.ghnOrderCode }, { trackingCode: baseGhnCode }],
          },
          include: { order: true },
        });

        if (!shipment?.order) {
          issueTypes.push("NOT_FOUND_INTERNAL_ORDER");
        }

        if (isPartialReturn) {
          issueTypes.push("PARTIAL_RETURN");
        }

        const partialRecord = shipment?.order
          ? await (this.prisma as any).partialDeliveryRecord.findFirst({
              where: {
                OR: [
                  { orderId: shipment.order.id },
                  { orderCode: shipment.order.orderCode },
                  { ghnTrackingCode: baseGhnCode },
                ],
              },
              orderBy: { createdAt: "desc" },
            })
          : null;

        const systemCodAmount = Number((shipment as any)?.codAmount || 0);
        const systemShippingFee = Number((shipment as any)?.shippingFee || 0);

        const adjustedCod = Number(partialRecord?.adjustedCod || 0);
        const originalCod = Number(partialRecord?.originalCod || 0);

        const partialAmountMatched =
          Boolean(partialRecord) && adjustedCod > 0 && row.codAmount === adjustedCod;

        if (shipment?.order) {
          if (row.codAmount && systemCodAmount && row.codAmount !== systemCodAmount) {
            if (hasPrInFile || isPartialReturn) {
              if (!partialRecord) {
                issueTypes.push("MISSING_PARTIAL_DELIVERY_RECORD");
              } else if (partialAmountMatched) {
                issueTypes.push("MATCHED_BY_PARTIAL_DELIVERY");
              } else {
                issueTypes.push("PARTIAL_DELIVERY_AMOUNT_MISMATCH");
              }
            } else {
              issueTypes.push("COD_MISMATCH");
            }
          }

          if (row.serviceFee && systemShippingFee && row.serviceFee !== systemShippingFee) {
            issueTypes.push("FEE_MISMATCH");
          }
        }

        const returnReceivedAt = partialRecord?.returnReceivedAt || null;
        const returnReceived = Boolean(returnReceivedAt);

        if ((hasPrInFile || isPartialReturn) && partialRecord && !returnReceived) {
          issueTypes.push("PARTIAL_RETURN_NOT_RECEIVED");
        }

        const reconciliationStatus = this.getReconciliationStatus(issueTypes);

        const savedRow = await this.prisma.ghnCodReconciliationRow.create({
          data: {
            batchId: batch.id,
            orderId: shipment?.order?.id || null,
            orderCode: shipment?.order?.orderCode || null,
            shipmentId: shipment?.id || null,
            ghnCode: row.ghnOrderCode,
            customerOrderCode: row.customerOrderCode || null,
            ghnStatus: row.ghnStatus || null,
            codAmount: row.codAmount || 0,
            serviceFee: row.serviceFee || 0,
            totalReconcileAmount: row.totalReconcileAmount || 0,
            reconciliationStatus,
            issues: issueTypes,
            partialDeliveryRecordId: partialRecord?.id || null,
            partialDeliveryAdjustedCod: adjustedCod || null,
            partialReturnReceived: partialRecord ? returnReceived : null,
          },
        });

        if (shipment?.id) {
          await this.prisma.shipment.update({
            where: { id: shipment.id },
            data: {
              codReconciliationStatus: reconciliationStatus,
              codReconciledAt: new Date(),
              codReconciliationBatchId: batch.id,
              codReconciliationRowId: savedRow.id,
              codReconciliationIssue: issueTypes.join(", "),
              codReconciliationAmount: row.totalReconcileAmount || 0,
            } as any,
          });
        }

        return {
          ...row,
          reconciliationRowId: savedRow.id,
          reconciliationStatus,
          reconciledAt: savedRow.createdAt,
          internalOrderId: shipment?.order?.id || null,
          systemOrderCode: shipment?.order?.orderCode || null,
          systemOrderStatus: shipment?.order?.status || null,
          systemCodAmount,
          systemShippingFee,
          hasPrInFile,
          partialDeliveryRecordId: partialRecord?.id || null,
          partialDeliveryAdjustedCod: adjustedCod,
          partialDeliveryOriginalCod: originalCod,
          partialDeliveryMatched: partialAmountMatched,
          partialReturnReceived: returnReceived,
          partialReturnReceivedAt: returnReceivedAt,
          issueTypes,
        };
      })
    );

    const matchedRows = enrichedRows.filter(
      (r) =>
        r.reconciliationStatus === "MATCHED" ||
        r.reconciliationStatus === "MATCHED_BY_PARTIAL_DELIVERY"
    ).length;

    const mismatchRows = enrichedRows.length - matchedRows;

    await this.prisma.ghnCodReconciliationBatch.update({
      where: { id: batch.id },
      data: {
        totalRows: enrichedRows.length,
        matchedRows,
        mismatchRows,
      },
    });

    return {
      batch: {
        id: batch.id,
        fileName: file.originalname,
        transferCode,
        transferDate,
        totalRows: enrichedRows.length,
        matchedRows,
        mismatchRows,
        totalCodAmount: summary.totalReconcile,
        totalFeeAmount: summary.transferFee,
        totalNetAmount: summary.netAmount,
        parserMode,
      },
      rows: enrichedRows,
      summary: {
        notFoundOrder: enrichedRows.filter((r) =>
          r.issueTypes.includes("NOT_FOUND_INTERNAL_ORDER")
        ).length,
        codMismatch: enrichedRows.filter((r) =>
          r.issueTypes.includes("COD_MISMATCH")
        ).length,
        feeMismatch: enrichedRows.filter((r) =>
          r.issueTypes.includes("FEE_MISMATCH")
        ).length,
        partialReturn: enrichedRows.filter((r) =>
          r.issueTypes.includes("PARTIAL_RETURN")
        ).length,
        matchedByPartialDelivery: enrichedRows.filter((r) =>
          r.issueTypes.includes("MATCHED_BY_PARTIAL_DELIVERY")
        ).length,
        partialReturnNotReceived: enrichedRows.filter((r) =>
          r.issueTypes.includes("PARTIAL_RETURN_NOT_RECEIVED")
        ).length,
        noMoney: 0,
      },
    };
  }

  private getReconciliationStatus(issueTypes: string[]): ReconciliationStatus {
    if (issueTypes.includes("NOT_FOUND_INTERNAL_ORDER")) return "NOT_FOUND";

    const blockingIssues = issueTypes.filter(
      (x) => x !== "MATCHED_BY_PARTIAL_DELIVERY"
    );

    if (
      issueTypes.includes("MATCHED_BY_PARTIAL_DELIVERY") &&
      blockingIssues.length === 0
    ) {
      return "MATCHED_BY_PARTIAL_DELIVERY";
    }

    if (issueTypes.length === 0) return "MATCHED";

    return "MISMATCH";
  }

  private sheetToMatrix(sheet: XLSX.WorkSheet, range: XLSX.Range) {
    const matrix: any[][] = [];

    for (let r = range.s.r; r <= range.e.r; r++) {
      const row: any[] = [];

      for (let c = range.s.c; c <= range.e.c; c++) {
        const addr = XLSX.utils.encode_cell({ r, c });
        const cell = sheet[addr];
        row.push(cell ? cell.v ?? "" : "");
      }

      matrix.push(row);
    }

    return matrix;
  }

  private extractSummarySmart(matrix: any[][], sheet: XLSX.WorkSheet) {
    let totalReconcile = 0;
    let transferFee = 0;
    let netAmount = 0;

    for (const row of matrix.slice(0, 60)) {
      for (let c = 0; c < row.length; c++) {
        const label = normalize(row[c]);

        if (label === "tong doi soat") {
          totalReconcile = this.findNextMoneyValue(row, c);
        }

        if (label === "phi chuyen khoan cod") {
          transferFee = this.findNextMoneyValue(row, c);
        }

        if (label === "thuc nhan") {
          netAmount = this.findNextMoneyValue(row, c);
        }
      }
    }

    if (!totalReconcile) totalReconcile = toMoneyNumber(sheet["B10"]?.v ?? 0);
    if (!transferFee) transferFee = toMoneyNumber(sheet["B12"]?.v ?? 0);
    if (!netAmount) netAmount = toMoneyNumber(sheet["B14"]?.v ?? 0);

    return { totalReconcile, transferFee, netAmount };
  }

  private findNextMoneyValue(row: any[], labelIndex: number) {
    for (let i = labelIndex + 1; i < row.length; i++) {
      const raw = row[i];
      if (raw === null || raw === undefined || raw === "") continue;

      const parsed = toMoneyNumber(raw);

      if (parsed !== 0 || String(raw).trim() === "0") {
        return parsed;
      }
    }

    return 0;
  }

  private findHeaderRowSmart(
    matrix: any[][]
  ): { headerRowIndex: number; headerMap: HeaderMap } | null {
    for (let rowIndex = 1; rowIndex < matrix.length; rowIndex++) {
      const parentRow = matrix[rowIndex - 1] || [];
      const row = matrix[rowIndex] || [];

      const normalizedRow = row.map((cell) => normalize(cell));
      const normalizedParent = parentRow.map((cell) => normalize(cell));

      const findCurrent = (keywords: string[]) =>
        normalizedRow.findIndex((cell) =>
          keywords.some((keyword) => cell.includes(keyword))
        );

      const findParent = (keywords: string[]) =>
        normalizedParent.findIndex((cell) =>
          keywords.some((keyword) => cell.includes(keyword))
        );

      const ghnOrderCodeIndex = findCurrent(["ma don ghn"]);
      const customerOrderCodeIndex = findCurrent(["ma don khach hang"]);

      if (ghnOrderCodeIndex < 0 || customerOrderCodeIndex < 0) continue;

      const headerMap: HeaderMap = {
        stt: findCurrent(["stt"]),
        ghnOrderCode: ghnOrderCodeIndex,
        customerOrderCode: customerOrderCodeIndex,
        storeName: findCurrent(["cua hang"]),
        recipientName: findCurrent(["nguoi nhan"]),
        recipientAddress: findCurrent(["dia chi nhan", "dia chi"]),
        createdDate: findCurrent(["ngay tao"]),
        deliveredDate: findCurrent(["ngay giao", "ngay giao/tra"]),
        ghnStatus: findCurrent(["trang thai"]),
        codAmount: findParent(["tien cod"]),
        failedDeliveryAmount: findParent(["giao that bai"]),
        prepaidAmount: findParent(["da thanh toan truoc"]),
        promotionAmount: findParent(["khuyen mai"]),
        shippingFee: findParent(["phi giao hang"]),
        redeliveryFee: findParent(["phi giao lai"]),
        insuranceFee: findParent(["phi khai gia"]),
        returnFee: findParent(["phi hoan hang"]),
        serviceFee: findParent(["phi dich vu"]),
        totalReconcileAmount: findParent(["tong doi soat"]),
      };

      const ok =
        headerMap.ghnOrderCode >= 0 &&
        headerMap.customerOrderCode >= 0 &&
        headerMap.storeName >= 0 &&
        headerMap.ghnStatus >= 0 &&
        headerMap.codAmount >= 0 &&
        headerMap.serviceFee >= 0 &&
        headerMap.totalReconcileAmount >= 0;

      if (ok) return { headerRowIndex: rowIndex, headerMap };
    }

    return null;
  }

  private getFallbackHeader(): { headerRowIndex: number; headerMap: HeaderMap } {
    return {
      headerRowIndex: 20,
      headerMap: {
        stt: 0,
        ghnOrderCode: 1,
        customerOrderCode: 2,
        storeName: 3,
        recipientName: 4,
        recipientAddress: 5,
        createdDate: 6,
        deliveredDate: 7,
        ghnStatus: 8,
        codAmount: 9,
        failedDeliveryAmount: 10,
        prepaidAmount: 11,
        promotionAmount: 12,
        shippingFee: 13,
        redeliveryFee: 14,
        insuranceFee: 15,
        returnFee: 16,
        serviceFee: 17,
        totalReconcileAmount: 18,
      },
    };
  }

  private parseRow(row: any[], headerMap: HeaderMap, rowNumber: number) {
    const value = (field: string) => {
      const index = headerMap[field];
      if (index === undefined || index < 0) return "";
      return row[index] ?? "";
    };

    const ghnOrderCode = cleanText(value("ghnOrderCode"));

    return {
      rowNumber,
      stt: cleanText(value("stt")),
      ghnOrderCode,
      customerOrderCode: cleanText(value("customerOrderCode")),
      storeName: cleanText(value("storeName")),
      recipientName: cleanText(value("recipientName")),
      recipientAddress: cleanText(value("recipientAddress")),
      createdDate: cleanText(value("createdDate")),
      deliveredDate: cleanText(value("deliveredDate")),
      ghnStatus: cleanText(value("ghnStatus")),
      codAmount: toMoneyNumber(value("codAmount")),
      failedDeliveryAmount: toMoneyNumber(value("failedDeliveryAmount")),
      prepaidAmount: toMoneyNumber(value("prepaidAmount")),
      promotionAmount: toMoneyNumber(value("promotionAmount")),
      shippingFee: toMoneyNumber(value("shippingFee")),
      redeliveryFee: toMoneyNumber(value("redeliveryFee")),
      insuranceFee: toMoneyNumber(value("insuranceFee")),
      returnFee: toMoneyNumber(value("returnFee")),
      serviceFee: toMoneyNumber(value("serviceFee")),
      totalReconcileAmount: toMoneyNumber(value("totalReconcileAmount")),
      partialReturnBaseCode: /_PR$/i.test(ghnOrderCode)
        ? ghnOrderCode.replace(/_PR$/i, "")
        : null,
    };
  }

  private detectTransferCode(matrix: any[][]) {
    for (const row of matrix.slice(0, 20)) {
      const joined = row.map((x) => cleanText(x)).join(" ");
      const found = joined.match(/COD_\d+_\d+/i);
      if (found?.[0]) return found[0];
    }

    return null;
  }

  private detectTransferDate(matrix: any[][]) {
    for (const row of matrix.slice(0, 20)) {
      for (let i = 0; i < row.length; i++) {
        const label = normalize(row[i]);
        if (label === "ngay") return cleanText(row[i + 1]);
      }
    }

    return null;
  }
}

function normalize(input: any) {
  return String(input || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[*:：]/g, "")
    .replace(/\r?\n/g, " ")
    .replace(/\s+/g, " ");
}

function cleanText(input: any) {
  return String(input || "").trim();
}

function toMoneyNumber(input: any) {
  if (input === null || input === undefined || input === "") return 0;

  const raw = String(input)
    .replace(/[₫đĐ,\s]/g, "")
    .replace(/\((.*?)\)/g, "-$1")
    .trim();

  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : 0;
}