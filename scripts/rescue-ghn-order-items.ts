import { PrismaService } from "../src/prisma/prisma.service";
import * as XLSX from "xlsx";
import { randomUUID } from "crypto";

type AnyRow = Record<string, any>;

const prisma = new PrismaService();

function arg(name: string, fallback = "") {
  const prefix = `--${name}=`;
  const found = process.argv.find((x) => x.startsWith(prefix));
  return found ? found.slice(prefix.length) : fallback;
}

function hasFlag(name: string) {
  return process.argv.includes(`--${name}`);
}

function normalizeText(value: any) {
  return String(value ?? "").trim();
}

function normalizeKey(value: any) {
  return normalizeText(value)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/đ/g, "d")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function toNumber(value: any) {
  if (value === null || value === undefined || value === "") return 0;
  if (typeof value === "number") return Number.isFinite(value) ? Math.round(value) : 0;

  let raw = String(value).trim().replace(/\s/g, "");
  if (!raw) return 0;

  if (raw.includes(".") && raw.includes(",")) {
    raw = raw.split(",")[0].replace(/\./g, "");
  } else if (raw.includes(",")) {
    const [left, right = ""] = raw.split(",");
    raw = right.length === 3 && left.length <= 3 ? raw.replace(/,/g, "") : left;
  } else if (raw.includes(".")) {
    raw = raw.replace(/\./g, "");
  }

  raw = raw.replace(/[^\d-]/g, "");
  const n = Number(raw);
  return Number.isFinite(n) ? Math.round(n) : 0;
}

function getCell(row: AnyRow, names: string[]) {
  const keys = Object.keys(row);
  for (const name of names) {
    const wanted = normalizeKey(name);
    const found = keys.find((k) => normalizeKey(k) === wanted);
    if (found) return row[found];
  }
  return "";
}

function parseItems(text: string) {
  const source = normalizeText(text);
  const items: Array<{ rawName: string; productCode: string; qty: number }> = [];

  // Match segments like: "Áo Sơ Mi OXFORD - SM927 [1 cái]"
  const regex = /(.+?)\s*\[\s*(\d+)\s*c[aá]i\s*\]/giu;
  let match: RegExpExecArray | null;

  while ((match = regex.exec(source))) {
    let rawName = normalizeText(match[1]).replace(/^,\s*/, "").replace(/,\s*$/, "").trim();
    const qty = Math.max(1, toNumber(match[2]));

    // Product code usually appears as SM927, QKK904, AP932...
    const codeMatches = rawName.toUpperCase().match(/\b[A-ZĐ]{1,6}\d{2,6}[A-Z0-9]*\b/g) || [];
    const productCode = normalizeText(codeMatches[codeMatches.length - 1] || "");

    items.push({ rawName, productCode, qty });
  }

  return items;
}

async function tableColumns(tableName: string) {
  const rows = await prisma.$queryRawUnsafe<Array<{ column_name: string; is_nullable: string; data_type: string }>>(
    `
      SELECT column_name, is_nullable, data_type
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = $1
      ORDER BY ordinal_position
    `,
    tableName
  );

  return rows;
}

function hasColumn(columns: Array<{ column_name: string }>, name: string) {
  return columns.some((c) => c.column_name === name);
}

function firstExistingColumn(columns: Array<{ column_name: string }>, names: string[]) {
  return names.find((name) => hasColumn(columns, name)) || "";
}

async function getOrderByCode(code: string) {
  const orderColumns = await tableColumns("Order");
  const codeColumn = firstExistingColumn(orderColumns, ["code", "orderCode", "orderNumber", "internalCode"]);
  if (!codeColumn) {
    throw new Error('Bảng "Order" không có cột mã đơn như code/orderCode/orderNumber/internalCode.');
  }

  const rows = await prisma.$queryRawUnsafe<any[]>(
    `SELECT * FROM "Order" WHERE "${codeColumn}" = $1 LIMIT 1`,
    code
  );
  return rows[0] || null;
}

async function countOrderItems(orderId: string) {
  const rows = await prisma.$queryRawUnsafe<Array<{ count: string }>>(
    `SELECT COUNT(*)::text AS count FROM "OrderItem" WHERE "orderId" = $1`,
    orderId
  );
  return Number(rows[0]?.count || 0);
}

async function findBranchFallback() {
  const branchColumns = await tableColumns("Branch");
  const hasName = hasColumn(branchColumns, "name");
  const hasCode = hasColumn(branchColumns, "code");

  const selectExpr = [
    `"id"`,
    hasName ? `"name"` : `'' AS "name"`,
    hasCode ? `"code"` : `'' AS "code"`,
  ].join(", ");

  const orderExpr = `
    CASE
      ${hasCode ? `WHEN lower(coalesce("code",'')) = 'qo' THEN 0` : ""}
      ${hasName ? `WHEN lower(coalesce("name",'')) LIKE '%quốc oai%' THEN 1` : ""}
      ${hasName ? `WHEN lower(coalesce("name",'')) LIKE '%quoc oai%' THEN 1` : ""}
      ${hasName ? `WHEN lower(coalesce("name",'')) LIKE '%kho%' THEN 2` : ""}
      ELSE 3
    END
  `;

  const rows = await prisma.$queryRawUnsafe<any[]>(
    `
      SELECT ${selectExpr}
      FROM "Branch"
      ORDER BY ${orderExpr}
      LIMIT 1
    `
  );

  return rows[0] || null;
}

async function findVariantForProductCode(productCode: string, branchId?: string | null) {
  if (!productCode) return { variant: null, reason: "NO_PRODUCT_CODE", candidates: 0 };

  const code = productCode.toUpperCase();

  const rows = await prisma.$queryRawUnsafe<any[]>(
    `
      SELECT
        pv.*,
        p.name AS "productName",
        COALESCE(SUM(ii."availableQty"), 0)::int AS "totalStock",
        COALESCE(SUM(CASE WHEN ii."branchId" = $2 THEN ii."availableQty" ELSE 0 END), 0)::int AS "branchStock"
      FROM "ProductVariant" pv
      JOIN "Product" p ON p.id = pv."productId"
      LEFT JOIN "InventoryItem" ii ON ii."variantId" = pv.id
      WHERE upper(pv.sku) = $1
         OR upper(pv.sku) LIKE $1 || '-%'
         OR upper(p.slug) = lower($1)
         OR upper(p.name) LIKE '%' || $1 || '%'
      GROUP BY pv.id, p.name
      ORDER BY
        CASE WHEN upper(pv.sku) = $1 THEN 0 ELSE 1 END,
        COALESCE(SUM(CASE WHEN ii."branchId" = $2 THEN ii."availableQty" ELSE 0 END), 0) DESC,
        COALESCE(SUM(ii."availableQty"), 0) DESC,
        pv."createdAt" ASC
      LIMIT 20
    `,
    code,
    branchId || ""
  );

  if (rows.length === 0) return { variant: null, reason: "NO_VARIANT_MATCH", candidates: 0 };

  // If there is only one candidate, use it.
  if (rows.length === 1) return { variant: rows[0], reason: "MATCH_ONE", candidates: 1 };

  // Prefer candidate with stock in order branch.
  const withBranchStock = rows.filter((r) => Number(r.branchStock || 0) > 0);
  if (withBranchStock.length === 1) {
    return { variant: withBranchStock[0], reason: "MATCH_BRANCH_STOCK", candidates: rows.length };
  }

  // Prefer candidate with total stock if unique.
  const withStock = rows.filter((r) => Number(r.totalStock || 0) > 0);
  if (withStock.length === 1) {
    return { variant: withStock[0], reason: "MATCH_TOTAL_STOCK", candidates: rows.length };
  }

  return { variant: null, reason: "AMBIGUOUS_VARIANT", candidates: rows.length };
}

async function insertOrderItem(params: {
  columns: Array<{ column_name: string }>;
  order: AnyRow;
  variant: AnyRow;
  item: { rawName: string; productCode: string; qty: number };
  unitPrice: number;
}) {
  const { columns, order, variant, item, unitPrice } = params;

  const data: AnyRow = {};

  if (hasColumn(columns, "id")) data.id = randomUUID();
  if (hasColumn(columns, "orderId")) data.orderId = order.id;
  if (hasColumn(columns, "productId")) data.productId = variant.productId || null;
  if (hasColumn(columns, "variantId")) data.variantId = variant.id || null;

  const productNameCol = firstExistingColumn(columns, ["productName", "name", "title"]);
  if (productNameCol) data[productNameCol] = variant.productName || item.rawName;

  if (hasColumn(columns, "sku")) data.sku = variant.sku || item.productCode;
  if (hasColumn(columns, "color")) data.color = variant.color || "";
  if (hasColumn(columns, "size")) data.size = variant.size || "";
  if (hasColumn(columns, "quantity")) data.quantity = item.qty;

  const unitPriceCol = firstExistingColumn(columns, ["unitPrice", "price", "salePrice"]);
  if (unitPriceCol) data[unitPriceCol] = unitPrice || toNumber(variant.price);

  const totalCol = firstExistingColumn(columns, ["totalPrice", "lineTotal", "total", "amount"]);
  if (totalCol) data[totalCol] = (unitPrice || toNumber(variant.price)) * item.qty;

  const costCol = firstExistingColumn(columns, ["costPrice", "unitCost", "cost"]);
  if (costCol) data[costCol] = toNumber(variant.costPrice);

  if (hasColumn(columns, "createdAt")) data.createdAt = new Date();
  if (hasColumn(columns, "updatedAt")) data.updatedAt = new Date();

  const keys = Object.keys(data);
  const quotedCols = keys.map((k) => `"${k}"`).join(", ");
  const placeholders = keys.map((_, i) => `$${i + 1}`).join(", ");
  const values = keys.map((k) => data[k]);

  await prisma.$executeRawUnsafe(
    `INSERT INTO "OrderItem" (${quotedCols}) VALUES (${placeholders})`,
    ...values
  );
}

async function deductInventory(params: {
  variantId: string;
  branchId: string;
  qty: number;
  orderCode: string;
  sku: string;
}) {
  const { variantId, branchId, qty, orderCode, sku } = params;

  await prisma.$executeRawUnsafe(
    `
      INSERT INTO "InventoryItem" ("id", "variantId", "branchId", "availableQty", "reservedQty", "incomingQty", "createdAt", "updatedAt")
      VALUES ($1, $2, $3, 0, 0, 0, NOW(), NOW())
      ON CONFLICT ("variantId", "branchId") DO NOTHING
    `,
    randomUUID(),
    variantId,
    branchId
  );

  await prisma.$executeRawUnsafe(
    `
      UPDATE "InventoryItem"
      SET "availableQty" = COALESCE("availableQty", 0) - $1,
          "updatedAt" = NOW()
      WHERE "variantId" = $2
        AND "branchId" = $3
    `,
    qty,
    variantId,
    branchId
  );

  const movementCols = await tableColumns("InventoryMovement");
  if (movementCols.length) {
    const data: AnyRow = {};
    if (hasColumn(movementCols, "id")) data.id = randomUUID();
    if (hasColumn(movementCols, "variantId")) data.variantId = variantId;
    if (hasColumn(movementCols, "branchId")) data.branchId = branchId;
    if (hasColumn(movementCols, "type")) data.type = "SALE";
    if (hasColumn(movementCols, "quantity")) data.quantity = -Math.abs(qty);
    if (hasColumn(movementCols, "qty")) data.qty = -Math.abs(qty);
    if (hasColumn(movementCols, "note")) data.note = `Khôi phục từ file GHN cho đơn ${orderCode}`;
    if (hasColumn(movementCols, "reference")) data.reference = orderCode;
    if (hasColumn(movementCols, "refCode")) data.refCode = orderCode;
    if (hasColumn(movementCols, "sku")) data.sku = sku;
    if (hasColumn(movementCols, "createdAt")) data.createdAt = new Date();
    if (hasColumn(movementCols, "updatedAt")) data.updatedAt = new Date();

    const keys = Object.keys(data);
    if (keys.length) {
      await prisma.$executeRawUnsafe(
        `INSERT INTO "InventoryMovement" (${keys.map((k) => `"${k}"`).join(", ")}) VALUES (${keys
          .map((_, i) => `$${i + 1}`)
          .join(", ")})`,
        ...keys.map((k) => data[k])
      );
    }
  }
}

async function main() {
  const file = arg("file");
  const commit = hasFlag("commit");
  const deductStock = hasFlag("deduct-stock");
  const replaceExisting = hasFlag("replace-existing");

  if (!file) {
    throw new Error(
      `Thiếu file. Ví dụ:
npx ts-node scripts/rescue-ghn-order-items.ts --file="/Users/xman/Downloads/DON-HANG.xlsx"
npx ts-node scripts/rescue-ghn-order-items.ts --file="/Users/xman/Downloads/DON-HANG.xlsx" --commit
npx ts-node scripts/rescue-ghn-order-items.ts --file="/Users/xman/Downloads/DON-HANG.xlsx" --commit --deduct-stock`
    );
  }

  const wb = XLSX.readFile(file);
  const sheetName = wb.SheetNames[0];
  const rows = XLSX.utils.sheet_to_json<AnyRow>(wb.Sheets[sheetName], { defval: "" });

  const orderItemColumns = await tableColumns("OrderItem");
  const fallbackBranch = await findBranchFallback();

  const report: AnyRow[] = [];
  let parsedLines = 0;
  let restoredLines = 0;
  let skippedExisting = 0;
  let missingOrder = 0;
  let missingVariant = 0;
  let ambiguousVariant = 0;
  let deductedLines = 0;

  for (const row of rows) {
    const orderCode = normalizeText(getCell(row, ["Mã đơn hàng riêng", "Ma don hang rieng", "Mã đơn riêng", "Order code"]));
    const goodsText = normalizeText(getCell(row, ["Tên hàng hóa", "Ten hang hoa", "Hàng hóa", "Products"]));
    const cod = toNumber(getCell(row, ["Tiền COD", "COD"]));
    const status = normalizeText(getCell(row, ["Trạng thái", "Trang thai"]));

    if (!orderCode || !orderCode.startsWith("ORD-") || !goodsText) continue;

    const order = await getOrderByCode(orderCode);
    if (!order) {
      missingOrder += 1;
      report.push({ orderCode, status, goodsText, result: "MISSING_ORDER" });
      continue;
    }

    const currentItemCount = await countOrderItems(order.id);
    if (currentItemCount > 0 && !replaceExisting) {
      skippedExisting += 1;
      report.push({ orderCode, status, goodsText, result: "SKIP_HAS_ITEMS", currentItemCount });
      continue;
    }

    if (currentItemCount > 0 && replaceExisting && commit) {
      await prisma.$executeRawUnsafe(`DELETE FROM "OrderItem" WHERE "orderId" = $1`, order.id);
    }

    const items = parseItems(goodsText);
    const totalQty = items.reduce((sum, item) => sum + item.qty, 0) || 1;
    const fallbackUnitPrice = Math.max(0, Math.round(cod / totalQty));
    const branchId = order.branchId || fallbackBranch?.id || "";

    for (const item of items) {
      parsedLines += 1;

      const match = await findVariantForProductCode(item.productCode, branchId);

      if (!match.variant) {
        if (match.reason === "AMBIGUOUS_VARIANT") ambiguousVariant += 1;
        else missingVariant += 1;

        report.push({
          orderCode,
          status,
          rawName: item.rawName,
          productCode: item.productCode,
          qty: item.qty,
          result: match.reason,
          candidates: match.candidates,
        });
        continue;
      }

      const unitPrice = toNumber(match.variant.price) || fallbackUnitPrice;

      report.push({
        orderCode,
        status,
        rawName: item.rawName,
        productCode: item.productCode,
        qty: item.qty,
        matchedSku: match.variant.sku,
        matchedProductName: match.variant.productName,
        color: match.variant.color,
        size: match.variant.size,
        unitPrice,
        branchId,
        result: commit ? "RESTORED" : "DRY_RUN_OK",
        matchReason: match.reason,
        candidates: match.candidates,
      });

      if (commit) {
        await insertOrderItem({
          columns: orderItemColumns,
          order,
          variant: match.variant,
          item,
          unitPrice,
        });
        restoredLines += 1;

        if (deductStock && branchId) {
          await deductInventory({
            variantId: match.variant.id,
            branchId,
            qty: item.qty,
            orderCode,
            sku: match.variant.sku,
          });
          deductedLines += 1;
        }
      }
    }
  }

  const outWb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(outWb, XLSX.utils.json_to_sheet(report), "preview");
  XLSX.writeFile(outWb, `ghn_orderitem_rescue_${commit ? "commit" : "dry_run"}_${Date.now()}.xlsx`);

  console.log({
    mode: commit ? "COMMIT" : "DRY_RUN",
    deductStock,
    sourceRows: rows.length,
    parsedLines,
    restoredLines,
    skippedExisting,
    missingOrder,
    missingVariant,
    ambiguousVariant,
    deductedLines,
    reportFile: `ghn_orderitem_rescue_${commit ? "commit" : "dry_run"}_<timestamp>.xlsx`,
  });
}

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
