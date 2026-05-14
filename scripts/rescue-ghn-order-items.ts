import { PrismaService } from "../src/prisma/prisma.service";
import * as XLSX from "xlsx";

const prisma = new PrismaService();

function arg(name: string) {
  const prefix = `--${name}=`;
  const found = process.argv.find((x) => x.startsWith(prefix));
  return found ? found.slice(prefix.length) : "";
}

function hasFlag(name: string) {
  return process.argv.includes(`--${name}`);
}

function normalize(value: any) {
  return String(value || "").trim();
}

function normalizeHeader(value: any) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/đ/g, "d")
    .replace(/[^a-z0-9]+/g, "");
}

function getCell(row: any, names: string[]) {
  const keys = Object.keys(row || {});

  for (const name of names) {
    const found = keys.find(
      (key) => normalizeHeader(key) === normalizeHeader(name)
    );

    if (found) return row[found];
  }

  return "";
}

async function tableColumns(tableName: string) {
  return prisma.$queryRawUnsafe<Array<{ column_name: string }>>(
    `
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = $1
      ORDER BY ordinal_position
    `,
    tableName
  );
}

function pickColumn(
  columns: Array<{ column_name: string }>,
  candidates: string[]
) {
  const set = new Set(columns.map((c) => c.column_name));

  return candidates.find((name) => set.has(name)) || "";
}

function extractProductCode(text: string) {
  const matches =
    text.toUpperCase().match(/\b[A-Z]{1,6}\d{2,6}[A-Z0-9-]*\b/g) || [];

  return matches[0] || "";
}

function parseQty(text: string) {
  const found = String(text || "").match(/\[(\d+)/);

  return found ? Math.max(1, Number(found[1] || 1)) : 1;
}

async function findOrder(
  orderCode: string,
  orderCodeColumn: string
) {
  const sql = `
    SELECT *
    FROM "Order"
    WHERE "${orderCodeColumn}" = $1
    LIMIT 1
  `;

  const rows = await prisma.$queryRawUnsafe<any[]>(
    sql,
    orderCode
  );

  return rows[0] || null;
}

async function findVariant(productCode: string) {
  const rows = await prisma.$queryRawUnsafe<any[]>(
    `
      SELECT
        pv.*,
        p.name AS "productName"
      FROM "ProductVariant" pv
      JOIN "Product" p
        ON p.id = pv."productId"
      WHERE upper(pv.sku) LIKE $1 || '%'
         OR upper(p.slug) = lower($1)
         OR upper(p.name) LIKE '%' || $1 || '%'
      ORDER BY pv."createdAt" ASC
      LIMIT 1
    `,
    productCode.toUpperCase()
  );

  return rows[0] || null;
}

async function existingOrderItemCount(orderId: string) {
  const rows = await prisma.$queryRawUnsafe<
    Array<{ count: string }>
  >(
    `
      SELECT COUNT(*)::text AS count
      FROM "OrderItem"
      WHERE "orderId" = $1
    `,
    orderId
  );

  return Number(rows[0]?.count || 0);
}

async function insertOrderItem(
  orderItemColumns: Array<{ column_name: string }>,
  orderId: string,
  variant: any,
  qty: number
) {
  const colSet = new Set(
    orderItemColumns.map((c) => c.column_name)
  );

  const price = Number(variant.price || 0);

  const data: Record<string, any> = {};

  if (colSet.has("id")) data.id = crypto.randomUUID();

  if (colSet.has("orderId")) data.orderId = orderId;

  if (colSet.has("productId"))
    data.productId = variant.productId || null;

  if (colSet.has("variantId"))
    data.variantId = variant.id || null;

  if (colSet.has("sku"))
    data.sku = variant.sku || "";

  if (colSet.has("productName"))
    data.productName = variant.productName || "";

  if (colSet.has("name"))
    data.name = variant.productName || "";

  if (colSet.has("color"))
    data.color = variant.color || "";

  if (colSet.has("size"))
    data.size = variant.size || "";

  if (colSet.has("quantity"))
    data.quantity = qty;

  if (colSet.has("qty"))
    data.qty = qty;

  if (colSet.has("unitPrice"))
    data.unitPrice = price;

  if (colSet.has("price"))
    data.price = price;

  if (colSet.has("costPrice"))
    data.costPrice = Number(
      variant.costPrice || 0
    );

  if (colSet.has("totalPrice"))
    data.totalPrice = price * qty;

  if (colSet.has("lineTotal"))
    data.lineTotal = price * qty;

  if (colSet.has("subtotal"))
    data.subtotal = price * qty;

  if (colSet.has("createdAt"))
    data.createdAt = new Date();

  if (colSet.has("updatedAt"))
    data.updatedAt = new Date();

  const keys = Object.keys(data);

  const cols = keys
    .map((k) => `"${k}"`)
    .join(", ");

  const params = keys
    .map((_, i) => `$${i + 1}`)
    .join(", ");

  const values = keys.map((k) => data[k]);

  await prisma.$executeRawUnsafe(
    `
      INSERT INTO "OrderItem"
      (${cols})
      VALUES
      (${params})
    `,
    ...values
  );
}

async function main() {
  const file = arg("file");

  const commit = hasFlag("commit");

  const replaceExisting =
    hasFlag("replace-existing");

  if (!file) {
    throw new Error("Thiếu --file");
  }

  const orderColumns = await tableColumns(
    "Order"
  );

  const orderCodeColumn = pickColumn(
    orderColumns,
    [
      "code",
      "orderCode",
      "internalCode",
      "customCode",
      "orderNumber",
      "displayCode",
      "shopOrderCode",
    ]
  );

  if (!orderCodeColumn) {
    console.log(
      "Các cột hiện có trong bảng Order:",
      orderColumns.map((c) => c.column_name)
    );

    throw new Error(
      'Không tìm thấy cột mã đơn trong bảng "Order".'
    );
  }

  const orderItemColumns =
    await tableColumns("OrderItem");

  console.log(
    "Dùng cột mã đơn:",
    orderCodeColumn
  );

  const workbook = XLSX.readFile(file);

  const sheet =
    workbook.Sheets[workbook.SheetNames[0]];

  const rows = XLSX.utils.sheet_to_json<any>(
    sheet,
    {
      defval: "",
    }
  );

  let parsed = 0;

  let ready = 0;

  let skippedHasItems = 0;

  let missingOrder = 0;

  let missingVariant = 0;

  let restored = 0;

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];

    const orderCode = normalize(
      getCell(row, [
        "Mã đơn hàng riêng",
        "Ma don hang rieng",
        "Mã đơn riêng",
        "Order code",
      ])
    );

    const goods = normalize(
      getCell(row, [
        "Tên hàng hóa",
        "Ten hang hoa",
        "Hàng hóa",
        "Products",
      ])
    );

    if (
      !orderCode ||
      !goods ||
      !orderCode.startsWith("ORD-")
    ) {
      continue;
    }

    const order = await findOrder(
      orderCode,
      orderCodeColumn
    );

    if (!order) {
      missingOrder += 1;
      continue;
    }

    const currentCount =
      await existingOrderItemCount(order.id);

    if (
      currentCount > 0 &&
      !replaceExisting
    ) {
      skippedHasItems += 1;
      continue;
    }

    const productCode =
      extractProductCode(goods);

    const qty = parseQty(goods);

    parsed += 1;

    if (!productCode) {
      missingVariant += 1;
      continue;
    }

    const variant =
      await findVariant(productCode);

    if (!variant) {
      missingVariant += 1;
      continue;
    }

    ready += 1;

    const percent = Math.round(
      ((i + 1) / rows.length) * 100
    );

    if (
      i % 20 === 0 ||
      i === rows.length - 1
    ) {
      console.log(
        `[${percent}%] dòng ${i + 1}/${rows.length} | parsed ${parsed} | ready ${ready} | restored ${restored} | thiếu đơn ${missingOrder} | thiếu SP ${missingVariant}`
      );
    }

    if (commit) {
      if (
        currentCount > 0 &&
        replaceExisting
      ) {
        await prisma.$executeRawUnsafe(
          `
            DELETE FROM "OrderItem"
            WHERE "orderId" = $1
          `,
          order.id
        );
      }

      await insertOrderItem(
        orderItemColumns,
        order.id,
        variant,
        qty
      );

      restored += 1;
    }
  }

  console.log({
    mode: commit
      ? "COMMIT"
      : "DRY_RUN",

    orderCodeColumn,

    totalRows: rows.length,

    parsed,

    ready,

    restored,

    skippedHasItems,

    missingOrder,

    missingVariant,
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