type CellGrid = Array<Array<any>>;

export type ParsedAttendanceRow = {
  attendanceCode: string;
  staffName: string;
  department?: string | null;
  branchName?: string | null;
  normalHours: number;
  overtimeHours: number;
  holidayHours: number;
  overtime3Hours: number;
  totalWorkHours: number;
  lateCount: number;
  lateMinutes: number;
  earlyCount: number;
  earlyMinutes: number;
  raw?: Record<string, any>;
};

function text(value: any) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}
function num(value: any) {
  if (typeof value === "number") return Number.isFinite(value) ? value : 0;
  const cleaned = String(value ?? "").replace(/,/g, ".").replace(/[^\d.-]/g, "");
  const parsed = Number(cleaned);
  return Number.isFinite(parsed) ? parsed : 0;
}
function norm(value: any) {
  return text(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function rowText(row: any[]) {
  return row.map(text).filter(Boolean).join(" ");
}

function mostCommon(values: string[]) {
  const counter = new Map<string, number>();
  values.map(text).filter(Boolean).forEach((v) => counter.set(v, (counter.get(v) || 0) + 1));
  return Array.from(counter.entries()).sort((a, b) => b[1] - a[1])[0]?.[0] || null;
}

function getSummaryNumber(block: any[][], label: string, preferredIndex = -1) {
  const labelNorm = norm(label);
  const row = block.find((r) => norm(r[0]).includes(labelNorm));
  if (!row) return 0;
  if (preferredIndex >= 0 && row[preferredIndex] !== undefined) return num(row[preferredIndex]);
  for (let i = row.length - 1; i >= 1; i--) {
    const n = num(row[i]);
    if (n) return n;
  }
  return 0;
}

function getCountAndMinutes(block: any[][], label: string) {
  const labelNorm = norm(label);
  const row = block.find((r) => norm(r[0]).includes(labelNorm));
  if (!row) return { count: 0, minutes: 0 };
  const numbers = row.slice(1).map(num).filter((v) => Number.isFinite(v));
  return {
    count: numbers[0] || 0,
    minutes: numbers[1] || numbers[numbers.length - 1] || 0,
  };
}

function extractStaffHeader(row: any[]) {
  const full = rowText(row);
  const codeMatch = full.match(/Mã\s*nhân\s*viên\s*[:：]?\s*([^\s]+)/i) || full.match(/Ma\s*nhan\s*vien\s*[:：]?\s*([^\s]+)/i);
  const nameMatch = full.match(/Tên\s*nhân\s*viên\s*[:：]?\s*(.*?)(?:\s+Bộ\s*phận\s*[:：]|\s+Bo\s*phan\s*[:：]|$)/i);
  const deptMatch = full.match(/Bộ\s*phận\s*[:：]?\s*(.*)$/i) || full.match(/Bo\s*phan\s*[:：]?\s*(.*)$/i);
  if (!codeMatch && !nameMatch) return null;
  return {
    attendanceCode: text(codeMatch?.[1] || ""),
    staffName: text(nameMatch?.[1] || ""),
    department: text(deptMatch?.[1] || "") || null,
  };
}

function detectBranchFromBlock(block: any[][]) {
  const places: string[] = [];
  for (const row of block) {
    const joined = rowText(row);
    if (/thái\s*hà|thai\s*ha/i.test(joined)) places.push("THÁI HÀ");
    if (/quốc\s*oai|quoc\s*oai/i.test(joined)) places.push("QUỐC OAI");
    if (/chùa\s*láng|chua\s*lang/i.test(joined)) places.push("CHÙA LÁNG");
    if (/xã\s*đàn|xa\s*dan/i.test(joined)) places.push("XÃ ĐÀN");
  }
  return mostCommon(places);
}

export function parseAttendanceWorkbook(buffer: Buffer | Uint8Array): ParsedAttendanceRow[] {
  let XLSX: any;
  try {
    // Dùng require để không làm fail build nếu môi trường chưa cài type package.
    XLSX = require("xlsx");
  } catch {
    throw new Error("Thiếu package xlsx. Chạy: npm install xlsx");
  }

  const workbook = XLSX.read(buffer, { type: "buffer", cellDates: true });
  const sheetName = workbook.SheetNames.find((name: string) => norm(name).includes("chi tiet")) || workbook.SheetNames[0];
  const sheet = workbook.Sheets[sheetName];
  const rows: CellGrid = XLSX.utils.sheet_to_json(sheet, { header: 1, raw: true, defval: "" });

  const starts: number[] = [];
  rows.forEach((row, index) => {
    if (/Mã\s*nhân\s*viên|Ma\s*nhan\s*vien/i.test(rowText(row))) starts.push(index);
  });

  const result: ParsedAttendanceRow[] = [];
  for (let i = 0; i < starts.length; i++) {
    const start = starts[i];
    const end = starts[i + 1] ?? rows.length;
    const header = extractStaffHeader(rows[start]);
    if (!header?.attendanceCode && !header?.staffName) continue;
    const block = rows.slice(start, end);

    const totalWorkHours = getSummaryNumber(block, "TỔNG", 2) || getSummaryNumber(block, "TONG", 2);
    const normalHours = getSummaryNumber(block, "Ngày thường", 2) || getSummaryNumber(block, "Ngay thuong", 2) || totalWorkHours;
    const overtimeHours = getSummaryNumber(block, "Tăng ca 1", 2) || getSummaryNumber(block, "Tang ca 1", 2);
    const holidayHours = getSummaryNumber(block, "Tăng ca 2", 2) || getSummaryNumber(block, "Tang ca 2", 2);
    const overtime3Hours = getSummaryNumber(block, "Tăng ca 3", 2) || getSummaryNumber(block, "Tang ca 3", 2);
    const late = getCountAndMinutes(block, "Đi trễ");
    const early = getCountAndMinutes(block, "Về sớm");

    const meaningful = totalWorkHours || normalHours || overtimeHours || holidayHours || late.minutes || early.minutes;
    if (!meaningful) continue;

    result.push({
      attendanceCode: header.attendanceCode,
      staffName: header.staffName,
      department: header.department,
      branchName: detectBranchFromBlock(block),
      normalHours,
      overtimeHours,
      holidayHours,
      overtime3Hours,
      totalWorkHours: totalWorkHours || normalHours + overtimeHours + holidayHours + overtime3Hours,
      lateCount: Math.round(late.count),
      lateMinutes: Math.round(late.minutes),
      earlyCount: Math.round(early.count),
      earlyMinutes: Math.round(early.minutes),
      raw: { sheetName, startRow: start + 1 },
    });
  }

  return result;
}
