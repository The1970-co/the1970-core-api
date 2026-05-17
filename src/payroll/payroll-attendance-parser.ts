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

function numericValuesBetween(row: any[], startIndex: number, stopPatterns: RegExp[] = []) {
  const values: number[] = [];
  for (let i = Math.max(0, startIndex); i < row.length; i++) {
    const cellText = norm(row[i]);
    if (cellText && stopPatterns.some((pattern) => pattern.test(cellText))) break;
    const value = num(row[i]);
    if (String(row[i] ?? "").trim() !== "" && Number.isFinite(value)) values.push(value);
  }
  return values;
}

function findLabelIndex(row: any[], label: string) {
  const labelNorm = norm(label);
  return row.findIndex((cell) => norm(cell).includes(labelNorm));
}

function findRowByLabel(block: any[][], label: string) {
  const labelNorm = norm(label);
  return block.find((r) => r.some((cell) => norm(cell).includes(labelNorm))) || null;
}

function getWorkHoursFromSummary(block: any[][], label: string) {
  const row = findRowByLabel(block, label);
  if (!row) return 0;
  const idx = findLabelIndex(row, label);
  const values = numericValuesBetween(row, idx + 1, [/tang ca/i, /di tre/i, /ve som/i]);
  // File vân tay đang có cặp: số công, số giờ. Lấy số giờ (giá trị thứ 2), không lấy số công.
  return values.length >= 2 ? values[1] : values[0] || 0;
}

function getFirstNumberAfterLabel(block: any[][], label: string) {
  for (const row of block) {
    const idx = findLabelIndex(row, label);
    if (idx < 0) continue;
    const values = numericValuesBetween(row, idx + 1, [/di tre/i, /ve som/i, /so lan/i, /so phut/i]);
    if (values.length) return values[0];
  }
  return 0;
}

function getRepeatedMetricPair(block: any[][], label: string) {
  const labelNorm = norm(label);
  for (const row of block) {
    const found: number[] = [];
    for (let i = 0; i < row.length; i++) {
      if (!norm(row[i]).includes(labelNorm)) continue;
      const values = numericValuesBetween(row, i + 1, [/so lan/i, /so phut/i, /tang ca/i, /di tre/i, /ve som/i]);
      found.push(values[0] || 0);
    }
    if (found.length) return { first: found[0] || 0, second: found[1] || 0 };
  }
  return { first: 0, second: 0 };
}

function getAttendanceLateEarly(block: any[][]) {
  const counts = getRepeatedMetricPair(block, "Số lần");
  const minutes = getRepeatedMetricPair(block, "Số phút");
  return {
    lateCount: counts.first,
    earlyCount: counts.second,
    lateMinutes: minutes.first,
    earlyMinutes: minutes.second,
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

    const totalWorkHours = getWorkHoursFromSummary(block, "TỔNG") || getWorkHoursFromSummary(block, "TONG");
    // Lương giờ dùng tổng giờ thực tế của tháng. Không lấy cột "số công".
    const normalHours = totalWorkHours || getWorkHoursFromSummary(block, "Ngày thường") || getWorkHoursFromSummary(block, "Ngay thuong");
    const overtimeHours = getFirstNumberAfterLabel(block, "Tăng ca 1") || getFirstNumberAfterLabel(block, "Tang ca 1");
    const holidayHours = getFirstNumberAfterLabel(block, "Tăng ca 2") || getFirstNumberAfterLabel(block, "Tang ca 2");
    const overtime3Hours = getFirstNumberAfterLabel(block, "Tăng ca 3") || getFirstNumberAfterLabel(block, "Tang ca 3");
    const attendance = getAttendanceLateEarly(block);

    const meaningful = totalWorkHours || normalHours || overtimeHours || holidayHours || attendance.lateMinutes || attendance.earlyMinutes;
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
      lateCount: Math.round(attendance.lateCount),
      lateMinutes: Math.round(attendance.lateMinutes),
      earlyCount: Math.round(attendance.earlyCount),
      earlyMinutes: Math.round(attendance.earlyMinutes),
      raw: { sheetName, startRow: start + 1 },
    });
  }

  return result;
}
