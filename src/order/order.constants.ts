export const ORDER_STATUS_LABEL = {
  NEW: "Đặt hàng",
  APPROVED: "Duyệt",
  PACKING: "Đóng gói",
  SHIPPED: "Xuất kho",
  COMPLETED: "Hoàn thành",
  CANCELLED: "Đã hủy",
} as const;

export const PAYMENT_STATUS_LABEL = {
  UNPAID: "Chưa thanh toán",
  PARTIAL: "Thanh toán một phần",
  PAID: "Đã thanh toán",
  REFUNDED: "Đã hoàn tiền",
} as const;

export const SHIPMENT_STATUS_LABEL = {
  NOT_CREATED: "Chưa tạo vận đơn",
  CREATED: "Đã tạo vận đơn",
  PICKING: "Đang lấy hàng",
  DELIVERING: "Đang giao",
  DELIVERED: "Giao thành công",
  FAILED: "Giao thất bại",
  RETURNING: "Đang hoàn",
  RETURNED: "Đã hoàn",
} as const;

export const ORDER_TIMELINE = [
  { key: "NEW", label: "Đặt hàng" },
  { key: "APPROVED", label: "Duyệt" },
  { key: "PACKING", label: "Đóng gói" },
  { key: "SHIPPED", label: "Xuất kho" },
  { key: "COMPLETED", label: "Hoàn thành" },
] as const;

export const CREATE_ORDER_MODES = [
  {
    value: "draft",
    title: "Tạo nháp",
    description: "Lưu đơn ở bước đặt hàng.",
    targetStatus: "NEW",
  },
  {
    value: "approve",
    title: "Tạo và duyệt",
    description: "Chuyển đơn sang bước duyệt để kho xử lý.",
    targetStatus: "APPROVED",
  },
  {
    value: "ship",
    title: "Tạo và xuất kho",
    description: "Đi thẳng tới xuất kho và gửi vận chuyển.",
    targetStatus: "SHIPPED",
  },
] as const;