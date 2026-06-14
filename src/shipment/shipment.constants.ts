export const AHAMOVE_DEFAULT_SERVICE_ID = "HAN-BIKE";

export const AHAMOVE_STATUS_LABELS: Record<string, string> = {
  ASSIGNING: "Đang tìm tài xế",
  IDLE: "Chờ tài xế",
  ACCEPTED: "Tài xế đã nhận",
  IN_PROCESS: "Đang giao",
  COMPLETED: "Đã giao thành công",
  CANCELLED: "Đã hủy",
  FAILED: "Giao thất bại",
};


export const SPX_STATUS_LABELS: Record<string, string> = {
  CREATED: "Đã tạo vận đơn",
  PICKING: "Đang lấy hàng",
  IN_TRANSIT: "Đang trung chuyển",
  DELIVERING: "Đang giao",
  DELIVERED: "Đã giao thành công",
  FAILED: "Giao thất bại",
  RETURNING: "Đang hoàn hàng",
  RETURNED: "Đã hoàn hàng",
  CANCELLED: "Đã hủy",
};
