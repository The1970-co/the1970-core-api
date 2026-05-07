export function mapAhamoveShippingStatus(input?: string | null) {
  const value = String(input || "").toUpperCase();

  if (!value) return "NOT_CREATED";
  if (value.includes("CANCEL")) return "CANCELLED";
  if (value.includes("COMPLETED")) return "DELIVERED";
  if (value.includes("IDLE") || value.includes("ASSIGNING")) return "CREATED";
  if (value.includes("ACCEPTED")) return "PICKING";
  if (value.includes("IN PROCESS") || value.includes("IN_PROCESS")) return "DELIVERING";
  if (value.includes("FAILED") || value.includes("FAIL")) return "FAILED";

  return value;
}

export function mapAhamoveOrderPatch(input?: string | null) {
  const status = mapAhamoveShippingStatus(input);

  if (status === "DELIVERED") {
    return {
      status: "COMPLETED",
      fulfillmentStatus: "FULFILLED",
    };
  }

  if (status === "CANCELLED" || status === "FAILED") {
    return {
      fulfillmentStatus: "RETURNED",
    };
  }

  if (status === "PICKING" || status === "DELIVERING" || status === "CREATED") {
    return {
      status: "SHIPPED",
      fulfillmentStatus: "PROCESSING",
    };
  }

  return {};
}
