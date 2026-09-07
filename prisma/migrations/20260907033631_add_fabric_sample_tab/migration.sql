-- DropIndex
DROP INDEX "CashVoucher_branchId_createdAt_idx";

-- DropIndex
DROP INDEX "CashVoucher_paymentSourceId_createdAt_idx";

-- DropIndex
DROP INDEX "CashVoucher_type_status_createdAt_idx";

-- DropIndex
DROP INDEX "Order_branchId_createdAt_desc_idx";

-- DropIndex
DROP INDEX "Order_createdAt_desc_idx";

-- DropIndex
DROP INDEX "Order_createdByStaffName_trgm_idx";

-- DropIndex
DROP INDEX "Order_customerName_trgm_idx";

-- DropIndex
DROP INDEX "Order_orderCode_idx";

-- DropIndex
DROP INDEX "Order_paymentStatus_createdAt_desc_idx";

-- DropIndex
DROP INDEX "Order_shippingAddressLine1_trgm_idx";

-- DropIndex
DROP INDEX "Order_shippingPhone_idx";

-- DropIndex
DROP INDEX "Order_status_createdAt_desc_idx";

-- DropIndex
DROP INDEX "OrderItem_productName_trgm_idx";

-- DropIndex
DROP INDEX "OrderItem_sku_idx";

-- DropIndex
DROP INDEX "OrderItem_sku_trgm_idx";

-- DropIndex
DROP INDEX "Shipment_carrier_idx";

-- DropIndex
DROP INDEX "Shipment_trackingCode_trgm_idx";

-- AlterTable
ALTER TABLE "DesignSample" ADD COLUMN     "fabricSampleReceivedAt" TIMESTAMP(3);

-- CreateIndex
CREATE INDEX "DesignSample_priorityLane_idx" ON "DesignSample"("priorityLane");

-- CreateIndex
CREATE INDEX "DesignSample_fabricSampleReceivedAt_idx" ON "DesignSample"("fabricSampleReceivedAt");
