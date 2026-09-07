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

-- CreateTable
CREATE TABLE "DesignSampleMaterialBoard" (
    "id" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "description" TEXT,
    "sortOrder" INTEGER NOT NULL DEFAULT 0,
    "createdById" TEXT,
    "createdByName" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "DesignSampleMaterialBoard_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "DesignSampleMaterialBoardItem" (
    "id" TEXT NOT NULL,
    "boardId" TEXT NOT NULL,
    "designSampleId" TEXT NOT NULL,
    "priorityRank" INTEGER,
    "sortOrder" INTEGER NOT NULL DEFAULT 0,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "DesignSampleMaterialBoardItem_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "DesignSampleMaterialBoard_name_key" ON "DesignSampleMaterialBoard"("name");

-- CreateIndex
CREATE INDEX "DesignSampleMaterialBoard_sortOrder_name_idx" ON "DesignSampleMaterialBoard"("sortOrder", "name");

-- CreateIndex
CREATE UNIQUE INDEX "DesignSampleMaterialBoardItem_designSampleId_key" ON "DesignSampleMaterialBoardItem"("designSampleId");

-- CreateIndex
CREATE INDEX "DesignSampleMaterialBoardItem_boardId_sortOrder_idx" ON "DesignSampleMaterialBoardItem"("boardId", "sortOrder");

-- CreateIndex
CREATE INDEX "DesignSampleMaterialBoardItem_boardId_priorityRank_idx" ON "DesignSampleMaterialBoardItem"("boardId", "priorityRank");

-- CreateIndex
CREATE UNIQUE INDEX "DesignSampleMaterialBoardItem_boardId_priorityRank_key" ON "DesignSampleMaterialBoardItem"("boardId", "priorityRank");

-- AddForeignKey
ALTER TABLE "DesignSampleMaterialBoardItem" ADD CONSTRAINT "DesignSampleMaterialBoardItem_boardId_fkey" FOREIGN KEY ("boardId") REFERENCES "DesignSampleMaterialBoard"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DesignSampleMaterialBoardItem" ADD CONSTRAINT "DesignSampleMaterialBoardItem_designSampleId_fkey" FOREIGN KEY ("designSampleId") REFERENCES "DesignSample"("id") ON DELETE CASCADE ON UPDATE CASCADE;
