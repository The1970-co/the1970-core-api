-- CreateEnum
CREATE TYPE "PurchaseReceiptStatus" AS ENUM ('DRAFT', 'PAYMENT_REQUESTED', 'PARTIALLY_PAID', 'PAID', 'STOCK_IMPORTED', 'COMPLETED', 'CANCELLED');

-- CreateEnum
CREATE TYPE "TransferStatus" AS ENUM ('DRAFT', 'COMPLETED', 'CANCELLED', 'PENDING', 'CONFIRMED', 'IN_TRANSIT');

-- CreateEnum
CREATE TYPE "StockTransferDirection" AS ENUM ('OUTBOUND_TO_BRANCH', 'INBOUND_FROM_BRANCH');

-- CreateEnum
CREATE TYPE "StockTransferSourceType" AS ENUM ('AUTO', 'REQUEST', 'MANUAL');

-- CreateEnum
CREATE TYPE "BranchNotificationType" AS ENUM ('TRANSFER_OUT_CREATED', 'TRANSFER_OUT_CONFIRMED', 'TRANSFER_IN_CREATED', 'TRANSFER_IN_CONFIRMED');

-- CreateEnum
CREATE TYPE "PromotionType" AS ENUM ('PRODUCT_DISCOUNT', 'ORDER_DISCOUNT');

-- CreateEnum
CREATE TYPE "PromotionDiscountType" AS ENUM ('PERCENT', 'FIXED_AMOUNT');

-- CreateEnum
CREATE TYPE "PromotionStatus" AS ENUM ('ACTIVE', 'INACTIVE');

-- CreateEnum
CREATE TYPE "OmniChannel" AS ENUM ('FACEBOOK', 'INSTAGRAM', 'SYSTEM');

-- CreateEnum
CREATE TYPE "OmniConversationStatus" AS ENUM ('OPEN', 'PENDING', 'PROCESSING', 'CLOSED', 'SPAM');

-- CreateEnum
CREATE TYPE "OmniMessageDirection" AS ENUM ('IN', 'OUT', 'SYSTEM');

-- CreateEnum
CREATE TYPE "OmniMessageType" AS ENUM ('TEXT', 'IMAGE', 'FILE', 'STICKER', 'UNKNOWN');

-- CreateEnum
CREATE TYPE "DesignSampleStatus" AS ENUM ('IDEA', 'FABRIC_SELECTED', 'SAMPLING', 'SAMPLE_READY', 'REVISING', 'APPROVED_FOR_PRODUCTION', 'IN_PRODUCTION', 'COMPLETED', 'ON_HOLD');

-- CreateEnum
CREATE TYPE "FabricReceiptStatus" AS ENUM ('DRAFT', 'RECEIVING', 'INSPECTING', 'COMPLETED', 'CANCELLED');

-- CreateEnum
CREATE TYPE "FabricPriceUnit" AS ENUM ('METER', 'KG', 'ROLL');

-- CreateEnum
CREATE TYPE "FabricOrderStatus" AS ENUM ('DRAFT', 'ORDERED', 'PARTIAL', 'RECEIVED', 'CANCELLED');

-- CreateEnum
CREATE TYPE "FabricImageType" AS ENUM ('FABRIC', 'DEFECT', 'ROUND_SAMPLE', 'SCALE', 'DOCUMENT', 'OTHER');

-- CreateEnum
CREATE TYPE "DesignSampleImageType" AS ENUM ('SAMPLE', 'REFERENCE', 'FITTING', 'RETURNED', 'OTHER');

-- CreateEnum
CREATE TYPE "FabricSampleDispatchStatus" AS ENUM ('SENT', 'RECEIVED', 'MAKING', 'RETURNED', 'REVISING', 'APPROVED', 'CANCELLED');

-- CreateEnum
CREATE TYPE "FabricBoardImageType" AS ENUM ('BOARD', 'SWATCH', 'COLOR', 'DOCUMENT', 'OTHER');

-- CreateEnum
CREATE TYPE "SampleTechnicalPersonRole" AS ENUM ('SAMPLE_MAKER', 'PATTERN_MAKER', 'BOTH');

-- CreateEnum
CREATE TYPE "ProductionOrderStatus" AS ENUM ('DRAFT', 'PLANNING', 'READY', 'SENT', 'CUTTING', 'SEWING', 'QC', 'COMPLETED', 'CANCELLED');

-- CreateEnum
CREATE TYPE "ProductionProductKind" AS ENUM ('SHIRT', 'PANTS', 'OTHER');

-- CreateEnum
CREATE TYPE "ProductionAccessoryUnit" AS ENUM ('PIECE', 'METER', 'ROLL', 'SET', 'KG', 'PACK', 'BOX', 'OTHER');

-- CreateEnum
CREATE TYPE "WebsitePublishStatus" AS ENUM ('DRAFT', 'PUBLISHED', 'ARCHIVED');

-- AlterEnum
-- This migration adds more than one value to an enum.
-- With PostgreSQL versions 11 and earlier, this is not possible
-- in a single migration. This can be worked around by creating
-- multiple migrations, each migration adding only one value to
-- the enum.


ALTER TYPE "InventoryMovementType" ADD VALUE 'TRANSFER_OUT';
ALTER TYPE "InventoryMovementType" ADD VALUE 'TRANSFER_IN';
ALTER TYPE "InventoryMovementType" ADD VALUE 'STOCKTAKE_ADJUST';

-- AlterEnum
BEGIN;
CREATE TYPE "OrderStatus_new" AS ENUM ('NEW', 'APPROVED', 'PACKING', 'SHIPPED', 'COMPLETED', 'CANCELLED');
ALTER TABLE "public"."Order" ALTER COLUMN "orderStatus" DROP DEFAULT;
ALTER TABLE "Order" ALTER COLUMN "status" TYPE "OrderStatus_new" USING ("status"::text::"OrderStatus_new");
ALTER TYPE "OrderStatus" RENAME TO "OrderStatus_old";
ALTER TYPE "OrderStatus_new" RENAME TO "OrderStatus";
DROP TYPE "public"."OrderStatus_old";
COMMIT;

-- AlterEnum
BEGIN;
CREATE TYPE "PaymentStatus_new" AS ENUM ('UNPAID', 'PARTIAL', 'PAID', 'PENDING_COD', 'REFUNDED', 'FAILED');
ALTER TABLE "public"."Order" ALTER COLUMN "paymentStatus" DROP DEFAULT;
ALTER TABLE "public"."Payment" ALTER COLUMN "status" DROP DEFAULT;
ALTER TABLE "Order" ALTER COLUMN "paymentStatus" TYPE "PaymentStatus_new" USING ("paymentStatus"::text::"PaymentStatus_new");
ALTER TABLE "Payment" ALTER COLUMN "status" TYPE "PaymentStatus_new" USING ("status"::text::"PaymentStatus_new");
ALTER TYPE "PaymentStatus" RENAME TO "PaymentStatus_old";
ALTER TYPE "PaymentStatus_new" RENAME TO "PaymentStatus";
DROP TYPE "public"."PaymentStatus_old";
ALTER TABLE "Order" ALTER COLUMN "paymentStatus" SET DEFAULT 'UNPAID';
ALTER TABLE "Payment" ALTER COLUMN "status" SET DEFAULT 'UNPAID';
COMMIT;

-- AlterEnum
ALTER TYPE "SalesChannel" ADD VALUE 'POS';

-- AlterEnum
ALTER TYPE "UserRole" ADD VALUE 'ADMIN';

-- DropIndex
DROP INDEX "InventoryItem_variantId_key";

-- AlterTable
ALTER TABLE "AdminUser" ADD COLUMN     "totpEnabled" BOOLEAN NOT NULL DEFAULT false,
ADD COLUMN     "totpSecret" TEXT;

-- AlterTable
ALTER TABLE "Customer" ADD COLUMN     "birthDate" TIMESTAMP(3),
ADD COLUMN     "customerGroup" TEXT,
ADD COLUMN     "customerNote" TEXT,
ADD COLUMN     "defaultDiscountPercent" DECIMAL(5,2),
ADD COLUMN     "gender" TEXT,
ADD COLUMN     "lastImportedAt" TIMESTAMP(3),
ADD COLUMN     "lastImportedSource" TEXT,
ADD COLUMN     "legacyCode" TEXT,
ADD COLUMN     "points" INTEGER NOT NULL DEFAULT 0,
ADD COLUMN     "pricePolicyName" TEXT;

-- AlterTable
ALTER TABLE "CustomerAddress" ADD COLUMN     "email" TEXT,
ADD COLUMN     "ghnDistrictId" INTEGER,
ADD COLUMN     "ghnWardCode" TEXT,
ADD COLUMN     "ghnWardIdV2" TEXT;

-- AlterTable
ALTER TABLE "InventoryItem" ADD COLUMN     "branchId" TEXT NOT NULL;

-- AlterTable
ALTER TABLE "InventoryMovement" ADD COLUMN     "afterQty" INTEGER,
ADD COLUMN     "beforeQty" INTEGER,
ADD COLUMN     "branchId" TEXT NOT NULL;

-- AlterTable
ALTER TABLE "Order" DROP COLUMN "discountTotal",
DROP COLUMN "grandTotal",
DROP COLUMN "orderStatus",
DROP COLUMN "subtotal",
ADD COLUMN     "assignedStaffId" TEXT,
ADD COLUMN     "assignedStaffName" TEXT,
ADD COLUMN     "billingAddressLabel" TEXT,
ADD COLUMN     "billingAddressLine1" TEXT,
ADD COLUMN     "billingAddressLine2" TEXT,
ADD COLUMN     "billingCity" TEXT,
ADD COLUMN     "billingCountry" TEXT,
ADD COLUMN     "billingDistrict" TEXT,
ADD COLUMN     "billingEmail" TEXT,
ADD COLUMN     "billingPhone" TEXT,
ADD COLUMN     "billingPostalCode" TEXT,
ADD COLUMN     "billingProvince" TEXT,
ADD COLUMN     "billingRecipientName" TEXT,
ADD COLUMN     "billingWard" TEXT,
ADD COLUMN     "branchId" TEXT,
ADD COLUMN     "createdByStaffId" TEXT,
ADD COLUMN     "createdByStaffName" TEXT,
ADD COLUMN     "customerAddressId" TEXT,
ADD COLUMN     "customerName" TEXT,
ADD COLUMN     "customerPhone" TEXT,
ADD COLUMN     "discountAmount" DECIMAL(14,2) NOT NULL DEFAULT 0,
ADD COLUMN     "finalAmount" DECIMAL(14,2) NOT NULL DEFAULT 0,
ADD COLUMN     "isPartialDelivery" BOOLEAN NOT NULL DEFAULT false,
ADD COLUMN     "omniConversationId" TEXT,
ADD COLUMN     "partialReason" TEXT,
ADD COLUMN     "quickOrderRequestId" TEXT,
ADD COLUMN     "shippingAddressLabel" TEXT,
ADD COLUMN     "shippingAddressLine1" TEXT,
ADD COLUMN     "shippingAddressLine2" TEXT,
ADD COLUMN     "shippingCity" TEXT,
ADD COLUMN     "shippingCountry" TEXT,
ADD COLUMN     "shippingDistrict" TEXT,
ADD COLUMN     "shippingEmail" TEXT,
ADD COLUMN     "shippingGhnDistrictId" INTEGER,
ADD COLUMN     "shippingGhnWardCode" TEXT,
ADD COLUMN     "shippingGhnWardIdV2" TEXT,
ADD COLUMN     "shippingPhone" TEXT,
ADD COLUMN     "shippingPostalCode" TEXT,
ADD COLUMN     "shippingProvince" TEXT,
ADD COLUMN     "shippingRecipientName" TEXT,
ADD COLUMN     "shippingWard" TEXT,
ADD COLUMN     "soldAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN     "source" TEXT,
ADD COLUMN     "status" "OrderStatus" NOT NULL DEFAULT 'NEW',
ADD COLUMN     "totalAmount" DECIMAL(14,2) NOT NULL DEFAULT 0,
ALTER COLUMN "paymentStatus" SET DEFAULT 'UNPAID';

-- AlterTable
ALTER TABLE "Payment" ADD COLUMN     "paymentSourceId" TEXT,
ALTER COLUMN "status" SET DEFAULT 'UNPAID';

-- AlterTable
ALTER TABLE "Product" ADD COLUMN     "categoryId" TEXT,
ADD COLUMN     "imageUrl" TEXT,
ADD COLUMN     "productType" TEXT,
ADD COLUMN     "weight" DOUBLE PRECISION,
ALTER COLUMN "brand" DROP DEFAULT;

-- AlterTable
ALTER TABLE "ProductVariant" DROP COLUMN "cost",
DROP COLUMN "priceUsd",
DROP COLUMN "priceVnd",
DROP COLUMN "weightGram",
ADD COLUMN     "compareAtPrice" DECIMAL(14,2),
ADD COLUMN     "costPrice" DECIMAL(14,2),
ADD COLUMN     "imageUrl" TEXT,
ADD COLUMN     "price" DECIMAL(14,2) NOT NULL,
ADD COLUMN     "variantName" TEXT;

-- AlterTable
ALTER TABLE "Shipment" ADD COLUMN     "ahamoveOrderId" TEXT,
ADD COLUMN     "ahamoveRaw" JSONB,
ADD COLUMN     "ahamoveStatus" TEXT,
ADD COLUMN     "ahamoveSubStatus" TEXT,
ADD COLUMN     "ahamoveTrackingUrl" TEXT,
ADD COLUMN     "codReconciledAt" TIMESTAMP(3),
ADD COLUMN     "codReconciliationAmount" INTEGER,
ADD COLUMN     "codReconciliationBatchId" TEXT,
ADD COLUMN     "codReconciliationIssue" TEXT,
ADD COLUMN     "codReconciliationRowId" TEXT,
ADD COLUMN     "codReconciliationStatus" TEXT DEFAULT 'NOT_RECONCILED',
ADD COLUMN     "fromAddress" TEXT,
ADD COLUMN     "fromName" TEXT,
ADD COLUMN     "fromPhone" TEXT,
ADD COLUMN     "lastSyncedAt" TIMESTAMP(3),
ADD COLUMN     "metadata" JSONB,
ADD COLUMN     "partnerStatus" TEXT,
ADD COLUMN     "toAddress" TEXT,
ADD COLUMN     "toName" TEXT,
ADD COLUMN     "toPhone" TEXT,
ADD COLUMN     "weight" INTEGER;

-- CreateTable
CREATE TABLE "ShipmentTimelineEvent" (
    "id" TEXT NOT NULL,
    "shipmentId" TEXT NOT NULL,
    "orderId" TEXT,
    "carrier" TEXT,
    "trackingCode" TEXT,
    "status" TEXT NOT NULL,
    "partnerStatus" TEXT,
    "title" TEXT NOT NULL,
    "description" TEXT,
    "driverName" TEXT,
    "driverPhone" TEXT,
    "driverPlate" TEXT,
    "eta" TEXT,
    "locationText" TEXT,
    "raw" JSONB,
    "source" TEXT NOT NULL DEFAULT 'system',
    "eventTime" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "ShipmentTimelineEvent_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "AhamoveShipment" (
    "id" TEXT NOT NULL,
    "shipmentId" TEXT NOT NULL,
    "orderId" TEXT,
    "ahamoveOrderId" TEXT NOT NULL,
    "serviceId" TEXT,
    "status" TEXT,
    "subStatus" TEXT,
    "trackingUrl" TEXT,
    "sharedLink" TEXT,
    "codAmount" DECIMAL(14,2),
    "shippingFee" DECIMAL(14,2),
    "fromName" TEXT,
    "fromPhone" TEXT,
    "fromAddress" TEXT,
    "toName" TEXT,
    "toPhone" TEXT,
    "toAddress" TEXT,
    "path" JSONB,
    "raw" JSONB,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,
    "lastSyncedAt" TIMESTAMP(3),

    CONSTRAINT "AhamoveShipment_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "StaffUser" (
    "id" TEXT NOT NULL,
    "code" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "username" TEXT,
    "email" TEXT,
    "phone" TEXT,
    "address" TEXT,
    "note" TEXT,
    "role" TEXT,
    "branchId" TEXT,
    "branchName" TEXT,
    "passwordHash" TEXT NOT NULL DEFAULT '',
    "secondPasswordHash" TEXT,
    "secondPasswordEnabled" BOOLEAN NOT NULL DEFAULT false,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "sessionVersion" INTEGER NOT NULL DEFAULT 1,
    "lastLoginAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "StaffUser_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "StaffSession" (
    "id" TEXT NOT NULL,
    "staffId" TEXT NOT NULL,
    "refreshTokenHash" TEXT NOT NULL,
    "deviceInfo" TEXT,
    "ipAddress" TEXT,
    "sessionVersion" INTEGER NOT NULL,
    "expiresAt" TIMESTAMP(3) NOT NULL,
    "revokedAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "StaffSession_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "StaffUserRole" (
    "id" TEXT NOT NULL,
    "staffId" TEXT NOT NULL,
    "roleCode" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "StaffUserRole_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "StaffRoleTemplate" (
    "id" TEXT NOT NULL,
    "roleCode" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "scope" TEXT NOT NULL DEFAULT 'ONE_BRANCH',
    "description" TEXT NOT NULL DEFAULT '',
    "note" TEXT NOT NULL DEFAULT '',
    "permissions" JSONB NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "StaffRoleTemplate_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "StaffBranchPermission" (
    "id" TEXT NOT NULL,
    "staffId" TEXT NOT NULL,
    "branchId" TEXT NOT NULL,
    "canView" BOOLEAN NOT NULL DEFAULT true,
    "canSell" BOOLEAN NOT NULL DEFAULT false,
    "canViewOwnOrders" BOOLEAN NOT NULL DEFAULT false,
    "canViewBranchOrders" BOOLEAN NOT NULL DEFAULT false,
    "canCreateOrder" BOOLEAN NOT NULL DEFAULT false,
    "canApproveOrder" BOOLEAN NOT NULL DEFAULT false,
    "canCancelOrder" BOOLEAN NOT NULL DEFAULT false,
    "canHandleReturn" BOOLEAN NOT NULL DEFAULT false,
    "canViewStock" BOOLEAN NOT NULL DEFAULT false,
    "canManageStock" BOOLEAN NOT NULL DEFAULT false,
    "canStocktake" BOOLEAN NOT NULL DEFAULT false,
    "canTransferStock" BOOLEAN NOT NULL DEFAULT false,
    "canReceiveStock" BOOLEAN NOT NULL DEFAULT false,
    "canViewCustomer" BOOLEAN NOT NULL DEFAULT false,
    "canEditCustomer" BOOLEAN NOT NULL DEFAULT false,
    "canExportProductExcel" BOOLEAN NOT NULL DEFAULT false,
    "canImportProductExcel" BOOLEAN NOT NULL DEFAULT false,
    "canExportOrderExcel" BOOLEAN NOT NULL DEFAULT false,
    "canExportInventoryExcel" BOOLEAN NOT NULL DEFAULT false,
    "canExportCustomerExcel" BOOLEAN NOT NULL DEFAULT false,
    "permissionKeys" TEXT[] DEFAULT ARRAY[]::TEXT[],
    "extraPermissionKeys" TEXT[] DEFAULT ARRAY[]::TEXT[],
    "deniedPermissionKeys" TEXT[] DEFAULT ARRAY[]::TEXT[],
    "canViewReport" BOOLEAN NOT NULL DEFAULT false,
    "canViewMoney" BOOLEAN NOT NULL DEFAULT false,
    "note" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "StaffBranchPermission_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Branch" (
    "id" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "address" TEXT,
    "phone" TEXT,
    "email" TEXT,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Branch_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ImportJob" (
    "id" TEXT NOT NULL,
    "type" TEXT NOT NULL,
    "fileName" TEXT NOT NULL,
    "status" TEXT NOT NULL,
    "totalRows" INTEGER NOT NULL DEFAULT 0,
    "successRows" INTEGER NOT NULL DEFAULT 0,
    "failedRows" INTEGER NOT NULL DEFAULT 0,
    "errorFileUrl" TEXT,
    "defaultBranchId" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "ImportJob_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ImportErrorLog" (
    "id" TEXT NOT NULL,
    "jobId" TEXT NOT NULL,
    "rowNumber" INTEGER,
    "message" TEXT NOT NULL,
    "rawData" JSONB,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "ImportErrorLog_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Category" (
    "id" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "code" TEXT NOT NULL,
    "slug" TEXT NOT NULL,
    "description" TEXT,
    "sortOrder" INTEGER NOT NULL DEFAULT 0,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Category_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "AdministrativeProvince" (
    "code" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "normalized" TEXT,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "sortOrder" INTEGER NOT NULL DEFAULT 0,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "AdministrativeProvince_pkey" PRIMARY KEY ("code")
);

-- CreateTable
CREATE TABLE "AdministrativeWard" (
    "code" TEXT NOT NULL,
    "provinceCode" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "normalized" TEXT,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "sortOrder" INTEGER NOT NULL DEFAULT 0,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "AdministrativeWard_pkey" PRIMARY KEY ("code")
);

-- CreateTable
CREATE TABLE "Supplier" (
    "id" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "code" TEXT NOT NULL,
    "phone" TEXT,
    "email" TEXT,
    "address" TEXT,
    "note" TEXT,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Supplier_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "FabricSupplier" (
    "id" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "code" TEXT NOT NULL,
    "phone" TEXT,
    "email" TEXT,
    "address" TEXT,
    "note" TEXT,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "FabricSupplier_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "PurchaseReceipt" (
    "id" TEXT NOT NULL,
    "receiptCode" TEXT NOT NULL,
    "supplierId" TEXT,
    "branchId" TEXT NOT NULL,
    "status" "PurchaseReceiptStatus" NOT NULL DEFAULT 'DRAFT',
    "note" TEXT,
    "createdById" TEXT,
    "confirmedAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,
    "adminUserId" TEXT,

    CONSTRAINT "PurchaseReceipt_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "PurchaseReceiptItem" (
    "id" TEXT NOT NULL,
    "receiptId" TEXT NOT NULL,
    "productId" TEXT,
    "variantId" TEXT NOT NULL,
    "sku" TEXT NOT NULL,
    "productName" TEXT NOT NULL,
    "color" TEXT,
    "size" TEXT,
    "qty" INTEGER NOT NULL,
    "unitCost" DECIMAL(14,2) NOT NULL,
    "lineTotal" DECIMAL(14,2) NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "PurchaseReceiptItem_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "StockTransfer" (
    "id" TEXT NOT NULL,
    "transferCode" TEXT NOT NULL,
    "fromBranchId" TEXT NOT NULL,
    "toBranchId" TEXT NOT NULL,
    "note" TEXT,
    "status" "TransferStatus" NOT NULL DEFAULT 'DRAFT',
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,
    "completedAt" TIMESTAMP(3),
    "confirmedAt" TIMESTAMP(3),
    "confirmedById" TEXT,
    "confirmedByName" TEXT,
    "createdById" TEXT,
    "createdByName" TEXT,
    "direction" "StockTransferDirection" NOT NULL DEFAULT 'OUTBOUND_TO_BRANCH',
    "sourceRefId" TEXT,
    "sourceType" "StockTransferSourceType" NOT NULL DEFAULT 'MANUAL',

    CONSTRAINT "StockTransfer_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "StockTransferItem" (
    "id" TEXT NOT NULL,
    "transferId" TEXT NOT NULL,
    "variantId" TEXT NOT NULL,
    "qty" INTEGER NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "color" TEXT,
    "productName" TEXT,
    "size" TEXT,
    "sku" TEXT,

    CONSTRAINT "StockTransferItem_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "BranchNotification" (
    "id" TEXT NOT NULL,
    "branchId" TEXT NOT NULL,
    "branchName" TEXT,
    "title" TEXT NOT NULL,
    "message" TEXT NOT NULL,
    "type" "BranchNotificationType" NOT NULL,
    "transferId" TEXT,
    "transferCode" TEXT,
    "isRead" BOOLEAN NOT NULL DEFAULT false,
    "readAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "BranchNotification_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ShipmentTrackingCache" (
    "id" TEXT NOT NULL,
    "shipmentId" TEXT NOT NULL,
    "carrier" TEXT NOT NULL,
    "trackingCode" TEXT NOT NULL,
    "payloadJson" JSONB,
    "normalizedJson" JSONB NOT NULL,
    "fetchedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "expiresAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "ShipmentTrackingCache_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ShipmentCodAuthCode" (
    "id" TEXT NOT NULL,
    "orderId" TEXT NOT NULL,
    "code" TEXT NOT NULL,
    "codAmount" INTEGER NOT NULL,
    "phone" TEXT,
    "expiresAt" TIMESTAMP(3) NOT NULL,
    "usedAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "ShipmentCodAuthCode_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "PartialDeliveryRecord" (
    "id" TEXT NOT NULL,
    "code" TEXT,
    "orderId" TEXT NOT NULL,
    "orderCode" TEXT NOT NULL,
    "ghnTrackingCode" TEXT,
    "originalCod" DECIMAL(14,2) NOT NULL,
    "adjustedCod" DECIMAL(14,2) NOT NULL,
    "shippingFee" DECIMAL(14,2) NOT NULL DEFAULT 0,
    "reason" TEXT,
    "approvedBy" TEXT,
    "approvedById" TEXT,
    "note" TEXT,
    "handledAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,
    "returnOrderId" TEXT,
    "returnOrderCode" TEXT,
    "returnTrackingCode" TEXT,
    "returnStatus" TEXT DEFAULT 'PENDING_RETURN',
    "returnReceivedAt" TIMESTAMP(3),
    "returnReceivedBy" TEXT,
    "returnStockReceiptId" TEXT,
    "returnStockNote" TEXT,

    CONSTRAINT "PartialDeliveryRecord_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "PartialDeliveryItem" (
    "id" TEXT NOT NULL,
    "recordId" TEXT NOT NULL,
    "orderItemId" TEXT,
    "variantId" TEXT,
    "productName" TEXT NOT NULL,
    "sku" TEXT NOT NULL,
    "color" TEXT,
    "size" TEXT,
    "orderedQty" INTEGER NOT NULL,
    "deliveredQty" INTEGER NOT NULL,
    "returnedQty" INTEGER NOT NULL DEFAULT 0,
    "actionType" TEXT NOT NULL DEFAULT 'KEPT',
    "unitPrice" DECIMAL(14,2) NOT NULL,
    "lineTotal" DECIMAL(14,2) NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "PartialDeliveryItem_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "PaymentSource" (
    "id" TEXT NOT NULL,
    "code" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "type" TEXT NOT NULL,
    "branchId" TEXT,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "sortOrder" INTEGER NOT NULL DEFAULT 0,
    "note" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "PaymentSource_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "StockTransferAutoConfig" (
    "id" TEXT NOT NULL,
    "isEnabled" BOOLEAN NOT NULL DEFAULT false,
    "runHour" INTEGER NOT NULL DEFAULT 9,
    "runMinute" INTEGER NOT NULL DEFAULT 0,
    "toBranchIds" TEXT[] DEFAULT ARRAY[]::TEXT[],
    "categoryNames" TEXT[] DEFAULT ARRAY[]::TEXT[],
    "branchMinTargets" JSONB,
    "maxPerVariant" INTEGER NOT NULL DEFAULT 5,
    "salesVelocityDays" INTEGER NOT NULL DEFAULT 14,
    "minSoldQty" INTEGER NOT NULL DEFAULT 0,
    "lastRunAt" TIMESTAMP(3),
    "lastRunDateKey" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "StockTransferAutoConfig_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "GhnCodReconciliationBatch" (
    "id" TEXT NOT NULL,
    "fileName" TEXT NOT NULL,
    "transferCode" TEXT,
    "transferDate" TEXT,
    "totalRows" INTEGER NOT NULL DEFAULT 0,
    "matchedRows" INTEGER NOT NULL DEFAULT 0,
    "mismatchRows" INTEGER NOT NULL DEFAULT 0,
    "totalAmount" INTEGER NOT NULL DEFAULT 0,
    "transferFee" INTEGER NOT NULL DEFAULT 0,
    "netAmount" INTEGER NOT NULL DEFAULT 0,
    "parserMode" TEXT,
    "sourceType" TEXT NOT NULL DEFAULT 'EXCEL',
    "note" TEXT,
    "status" TEXT NOT NULL DEFAULT 'DRAFT',
    "savedAt" TIMESTAMP(3),
    "savedById" TEXT,
    "confirmedAt" TIMESTAMP(3),
    "confirmedById" TEXT,
    "paidAt" TIMESTAMP(3),
    "paidById" TEXT,
    "paymentSourceId" TEXT,
    "paymentAmount" INTEGER NOT NULL DEFAULT 0,
    "paymentNote" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "GhnCodReconciliationBatch_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "GhnCodReconciliationRow" (
    "id" TEXT NOT NULL,
    "batchId" TEXT NOT NULL,
    "orderId" TEXT,
    "orderCode" TEXT,
    "shipmentId" TEXT,
    "ghnCode" TEXT NOT NULL,
    "customerOrderCode" TEXT,
    "ghnStatus" TEXT,
    "codAmount" INTEGER NOT NULL DEFAULT 0,
    "serviceFee" INTEGER NOT NULL DEFAULT 0,
    "totalReconcileAmount" INTEGER NOT NULL DEFAULT 0,
    "reconciliationStatus" TEXT NOT NULL,
    "issues" TEXT[] DEFAULT ARRAY[]::TEXT[],
    "partialDeliveryRecordId" TEXT,
    "partialDeliveryAdjustedCod" INTEGER,
    "partialReturnReceived" BOOLEAN,
    "inputCode" TEXT,
    "sourceType" TEXT NOT NULL DEFAULT 'EXCEL',
    "actionStatus" TEXT NOT NULL DEFAULT 'DRAFT',
    "actionNote" TEXT,
    "savedAt" TIMESTAMP(3),
    "savedById" TEXT,
    "confirmedAt" TIMESTAMP(3),
    "confirmedById" TEXT,
    "paidAt" TIMESTAMP(3),
    "paidById" TEXT,
    "paymentSourceId" TEXT,
    "paymentAmount" INTEGER NOT NULL DEFAULT 0,
    "paymentNote" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "GhnCodReconciliationRow_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "StocktakeSession" (
    "id" TEXT NOT NULL,
    "branchId" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "note" TEXT,
    "status" TEXT NOT NULL DEFAULT 'DRAFT',
    "createdById" TEXT,
    "startedAt" TIMESTAMP(3),
    "finishedAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,
    "appliedAt" TIMESTAMP(3),
    "snapshotPurgedAt" TIMESTAMP(3),

    CONSTRAINT "StocktakeSession_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "StocktakeWorker" (
    "id" TEXT NOT NULL,
    "sessionId" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "userId" TEXT,
    "zone" TEXT,
    "deviceName" TEXT,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "status" TEXT NOT NULL DEFAULT 'IN_PROGRESS',
    "startedAt" TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP,
    "finishedAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "StocktakeWorker_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "StocktakeScanEvent" (
    "id" TEXT NOT NULL,
    "sessionId" TEXT NOT NULL,
    "workerId" TEXT,
    "branchId" TEXT NOT NULL,
    "variantId" TEXT,
    "sku" TEXT NOT NULL,
    "barcode" TEXT,
    "qtyDelta" INTEGER NOT NULL DEFAULT 1,
    "zone" TEXT,
    "locationCode" TEXT,
    "status" TEXT NOT NULL DEFAULT 'OK',
    "note" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "StocktakeScanEvent_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "WarehouseMap" (
    "id" TEXT NOT NULL,
    "branchId" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "code" TEXT NOT NULL,
    "note" TEXT,
    "width" INTEGER NOT NULL DEFAULT 1200,
    "height" INTEGER NOT NULL DEFAULT 800,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "WarehouseMap_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "WarehouseRack" (
    "id" TEXT NOT NULL,
    "mapId" TEXT NOT NULL,
    "branchId" TEXT NOT NULL,
    "code" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "zone" TEXT NOT NULL,
    "aisle" TEXT NOT NULL,
    "rackNo" TEXT NOT NULL,
    "floors" INTEGER NOT NULL DEFAULT 5,
    "x" INTEGER NOT NULL DEFAULT 0,
    "y" INTEGER NOT NULL DEFAULT 0,
    "w" INTEGER NOT NULL DEFAULT 120,
    "h" INTEGER NOT NULL DEFAULT 260,
    "rotation" INTEGER NOT NULL DEFAULT 0,
    "status" TEXT NOT NULL DEFAULT 'PENDING',
    "note" TEXT,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "WarehouseRack_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "WarehouseShelf" (
    "id" TEXT NOT NULL,
    "rackId" TEXT NOT NULL,
    "code" TEXT NOT NULL,
    "floorNo" INTEGER NOT NULL,
    "label" TEXT,
    "capacity" INTEGER NOT NULL DEFAULT 0,
    "note" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "WarehouseShelf_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ProductVariantLocation" (
    "id" TEXT NOT NULL,
    "variantId" TEXT NOT NULL,
    "branchId" TEXT,
    "areaId" TEXT,
    "rackId" TEXT,
    "shelfId" TEXT,
    "minQty" INTEGER,
    "maxQty" INTEGER,
    "note" TEXT,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,
    "isPrimary" BOOLEAN NOT NULL DEFAULT false,

    CONSTRAINT "ProductVariantLocation_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "StocktakeArea" (
    "id" TEXT NOT NULL,
    "sessionId" TEXT NOT NULL,
    "branchId" TEXT NOT NULL,
    "mapId" TEXT,
    "scopeType" TEXT NOT NULL,
    "aisle" TEXT,
    "rackId" TEXT,
    "rackCode" TEXT,
    "label" TEXT NOT NULL,
    "status" TEXT NOT NULL DEFAULT 'PENDING',
    "startedAt" TIMESTAMP(3),
    "finishedAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "StocktakeArea_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "WarehouseFloor" (
    "id" TEXT NOT NULL,
    "mapId" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "level" INTEGER NOT NULL DEFAULT 1,
    "note" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "WarehouseFloor_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "WarehouseZone" (
    "id" TEXT NOT NULL,
    "mapId" TEXT NOT NULL,
    "floorId" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "type" TEXT NOT NULL DEFAULT 'STORAGE',
    "x" DOUBLE PRECISION NOT NULL DEFAULT 0,
    "y" DOUBLE PRECISION NOT NULL DEFAULT 0,
    "width" DOUBLE PRECISION NOT NULL DEFAULT 500,
    "height" DOUBLE PRECISION NOT NULL DEFAULT 300,
    "color" TEXT,
    "note" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "WarehouseZone_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "WarehouseDoor" (
    "id" TEXT NOT NULL,
    "mapId" TEXT NOT NULL,
    "floorId" TEXT NOT NULL,
    "name" TEXT NOT NULL DEFAULT 'Cửa kho',
    "side" TEXT NOT NULL DEFAULT 'BOTTOM',
    "x" DOUBLE PRECISION NOT NULL DEFAULT 0,
    "y" DOUBLE PRECISION NOT NULL DEFAULT 0,
    "width" DOUBLE PRECISION NOT NULL DEFAULT 180,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "WarehouseDoor_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "StocktakeSnapshot" (
    "id" TEXT NOT NULL,
    "sessionId" TEXT NOT NULL,
    "branchId" TEXT NOT NULL,
    "variantId" TEXT NOT NULL,
    "snapshotQty" INTEGER NOT NULL DEFAULT 0,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "StocktakeSnapshot_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "StocktakeResult" (
    "id" TEXT NOT NULL,
    "sessionId" TEXT NOT NULL,
    "branchId" TEXT NOT NULL,
    "totalRows" INTEGER NOT NULL DEFAULT 0,
    "countedSku" INTEGER NOT NULL DEFAULT 0,
    "matchedSku" INTEGER NOT NULL DEFAULT 0,
    "mismatchSku" INTEGER NOT NULL DEFAULT 0,
    "notFoundSku" INTEGER NOT NULL DEFAULT 0,
    "totalSnapshotQty" INTEGER NOT NULL DEFAULT 0,
    "totalCountedQty" INTEGER NOT NULL DEFAULT 0,
    "totalDiffQty" INTEGER NOT NULL DEFAULT 0,
    "totalDiffValue" DECIMAL(14,2) NOT NULL DEFAULT 0,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "StocktakeResult_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "StocktakeResultItem" (
    "id" TEXT NOT NULL,
    "resultId" TEXT NOT NULL,
    "sessionId" TEXT NOT NULL,
    "branchId" TEXT NOT NULL,
    "variantId" TEXT,
    "sku" TEXT NOT NULL,
    "barcode" TEXT,
    "productName" TEXT,
    "color" TEXT,
    "size" TEXT,
    "snapshotQty" INTEGER NOT NULL DEFAULT 0,
    "countedQty" INTEGER NOT NULL DEFAULT 0,
    "diffQty" INTEGER NOT NULL DEFAULT 0,
    "beforeApplyQty" INTEGER,
    "afterApplyQty" INTEGER,
    "unitCost" DECIMAL(14,2) NOT NULL DEFAULT 0,
    "diffValue" DECIMAL(14,2) NOT NULL DEFAULT 0,
    "status" TEXT NOT NULL DEFAULT 'MATCH',
    "statusLabel" TEXT,
    "diffType" TEXT,
    "eventCount" INTEGER NOT NULL DEFAULT 0,
    "workerId" TEXT,
    "workerName" TEXT,
    "zone" TEXT,
    "areaId" TEXT,
    "rackId" TEXT,
    "rackCode" TEXT,
    "locationCode" TEXT,
    "lastScannedAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "StocktakeResultItem_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "StocktakeCount" (
    "id" TEXT NOT NULL,
    "sessionId" TEXT NOT NULL,
    "workerId" TEXT,
    "branchId" TEXT NOT NULL,
    "variantId" TEXT,
    "sku" TEXT NOT NULL,
    "countedQty" INTEGER NOT NULL DEFAULT 0,
    "eventCount" INTEGER NOT NULL DEFAULT 0,
    "zone" TEXT,
    "areaId" TEXT,
    "rackId" TEXT,
    "rackCode" TEXT,
    "locationCode" TEXT,
    "status" TEXT NOT NULL DEFAULT 'OK',
    "lastScannedAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "StocktakeCount_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ReturnExchange" (
    "id" TEXT NOT NULL,
    "code" TEXT NOT NULL,
    "originalOrderId" TEXT NOT NULL,
    "originalBranchId" TEXT,
    "originalStaffId" TEXT,
    "originalStaffName" TEXT,
    "handledByStaffId" TEXT,
    "handledByStaffName" TEXT,
    "handledAtBranchId" TEXT,
    "returnReceiveBranchId" TEXT,
    "exchangeIssueBranchId" TEXT,
    "type" TEXT NOT NULL,
    "status" TEXT NOT NULL DEFAULT 'DRAFT',
    "returnAmount" DECIMAL(14,2) NOT NULL DEFAULT 0,
    "exchangeAmount" DECIMAL(14,2) NOT NULL DEFAULT 0,
    "differenceAmount" DECIMAL(14,2) NOT NULL DEFAULT 0,
    "refundAmount" DECIMAL(14,2) NOT NULL DEFAULT 0,
    "extraChargeAmount" DECIMAL(14,2) NOT NULL DEFAULT 0,
    "refundPaymentSourceId" TEXT,
    "extraChargePaymentSourceId" TEXT,
    "note" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,
    "shippingFee" DECIMAL(14,2) NOT NULL DEFAULT 0,
    "customerPayableAmount" DECIMAL(14,2) NOT NULL DEFAULT 0,
    "exchangeOrderId" TEXT,
    "exchangeOrderCode" TEXT,
    "exchangeShipmentId" TEXT,
    "exchangeTrackingCode" TEXT,
    "exchangeCarrier" TEXT,

    CONSTRAINT "ReturnExchange_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ReturnExchangeItem" (
    "id" TEXT NOT NULL,
    "returnExchangeId" TEXT NOT NULL,
    "itemType" TEXT NOT NULL DEFAULT 'RETURN',
    "orderItemId" TEXT,
    "variantId" TEXT,
    "sku" TEXT,
    "productName" TEXT,
    "qty" INTEGER NOT NULL,
    "unitPrice" DECIMAL(14,2) NOT NULL DEFAULT 0,
    "refundPrice" DECIMAL(14,2) NOT NULL DEFAULT 0,
    "lineTotal" DECIMAL(14,2) NOT NULL DEFAULT 0,
    "reason" TEXT,

    CONSTRAINT "ReturnExchangeItem_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "CashVoucher" (
    "id" TEXT NOT NULL,
    "code" TEXT NOT NULL,
    "direction" TEXT NOT NULL,
    "voucherType" TEXT NOT NULL,
    "voucherCode" TEXT,
    "type" TEXT,
    "category" TEXT,
    "title" TEXT,
    "status" TEXT,
    "partnerName" TEXT,
    "partnerPhone" TEXT,
    "createdById" TEXT,
    "createdByName" TEXT,
    "confirmedAt" TIMESTAMP(3),
    "confirmedById" TEXT,
    "confirmedByName" TEXT,
    "cancelledAt" TIMESTAMP(3),
    "cancelledById" TEXT,
    "cancelledByName" TEXT,
    "amount" DECIMAL(14,2) NOT NULL DEFAULT 0,
    "paymentSourceId" TEXT,
    "branchId" TEXT,
    "staffId" TEXT,
    "staffName" TEXT,
    "customerName" TEXT,
    "customerPhone" TEXT,
    "refType" TEXT,
    "refId" TEXT,
    "note" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "CashVoucher_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "DailyCashBalance" (
    "id" TEXT NOT NULL,
    "date" DATE NOT NULL,
    "branchId" TEXT NOT NULL,
    "paymentSourceId" TEXT NOT NULL,
    "openingBalance" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "totalReceipt" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "totalPayment" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "netAmount" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "closingBalance" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "countedAmount" DECIMAL(18,2),
    "differenceAmount" DECIMAL(18,2),
    "status" TEXT NOT NULL DEFAULT 'OPEN',
    "note" TEXT,
    "lockedAt" TIMESTAMP(3),
    "lockedById" TEXT,
    "lockedByName" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "DailyCashBalance_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Promotion" (
    "id" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "type" "PromotionType" NOT NULL,
    "status" "PromotionStatus" NOT NULL DEFAULT 'ACTIVE',
    "discountType" "PromotionDiscountType" NOT NULL,
    "discountValue" DECIMAL(18,2) NOT NULL,
    "minOrderAmount" DECIMAL(18,2),
    "branchId" TEXT,
    "salesChannel" "SalesChannel",
    "startAt" TIMESTAMP(3),
    "endAt" TIMESTAMP(3),
    "priority" INTEGER NOT NULL DEFAULT 0,
    "note" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Promotion_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "PromotionProduct" (
    "id" TEXT NOT NULL,
    "promotionId" TEXT NOT NULL,
    "productId" TEXT,
    "variantId" TEXT,

    CONSTRAINT "PromotionProduct_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "PurchaseReceiptPayment" (
    "id" TEXT NOT NULL,
    "receiptId" TEXT NOT NULL,
    "paymentSourceId" TEXT,
    "amount" DECIMAL(14,2) NOT NULL DEFAULT 0,
    "note" TEXT,
    "paidById" TEXT,
    "paidByName" TEXT,
    "paidAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "PurchaseReceiptPayment_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "StaffBranchRole" (
    "id" TEXT NOT NULL,
    "staffId" TEXT NOT NULL,
    "branchId" TEXT NOT NULL,
    "roleCode" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "StaffBranchRole_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Department" (
    "id" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "code" TEXT NOT NULL,
    "description" TEXT NOT NULL DEFAULT '',
    "color" TEXT NOT NULL DEFAULT '#6366f1',
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Department_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "StaffDepartment" (
    "id" TEXT NOT NULL,
    "staffId" TEXT NOT NULL,
    "departmentId" TEXT NOT NULL,
    "isHead" BOOLEAN NOT NULL DEFAULT false,
    "joinedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "StaffDepartment_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "RbacSnapshot" (
    "id" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "description" TEXT,
    "payload" JSONB NOT NULL,
    "createdById" TEXT,
    "createdByName" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "restoredAt" TIMESTAMP(3),
    "restoredById" TEXT,
    "restoredByName" TEXT,

    CONSTRAINT "RbacSnapshot_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "PayrollPeriod" (
    "id" TEXT NOT NULL,
    "code" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "fromDate" TIMESTAMP(3) NOT NULL,
    "toDate" TIMESTAMP(3) NOT NULL,
    "branchId" TEXT,
    "branchName" TEXT,
    "status" TEXT NOT NULL DEFAULT 'DRAFT',
    "totalStaff" INTEGER NOT NULL DEFAULT 0,
    "totalOrders" INTEGER NOT NULL DEFAULT 0,
    "totalItems" INTEGER NOT NULL DEFAULT 0,
    "totalRevenue" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "totalHourlyAmount" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "totalTaggedProductAmount" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "totalMealAllowance" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "totalInsuranceDeduction" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "totalGhnCodBonus" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "totalAttendanceWarnings" INTEGER NOT NULL DEFAULT 0,
    "totalLateMinutes" INTEGER NOT NULL DEFAULT 0,
    "totalEarlyMinutes" INTEGER NOT NULL DEFAULT 0,
    "attendanceImportedAt" TIMESTAMP(3),
    "attendanceImportFileName" TEXT,
    "totalGross" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "totalNet" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "totalPaid" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "lockedAt" TIMESTAMP(3),
    "lockedById" TEXT,
    "lockedByName" TEXT,
    "paidAt" TIMESTAMP(3),
    "paidById" TEXT,
    "paidByName" TEXT,
    "paymentSourceId" TEXT,
    "paymentVoucherId" TEXT,
    "note" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "PayrollPeriod_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "PayrollLine" (
    "id" TEXT NOT NULL,
    "periodId" TEXT NOT NULL,
    "staffId" TEXT NOT NULL,
    "staffCode" TEXT,
    "staffName" TEXT NOT NULL,
    "branchId" TEXT,
    "branchName" TEXT,
    "salaryType" TEXT NOT NULL DEFAULT 'MONTHLY',
    "baseSalary" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "dailyRate" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "workingDays" DECIMAL(8,2) NOT NULL DEFAULT 0,
    "standardDays" DECIMAL(8,2) NOT NULL DEFAULT 26,
    "proratedSalary" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "orderAttributionMode" TEXT NOT NULL DEFAULT 'ASSIGNED_OR_CREATOR',
    "successOrderCount" INTEGER NOT NULL DEFAULT 0,
    "successItemQty" INTEGER NOT NULL DEFAULT 0,
    "revenueAmount" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "attendanceCode" TEXT,
    "attendanceMatchedBy" TEXT,
    "attendanceRawName" TEXT,
    "attendanceSourceFile" TEXT,
    "attendanceImportedAt" TIMESTAMP(3),
    "lateCount" INTEGER NOT NULL DEFAULT 0,
    "lateMinutes" INTEGER NOT NULL DEFAULT 0,
    "earlyCount" INTEGER NOT NULL DEFAULT 0,
    "earlyMinutes" INTEGER NOT NULL DEFAULT 0,
    "attendanceWarningLevel" TEXT,
    "attendanceWarningNote" TEXT,
    "normalHours" DECIMAL(10,2) NOT NULL DEFAULT 0,
    "overtimeHours" DECIMAL(10,2) NOT NULL DEFAULT 0,
    "overtimeRate" DECIMAL(8,2) NOT NULL DEFAULT 1,
    "holidayHours" DECIMAL(10,2) NOT NULL DEFAULT 0,
    "overtime3Hours" DECIMAL(10,2) NOT NULL DEFAULT 0,
    "overtime4Hours" DECIMAL(10,2) NOT NULL DEFAULT 0,
    "holidayRate" DECIMAL(8,2) NOT NULL DEFAULT 2,
    "convertedWorkingHours" DECIMAL(10,2) NOT NULL DEFAULT 0,
    "hourlyRate" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "hourlyAmount" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "overtimeAmount" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "overtimeBreakdown" JSONB,
    "paidLeaveDays" DECIMAL(8,2) NOT NULL DEFAULT 0,
    "paidLeaveHoursPerDay" DECIMAL(8,2) NOT NULL DEFAULT 0,
    "paidLeaveAmount" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "mealAllowanceAmount" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "insuranceDeduction" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "taggedProductQty" INTEGER NOT NULL DEFAULT 0,
    "taggedProductRate" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "taggedProductAmount" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "ghnCodOrderCount" INTEGER NOT NULL DEFAULT 0,
    "ghnCodBonusPerOrder" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "ghnCodBonusAmount" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "commissionByOrder" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "commissionByItem" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "commissionByPercent" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "commissionTotal" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "bonus" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "allowance" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "advance" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "deduction" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "grossPay" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "netPay" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "paidAmount" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "status" TEXT NOT NULL DEFAULT 'DRAFT',
    "note" TEXT,
    "paidAt" TIMESTAMP(3),
    "paidById" TEXT,
    "paidByName" TEXT,
    "paymentSourceId" TEXT,
    "paymentVoucherId" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "PayrollLine_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "PayrollOrderLink" (
    "id" TEXT NOT NULL,
    "payrollLineId" TEXT NOT NULL,
    "orderId" TEXT NOT NULL,
    "orderCode" TEXT NOT NULL,
    "branchId" TEXT,
    "salesChannel" TEXT,
    "orderDate" TIMESTAMP(3),
    "completedAt" TIMESTAMP(3),
    "revenueAmount" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "itemQty" INTEGER NOT NULL DEFAULT 0,
    "commissionByOrder" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "commissionByItem" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "commissionByPercent" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "commissionTotal" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "attributedStaffId" TEXT,
    "attributedStaffName" TEXT,
    "attributionSource" TEXT,
    "reason" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "PayrollOrderLink_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "PayrollConfig" (
    "id" TEXT NOT NULL,
    "staffId" TEXT NOT NULL,
    "staffCode" TEXT,
    "staffName" TEXT,
    "branchId" TEXT,
    "branchName" TEXT,
    "attendanceCode" TEXT,
    "sourceTemplateId" TEXT,
    "salaryType" TEXT NOT NULL DEFAULT 'MONTHLY',
    "baseSalary" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "dailyRate" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "standardWorkingDays" DECIMAL(8,2) NOT NULL DEFAULT 26,
    "orderAttributionMode" TEXT NOT NULL DEFAULT 'ASSIGNED_OR_CREATOR',
    "commissionPerOrderEnabled" BOOLEAN NOT NULL DEFAULT false,
    "commissionPerOrderAmount" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "commissionPerItemEnabled" BOOLEAN NOT NULL DEFAULT false,
    "commissionPerItemAmount" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "commissionPercentEnabled" BOOLEAN NOT NULL DEFAULT false,
    "commissionRate" DECIMAL(8,4) NOT NULL DEFAULT 0,
    "hourlyEnabled" BOOLEAN NOT NULL DEFAULT false,
    "hourlyRate" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "standardHoursPerDay" DECIMAL(8,2) NOT NULL DEFAULT 9.5,
    "overtimeRate" DECIMAL(8,2) NOT NULL DEFAULT 1,
    "holidayRate" DECIMAL(8,2) NOT NULL DEFAULT 2,
    "overtimeConfigs" JSONB,
    "paidLeaveEnabled" BOOLEAN NOT NULL DEFAULT false,
    "paidLeaveHoursPerDay" DECIMAL(8,2) NOT NULL DEFAULT 9.5,
    "mealAllowanceEnabled" BOOLEAN NOT NULL DEFAULT false,
    "mealHoursPerUnit" DECIMAL(8,2) NOT NULL DEFAULT 9.5,
    "mealAmountPerUnit" DECIMAL(18,2) NOT NULL DEFAULT 30000,
    "insuranceDeductionAmount" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "taggedProductEnabled" BOOLEAN NOT NULL DEFAULT false,
    "taggedProductRate" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "ghnCodBonusEnabled" BOOLEAN NOT NULL DEFAULT false,
    "ghnCodBonusPerOrder" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "applyPos" BOOLEAN NOT NULL DEFAULT true,
    "applyOnline" BOOLEAN NOT NULL DEFAULT true,
    "applyFacebook" BOOLEAN NOT NULL DEFAULT true,
    "applyCod" BOOLEAN NOT NULL DEFAULT true,
    "allowanceDefault" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "note" TEXT,
    "effectiveFrom" TIMESTAMP(3) NOT NULL,
    "effectiveTo" TIMESTAMP(3),
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "PayrollConfig_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "PayrollSettings" (
    "id" TEXT NOT NULL DEFAULT 'default',
    "autoCreateEnabled" BOOLEAN NOT NULL DEFAULT false,
    "autoCreateDay" INTEGER NOT NULL DEFAULT 1,
    "cycleMode" TEXT NOT NULL DEFAULT 'MONTHLY',
    "cycleStartDay" INTEGER NOT NULL DEFAULT 1,
    "cycleEndDay" INTEGER,
    "autoCalculateMode" TEXT NOT NULL DEFAULT 'MANUAL',
    "autoLockEnabled" BOOLEAN NOT NULL DEFAULT false,
    "autoLockAfterDays" INTEGER NOT NULL DEFAULT 0,
    "reminderEnabled" BOOLEAN NOT NULL DEFAULT true,
    "lateWarningCount" INTEGER NOT NULL DEFAULT 3,
    "lateWarningMinutes" INTEGER NOT NULL DEFAULT 60,
    "lateCriticalCount" INTEGER NOT NULL DEFAULT 5,
    "lateCriticalMinutes" INTEGER NOT NULL DEFAULT 120,
    "earlyWarningCount" INTEGER NOT NULL DEFAULT 3,
    "earlyWarningMinutes" INTEGER NOT NULL DEFAULT 60,
    "earlyCriticalCount" INTEGER NOT NULL DEFAULT 5,
    "earlyCriticalMinutes" INTEGER NOT NULL DEFAULT 120,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "PayrollSettings_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "PayrollAttendanceImport" (
    "id" TEXT NOT NULL,
    "periodId" TEXT NOT NULL,
    "fileName" TEXT,
    "totalRows" INTEGER NOT NULL DEFAULT 0,
    "matchedRows" INTEGER NOT NULL DEFAULT 0,
    "unmatchedRows" INTEGER NOT NULL DEFAULT 0,
    "totalHours" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "totalLateMinutes" INTEGER NOT NULL DEFAULT 0,
    "totalEarlyMinutes" INTEGER NOT NULL DEFAULT 0,
    "createdById" TEXT,
    "createdByName" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "PayrollAttendanceImport_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "PayrollAdjustment" (
    "id" TEXT NOT NULL,
    "payrollLineId" TEXT NOT NULL,
    "type" TEXT NOT NULL,
    "amount" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "reason" TEXT,
    "createdById" TEXT,
    "createdByName" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "PayrollAdjustment_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "PayrollBranchConfigTemplate" (
    "id" TEXT NOT NULL,
    "branchId" TEXT NOT NULL,
    "branchName" TEXT,
    "name" TEXT NOT NULL DEFAULT 'Cấu hình mặc định',
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "salaryType" TEXT NOT NULL DEFAULT 'MONTHLY',
    "baseSalary" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "dailyRate" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "standardWorkingDays" DECIMAL(8,2) NOT NULL DEFAULT 26,
    "orderAttributionMode" TEXT NOT NULL DEFAULT 'ASSIGNED_OR_CREATOR',
    "commissionPerOrderEnabled" BOOLEAN NOT NULL DEFAULT false,
    "commissionPerOrderAmount" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "commissionPerItemEnabled" BOOLEAN NOT NULL DEFAULT false,
    "commissionPerItemAmount" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "commissionPercentEnabled" BOOLEAN NOT NULL DEFAULT false,
    "commissionRate" DECIMAL(8,4) NOT NULL DEFAULT 0,
    "hourlyEnabled" BOOLEAN NOT NULL DEFAULT false,
    "hourlyRate" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "standardHoursPerDay" DECIMAL(8,2) NOT NULL DEFAULT 9.5,
    "overtimeRate" DECIMAL(8,2) NOT NULL DEFAULT 1,
    "holidayRate" DECIMAL(8,2) NOT NULL DEFAULT 2,
    "overtimeConfigs" JSONB,
    "paidLeaveEnabled" BOOLEAN NOT NULL DEFAULT false,
    "paidLeaveHoursPerDay" DECIMAL(8,2) NOT NULL DEFAULT 9.5,
    "mealAllowanceEnabled" BOOLEAN NOT NULL DEFAULT false,
    "mealHoursPerUnit" DECIMAL(8,2) NOT NULL DEFAULT 9.5,
    "mealAmountPerUnit" DECIMAL(18,2) NOT NULL DEFAULT 30000,
    "insuranceDeductionAmount" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "taggedProductEnabled" BOOLEAN NOT NULL DEFAULT false,
    "taggedProductRate" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "ghnCodBonusEnabled" BOOLEAN NOT NULL DEFAULT false,
    "ghnCodBonusPerOrder" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "applyPos" BOOLEAN NOT NULL DEFAULT true,
    "applyOnline" BOOLEAN NOT NULL DEFAULT true,
    "applyFacebook" BOOLEAN NOT NULL DEFAULT true,
    "applyCod" BOOLEAN NOT NULL DEFAULT true,
    "allowanceDefault" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "note" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "PayrollBranchConfigTemplate_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "MetaAdAccount" (
    "id" TEXT NOT NULL,
    "metaAccountId" TEXT NOT NULL,
    "accountId" TEXT,
    "name" TEXT,
    "currency" TEXT,
    "timezoneName" TEXT,
    "accountStatus" TEXT,
    "businessId" TEXT,
    "businessName" TEXT,
    "rawJson" JSONB,
    "lastSyncedAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "MetaAdAccount_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "MetaCampaign" (
    "id" TEXT NOT NULL,
    "metaAccountId" TEXT NOT NULL,
    "metaCampaignId" TEXT NOT NULL,
    "name" TEXT,
    "status" TEXT,
    "effectiveStatus" TEXT,
    "objective" TEXT,
    "buyingType" TEXT,
    "dailyBudget" DECIMAL(18,2),
    "lifetimeBudget" DECIMAL(18,2),
    "startTime" TIMESTAMP(3),
    "stopTime" TIMESTAMP(3),
    "rawJson" JSONB,
    "lastSyncedAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "MetaCampaign_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "MetaAdSet" (
    "id" TEXT NOT NULL,
    "metaAccountId" TEXT NOT NULL,
    "metaCampaignId" TEXT,
    "metaAdSetId" TEXT NOT NULL,
    "name" TEXT,
    "status" TEXT,
    "effectiveStatus" TEXT,
    "optimizationGoal" TEXT,
    "billingEvent" TEXT,
    "bidStrategy" TEXT,
    "dailyBudget" DECIMAL(18,2),
    "lifetimeBudget" DECIMAL(18,2),
    "startTime" TIMESTAMP(3),
    "endTime" TIMESTAMP(3),
    "targetingJson" JSONB,
    "rawJson" JSONB,
    "lastSyncedAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "MetaAdSet_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "MetaAd" (
    "id" TEXT NOT NULL,
    "metaAccountId" TEXT NOT NULL,
    "metaCampaignId" TEXT,
    "metaAdSetId" TEXT,
    "metaAdId" TEXT NOT NULL,
    "metaCreativeId" TEXT,
    "name" TEXT,
    "status" TEXT,
    "effectiveStatus" TEXT,
    "previewShareableLink" TEXT,
    "thumbnailUrl" TEXT,
    "imageUrl" TEXT,
    "videoId" TEXT,
    "postId" TEXT,
    "pageId" TEXT,
    "callToActionType" TEXT,
    "creativeJson" JSONB,
    "rawJson" JSONB,
    "lastSyncedAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "MetaAd_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "MetaAdInsightDaily" (
    "id" TEXT NOT NULL,
    "metaAccountId" TEXT NOT NULL,
    "level" TEXT NOT NULL,
    "dateStart" TIMESTAMP(3) NOT NULL,
    "dateStop" TIMESTAMP(3) NOT NULL,
    "metaCampaignId" TEXT,
    "metaAdSetId" TEXT,
    "metaAdId" TEXT,
    "campaignName" TEXT,
    "adSetName" TEXT,
    "adName" TEXT,
    "spend" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "impressions" INTEGER NOT NULL DEFAULT 0,
    "reach" INTEGER NOT NULL DEFAULT 0,
    "clicks" INTEGER NOT NULL DEFAULT 0,
    "inlineLinkClicks" INTEGER NOT NULL DEFAULT 0,
    "cpc" DECIMAL(18,4) NOT NULL DEFAULT 0,
    "cpm" DECIMAL(18,4) NOT NULL DEFAULT 0,
    "ctr" DECIMAL(18,4) NOT NULL DEFAULT 0,
    "purchases" INTEGER NOT NULL DEFAULT 0,
    "purchaseValue" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "costPerPurchase" DECIMAL(18,4) NOT NULL DEFAULT 0,
    "roas" DECIMAL(18,4) NOT NULL DEFAULT 0,
    "actionsJson" JSONB,
    "actionValuesJson" JSONB,
    "rawJson" JSONB,
    "syncedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "MetaAdInsightDaily_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "MetaSyncLog" (
    "id" TEXT NOT NULL,
    "metaAccountId" TEXT,
    "syncType" TEXT NOT NULL,
    "status" TEXT NOT NULL,
    "range" TEXT,
    "fromDate" TIMESTAMP(3),
    "toDate" TIMESTAMP(3),
    "startedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "finishedAt" TIMESTAMP(3),
    "durationMs" INTEGER,
    "scanned" INTEGER NOT NULL DEFAULT 0,
    "upserted" INTEGER NOT NULL DEFAULT 0,
    "failed" INTEGER NOT NULL DEFAULT 0,
    "message" TEXT,
    "errorJson" JSONB,
    "createdById" TEXT,
    "createdByName" TEXT,

    CONSTRAINT "MetaSyncLog_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "OmniInboxPage" (
    "id" TEXT NOT NULL,
    "channel" "OmniChannel" NOT NULL,
    "providerPageId" TEXT NOT NULL,
    "pageName" TEXT NOT NULL,
    "accessTokenEnc" TEXT,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "lastWebhookAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "OmniInboxPage_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "OmniCustomer" (
    "id" TEXT NOT NULL,
    "providerUserId" TEXT,
    "name" TEXT NOT NULL,
    "avatarUrl" TEXT,
    "phone" TEXT,
    "address" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "OmniCustomer_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "OmniAssignmentSetting" (
    "id" TEXT NOT NULL DEFAULT 'default',
    "isActive" BOOLEAN NOT NULL DEFAULT false,
    "mode" TEXT NOT NULL DEFAULT 'AUTO',
    "priorityOrder" JSONB NOT NULL,
    "requireOnline" BOOLEAN NOT NULL DEFAULT true,
    "branchPriorityEnabled" BOOLEAN NOT NULL DEFAULT true,
    "lowestLoadEnabled" BOOLEAN NOT NULL DEFAULT true,
    "draftOwnerPriorityEnabled" BOOLEAN NOT NULL DEFAULT true,
    "keepPreviousAssignee" BOOLEAN NOT NULL DEFAULT true,
    "keepPreviousDays" INTEGER NOT NULL DEFAULT 7,
    "reassignIfAssigneeOffline" BOOLEAN NOT NULL DEFAULT true,
    "workingHoursOnly" BOOLEAN NOT NULL DEFAULT false,
    "workStartMinute" INTEGER NOT NULL DEFAULT 480,
    "workEndMinute" INTEGER NOT NULL DEFAULT 1320,
    "workDays" JSONB NOT NULL,
    "outsideHoursMode" TEXT NOT NULL DEFAULT 'QUEUE',
    "onlineWindowSeconds" INTEGER NOT NULL DEFAULT 90,
    "maxActiveEnabled" BOOLEAN NOT NULL DEFAULT true,
    "maxActiveConversations" INTEGER NOT NULL DEFAULT 20,
    "maxUnreadEnabled" BOOLEAN NOT NULL DEFAULT true,
    "maxUnreadConversations" INTEGER NOT NULL DEFAULT 10,
    "branchRoutingEnabled" BOOLEAN NOT NULL DEFAULT true,
    "fallbackBranchId" TEXT,
    "noCandidateMode" TEXT NOT NULL DEFAULT 'UNASSIGNED',
    "onlyAssignedCanView" BOOLEAN NOT NULL DEFAULT true,
    "managerCanViewBranch" BOOLEAN NOT NULL DEFAULT true,
    "onlyAssignedCanReply" BOOLEAN NOT NULL DEFAULT true,
    "shuffleEachRound" BOOLEAN NOT NULL DEFAULT true,
    "reassignUnreadEnabled" BOOLEAN NOT NULL DEFAULT false,
    "reassignAfterMinutes" INTEGER NOT NULL DEFAULT 10,
    "morningQueueEnabled" BOOLEAN NOT NULL DEFAULT true,
    "morningQueueInitialBatchSize" INTEGER NOT NULL DEFAULT 20,
    "morningQueueRepeatIntervalMinutes" INTEGER NOT NULL DEFAULT 2,
    "morningQueueRepeatBatchSize" INTEGER NOT NULL DEFAULT 3,
    "morningQueueRunDate" TEXT,
    "morningQueueInitialDone" BOOLEAN NOT NULL DEFAULT false,
    "morningQueueLastRunAt" TIMESTAMP(3),
    "morningQueueLastOnlineCount" INTEGER NOT NULL DEFAULT 0,
    "lastAssignedStaffId" TEXT,
    "updatedById" TEXT,
    "updatedByName" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "OmniAssignmentSetting_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "OmniAssignmentMember" (
    "id" TEXT NOT NULL,
    "settingId" TEXT NOT NULL DEFAULT 'default',
    "staffId" TEXT NOT NULL,
    "staffName" TEXT NOT NULL,
    "branchId" TEXT,
    "branchName" TEXT,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "receiveMessages" BOOLEAN NOT NULL DEFAULT true,
    "receiveComments" BOOLEAN NOT NULL DEFAULT false,
    "sortOrder" INTEGER NOT NULL DEFAULT 0,
    "weight" INTEGER NOT NULL DEFAULT 1,
    "maxActiveConversations" INTEGER,
    "maxUnreadConversations" INTEGER,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "OmniAssignmentMember_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "OmniStaffPresence" (
    "staffId" TEXT NOT NULL,
    "staffName" TEXT,
    "status" TEXT NOT NULL DEFAULT 'OFFLINE',
    "manualAway" BOOLEAN NOT NULL DEFAULT false,
    "activeBranchId" TEXT,
    "lastHeartbeatAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "lastActiveAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "OmniStaffPresence_pkey" PRIMARY KEY ("staffId")
);

-- CreateTable
CREATE TABLE "OmniAssignmentHistory" (
    "id" TEXT NOT NULL,
    "conversationId" TEXT NOT NULL,
    "customerName" TEXT,
    "channel" "OmniChannel",
    "branchId" TEXT,
    "previousStaffId" TEXT,
    "previousStaffName" TEXT,
    "assignedStaffId" TEXT,
    "assignedStaffName" TEXT,
    "action" TEXT NOT NULL,
    "reason" TEXT,
    "decisionDetail" JSONB,
    "triggerType" TEXT,
    "createdById" TEXT,
    "createdByName" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "OmniAssignmentHistory_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "OmniQuickReplyTemplate" (
    "id" TEXT NOT NULL,
    "title" TEXT,
    "content" TEXT NOT NULL,
    "normalizedText" TEXT NOT NULL,
    "category" TEXT,
    "sortOrder" INTEGER NOT NULL DEFAULT 0,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdById" TEXT,
    "createdByName" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "OmniQuickReplyTemplate_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "OmniConversation" (
    "id" TEXT NOT NULL,
    "channel" "OmniChannel" NOT NULL,
    "providerThreadId" TEXT NOT NULL,
    "pageId" TEXT,
    "customerId" TEXT,
    "status" "OmniConversationStatus" NOT NULL DEFAULT 'OPEN',
    "assigneeId" TEXT,
    "assigneeName" TEXT,
    "branchId" TEXT,
    "lastMessageText" TEXT,
    "lastMessageAt" TIMESTAMP(3),
    "unreadCount" INTEGER NOT NULL DEFAULT 0,
    "referralSource" TEXT,
    "referralType" TEXT,
    "referralRef" TEXT,
    "referralIdentifier" TEXT,
    "adId" TEXT,
    "adPostId" TEXT,
    "adProductId" TEXT,
    "adTitle" TEXT,
    "adBody" TEXT,
    "adImageUrl" TEXT,
    "adVideoUrl" TEXT,
    "adUrl" TEXT,
    "adReferral" JSONB,
    "adFirstSeenAt" TIMESTAMP(3),
    "lockedById" TEXT,
    "lockedAt" TIMESTAMP(3),
    "closedAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "OmniConversation_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "OmniMessage" (
    "id" TEXT NOT NULL,
    "conversationId" TEXT NOT NULL,
    "providerMessageId" TEXT,
    "direction" "OmniMessageDirection" NOT NULL,
    "type" "OmniMessageType" NOT NULL DEFAULT 'TEXT',
    "text" TEXT,
    "attachmentUrl" TEXT,
    "senderId" TEXT,
    "senderName" TEXT,
    "sentAt" TIMESTAMP(3) NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "OmniMessage_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "OmniTagTemplate" (
    "id" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "normalizedName" TEXT NOT NULL,
    "color" TEXT,
    "sortOrder" INTEGER NOT NULL DEFAULT 0,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdById" TEXT,
    "createdByName" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "OmniTagTemplate_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "OmniConversationTag" (
    "id" TEXT NOT NULL,
    "conversationId" TEXT NOT NULL,
    "tag" TEXT NOT NULL,

    CONSTRAINT "OmniConversationTag_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "OmniNoteTemplate" (
    "id" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "normalizedName" TEXT NOT NULL,
    "color" TEXT,
    "sortOrder" INTEGER NOT NULL DEFAULT 0,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdById" TEXT,
    "createdByName" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "OmniNoteTemplate_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "OmniConversationNote" (
    "id" TEXT NOT NULL,
    "conversationId" TEXT NOT NULL,
    "templateId" TEXT,
    "staffId" TEXT,
    "staffName" TEXT,
    "note" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "OmniConversationNote_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "LocalDeliveryReconciliation" (
    "id" TEXT NOT NULL,
    "code" TEXT,
    "orderId" TEXT NOT NULL,
    "shipmentId" TEXT,
    "branchId" TEXT,
    "carrier" TEXT,
    "trackingCode" TEXT,
    "status" TEXT NOT NULL DEFAULT 'DRAFT',
    "codAmount" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "shippingFee" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "paidAmount" DECIMAL(18,2) NOT NULL DEFAULT 0,
    "paymentRows" JSONB,
    "note" TEXT,
    "createdById" TEXT,
    "createdByName" TEXT,
    "confirmedById" TEXT,
    "confirmedByName" TEXT,
    "paidById" TEXT,
    "paidByName" TEXT,
    "cancelledById" TEXT,
    "cancelledByName" TEXT,
    "createdAt" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "confirmedAt" TIMESTAMP(6),
    "paidAt" TIMESTAMP(6),
    "cancelledAt" TIMESTAMP(6),

    CONSTRAINT "LocalDeliveryReconciliation_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "SystemSetting" (
    "key" TEXT NOT NULL,
    "value" JSONB NOT NULL,
    "note" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "SystemSetting_pkey" PRIMARY KEY ("key")
);

-- CreateTable
CREATE TABLE "MobilePushToken" (
    "id" TEXT NOT NULL,
    "userId" TEXT,
    "staffId" TEXT,
    "branchId" TEXT,
    "platform" TEXT NOT NULL,
    "provider" TEXT NOT NULL,
    "token" TEXT NOT NULL,
    "deviceId" TEXT,
    "appVersion" TEXT,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "lastSeenAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "MobilePushToken_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "FabricBoard" (
    "id" TEXT NOT NULL,
    "supplierId" TEXT NOT NULL,
    "boardCode" TEXT NOT NULL,
    "fabricCode" TEXT,
    "name" TEXT,
    "composition" TEXT,
    "expectedGsm" DECIMAL(10,2),
    "referencePriceVnd" DECIMAL(14,0),
    "referencePriceUnit" TEXT NOT NULL DEFAULT 'METER',
    "seasons" TEXT[] DEFAULT ARRAY[]::TEXT[],
    "productGroups" TEXT[] DEFAULT ARRAY[]::TEXT[],
    "coverImageUrl" TEXT,
    "note" TEXT,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdById" TEXT,
    "createdByName" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "FabricBoard_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "FabricBoardColor" (
    "id" TEXT NOT NULL,
    "fabricBoardId" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "code" TEXT,
    "imageUrl" TEXT,
    "note" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "FabricBoardColor_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "FabricBoardImage" (
    "id" TEXT NOT NULL,
    "fabricBoardId" TEXT NOT NULL,
    "type" "FabricBoardImageType" NOT NULL DEFAULT 'BOARD',
    "url" TEXT NOT NULL,
    "caption" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "FabricBoardImage_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "FabricOrder" (
    "id" TEXT NOT NULL,
    "code" TEXT NOT NULL,
    "supplierId" TEXT NOT NULL,
    "status" "FabricOrderStatus" NOT NULL DEFAULT 'DRAFT',
    "orderedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "expectedAt" TIMESTAMP(3),
    "note" TEXT,
    "imageUrls" TEXT[] DEFAULT ARRAY[]::TEXT[],
    "createdById" TEXT,
    "createdByName" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "FabricOrder_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "FabricOrderItem" (
    "id" TEXT NOT NULL,
    "fabricOrderId" TEXT NOT NULL,
    "fabricBoardId" TEXT NOT NULL,
    "fabricColorId" TEXT,
    "designSampleId" TEXT,
    "quantity" DECIMAL(14,3) NOT NULL,
    "unit" TEXT NOT NULL DEFAULT 'METER',
    "unitPriceVnd" DECIMAL(14,0),
    "lineTotalVnd" DECIMAL(16,0),
    "note" TEXT,
    "boardCodeSnapshot" TEXT NOT NULL,
    "fabricCodeSnapshot" TEXT,
    "fabricNameSnapshot" TEXT,
    "colorNameSnapshot" TEXT,
    "colorCodeSnapshot" TEXT,
    "sampleCodeSnapshot" TEXT,
    "sampleNameSnapshot" TEXT,
    "supplierCodeSnapshot" TEXT,
    "supplierNameSnapshot" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "FabricOrderItem_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "FabricSampleDispatch" (
    "id" TEXT NOT NULL,
    "designSampleId" TEXT NOT NULL,
    "fabricBoardId" TEXT NOT NULL,
    "fabricColorId" TEXT,
    "colorName" TEXT,
    "colorCode" TEXT,
    "recipientName" TEXT NOT NULL,
    "recipientType" TEXT,
    "recipientContact" TEXT,
    "sentAt" TIMESTAMP(3) NOT NULL,
    "sentById" TEXT,
    "sentByName" TEXT,
    "dueDate" TIMESTAMP(3),
    "returnedAt" TIMESTAMP(3),
    "status" "FabricSampleDispatchStatus" NOT NULL DEFAULT 'SENT',
    "note" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "FabricSampleDispatch_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "SampleTechnicalPerson" (
    "id" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "role" "SampleTechnicalPersonRole" NOT NULL,
    "phone" TEXT,
    "note" TEXT,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "SampleTechnicalPerson_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "DesignSample" (
    "id" TEXT NOT NULL,
    "code" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "year" INTEGER NOT NULL,
    "season" TEXT,
    "category" TEXT,
    "supplierId" TEXT,
    "fabricBoardCode" TEXT,
    "fabricCode" TEXT,
    "fabricComposition" TEXT,
    "fabricBoardId" TEXT,
    "fabricColorId" TEXT,
    "fabricColorName" TEXT,
    "fabricColorCode" TEXT,
    "sampleFactoryId" TEXT,
    "sampleFactoryName" TEXT,
    "sampleMakerId" TEXT,
    "sampleMakerName" TEXT,
    "patternMakerId" TEXT,
    "patternMakerName" TEXT,
    "producedProductId" TEXT,
    "priorityRank" INTEGER,
    "priorityLane" TEXT NOT NULL DEFAULT 'IDEA',
    "status" "DesignSampleStatus" NOT NULL DEFAULT 'IDEA',
    "assigneeStaffId" TEXT,
    "assigneeName" TEXT,
    "nextAction" TEXT,
    "dueDate" TIMESTAMP(3),
    "coverImageUrl" TEXT,
    "note" TEXT,
    "technicalNote" TEXT,
    "createdById" TEXT,
    "createdByName" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "DesignSample_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "DesignSampleIdeaBoard" (
    "id" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "description" TEXT,
    "createdById" TEXT,
    "createdByName" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "DesignSampleIdeaBoard_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "DesignSampleIdeaBoardItem" (
    "id" TEXT NOT NULL,
    "boardId" TEXT NOT NULL,
    "designSampleId" TEXT NOT NULL,
    "sortOrder" INTEGER NOT NULL DEFAULT 0,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "DesignSampleIdeaBoardItem_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "MeasurementTemplate" (
    "id" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "productKind" "ProductionProductKind" NOT NULL DEFAULT 'OTHER',
    "unitDefault" TEXT NOT NULL DEFAULT 'cm',
    "note" TEXT,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdById" TEXT,
    "createdByName" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "MeasurementTemplate_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "MeasurementTemplateSize" (
    "id" TEXT NOT NULL,
    "templateId" TEXT NOT NULL,
    "size" TEXT NOT NULL,
    "sortOrder" INTEGER NOT NULL DEFAULT 0,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "MeasurementTemplateSize_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "MeasurementTemplateRow" (
    "id" TEXT NOT NULL,
    "templateId" TEXT NOT NULL,
    "code" TEXT,
    "name" TEXT NOT NULL,
    "nameEn" TEXT,
    "unit" TEXT NOT NULL DEFAULT 'cm',
    "sortOrder" INTEGER NOT NULL DEFAULT 0,
    "note" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "MeasurementTemplateRow_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "MeasurementTemplateValue" (
    "id" TEXT NOT NULL,
    "rowId" TEXT NOT NULL,
    "sizeId" TEXT NOT NULL,
    "value" DECIMAL(10,3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "MeasurementTemplateValue_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "DesignSampleMeasurementProfile" (
    "id" TEXT NOT NULL,
    "designSampleId" TEXT NOT NULL,
    "sourceTemplateId" TEXT,
    "templateNameSnapshot" TEXT NOT NULL,
    "productKind" "ProductionProductKind" NOT NULL DEFAULT 'OTHER',
    "unitDefault" TEXT NOT NULL DEFAULT 'cm',
    "note" TEXT,
    "createdById" TEXT,
    "createdByName" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "DesignSampleMeasurementProfile_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "DesignSampleMeasurementSize" (
    "id" TEXT NOT NULL,
    "profileId" TEXT NOT NULL,
    "size" TEXT NOT NULL,
    "sortOrder" INTEGER NOT NULL DEFAULT 0,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "DesignSampleMeasurementSize_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "DesignSampleMeasurementRow" (
    "id" TEXT NOT NULL,
    "profileId" TEXT NOT NULL,
    "code" TEXT,
    "name" TEXT NOT NULL,
    "nameEn" TEXT,
    "unit" TEXT NOT NULL DEFAULT 'cm',
    "sortOrder" INTEGER NOT NULL DEFAULT 0,
    "note" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "DesignSampleMeasurementRow_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "DesignSampleMeasurementValue" (
    "id" TEXT NOT NULL,
    "rowId" TEXT NOT NULL,
    "sizeId" TEXT NOT NULL,
    "value" DECIMAL(10,3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "DesignSampleMeasurementValue_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "DesignSampleColor" (
    "id" TEXT NOT NULL,
    "designSampleId" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "code" TEXT,
    "status" "DesignSampleStatus" NOT NULL DEFAULT 'IDEA',
    "note" TEXT,
    "imageUrl" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "DesignSampleColor_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "DesignSampleImage" (
    "id" TEXT NOT NULL,
    "designSampleId" TEXT NOT NULL,
    "type" "DesignSampleImageType" NOT NULL DEFAULT 'SAMPLE',
    "url" TEXT NOT NULL,
    "caption" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "DesignSampleImage_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "DesignSampleProgressLog" (
    "id" TEXT NOT NULL,
    "designSampleId" TEXT NOT NULL,
    "fromStatus" "DesignSampleStatus",
    "toStatus" "DesignSampleStatus" NOT NULL,
    "note" TEXT,
    "actorId" TEXT,
    "actorName" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "DesignSampleProgressLog_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "FabricReceipt" (
    "id" TEXT NOT NULL,
    "receiptCode" TEXT NOT NULL,
    "designSampleId" TEXT,
    "productId" TEXT,
    "fabricBoardId" TEXT,
    "fabricColorId" TEXT,
    "supplierId" TEXT,
    "branchId" TEXT,
    "fabricBoardCode" TEXT,
    "fabricCode" TEXT,
    "fabricName" TEXT,
    "colorName" TEXT,
    "colorCode" TEXT,
    "lotCode" TEXT,
    "supplierDeclaredM" DECIMAL(12,3),
    "supplierDeclaredKg" DECIMAL(12,3),
    "actualM" DECIMAL(12,3),
    "actualKg" DECIMAL(12,3),
    "rollCount" INTEGER NOT NULL DEFAULT 0,
    "unitPrice" DECIMAL(14,2),
    "priceUnit" "FabricPriceUnit" NOT NULL DEFAULT 'METER',
    "priceCurrency" TEXT NOT NULL DEFAULT 'VND',
    "exchangeRateToVnd" DECIMAL(14,4),
    "unitPriceVnd" DECIMAL(14,2),
    "expectedGsm" DECIMAL(10,2),
    "measuredGsm" DECIMAL(10,2),
    "varianceApproved" BOOLEAN NOT NULL DEFAULT false,
    "varianceApprovedBy" TEXT,
    "varianceApprovedAt" TIMESTAMP(3),
    "status" "FabricReceiptStatus" NOT NULL DEFAULT 'DRAFT',
    "receivedAt" TIMESTAMP(3),
    "completedAt" TIMESTAMP(3),
    "receivedByStaffId" TEXT,
    "receivedByName" TEXT,
    "note" TEXT,
    "createdById" TEXT,
    "createdByName" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "FabricReceipt_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "FabricReceiptRoll" (
    "id" TEXT NOT NULL,
    "fabricReceiptId" TEXT NOT NULL,
    "sortOrder" INTEGER NOT NULL DEFAULT 0,
    "fabricCode" TEXT,
    "rollCode" TEXT,
    "colorName" TEXT,
    "colorCode" TEXT,
    "supplierDeclaredM" DECIMAL(12,3),
    "supplierDeclaredKg" DECIMAL(12,3),
    "actualM" DECIMAL(12,3),
    "actualKg" DECIMAL(12,3),
    "measuredGsm" DECIMAL(10,2),
    "unitPriceCny" DECIMAL(14,4),
    "priceUnit" "FabricPriceUnit" NOT NULL DEFAULT 'METER',
    "defectNote" TEXT,
    "passed" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "FabricReceiptRoll_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "FabricReceiptFabricConfig" (
    "id" TEXT NOT NULL,
    "fabricReceiptId" TEXT NOT NULL,
    "fabricCode" TEXT NOT NULL,
    "materialName" TEXT,
    "supplierId" TEXT,
    "fabricBoardCode" TEXT,
    "fabricWidthCm" DECIMAL(10,2),
    "productId" TEXT,
    "designSampleId" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "FabricReceiptFabricConfig_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "FabricReceiptFabricCost" (
    "id" TEXT NOT NULL,
    "fabricReceiptId" TEXT NOT NULL,
    "fabricCode" TEXT NOT NULL,
    "chinaShippingCny" DECIMAL(14,2),
    "vietnamShippingRateVndPerKg" DECIMAL(14,2),
    "vietnamShippingVnd" DECIMAL(14,2),
    "note" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "FabricReceiptFabricCost_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "FabricReceiptColorMap" (
    "id" TEXT NOT NULL,
    "fabricReceiptId" TEXT NOT NULL,
    "fabricCode" TEXT NOT NULL,
    "colorName" TEXT NOT NULL,
    "colorCode" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "FabricReceiptColorMap_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "FabricMeasurement" (
    "id" TEXT NOT NULL,
    "fabricReceiptId" TEXT NOT NULL,
    "rollId" TEXT,
    "areaCm2" DECIMAL(10,2) NOT NULL,
    "weightGrams" DECIMAL(10,4) NOT NULL,
    "gsm" DECIMAL(10,2) NOT NULL,
    "positionLabel" TEXT,
    "imageUrl" TEXT,
    "note" TEXT,
    "measuredById" TEXT,
    "measuredByName" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "FabricMeasurement_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "FabricReceiptImage" (
    "id" TEXT NOT NULL,
    "fabricReceiptId" TEXT NOT NULL,
    "rollId" TEXT,
    "type" "FabricImageType" NOT NULL DEFAULT 'FABRIC',
    "url" TEXT NOT NULL,
    "caption" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "FabricReceiptImage_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ProductionPartner" (
    "id" TEXT NOT NULL,
    "code" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "contactName" TEXT,
    "phone" TEXT,
    "email" TEXT,
    "address" TEXT,
    "note" TEXT,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "ProductionPartner_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ProductionAccessorySupplier" (
    "id" TEXT NOT NULL,
    "code" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "phone" TEXT,
    "email" TEXT,
    "address" TEXT,
    "note" TEXT,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "ProductionAccessorySupplier_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ProductionAccessoryItem" (
    "id" TEXT NOT NULL,
    "code" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "typeName" TEXT NOT NULL,
    "imageUrl" TEXT,
    "unit" "ProductionAccessoryUnit" NOT NULL DEFAULT 'PIECE',
    "stockQty" DECIMAL(14,3) NOT NULL DEFAULT 0,
    "unitPrice" DECIMAL(14,2),
    "supplierId" TEXT,
    "specifications" JSONB,
    "note" TEXT,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "ProductionAccessoryItem_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ProductionAccessoryReceipt" (
    "id" TEXT NOT NULL,
    "code" TEXT NOT NULL,
    "supplierId" TEXT,
    "receivedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "receivedById" TEXT,
    "receivedByName" TEXT,
    "note" TEXT,
    "status" TEXT NOT NULL DEFAULT 'DRAFT',
    "postedAt" TIMESTAMP(3),
    "postedById" TEXT,
    "postedByName" TEXT,
    "createdById" TEXT,
    "createdByName" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "ProductionAccessoryReceipt_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ProductionAccessoryReceiptItem" (
    "id" TEXT NOT NULL,
    "receiptId" TEXT NOT NULL,
    "accessoryItemId" TEXT NOT NULL,
    "accessoryCodeSnapshot" TEXT NOT NULL,
    "accessoryNameSnapshot" TEXT NOT NULL,
    "unit" "ProductionAccessoryUnit" NOT NULL,
    "qty" DECIMAL(14,3) NOT NULL,
    "unitPrice" DECIMAL(14,2),
    "note" TEXT,
    "sortOrder" INTEGER NOT NULL DEFAULT 0,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "ProductionAccessoryReceiptItem_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ProductionAccessoryTemplate" (
    "id" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "productKind" "ProductionProductKind" NOT NULL DEFAULT 'OTHER',
    "sourceType" TEXT NOT NULL DEFAULT 'MANUAL',
    "sourceFileName" TEXT,
    "createdById" TEXT,
    "createdByName" TEXT,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "ProductionAccessoryTemplate_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ProductionAccessoryTemplateItem" (
    "id" TEXT NOT NULL,
    "templateId" TEXT NOT NULL,
    "accessoryItemId" TEXT,
    "accessoryCodeSnapshot" TEXT,
    "accessoryNameSnapshot" TEXT,
    "qtyPerProduct" DECIMAL(12,4) NOT NULL,
    "wastePercent" DECIMAL(7,3) NOT NULL DEFAULT 0,
    "sizeScoped" BOOLEAN NOT NULL DEFAULT false,
    "note" TEXT,
    "sortOrder" INTEGER NOT NULL DEFAULT 0,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "ProductionAccessoryTemplateItem_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "SampleProductionSpec" (
    "id" TEXT NOT NULL,
    "designSampleId" TEXT NOT NULL,
    "productKind" "ProductionProductKind" NOT NULL DEFAULT 'OTHER',
    "fabricWidthCm" DECIMAL(10,2),
    "fabricConsumptionM" DECIMAL(10,4),
    "fabricWastePercent" DECIMAL(7,3) NOT NULL DEFAULT 0,
    "sizeSet" JSONB,
    "defaultSizeRatio" JSONB,
    "note" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "SampleProductionSpec_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "SampleAccessorySpec" (
    "id" TEXT NOT NULL,
    "designSampleId" TEXT NOT NULL,
    "accessoryItemId" TEXT NOT NULL,
    "qtyPerProduct" DECIMAL(12,4) NOT NULL,
    "wastePercent" DECIMAL(7,3) NOT NULL DEFAULT 0,
    "sizeScoped" BOOLEAN NOT NULL DEFAULT false,
    "note" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "SampleAccessorySpec_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ProductionOrder" (
    "id" TEXT NOT NULL,
    "code" TEXT NOT NULL,
    "sourceType" TEXT NOT NULL DEFAULT 'SAMPLE',
    "designSampleId" TEXT,
    "productId" TEXT,
    "sourceCode" TEXT NOT NULL DEFAULT '',
    "sourceName" TEXT,
    "sourceImageUrl" TEXT,
    "productionPartnerId" TEXT NOT NULL,
    "status" "ProductionOrderStatus" NOT NULL DEFAULT 'DRAFT',
    "plannedStartAt" TIMESTAMP(3),
    "dueDate" TIMESTAMP(3),
    "sentAt" TIMESTAMP(3),
    "productKind" "ProductionProductKind" NOT NULL DEFAULT 'OTHER',
    "fabricWidthCm" DECIMAL(10,2),
    "fabricConsumptionM" DECIMAL(10,4),
    "fabricWastePercent" DECIMAL(7,3) NOT NULL DEFAULT 0,
    "liningFabricConsumptionM" DECIMAL(10,4),
    "liningFabricWastePercent" DECIMAL(7,3) NOT NULL DEFAULT 0,
    "liningFabricComponents" JSONB,
    "liningFabricAssignments" JSONB,
    "productionExtraCosts" JSONB,
    "productionPriceMultiplier" DECIMAL(6,2) NOT NULL DEFAULT 2.2,
    "sizeSet" JSONB,
    "sizeRatio" JSONB,
    "plannedQtyOverride" INTEGER,
    "note" TEXT,
    "createdById" TEXT,
    "createdByName" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "ProductionOrder_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ProductionOrderRoll" (
    "id" TEXT NOT NULL,
    "productionOrderId" TEXT NOT NULL,
    "fabricReceiptRollId" TEXT NOT NULL,
    "fabricReceiptId" TEXT,
    "rollCode" TEXT,
    "colorName" TEXT,
    "colorCode" TEXT,
    "availableM" DECIMAL(12,3),
    "availableKg" DECIMAL(12,3),
    "allocatedM" DECIMAL(12,3),
    "allocatedKg" DECIMAL(12,3),
    "imageUrl" TEXT,
    "fabricRole" TEXT NOT NULL DEFAULT 'MAIN',
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "ProductionOrderRoll_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ProductionSizePlan" (
    "id" TEXT NOT NULL,
    "productionOrderId" TEXT NOT NULL,
    "colorName" TEXT NOT NULL,
    "colorCode" TEXT,
    "size" TEXT NOT NULL,
    "ratio" INTEGER NOT NULL DEFAULT 0,
    "plannedQty" INTEGER NOT NULL DEFAULT 0,
    "actualQty" INTEGER,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "ProductionSizePlan_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ProductionCutQtyHistory" (
    "id" TEXT NOT NULL,
    "productionOrderId" TEXT NOT NULL,
    "colorName" TEXT NOT NULL,
    "colorCode" TEXT,
    "size" TEXT NOT NULL,
    "plannedQty" INTEGER NOT NULL DEFAULT 0,
    "previousActualQty" INTEGER,
    "actualQty" INTEGER,
    "changeType" TEXT NOT NULL DEFAULT 'ACTUAL_UPDATE',
    "createdById" TEXT,
    "createdByName" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "ProductionCutQtyHistory_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ProductionOrderAccessorySpec" (
    "id" TEXT NOT NULL,
    "productionOrderId" TEXT NOT NULL,
    "accessoryItemId" TEXT NOT NULL,
    "qtyPerProduct" DECIMAL(12,4) NOT NULL,
    "wastePercent" DECIMAL(7,3) NOT NULL DEFAULT 0,
    "sizeScoped" BOOLEAN NOT NULL DEFAULT false,
    "note" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "ProductionOrderAccessorySpec_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ProductionMaterialCalc" (
    "id" TEXT NOT NULL,
    "productionOrderId" TEXT NOT NULL,
    "accessoryItemId" TEXT NOT NULL,
    "accessoryCode" TEXT,
    "accessoryName" TEXT NOT NULL,
    "unit" TEXT NOT NULL,
    "sizeLabel" TEXT,
    "qtyPerProduct" DECIMAL(12,4) NOT NULL,
    "wastePercent" DECIMAL(7,3) NOT NULL DEFAULT 0,
    "baseQty" DECIMAL(14,3) NOT NULL,
    "requiredQty" DECIMAL(14,3) NOT NULL,
    "stockQtySnapshot" DECIMAL(14,3),
    "shortageQty" DECIMAL(14,3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "ProductionMaterialCalc_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ProductionNplIssue" (
    "id" TEXT NOT NULL,
    "productionOrderId" TEXT NOT NULL,
    "roundNo" INTEGER NOT NULL,
    "note" TEXT,
    "createdById" TEXT,
    "createdByName" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "ProductionNplIssue_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ProductionNplIssueItem" (
    "id" TEXT NOT NULL,
    "issueId" TEXT NOT NULL,
    "productionOrderId" TEXT NOT NULL,
    "accessoryItemId" TEXT NOT NULL,
    "accessoryCode" TEXT,
    "accessoryName" TEXT NOT NULL,
    "sizeLabel" TEXT,
    "sizeKey" TEXT NOT NULL DEFAULT '',
    "unit" TEXT NOT NULL,
    "requiredQtyAtIssue" DECIMAL(14,3) NOT NULL,
    "issuedBeforeQty" DECIMAL(14,3) NOT NULL DEFAULT 0,
    "issuedQty" DECIMAL(14,3) NOT NULL,
    "remainingAfterQty" DECIMAL(14,3) NOT NULL DEFAULT 0,
    "stockBeforeQty" DECIMAL(14,3),
    "stockAfterQty" DECIMAL(14,3),
    "note" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "ProductionNplIssueItem_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ProductionNplIssueNote" (
    "id" TEXT NOT NULL,
    "productionOrderId" TEXT NOT NULL,
    "accessoryItemId" TEXT NOT NULL,
    "sizeKey" TEXT NOT NULL DEFAULT '',
    "note" TEXT,
    "updatedById" TEXT,
    "updatedByName" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "ProductionNplIssueNote_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "CustomerAccount" (
    "id" TEXT NOT NULL,
    "customerId" TEXT NOT NULL,
    "phone" TEXT NOT NULL,
    "email" TEXT,
    "passwordHash" TEXT,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "emailVerifiedAt" TIMESTAMP(3),
    "phoneVerifiedAt" TIMESTAMP(3),
    "sessionVersion" INTEGER NOT NULL DEFAULT 1,
    "lastLoginAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "CustomerAccount_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "CustomerSession" (
    "id" TEXT NOT NULL,
    "accountId" TEXT NOT NULL,
    "refreshTokenHash" TEXT NOT NULL,
    "deviceInfo" TEXT,
    "ipAddress" TEXT,
    "sessionVersion" INTEGER NOT NULL DEFAULT 1,
    "expiresAt" TIMESTAMP(3) NOT NULL,
    "revokedAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "CustomerSession_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "CustomerOtp" (
    "id" TEXT NOT NULL,
    "accountId" TEXT,
    "phone" TEXT NOT NULL,
    "purpose" TEXT NOT NULL,
    "codeHash" TEXT NOT NULL,
    "expiresAt" TIMESTAMP(3) NOT NULL,
    "verifiedAt" TIMESTAMP(3),
    "attempts" INTEGER NOT NULL DEFAULT 0,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "CustomerOtp_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "WebsiteProduct" (
    "id" TEXT NOT NULL,
    "productId" TEXT NOT NULL,
    "slug" TEXT NOT NULL,
    "status" "WebsitePublishStatus" NOT NULL DEFAULT 'DRAFT',
    "marketVn" BOOLEAN NOT NULL DEFAULT true,
    "marketInternational" BOOLEAN NOT NULL DEFAULT false,
    "featured" BOOLEAN NOT NULL DEFAULT false,
    "sortOrder" INTEGER NOT NULL DEFAULT 0,
    "titleVi" TEXT NOT NULL,
    "titleEn" TEXT,
    "shortDescriptionVi" TEXT,
    "shortDescriptionEn" TEXT,
    "descriptionVi" TEXT,
    "descriptionEn" TEXT,
    "coverImageUrl" TEXT,
    "seoTitleVi" TEXT,
    "seoTitleEn" TEXT,
    "seoDescriptionVi" TEXT,
    "seoDescriptionEn" TEXT,
    "publishedAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "WebsiteProduct_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "WebsiteProductImage" (
    "id" TEXT NOT NULL,
    "websiteProductId" TEXT NOT NULL,
    "url" TEXT NOT NULL,
    "altVi" TEXT,
    "altEn" TEXT,
    "sortOrder" INTEGER NOT NULL DEFAULT 0,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "WebsiteProductImage_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "ShipmentTimelineEvent_shipmentId_idx" ON "ShipmentTimelineEvent"("shipmentId");

-- CreateIndex
CREATE INDEX "ShipmentTimelineEvent_orderId_idx" ON "ShipmentTimelineEvent"("orderId");

-- CreateIndex
CREATE INDEX "ShipmentTimelineEvent_carrier_idx" ON "ShipmentTimelineEvent"("carrier");

-- CreateIndex
CREATE INDEX "ShipmentTimelineEvent_status_idx" ON "ShipmentTimelineEvent"("status");

-- CreateIndex
CREATE INDEX "ShipmentTimelineEvent_eventTime_idx" ON "ShipmentTimelineEvent"("eventTime");

-- CreateIndex
CREATE UNIQUE INDEX "AhamoveShipment_shipmentId_key" ON "AhamoveShipment"("shipmentId");

-- CreateIndex
CREATE UNIQUE INDEX "AhamoveShipment_ahamoveOrderId_key" ON "AhamoveShipment"("ahamoveOrderId");

-- CreateIndex
CREATE INDEX "AhamoveShipment_orderId_idx" ON "AhamoveShipment"("orderId");

-- CreateIndex
CREATE INDEX "AhamoveShipment_status_idx" ON "AhamoveShipment"("status");

-- CreateIndex
CREATE INDEX "AhamoveShipment_serviceId_idx" ON "AhamoveShipment"("serviceId");

-- CreateIndex
CREATE INDEX "AhamoveShipment_lastSyncedAt_idx" ON "AhamoveShipment"("lastSyncedAt");

-- CreateIndex
CREATE UNIQUE INDEX "StaffUser_code_key" ON "StaffUser"("code");

-- CreateIndex
CREATE UNIQUE INDEX "StaffUser_username_key" ON "StaffUser"("username");

-- CreateIndex
CREATE UNIQUE INDEX "StaffUser_email_key" ON "StaffUser"("email");

-- CreateIndex
CREATE INDEX "StaffUser_branchId_idx" ON "StaffUser"("branchId");

-- CreateIndex
CREATE INDEX "StaffUser_email_idx" ON "StaffUser"("email");

-- CreateIndex
CREATE INDEX "StaffUser_phone_idx" ON "StaffUser"("phone");

-- CreateIndex
CREATE INDEX "StaffUser_isActive_idx" ON "StaffUser"("isActive");

-- CreateIndex
CREATE INDEX "StaffSession_staffId_idx" ON "StaffSession"("staffId");

-- CreateIndex
CREATE INDEX "StaffSession_expiresAt_idx" ON "StaffSession"("expiresAt");

-- CreateIndex
CREATE INDEX "StaffSession_revokedAt_idx" ON "StaffSession"("revokedAt");

-- CreateIndex
CREATE INDEX "StaffUserRole_roleCode_idx" ON "StaffUserRole"("roleCode");

-- CreateIndex
CREATE UNIQUE INDEX "StaffUserRole_staffId_roleCode_key" ON "StaffUserRole"("staffId", "roleCode");

-- CreateIndex
CREATE UNIQUE INDEX "StaffRoleTemplate_roleCode_key" ON "StaffRoleTemplate"("roleCode");

-- CreateIndex
CREATE INDEX "StaffRoleTemplate_roleCode_idx" ON "StaffRoleTemplate"("roleCode");

-- CreateIndex
CREATE INDEX "StaffBranchPermission_staffId_idx" ON "StaffBranchPermission"("staffId");

-- CreateIndex
CREATE INDEX "StaffBranchPermission_branchId_idx" ON "StaffBranchPermission"("branchId");

-- CreateIndex
CREATE UNIQUE INDEX "StaffBranchPermission_staffId_branchId_key" ON "StaffBranchPermission"("staffId", "branchId");

-- CreateIndex
CREATE INDEX "ImportErrorLog_jobId_idx" ON "ImportErrorLog"("jobId");

-- CreateIndex
CREATE UNIQUE INDEX "Category_code_key" ON "Category"("code");

-- CreateIndex
CREATE UNIQUE INDEX "Category_slug_key" ON "Category"("slug");

-- CreateIndex
CREATE INDEX "Category_isActive_idx" ON "Category"("isActive");

-- CreateIndex
CREATE INDEX "AdministrativeProvince_isActive_idx" ON "AdministrativeProvince"("isActive");

-- CreateIndex
CREATE INDEX "AdministrativeProvince_sortOrder_idx" ON "AdministrativeProvince"("sortOrder");

-- CreateIndex
CREATE INDEX "AdministrativeProvince_normalized_idx" ON "AdministrativeProvince"("normalized");

-- CreateIndex
CREATE INDEX "AdministrativeWard_provinceCode_idx" ON "AdministrativeWard"("provinceCode");

-- CreateIndex
CREATE INDEX "AdministrativeWard_isActive_idx" ON "AdministrativeWard"("isActive");

-- CreateIndex
CREATE INDEX "AdministrativeWard_sortOrder_idx" ON "AdministrativeWard"("sortOrder");

-- CreateIndex
CREATE INDEX "AdministrativeWard_normalized_idx" ON "AdministrativeWard"("normalized");

-- CreateIndex
CREATE UNIQUE INDEX "Supplier_code_key" ON "Supplier"("code");

-- CreateIndex
CREATE INDEX "Supplier_name_idx" ON "Supplier"("name");

-- CreateIndex
CREATE INDEX "Supplier_isActive_idx" ON "Supplier"("isActive");

-- CreateIndex
CREATE UNIQUE INDEX "FabricSupplier_code_key" ON "FabricSupplier"("code");

-- CreateIndex
CREATE INDEX "FabricSupplier_name_idx" ON "FabricSupplier"("name");

-- CreateIndex
CREATE INDEX "FabricSupplier_isActive_idx" ON "FabricSupplier"("isActive");

-- CreateIndex
CREATE UNIQUE INDEX "PurchaseReceipt_receiptCode_key" ON "PurchaseReceipt"("receiptCode");

-- CreateIndex
CREATE INDEX "PurchaseReceipt_supplierId_idx" ON "PurchaseReceipt"("supplierId");

-- CreateIndex
CREATE INDEX "PurchaseReceipt_branchId_idx" ON "PurchaseReceipt"("branchId");

-- CreateIndex
CREATE INDEX "PurchaseReceipt_status_idx" ON "PurchaseReceipt"("status");

-- CreateIndex
CREATE INDEX "PurchaseReceiptItem_receiptId_idx" ON "PurchaseReceiptItem"("receiptId");

-- CreateIndex
CREATE INDEX "PurchaseReceiptItem_variantId_idx" ON "PurchaseReceiptItem"("variantId");

-- CreateIndex
CREATE UNIQUE INDEX "StockTransfer_transferCode_key" ON "StockTransfer"("transferCode");

-- CreateIndex
CREATE INDEX "StockTransfer_fromBranchId_idx" ON "StockTransfer"("fromBranchId");

-- CreateIndex
CREATE INDEX "StockTransfer_toBranchId_idx" ON "StockTransfer"("toBranchId");

-- CreateIndex
CREATE INDEX "StockTransfer_status_idx" ON "StockTransfer"("status");

-- CreateIndex
CREATE INDEX "StockTransfer_direction_status_createdAt_idx" ON "StockTransfer"("direction", "status", "createdAt");

-- CreateIndex
CREATE INDEX "StockTransfer_sourceType_createdAt_idx" ON "StockTransfer"("sourceType", "createdAt");

-- CreateIndex
CREATE INDEX "StockTransferItem_transferId_idx" ON "StockTransferItem"("transferId");

-- CreateIndex
CREATE INDEX "StockTransferItem_variantId_idx" ON "StockTransferItem"("variantId");

-- CreateIndex
CREATE INDEX "BranchNotification_branchId_isRead_createdAt_idx" ON "BranchNotification"("branchId", "isRead", "createdAt");

-- CreateIndex
CREATE INDEX "BranchNotification_transferId_idx" ON "BranchNotification"("transferId");

-- CreateIndex
CREATE INDEX "BranchNotification_type_createdAt_idx" ON "BranchNotification"("type", "createdAt");

-- CreateIndex
CREATE INDEX "ShipmentTrackingCache_shipmentId_idx" ON "ShipmentTrackingCache"("shipmentId");

-- CreateIndex
CREATE INDEX "ShipmentTrackingCache_trackingCode_idx" ON "ShipmentTrackingCache"("trackingCode");

-- CreateIndex
CREATE INDEX "ShipmentTrackingCache_expiresAt_idx" ON "ShipmentTrackingCache"("expiresAt");

-- CreateIndex
CREATE INDEX "ShipmentCodAuthCode_orderId_idx" ON "ShipmentCodAuthCode"("orderId");

-- CreateIndex
CREATE UNIQUE INDEX "PartialDeliveryRecord_code_key" ON "PartialDeliveryRecord"("code");

-- CreateIndex
CREATE INDEX "PartialDeliveryRecord_orderId_idx" ON "PartialDeliveryRecord"("orderId");

-- CreateIndex
CREATE INDEX "PartialDeliveryRecord_orderCode_idx" ON "PartialDeliveryRecord"("orderCode");

-- CreateIndex
CREATE INDEX "PartialDeliveryRecord_ghnTrackingCode_idx" ON "PartialDeliveryRecord"("ghnTrackingCode");

-- CreateIndex
CREATE INDEX "PartialDeliveryRecord_returnOrderId_idx" ON "PartialDeliveryRecord"("returnOrderId");

-- CreateIndex
CREATE INDEX "PartialDeliveryRecord_returnOrderCode_idx" ON "PartialDeliveryRecord"("returnOrderCode");

-- CreateIndex
CREATE INDEX "PartialDeliveryRecord_returnTrackingCode_idx" ON "PartialDeliveryRecord"("returnTrackingCode");

-- CreateIndex
CREATE INDEX "PartialDeliveryRecord_returnStatus_idx" ON "PartialDeliveryRecord"("returnStatus");

-- CreateIndex
CREATE INDEX "PartialDeliveryItem_recordId_idx" ON "PartialDeliveryItem"("recordId");

-- CreateIndex
CREATE INDEX "PartialDeliveryItem_orderItemId_idx" ON "PartialDeliveryItem"("orderItemId");

-- CreateIndex
CREATE INDEX "PartialDeliveryItem_variantId_idx" ON "PartialDeliveryItem"("variantId");

-- CreateIndex
CREATE INDEX "PartialDeliveryItem_actionType_idx" ON "PartialDeliveryItem"("actionType");

-- CreateIndex
CREATE UNIQUE INDEX "PaymentSource_code_key" ON "PaymentSource"("code");

-- CreateIndex
CREATE INDEX "GhnCodReconciliationBatch_transferCode_idx" ON "GhnCodReconciliationBatch"("transferCode");

-- CreateIndex
CREATE INDEX "GhnCodReconciliationBatch_transferDate_idx" ON "GhnCodReconciliationBatch"("transferDate");

-- CreateIndex
CREATE INDEX "GhnCodReconciliationBatch_sourceType_idx" ON "GhnCodReconciliationBatch"("sourceType");

-- CreateIndex
CREATE INDEX "GhnCodReconciliationBatch_status_idx" ON "GhnCodReconciliationBatch"("status");

-- CreateIndex
CREATE INDEX "GhnCodReconciliationBatch_confirmedAt_idx" ON "GhnCodReconciliationBatch"("confirmedAt");

-- CreateIndex
CREATE INDEX "GhnCodReconciliationBatch_paidAt_idx" ON "GhnCodReconciliationBatch"("paidAt");

-- CreateIndex
CREATE INDEX "GhnCodReconciliationBatch_paymentSourceId_idx" ON "GhnCodReconciliationBatch"("paymentSourceId");

-- CreateIndex
CREATE INDEX "GhnCodReconciliationRow_batchId_idx" ON "GhnCodReconciliationRow"("batchId");

-- CreateIndex
CREATE INDEX "GhnCodReconciliationRow_orderId_idx" ON "GhnCodReconciliationRow"("orderId");

-- CreateIndex
CREATE INDEX "GhnCodReconciliationRow_shipmentId_idx" ON "GhnCodReconciliationRow"("shipmentId");

-- CreateIndex
CREATE INDEX "GhnCodReconciliationRow_ghnCode_idx" ON "GhnCodReconciliationRow"("ghnCode");

-- CreateIndex
CREATE INDEX "GhnCodReconciliationRow_sourceType_idx" ON "GhnCodReconciliationRow"("sourceType");

-- CreateIndex
CREATE INDEX "GhnCodReconciliationRow_actionStatus_idx" ON "GhnCodReconciliationRow"("actionStatus");

-- CreateIndex
CREATE INDEX "GhnCodReconciliationRow_confirmedAt_idx" ON "GhnCodReconciliationRow"("confirmedAt");

-- CreateIndex
CREATE INDEX "GhnCodReconciliationRow_paidAt_idx" ON "GhnCodReconciliationRow"("paidAt");

-- CreateIndex
CREATE INDEX "GhnCodReconciliationRow_paymentSourceId_idx" ON "GhnCodReconciliationRow"("paymentSourceId");

-- CreateIndex
CREATE INDEX "StocktakeSession_branchId_idx" ON "StocktakeSession"("branchId");

-- CreateIndex
CREATE INDEX "StocktakeSession_status_idx" ON "StocktakeSession"("status");

-- CreateIndex
CREATE INDEX "StocktakeWorker_sessionId_idx" ON "StocktakeWorker"("sessionId");

-- CreateIndex
CREATE INDEX "StocktakeScanEvent_sessionId_idx" ON "StocktakeScanEvent"("sessionId");

-- CreateIndex
CREATE INDEX "StocktakeScanEvent_workerId_idx" ON "StocktakeScanEvent"("workerId");

-- CreateIndex
CREATE INDEX "StocktakeScanEvent_variantId_idx" ON "StocktakeScanEvent"("variantId");

-- CreateIndex
CREATE INDEX "StocktakeScanEvent_sku_idx" ON "StocktakeScanEvent"("sku");

-- CreateIndex
CREATE UNIQUE INDEX "WarehouseMap_code_key" ON "WarehouseMap"("code");

-- CreateIndex
CREATE INDEX "WarehouseMap_branchId_idx" ON "WarehouseMap"("branchId");

-- CreateIndex
CREATE INDEX "WarehouseMap_code_idx" ON "WarehouseMap"("code");

-- CreateIndex
CREATE UNIQUE INDEX "WarehouseRack_code_key" ON "WarehouseRack"("code");

-- CreateIndex
CREATE INDEX "WarehouseRack_mapId_idx" ON "WarehouseRack"("mapId");

-- CreateIndex
CREATE INDEX "WarehouseRack_branchId_idx" ON "WarehouseRack"("branchId");

-- CreateIndex
CREATE INDEX "WarehouseRack_zone_idx" ON "WarehouseRack"("zone");

-- CreateIndex
CREATE INDEX "WarehouseRack_status_idx" ON "WarehouseRack"("status");

-- CreateIndex
CREATE UNIQUE INDEX "WarehouseShelf_code_key" ON "WarehouseShelf"("code");

-- CreateIndex
CREATE INDEX "WarehouseShelf_rackId_idx" ON "WarehouseShelf"("rackId");

-- CreateIndex
CREATE INDEX "ProductVariantLocation_variantId_idx" ON "ProductVariantLocation"("variantId");

-- CreateIndex
CREATE INDEX "ProductVariantLocation_branchId_idx" ON "ProductVariantLocation"("branchId");

-- CreateIndex
CREATE INDEX "ProductVariantLocation_areaId_idx" ON "ProductVariantLocation"("areaId");

-- CreateIndex
CREATE INDEX "ProductVariantLocation_rackId_idx" ON "ProductVariantLocation"("rackId");

-- CreateIndex
CREATE INDEX "ProductVariantLocation_shelfId_idx" ON "ProductVariantLocation"("shelfId");

-- CreateIndex
CREATE INDEX "StocktakeArea_sessionId_idx" ON "StocktakeArea"("sessionId");

-- CreateIndex
CREATE INDEX "StocktakeArea_branchId_idx" ON "StocktakeArea"("branchId");

-- CreateIndex
CREATE INDEX "StocktakeArea_mapId_idx" ON "StocktakeArea"("mapId");

-- CreateIndex
CREATE INDEX "StocktakeArea_scopeType_idx" ON "StocktakeArea"("scopeType");

-- CreateIndex
CREATE INDEX "StocktakeArea_status_idx" ON "StocktakeArea"("status");

-- CreateIndex
CREATE INDEX "WarehouseFloor_mapId_idx" ON "WarehouseFloor"("mapId");

-- CreateIndex
CREATE INDEX "WarehouseZone_mapId_idx" ON "WarehouseZone"("mapId");

-- CreateIndex
CREATE INDEX "WarehouseZone_floorId_idx" ON "WarehouseZone"("floorId");

-- CreateIndex
CREATE INDEX "WarehouseZone_type_idx" ON "WarehouseZone"("type");

-- CreateIndex
CREATE INDEX "WarehouseDoor_mapId_idx" ON "WarehouseDoor"("mapId");

-- CreateIndex
CREATE INDEX "WarehouseDoor_floorId_idx" ON "WarehouseDoor"("floorId");

-- CreateIndex
CREATE INDEX "StocktakeSnapshot_sessionId_idx" ON "StocktakeSnapshot"("sessionId");

-- CreateIndex
CREATE INDEX "StocktakeSnapshot_branchId_idx" ON "StocktakeSnapshot"("branchId");

-- CreateIndex
CREATE INDEX "StocktakeSnapshot_variantId_idx" ON "StocktakeSnapshot"("variantId");

-- CreateIndex
CREATE UNIQUE INDEX "StocktakeSnapshot_sessionId_variantId_branchId_key" ON "StocktakeSnapshot"("sessionId", "variantId", "branchId");

-- CreateIndex
CREATE UNIQUE INDEX "StocktakeResult_sessionId_key" ON "StocktakeResult"("sessionId");

-- CreateIndex
CREATE INDEX "StocktakeResult_branchId_idx" ON "StocktakeResult"("branchId");

-- CreateIndex
CREATE INDEX "StocktakeResult_createdAt_idx" ON "StocktakeResult"("createdAt");

-- CreateIndex
CREATE INDEX "StocktakeResultItem_resultId_idx" ON "StocktakeResultItem"("resultId");

-- CreateIndex
CREATE INDEX "StocktakeResultItem_sessionId_idx" ON "StocktakeResultItem"("sessionId");

-- CreateIndex
CREATE INDEX "StocktakeResultItem_branchId_idx" ON "StocktakeResultItem"("branchId");

-- CreateIndex
CREATE INDEX "StocktakeResultItem_variantId_idx" ON "StocktakeResultItem"("variantId");

-- CreateIndex
CREATE INDEX "StocktakeResultItem_sku_idx" ON "StocktakeResultItem"("sku");

-- CreateIndex
CREATE INDEX "StocktakeResultItem_status_idx" ON "StocktakeResultItem"("status");

-- CreateIndex
CREATE UNIQUE INDEX "StocktakeResultItem_sessionId_branchId_sku_key" ON "StocktakeResultItem"("sessionId", "branchId", "sku");

-- CreateIndex
CREATE INDEX "StocktakeCount_sessionId_idx" ON "StocktakeCount"("sessionId");

-- CreateIndex
CREATE INDEX "StocktakeCount_workerId_idx" ON "StocktakeCount"("workerId");

-- CreateIndex
CREATE INDEX "StocktakeCount_branchId_idx" ON "StocktakeCount"("branchId");

-- CreateIndex
CREATE INDEX "StocktakeCount_variantId_idx" ON "StocktakeCount"("variantId");

-- CreateIndex
CREATE INDEX "StocktakeCount_areaId_idx" ON "StocktakeCount"("areaId");

-- CreateIndex
CREATE INDEX "StocktakeCount_rackId_idx" ON "StocktakeCount"("rackId");

-- CreateIndex
CREATE UNIQUE INDEX "StocktakeCount_sessionId_branchId_sku_workerId_key" ON "StocktakeCount"("sessionId", "branchId", "sku", "workerId");

-- CreateIndex
CREATE UNIQUE INDEX "ReturnExchange_code_key" ON "ReturnExchange"("code");

-- CreateIndex
CREATE INDEX "ReturnExchange_originalOrderId_idx" ON "ReturnExchange"("originalOrderId");

-- CreateIndex
CREATE INDEX "ReturnExchange_originalBranchId_idx" ON "ReturnExchange"("originalBranchId");

-- CreateIndex
CREATE INDEX "ReturnExchange_handledAtBranchId_idx" ON "ReturnExchange"("handledAtBranchId");

-- CreateIndex
CREATE INDEX "ReturnExchange_returnReceiveBranchId_idx" ON "ReturnExchange"("returnReceiveBranchId");

-- CreateIndex
CREATE INDEX "ReturnExchange_status_idx" ON "ReturnExchange"("status");

-- CreateIndex
CREATE INDEX "ReturnExchange_createdAt_idx" ON "ReturnExchange"("createdAt");

-- CreateIndex
CREATE INDEX "ReturnExchange_exchangeOrderId_idx" ON "ReturnExchange"("exchangeOrderId");

-- CreateIndex
CREATE INDEX "ReturnExchange_exchangeTrackingCode_idx" ON "ReturnExchange"("exchangeTrackingCode");

-- CreateIndex
CREATE INDEX "ReturnExchangeItem_returnExchangeId_idx" ON "ReturnExchangeItem"("returnExchangeId");

-- CreateIndex
CREATE INDEX "ReturnExchangeItem_itemType_idx" ON "ReturnExchangeItem"("itemType");

-- CreateIndex
CREATE INDEX "ReturnExchangeItem_variantId_idx" ON "ReturnExchangeItem"("variantId");

-- CreateIndex
CREATE UNIQUE INDEX "CashVoucher_code_key" ON "CashVoucher"("code");

-- CreateIndex
CREATE INDEX "CashVoucher_direction_idx" ON "CashVoucher"("direction");

-- CreateIndex
CREATE INDEX "CashVoucher_voucherType_idx" ON "CashVoucher"("voucherType");

-- CreateIndex
CREATE INDEX "CashVoucher_voucherCode_idx" ON "CashVoucher"("voucherCode");

-- CreateIndex
CREATE INDEX "CashVoucher_type_idx" ON "CashVoucher"("type");

-- CreateIndex
CREATE INDEX "CashVoucher_category_idx" ON "CashVoucher"("category");

-- CreateIndex
CREATE INDEX "CashVoucher_status_idx" ON "CashVoucher"("status");

-- CreateIndex
CREATE INDEX "CashVoucher_paymentSourceId_idx" ON "CashVoucher"("paymentSourceId");

-- CreateIndex
CREATE INDEX "CashVoucher_branchId_idx" ON "CashVoucher"("branchId");

-- CreateIndex
CREATE INDEX "CashVoucher_refType_refId_idx" ON "CashVoucher"("refType", "refId");

-- CreateIndex
CREATE INDEX "CashVoucher_createdAt_idx" ON "CashVoucher"("createdAt");

-- CreateIndex
CREATE INDEX "DailyCashBalance_date_idx" ON "DailyCashBalance"("date");

-- CreateIndex
CREATE INDEX "DailyCashBalance_branchId_idx" ON "DailyCashBalance"("branchId");

-- CreateIndex
CREATE INDEX "DailyCashBalance_paymentSourceId_idx" ON "DailyCashBalance"("paymentSourceId");

-- CreateIndex
CREATE INDEX "DailyCashBalance_status_idx" ON "DailyCashBalance"("status");

-- CreateIndex
CREATE UNIQUE INDEX "DailyCashBalance_date_branchId_paymentSourceId_key" ON "DailyCashBalance"("date", "branchId", "paymentSourceId");

-- CreateIndex
CREATE INDEX "Promotion_status_idx" ON "Promotion"("status");

-- CreateIndex
CREATE INDEX "Promotion_type_idx" ON "Promotion"("type");

-- CreateIndex
CREATE INDEX "Promotion_branchId_idx" ON "Promotion"("branchId");

-- CreateIndex
CREATE INDEX "Promotion_salesChannel_idx" ON "Promotion"("salesChannel");

-- CreateIndex
CREATE INDEX "PromotionProduct_productId_idx" ON "PromotionProduct"("productId");

-- CreateIndex
CREATE INDEX "PromotionProduct_variantId_idx" ON "PromotionProduct"("variantId");

-- CreateIndex
CREATE UNIQUE INDEX "PromotionProduct_promotionId_productId_key" ON "PromotionProduct"("promotionId", "productId");

-- CreateIndex
CREATE INDEX "PurchaseReceiptPayment_receiptId_idx" ON "PurchaseReceiptPayment"("receiptId");

-- CreateIndex
CREATE INDEX "PurchaseReceiptPayment_paymentSourceId_idx" ON "PurchaseReceiptPayment"("paymentSourceId");

-- CreateIndex
CREATE INDEX "PurchaseReceiptPayment_paidAt_idx" ON "PurchaseReceiptPayment"("paidAt");

-- CreateIndex
CREATE INDEX "StaffBranchRole_staffId_idx" ON "StaffBranchRole"("staffId");

-- CreateIndex
CREATE INDEX "StaffBranchRole_branchId_idx" ON "StaffBranchRole"("branchId");

-- CreateIndex
CREATE UNIQUE INDEX "StaffBranchRole_staffId_branchId_key" ON "StaffBranchRole"("staffId", "branchId");

-- CreateIndex
CREATE UNIQUE INDEX "Department_code_key" ON "Department"("code");

-- CreateIndex
CREATE INDEX "Department_isActive_idx" ON "Department"("isActive");

-- CreateIndex
CREATE INDEX "StaffDepartment_staffId_idx" ON "StaffDepartment"("staffId");

-- CreateIndex
CREATE INDEX "StaffDepartment_departmentId_idx" ON "StaffDepartment"("departmentId");

-- CreateIndex
CREATE UNIQUE INDEX "StaffDepartment_staffId_departmentId_key" ON "StaffDepartment"("staffId", "departmentId");

-- CreateIndex
CREATE INDEX "RbacSnapshot_createdAt_idx" ON "RbacSnapshot"("createdAt");

-- CreateIndex
CREATE UNIQUE INDEX "PayrollPeriod_code_key" ON "PayrollPeriod"("code");

-- CreateIndex
CREATE INDEX "PayrollPeriod_fromDate_idx" ON "PayrollPeriod"("fromDate");

-- CreateIndex
CREATE INDEX "PayrollPeriod_toDate_idx" ON "PayrollPeriod"("toDate");

-- CreateIndex
CREATE INDEX "PayrollPeriod_branchId_idx" ON "PayrollPeriod"("branchId");

-- CreateIndex
CREATE INDEX "PayrollPeriod_status_idx" ON "PayrollPeriod"("status");

-- CreateIndex
CREATE INDEX "PayrollLine_periodId_idx" ON "PayrollLine"("periodId");

-- CreateIndex
CREATE INDEX "PayrollLine_staffId_idx" ON "PayrollLine"("staffId");

-- CreateIndex
CREATE INDEX "PayrollLine_branchId_idx" ON "PayrollLine"("branchId");

-- CreateIndex
CREATE INDEX "PayrollLine_status_idx" ON "PayrollLine"("status");

-- CreateIndex
CREATE UNIQUE INDEX "PayrollLine_periodId_staffId_branchId_key" ON "PayrollLine"("periodId", "staffId", "branchId");

-- CreateIndex
CREATE INDEX "PayrollOrderLink_orderId_idx" ON "PayrollOrderLink"("orderId");

-- CreateIndex
CREATE INDEX "PayrollOrderLink_orderCode_idx" ON "PayrollOrderLink"("orderCode");

-- CreateIndex
CREATE INDEX "PayrollOrderLink_branchId_idx" ON "PayrollOrderLink"("branchId");

-- CreateIndex
CREATE INDEX "PayrollOrderLink_orderDate_idx" ON "PayrollOrderLink"("orderDate");

-- CreateIndex
CREATE UNIQUE INDEX "PayrollOrderLink_payrollLineId_orderId_key" ON "PayrollOrderLink"("payrollLineId", "orderId");

-- CreateIndex
CREATE INDEX "PayrollConfig_staffId_idx" ON "PayrollConfig"("staffId");

-- CreateIndex
CREATE INDEX "PayrollConfig_branchId_idx" ON "PayrollConfig"("branchId");

-- CreateIndex
CREATE INDEX "PayrollConfig_isActive_idx" ON "PayrollConfig"("isActive");

-- CreateIndex
CREATE INDEX "PayrollConfig_effectiveFrom_idx" ON "PayrollConfig"("effectiveFrom");

-- CreateIndex
CREATE INDEX "PayrollConfig_sourceTemplateId_idx" ON "PayrollConfig"("sourceTemplateId");

-- CreateIndex
CREATE INDEX "PayrollAttendanceImport_periodId_idx" ON "PayrollAttendanceImport"("periodId");

-- CreateIndex
CREATE INDEX "PayrollAttendanceImport_createdAt_idx" ON "PayrollAttendanceImport"("createdAt");

-- CreateIndex
CREATE INDEX "PayrollAdjustment_payrollLineId_idx" ON "PayrollAdjustment"("payrollLineId");

-- CreateIndex
CREATE INDEX "PayrollAdjustment_type_idx" ON "PayrollAdjustment"("type");

-- CreateIndex
CREATE INDEX "PayrollAdjustment_createdAt_idx" ON "PayrollAdjustment"("createdAt");

-- CreateIndex
CREATE INDEX "PayrollBranchConfigTemplate_branchId_idx" ON "PayrollBranchConfigTemplate"("branchId");

-- CreateIndex
CREATE INDEX "PayrollBranchConfigTemplate_isActive_idx" ON "PayrollBranchConfigTemplate"("isActive");

-- CreateIndex
CREATE UNIQUE INDEX "PayrollBranchConfigTemplate_branchId_name_key" ON "PayrollBranchConfigTemplate"("branchId", "name");

-- CreateIndex
CREATE UNIQUE INDEX "MetaAdAccount_metaAccountId_key" ON "MetaAdAccount"("metaAccountId");

-- CreateIndex
CREATE INDEX "MetaAdAccount_accountStatus_idx" ON "MetaAdAccount"("accountStatus");

-- CreateIndex
CREATE INDEX "MetaAdAccount_lastSyncedAt_idx" ON "MetaAdAccount"("lastSyncedAt");

-- CreateIndex
CREATE UNIQUE INDEX "MetaCampaign_metaCampaignId_key" ON "MetaCampaign"("metaCampaignId");

-- CreateIndex
CREATE INDEX "MetaCampaign_metaAccountId_idx" ON "MetaCampaign"("metaAccountId");

-- CreateIndex
CREATE INDEX "MetaCampaign_status_idx" ON "MetaCampaign"("status");

-- CreateIndex
CREATE INDEX "MetaCampaign_effectiveStatus_idx" ON "MetaCampaign"("effectiveStatus");

-- CreateIndex
CREATE INDEX "MetaCampaign_lastSyncedAt_idx" ON "MetaCampaign"("lastSyncedAt");

-- CreateIndex
CREATE UNIQUE INDEX "MetaAdSet_metaAdSetId_key" ON "MetaAdSet"("metaAdSetId");

-- CreateIndex
CREATE INDEX "MetaAdSet_metaAccountId_idx" ON "MetaAdSet"("metaAccountId");

-- CreateIndex
CREATE INDEX "MetaAdSet_metaCampaignId_idx" ON "MetaAdSet"("metaCampaignId");

-- CreateIndex
CREATE INDEX "MetaAdSet_status_idx" ON "MetaAdSet"("status");

-- CreateIndex
CREATE INDEX "MetaAdSet_effectiveStatus_idx" ON "MetaAdSet"("effectiveStatus");

-- CreateIndex
CREATE INDEX "MetaAdSet_lastSyncedAt_idx" ON "MetaAdSet"("lastSyncedAt");

-- CreateIndex
CREATE UNIQUE INDEX "MetaAd_metaAdId_key" ON "MetaAd"("metaAdId");

-- CreateIndex
CREATE INDEX "MetaAd_metaAccountId_idx" ON "MetaAd"("metaAccountId");

-- CreateIndex
CREATE INDEX "MetaAd_metaCampaignId_idx" ON "MetaAd"("metaCampaignId");

-- CreateIndex
CREATE INDEX "MetaAd_metaAdSetId_idx" ON "MetaAd"("metaAdSetId");

-- CreateIndex
CREATE INDEX "MetaAd_metaCreativeId_idx" ON "MetaAd"("metaCreativeId");

-- CreateIndex
CREATE INDEX "MetaAd_status_idx" ON "MetaAd"("status");

-- CreateIndex
CREATE INDEX "MetaAd_effectiveStatus_idx" ON "MetaAd"("effectiveStatus");

-- CreateIndex
CREATE INDEX "MetaAd_lastSyncedAt_idx" ON "MetaAd"("lastSyncedAt");

-- CreateIndex
CREATE INDEX "MetaAdInsightDaily_metaAccountId_level_dateStart_idx" ON "MetaAdInsightDaily"("metaAccountId", "level", "dateStart");

-- CreateIndex
CREATE INDEX "MetaAdInsightDaily_metaCampaignId_idx" ON "MetaAdInsightDaily"("metaCampaignId");

-- CreateIndex
CREATE INDEX "MetaAdInsightDaily_metaAdSetId_idx" ON "MetaAdInsightDaily"("metaAdSetId");

-- CreateIndex
CREATE INDEX "MetaAdInsightDaily_metaAdId_idx" ON "MetaAdInsightDaily"("metaAdId");

-- CreateIndex
CREATE UNIQUE INDEX "MetaAdInsightDaily_metaAccountId_level_dateStart_metaCampai_key" ON "MetaAdInsightDaily"("metaAccountId", "level", "dateStart", "metaCampaignId", "metaAdSetId", "metaAdId");

-- CreateIndex
CREATE INDEX "MetaSyncLog_metaAccountId_idx" ON "MetaSyncLog"("metaAccountId");

-- CreateIndex
CREATE INDEX "MetaSyncLog_syncType_idx" ON "MetaSyncLog"("syncType");

-- CreateIndex
CREATE INDEX "MetaSyncLog_status_idx" ON "MetaSyncLog"("status");

-- CreateIndex
CREATE INDEX "MetaSyncLog_startedAt_idx" ON "MetaSyncLog"("startedAt");

-- CreateIndex
CREATE UNIQUE INDEX "OmniInboxPage_providerPageId_key" ON "OmniInboxPage"("providerPageId");

-- CreateIndex
CREATE INDEX "OmniInboxPage_channel_isActive_idx" ON "OmniInboxPage"("channel", "isActive");

-- CreateIndex
CREATE UNIQUE INDEX "OmniCustomer_providerUserId_key" ON "OmniCustomer"("providerUserId");

-- CreateIndex
CREATE INDEX "OmniCustomer_phone_idx" ON "OmniCustomer"("phone");

-- CreateIndex
CREATE INDEX "OmniCustomer_name_idx" ON "OmniCustomer"("name");

-- CreateIndex
CREATE INDEX "OmniAssignmentSetting_isActive_mode_idx" ON "OmniAssignmentSetting"("isActive", "mode");

-- CreateIndex
CREATE INDEX "OmniAssignmentMember_settingId_isActive_sortOrder_idx" ON "OmniAssignmentMember"("settingId", "isActive", "sortOrder");

-- CreateIndex
CREATE INDEX "OmniAssignmentMember_staffId_idx" ON "OmniAssignmentMember"("staffId");

-- CreateIndex
CREATE INDEX "OmniAssignmentMember_branchId_idx" ON "OmniAssignmentMember"("branchId");

-- CreateIndex
CREATE UNIQUE INDEX "OmniAssignmentMember_settingId_staffId_key" ON "OmniAssignmentMember"("settingId", "staffId");

-- CreateIndex
CREATE INDEX "OmniStaffPresence_status_lastHeartbeatAt_idx" ON "OmniStaffPresence"("status", "lastHeartbeatAt");

-- CreateIndex
CREATE INDEX "OmniStaffPresence_activeBranchId_status_idx" ON "OmniStaffPresence"("activeBranchId", "status");

-- CreateIndex
CREATE INDEX "OmniAssignmentHistory_conversationId_createdAt_idx" ON "OmniAssignmentHistory"("conversationId", "createdAt");

-- CreateIndex
CREATE INDEX "OmniAssignmentHistory_assignedStaffId_createdAt_idx" ON "OmniAssignmentHistory"("assignedStaffId", "createdAt");

-- CreateIndex
CREATE INDEX "OmniAssignmentHistory_action_createdAt_idx" ON "OmniAssignmentHistory"("action", "createdAt");

-- CreateIndex
CREATE UNIQUE INDEX "OmniQuickReplyTemplate_normalizedText_key" ON "OmniQuickReplyTemplate"("normalizedText");

-- CreateIndex
CREATE INDEX "OmniQuickReplyTemplate_isActive_sortOrder_idx" ON "OmniQuickReplyTemplate"("isActive", "sortOrder");

-- CreateIndex
CREATE INDEX "OmniQuickReplyTemplate_category_isActive_idx" ON "OmniQuickReplyTemplate"("category", "isActive");

-- CreateIndex
CREATE UNIQUE INDEX "OmniConversation_providerThreadId_key" ON "OmniConversation"("providerThreadId");

-- CreateIndex
CREATE INDEX "OmniConversation_status_updatedAt_idx" ON "OmniConversation"("status", "updatedAt");

-- CreateIndex
CREATE INDEX "OmniConversation_assigneeId_status_idx" ON "OmniConversation"("assigneeId", "status");

-- CreateIndex
CREATE INDEX "OmniConversation_branchId_status_idx" ON "OmniConversation"("branchId", "status");

-- CreateIndex
CREATE INDEX "OmniConversation_lastMessageAt_idx" ON "OmniConversation"("lastMessageAt");

-- CreateIndex
CREATE INDEX "OmniConversation_adId_idx" ON "OmniConversation"("adId");

-- CreateIndex
CREATE INDEX "OmniConversation_adPostId_idx" ON "OmniConversation"("adPostId");

-- CreateIndex
CREATE UNIQUE INDEX "OmniMessage_providerMessageId_key" ON "OmniMessage"("providerMessageId");

-- CreateIndex
CREATE INDEX "OmniMessage_conversationId_sentAt_idx" ON "OmniMessage"("conversationId", "sentAt");

-- CreateIndex
CREATE INDEX "OmniMessage_direction_sentAt_idx" ON "OmniMessage"("direction", "sentAt");

-- CreateIndex
CREATE UNIQUE INDEX "OmniTagTemplate_normalizedName_key" ON "OmniTagTemplate"("normalizedName");

-- CreateIndex
CREATE INDEX "OmniTagTemplate_isActive_sortOrder_idx" ON "OmniTagTemplate"("isActive", "sortOrder");

-- CreateIndex
CREATE INDEX "OmniConversationTag_tag_idx" ON "OmniConversationTag"("tag");

-- CreateIndex
CREATE UNIQUE INDEX "OmniConversationTag_conversationId_tag_key" ON "OmniConversationTag"("conversationId", "tag");

-- CreateIndex
CREATE UNIQUE INDEX "OmniNoteTemplate_normalizedName_key" ON "OmniNoteTemplate"("normalizedName");

-- CreateIndex
CREATE INDEX "OmniNoteTemplate_isActive_sortOrder_idx" ON "OmniNoteTemplate"("isActive", "sortOrder");

-- CreateIndex
CREATE INDEX "OmniConversationNote_conversationId_createdAt_idx" ON "OmniConversationNote"("conversationId", "createdAt");

-- CreateIndex
CREATE INDEX "OmniConversationNote_templateId_idx" ON "OmniConversationNote"("templateId");

-- CreateIndex
CREATE UNIQUE INDEX "LocalDeliveryReconciliation_code_key" ON "LocalDeliveryReconciliation"("code");

-- CreateIndex
CREATE INDEX "LocalDeliveryReconciliation_branch_createdAt_idx" ON "LocalDeliveryReconciliation"("branchId", "createdAt");

-- CreateIndex
CREATE INDEX "LocalDeliveryReconciliation_order_status_idx" ON "LocalDeliveryReconciliation"("orderId", "status", "createdAt");

-- CreateIndex
CREATE INDEX "LocalDeliveryReconciliation_shipment_status_idx" ON "LocalDeliveryReconciliation"("shipmentId", "status", "createdAt");

-- CreateIndex
CREATE UNIQUE INDEX "MobilePushToken_token_key" ON "MobilePushToken"("token");

-- CreateIndex
CREATE INDEX "MobilePushToken_userId_idx" ON "MobilePushToken"("userId");

-- CreateIndex
CREATE INDEX "MobilePushToken_staffId_idx" ON "MobilePushToken"("staffId");

-- CreateIndex
CREATE INDEX "MobilePushToken_branchId_idx" ON "MobilePushToken"("branchId");

-- CreateIndex
CREATE INDEX "MobilePushToken_platform_idx" ON "MobilePushToken"("platform");

-- CreateIndex
CREATE INDEX "MobilePushToken_provider_idx" ON "MobilePushToken"("provider");

-- CreateIndex
CREATE INDEX "MobilePushToken_isActive_idx" ON "MobilePushToken"("isActive");

-- CreateIndex
CREATE INDEX "FabricBoard_supplierId_idx" ON "FabricBoard"("supplierId");

-- CreateIndex
CREATE INDEX "FabricBoard_boardCode_idx" ON "FabricBoard"("boardCode");

-- CreateIndex
CREATE INDEX "FabricBoard_fabricCode_idx" ON "FabricBoard"("fabricCode");

-- CreateIndex
CREATE INDEX "FabricBoard_isActive_idx" ON "FabricBoard"("isActive");

-- CreateIndex
CREATE INDEX "FabricBoard_updatedAt_idx" ON "FabricBoard"("updatedAt");

-- CreateIndex
CREATE UNIQUE INDEX "FabricBoard_supplierId_boardCode_fabricCode_key" ON "FabricBoard"("supplierId", "boardCode", "fabricCode");

-- CreateIndex
CREATE INDEX "FabricBoardColor_fabricBoardId_idx" ON "FabricBoardColor"("fabricBoardId");

-- CreateIndex
CREATE INDEX "FabricBoardColor_name_idx" ON "FabricBoardColor"("name");

-- CreateIndex
CREATE UNIQUE INDEX "FabricBoardColor_fabricBoardId_code_key" ON "FabricBoardColor"("fabricBoardId", "code");

-- CreateIndex
CREATE INDEX "FabricBoardImage_fabricBoardId_idx" ON "FabricBoardImage"("fabricBoardId");

-- CreateIndex
CREATE INDEX "FabricBoardImage_type_idx" ON "FabricBoardImage"("type");

-- CreateIndex
CREATE UNIQUE INDEX "FabricOrder_code_key" ON "FabricOrder"("code");

-- CreateIndex
CREATE INDEX "FabricOrder_supplierId_idx" ON "FabricOrder"("supplierId");

-- CreateIndex
CREATE INDEX "FabricOrder_status_idx" ON "FabricOrder"("status");

-- CreateIndex
CREATE INDEX "FabricOrder_orderedAt_idx" ON "FabricOrder"("orderedAt");

-- CreateIndex
CREATE INDEX "FabricOrder_updatedAt_idx" ON "FabricOrder"("updatedAt");

-- CreateIndex
CREATE INDEX "FabricOrderItem_fabricOrderId_idx" ON "FabricOrderItem"("fabricOrderId");

-- CreateIndex
CREATE INDEX "FabricOrderItem_fabricBoardId_idx" ON "FabricOrderItem"("fabricBoardId");

-- CreateIndex
CREATE INDEX "FabricOrderItem_fabricColorId_idx" ON "FabricOrderItem"("fabricColorId");

-- CreateIndex
CREATE INDEX "FabricOrderItem_designSampleId_idx" ON "FabricOrderItem"("designSampleId");

-- CreateIndex
CREATE INDEX "FabricSampleDispatch_designSampleId_idx" ON "FabricSampleDispatch"("designSampleId");

-- CreateIndex
CREATE INDEX "FabricSampleDispatch_fabricBoardId_idx" ON "FabricSampleDispatch"("fabricBoardId");

-- CreateIndex
CREATE INDEX "FabricSampleDispatch_fabricColorId_idx" ON "FabricSampleDispatch"("fabricColorId");

-- CreateIndex
CREATE INDEX "FabricSampleDispatch_status_idx" ON "FabricSampleDispatch"("status");

-- CreateIndex
CREATE INDEX "FabricSampleDispatch_sentAt_idx" ON "FabricSampleDispatch"("sentAt");

-- CreateIndex
CREATE INDEX "FabricSampleDispatch_recipientName_idx" ON "FabricSampleDispatch"("recipientName");

-- CreateIndex
CREATE INDEX "SampleTechnicalPerson_role_idx" ON "SampleTechnicalPerson"("role");

-- CreateIndex
CREATE INDEX "SampleTechnicalPerson_isActive_idx" ON "SampleTechnicalPerson"("isActive");

-- CreateIndex
CREATE INDEX "SampleTechnicalPerson_name_idx" ON "SampleTechnicalPerson"("name");

-- CreateIndex
CREATE UNIQUE INDEX "DesignSample_code_key" ON "DesignSample"("code");

-- CreateIndex
CREATE INDEX "DesignSample_year_idx" ON "DesignSample"("year");

-- CreateIndex
CREATE INDEX "DesignSample_status_idx" ON "DesignSample"("status");

-- CreateIndex
CREATE INDEX "DesignSample_supplierId_idx" ON "DesignSample"("supplierId");

-- CreateIndex
CREATE INDEX "DesignSample_fabricBoardId_idx" ON "DesignSample"("fabricBoardId");

-- CreateIndex
CREATE INDEX "DesignSample_fabricColorId_idx" ON "DesignSample"("fabricColorId");

-- CreateIndex
CREATE INDEX "DesignSample_producedProductId_idx" ON "DesignSample"("producedProductId");

-- CreateIndex
CREATE INDEX "DesignSample_sampleMakerId_idx" ON "DesignSample"("sampleMakerId");

-- CreateIndex
CREATE INDEX "DesignSample_patternMakerId_idx" ON "DesignSample"("patternMakerId");

-- CreateIndex
CREATE INDEX "DesignSample_assigneeStaffId_idx" ON "DesignSample"("assigneeStaffId");

-- CreateIndex
CREATE INDEX "DesignSample_updatedAt_idx" ON "DesignSample"("updatedAt");

-- CreateIndex
CREATE UNIQUE INDEX "DesignSample_priorityLane_priorityRank_key" ON "DesignSample"("priorityLane", "priorityRank");

-- CreateIndex
CREATE INDEX "DesignSampleIdeaBoard_name_idx" ON "DesignSampleIdeaBoard"("name");

-- CreateIndex
CREATE INDEX "DesignSampleIdeaBoard_updatedAt_idx" ON "DesignSampleIdeaBoard"("updatedAt");

-- CreateIndex
CREATE INDEX "DesignSampleIdeaBoardItem_boardId_sortOrder_idx" ON "DesignSampleIdeaBoardItem"("boardId", "sortOrder");

-- CreateIndex
CREATE INDEX "DesignSampleIdeaBoardItem_designSampleId_idx" ON "DesignSampleIdeaBoardItem"("designSampleId");

-- CreateIndex
CREATE UNIQUE INDEX "DesignSampleIdeaBoardItem_boardId_designSampleId_key" ON "DesignSampleIdeaBoardItem"("boardId", "designSampleId");

-- CreateIndex
CREATE INDEX "MeasurementTemplate_name_idx" ON "MeasurementTemplate"("name");

-- CreateIndex
CREATE INDEX "MeasurementTemplate_productKind_idx" ON "MeasurementTemplate"("productKind");

-- CreateIndex
CREATE INDEX "MeasurementTemplate_isActive_idx" ON "MeasurementTemplate"("isActive");

-- CreateIndex
CREATE INDEX "MeasurementTemplate_updatedAt_idx" ON "MeasurementTemplate"("updatedAt");

-- CreateIndex
CREATE INDEX "MeasurementTemplateSize_templateId_sortOrder_idx" ON "MeasurementTemplateSize"("templateId", "sortOrder");

-- CreateIndex
CREATE UNIQUE INDEX "MeasurementTemplateSize_templateId_size_key" ON "MeasurementTemplateSize"("templateId", "size");

-- CreateIndex
CREATE INDEX "MeasurementTemplateRow_templateId_sortOrder_idx" ON "MeasurementTemplateRow"("templateId", "sortOrder");

-- CreateIndex
CREATE INDEX "MeasurementTemplateRow_templateId_name_idx" ON "MeasurementTemplateRow"("templateId", "name");

-- CreateIndex
CREATE INDEX "MeasurementTemplateValue_rowId_idx" ON "MeasurementTemplateValue"("rowId");

-- CreateIndex
CREATE INDEX "MeasurementTemplateValue_sizeId_idx" ON "MeasurementTemplateValue"("sizeId");

-- CreateIndex
CREATE UNIQUE INDEX "MeasurementTemplateValue_rowId_sizeId_key" ON "MeasurementTemplateValue"("rowId", "sizeId");

-- CreateIndex
CREATE UNIQUE INDEX "DesignSampleMeasurementProfile_designSampleId_key" ON "DesignSampleMeasurementProfile"("designSampleId");

-- CreateIndex
CREATE INDEX "DesignSampleMeasurementProfile_sourceTemplateId_idx" ON "DesignSampleMeasurementProfile"("sourceTemplateId");

-- CreateIndex
CREATE INDEX "DesignSampleMeasurementProfile_productKind_idx" ON "DesignSampleMeasurementProfile"("productKind");

-- CreateIndex
CREATE INDEX "DesignSampleMeasurementProfile_updatedAt_idx" ON "DesignSampleMeasurementProfile"("updatedAt");

-- CreateIndex
CREATE INDEX "DesignSampleMeasurementSize_profileId_sortOrder_idx" ON "DesignSampleMeasurementSize"("profileId", "sortOrder");

-- CreateIndex
CREATE UNIQUE INDEX "DesignSampleMeasurementSize_profileId_size_key" ON "DesignSampleMeasurementSize"("profileId", "size");

-- CreateIndex
CREATE INDEX "DesignSampleMeasurementRow_profileId_sortOrder_idx" ON "DesignSampleMeasurementRow"("profileId", "sortOrder");

-- CreateIndex
CREATE INDEX "DesignSampleMeasurementRow_profileId_name_idx" ON "DesignSampleMeasurementRow"("profileId", "name");

-- CreateIndex
CREATE INDEX "DesignSampleMeasurementValue_rowId_idx" ON "DesignSampleMeasurementValue"("rowId");

-- CreateIndex
CREATE INDEX "DesignSampleMeasurementValue_sizeId_idx" ON "DesignSampleMeasurementValue"("sizeId");

-- CreateIndex
CREATE UNIQUE INDEX "DesignSampleMeasurementValue_rowId_sizeId_key" ON "DesignSampleMeasurementValue"("rowId", "sizeId");

-- CreateIndex
CREATE INDEX "DesignSampleColor_designSampleId_idx" ON "DesignSampleColor"("designSampleId");

-- CreateIndex
CREATE INDEX "DesignSampleColor_status_idx" ON "DesignSampleColor"("status");

-- CreateIndex
CREATE INDEX "DesignSampleImage_designSampleId_idx" ON "DesignSampleImage"("designSampleId");

-- CreateIndex
CREATE INDEX "DesignSampleProgressLog_designSampleId_createdAt_idx" ON "DesignSampleProgressLog"("designSampleId", "createdAt");

-- CreateIndex
CREATE INDEX "DesignSampleProgressLog_toStatus_idx" ON "DesignSampleProgressLog"("toStatus");

-- CreateIndex
CREATE UNIQUE INDEX "FabricReceipt_receiptCode_key" ON "FabricReceipt"("receiptCode");

-- CreateIndex
CREATE INDEX "FabricReceipt_designSampleId_idx" ON "FabricReceipt"("designSampleId");

-- CreateIndex
CREATE INDEX "FabricReceipt_productId_idx" ON "FabricReceipt"("productId");

-- CreateIndex
CREATE INDEX "FabricReceipt_fabricBoardId_idx" ON "FabricReceipt"("fabricBoardId");

-- CreateIndex
CREATE INDEX "FabricReceipt_fabricColorId_idx" ON "FabricReceipt"("fabricColorId");

-- CreateIndex
CREATE INDEX "FabricReceipt_supplierId_idx" ON "FabricReceipt"("supplierId");

-- CreateIndex
CREATE INDEX "FabricReceipt_branchId_idx" ON "FabricReceipt"("branchId");

-- CreateIndex
CREATE INDEX "FabricReceipt_status_idx" ON "FabricReceipt"("status");

-- CreateIndex
CREATE INDEX "FabricReceipt_receivedAt_idx" ON "FabricReceipt"("receivedAt");

-- CreateIndex
CREATE INDEX "FabricReceipt_receivedByStaffId_idx" ON "FabricReceipt"("receivedByStaffId");

-- CreateIndex
CREATE INDEX "FabricReceipt_updatedAt_idx" ON "FabricReceipt"("updatedAt");

-- CreateIndex
CREATE INDEX "FabricReceiptRoll_fabricReceiptId_idx" ON "FabricReceiptRoll"("fabricReceiptId");

-- CreateIndex
CREATE INDEX "FabricReceiptRoll_fabricReceiptId_sortOrder_idx" ON "FabricReceiptRoll"("fabricReceiptId", "sortOrder");

-- CreateIndex
CREATE INDEX "FabricReceiptRoll_fabricCode_idx" ON "FabricReceiptRoll"("fabricCode");

-- CreateIndex
CREATE INDEX "FabricReceiptFabricConfig_fabricReceiptId_idx" ON "FabricReceiptFabricConfig"("fabricReceiptId");

-- CreateIndex
CREATE INDEX "FabricReceiptFabricConfig_fabricCode_idx" ON "FabricReceiptFabricConfig"("fabricCode");

-- CreateIndex
CREATE INDEX "FabricReceiptFabricConfig_productId_idx" ON "FabricReceiptFabricConfig"("productId");

-- CreateIndex
CREATE INDEX "FabricReceiptFabricConfig_designSampleId_idx" ON "FabricReceiptFabricConfig"("designSampleId");

-- CreateIndex
CREATE UNIQUE INDEX "FabricReceiptFabricConfig_fabricReceiptId_fabricCode_key" ON "FabricReceiptFabricConfig"("fabricReceiptId", "fabricCode");

-- CreateIndex
CREATE INDEX "FabricReceiptFabricCost_fabricReceiptId_idx" ON "FabricReceiptFabricCost"("fabricReceiptId");

-- CreateIndex
CREATE INDEX "FabricReceiptFabricCost_fabricCode_idx" ON "FabricReceiptFabricCost"("fabricCode");

-- CreateIndex
CREATE UNIQUE INDEX "FabricReceiptFabricCost_fabricReceiptId_fabricCode_key" ON "FabricReceiptFabricCost"("fabricReceiptId", "fabricCode");

-- CreateIndex
CREATE INDEX "FabricReceiptColorMap_fabricReceiptId_idx" ON "FabricReceiptColorMap"("fabricReceiptId");

-- CreateIndex
CREATE INDEX "FabricReceiptColorMap_fabricCode_idx" ON "FabricReceiptColorMap"("fabricCode");

-- CreateIndex
CREATE INDEX "FabricReceiptColorMap_colorCode_idx" ON "FabricReceiptColorMap"("colorCode");

-- CreateIndex
CREATE UNIQUE INDEX "FabricReceiptColorMap_fabricReceiptId_fabricCode_colorName__key" ON "FabricReceiptColorMap"("fabricReceiptId", "fabricCode", "colorName", "colorCode");

-- CreateIndex
CREATE INDEX "FabricMeasurement_fabricReceiptId_createdAt_idx" ON "FabricMeasurement"("fabricReceiptId", "createdAt");

-- CreateIndex
CREATE INDEX "FabricMeasurement_rollId_idx" ON "FabricMeasurement"("rollId");

-- CreateIndex
CREATE INDEX "FabricReceiptImage_fabricReceiptId_idx" ON "FabricReceiptImage"("fabricReceiptId");

-- CreateIndex
CREATE INDEX "FabricReceiptImage_rollId_idx" ON "FabricReceiptImage"("rollId");

-- CreateIndex
CREATE INDEX "FabricReceiptImage_type_idx" ON "FabricReceiptImage"("type");

-- CreateIndex
CREATE UNIQUE INDEX "ProductionPartner_code_key" ON "ProductionPartner"("code");

-- CreateIndex
CREATE INDEX "ProductionPartner_name_idx" ON "ProductionPartner"("name");

-- CreateIndex
CREATE INDEX "ProductionPartner_isActive_idx" ON "ProductionPartner"("isActive");

-- CreateIndex
CREATE UNIQUE INDEX "ProductionAccessorySupplier_code_key" ON "ProductionAccessorySupplier"("code");

-- CreateIndex
CREATE INDEX "ProductionAccessorySupplier_name_idx" ON "ProductionAccessorySupplier"("name");

-- CreateIndex
CREATE INDEX "ProductionAccessorySupplier_isActive_idx" ON "ProductionAccessorySupplier"("isActive");

-- CreateIndex
CREATE UNIQUE INDEX "ProductionAccessoryItem_code_key" ON "ProductionAccessoryItem"("code");

-- CreateIndex
CREATE INDEX "ProductionAccessoryItem_typeName_idx" ON "ProductionAccessoryItem"("typeName");

-- CreateIndex
CREATE INDEX "ProductionAccessoryItem_supplierId_idx" ON "ProductionAccessoryItem"("supplierId");

-- CreateIndex
CREATE INDEX "ProductionAccessoryItem_isActive_idx" ON "ProductionAccessoryItem"("isActive");

-- CreateIndex
CREATE UNIQUE INDEX "ProductionAccessoryReceipt_code_key" ON "ProductionAccessoryReceipt"("code");

-- CreateIndex
CREATE INDEX "ProductionAccessoryReceipt_supplierId_idx" ON "ProductionAccessoryReceipt"("supplierId");

-- CreateIndex
CREATE INDEX "ProductionAccessoryReceipt_receivedAt_idx" ON "ProductionAccessoryReceipt"("receivedAt");

-- CreateIndex
CREATE INDEX "ProductionAccessoryReceipt_createdAt_idx" ON "ProductionAccessoryReceipt"("createdAt");

-- CreateIndex
CREATE INDEX "ProductionAccessoryReceiptItem_receiptId_sortOrder_idx" ON "ProductionAccessoryReceiptItem"("receiptId", "sortOrder");

-- CreateIndex
CREATE INDEX "ProductionAccessoryReceiptItem_accessoryItemId_idx" ON "ProductionAccessoryReceiptItem"("accessoryItemId");

-- CreateIndex
CREATE UNIQUE INDEX "ProductionAccessoryTemplate_name_key" ON "ProductionAccessoryTemplate"("name");

-- CreateIndex
CREATE INDEX "ProductionAccessoryTemplate_productKind_idx" ON "ProductionAccessoryTemplate"("productKind");

-- CreateIndex
CREATE INDEX "ProductionAccessoryTemplate_isActive_idx" ON "ProductionAccessoryTemplate"("isActive");

-- CreateIndex
CREATE INDEX "ProductionAccessoryTemplate_updatedAt_idx" ON "ProductionAccessoryTemplate"("updatedAt");

-- CreateIndex
CREATE INDEX "ProductionAccessoryTemplateItem_templateId_sortOrder_idx" ON "ProductionAccessoryTemplateItem"("templateId", "sortOrder");

-- CreateIndex
CREATE INDEX "ProductionAccessoryTemplateItem_accessoryItemId_idx" ON "ProductionAccessoryTemplateItem"("accessoryItemId");

-- CreateIndex
CREATE UNIQUE INDEX "SampleProductionSpec_designSampleId_key" ON "SampleProductionSpec"("designSampleId");

-- CreateIndex
CREATE INDEX "SampleProductionSpec_designSampleId_idx" ON "SampleProductionSpec"("designSampleId");

-- CreateIndex
CREATE INDEX "SampleAccessorySpec_designSampleId_idx" ON "SampleAccessorySpec"("designSampleId");

-- CreateIndex
CREATE INDEX "SampleAccessorySpec_accessoryItemId_idx" ON "SampleAccessorySpec"("accessoryItemId");

-- CreateIndex
CREATE UNIQUE INDEX "SampleAccessorySpec_designSampleId_accessoryItemId_key" ON "SampleAccessorySpec"("designSampleId", "accessoryItemId");

-- CreateIndex
CREATE UNIQUE INDEX "ProductionOrder_code_key" ON "ProductionOrder"("code");

-- CreateIndex
CREATE INDEX "ProductionOrder_sourceType_idx" ON "ProductionOrder"("sourceType");

-- CreateIndex
CREATE INDEX "ProductionOrder_designSampleId_idx" ON "ProductionOrder"("designSampleId");

-- CreateIndex
CREATE INDEX "ProductionOrder_productId_idx" ON "ProductionOrder"("productId");

-- CreateIndex
CREATE INDEX "ProductionOrder_sourceCode_idx" ON "ProductionOrder"("sourceCode");

-- CreateIndex
CREATE INDEX "ProductionOrder_productionPartnerId_idx" ON "ProductionOrder"("productionPartnerId");

-- CreateIndex
CREATE INDEX "ProductionOrder_status_idx" ON "ProductionOrder"("status");

-- CreateIndex
CREATE INDEX "ProductionOrder_dueDate_idx" ON "ProductionOrder"("dueDate");

-- CreateIndex
CREATE INDEX "ProductionOrder_updatedAt_idx" ON "ProductionOrder"("updatedAt");

-- CreateIndex
CREATE INDEX "ProductionOrderRoll_productionOrderId_idx" ON "ProductionOrderRoll"("productionOrderId");

-- CreateIndex
CREATE INDEX "ProductionOrderRoll_productionOrderId_fabricRole_idx" ON "ProductionOrderRoll"("productionOrderId", "fabricRole");

-- CreateIndex
CREATE INDEX "ProductionOrderRoll_fabricReceiptRollId_idx" ON "ProductionOrderRoll"("fabricReceiptRollId");

-- CreateIndex
CREATE INDEX "ProductionOrderRoll_colorName_idx" ON "ProductionOrderRoll"("colorName");

-- CreateIndex
CREATE UNIQUE INDEX "ProductionOrderRoll_productionOrderId_fabricReceiptRollId_key" ON "ProductionOrderRoll"("productionOrderId", "fabricReceiptRollId");

-- CreateIndex
CREATE INDEX "ProductionSizePlan_productionOrderId_idx" ON "ProductionSizePlan"("productionOrderId");

-- CreateIndex
CREATE INDEX "ProductionSizePlan_colorName_idx" ON "ProductionSizePlan"("colorName");

-- CreateIndex
CREATE INDEX "ProductionSizePlan_size_idx" ON "ProductionSizePlan"("size");

-- CreateIndex
CREATE UNIQUE INDEX "ProductionSizePlan_productionOrderId_colorName_size_key" ON "ProductionSizePlan"("productionOrderId", "colorName", "size");

-- CreateIndex
CREATE INDEX "ProductionCutQtyHistory_productionOrderId_idx" ON "ProductionCutQtyHistory"("productionOrderId");

-- CreateIndex
CREATE INDEX "ProductionCutQtyHistory_productionOrderId_createdAt_idx" ON "ProductionCutQtyHistory"("productionOrderId", "createdAt");

-- CreateIndex
CREATE INDEX "ProductionCutQtyHistory_colorName_idx" ON "ProductionCutQtyHistory"("colorName");

-- CreateIndex
CREATE INDEX "ProductionCutQtyHistory_size_idx" ON "ProductionCutQtyHistory"("size");

-- CreateIndex
CREATE INDEX "ProductionOrderAccessorySpec_productionOrderId_idx" ON "ProductionOrderAccessorySpec"("productionOrderId");

-- CreateIndex
CREATE INDEX "ProductionOrderAccessorySpec_accessoryItemId_idx" ON "ProductionOrderAccessorySpec"("accessoryItemId");

-- CreateIndex
CREATE UNIQUE INDEX "ProductionOrderAccessorySpec_productionOrderId_accessoryIte_key" ON "ProductionOrderAccessorySpec"("productionOrderId", "accessoryItemId");

-- CreateIndex
CREATE INDEX "ProductionMaterialCalc_productionOrderId_idx" ON "ProductionMaterialCalc"("productionOrderId");

-- CreateIndex
CREATE INDEX "ProductionMaterialCalc_accessoryItemId_idx" ON "ProductionMaterialCalc"("accessoryItemId");

-- CreateIndex
CREATE INDEX "ProductionMaterialCalc_sizeLabel_idx" ON "ProductionMaterialCalc"("sizeLabel");

-- CreateIndex
CREATE INDEX "ProductionNplIssue_productionOrderId_idx" ON "ProductionNplIssue"("productionOrderId");

-- CreateIndex
CREATE INDEX "ProductionNplIssue_productionOrderId_createdAt_idx" ON "ProductionNplIssue"("productionOrderId", "createdAt");

-- CreateIndex
CREATE UNIQUE INDEX "ProductionNplIssue_productionOrderId_roundNo_key" ON "ProductionNplIssue"("productionOrderId", "roundNo");

-- CreateIndex
CREATE INDEX "ProductionNplIssueItem_issueId_idx" ON "ProductionNplIssueItem"("issueId");

-- CreateIndex
CREATE INDEX "ProductionNplIssueItem_productionOrderId_idx" ON "ProductionNplIssueItem"("productionOrderId");

-- CreateIndex
CREATE INDEX "ProductionNplIssueItem_productionOrderId_accessoryItemId_si_idx" ON "ProductionNplIssueItem"("productionOrderId", "accessoryItemId", "sizeKey");

-- CreateIndex
CREATE INDEX "ProductionNplIssueItem_accessoryItemId_idx" ON "ProductionNplIssueItem"("accessoryItemId");

-- CreateIndex
CREATE INDEX "ProductionNplIssueNote_productionOrderId_idx" ON "ProductionNplIssueNote"("productionOrderId");

-- CreateIndex
CREATE INDEX "ProductionNplIssueNote_accessoryItemId_idx" ON "ProductionNplIssueNote"("accessoryItemId");

-- CreateIndex
CREATE UNIQUE INDEX "ProductionNplIssueNote_productionOrderId_accessoryItemId_si_key" ON "ProductionNplIssueNote"("productionOrderId", "accessoryItemId", "sizeKey");

-- CreateIndex
CREATE UNIQUE INDEX "CustomerAccount_customerId_key" ON "CustomerAccount"("customerId");

-- CreateIndex
CREATE UNIQUE INDEX "CustomerAccount_phone_key" ON "CustomerAccount"("phone");

-- CreateIndex
CREATE UNIQUE INDEX "CustomerAccount_email_key" ON "CustomerAccount"("email");

-- CreateIndex
CREATE INDEX "CustomerAccount_customerId_idx" ON "CustomerAccount"("customerId");

-- CreateIndex
CREATE INDEX "CustomerAccount_phone_idx" ON "CustomerAccount"("phone");

-- CreateIndex
CREATE INDEX "CustomerAccount_email_idx" ON "CustomerAccount"("email");

-- CreateIndex
CREATE INDEX "CustomerSession_accountId_idx" ON "CustomerSession"("accountId");

-- CreateIndex
CREATE INDEX "CustomerSession_expiresAt_idx" ON "CustomerSession"("expiresAt");

-- CreateIndex
CREATE INDEX "CustomerSession_revokedAt_idx" ON "CustomerSession"("revokedAt");

-- CreateIndex
CREATE INDEX "CustomerOtp_phone_purpose_idx" ON "CustomerOtp"("phone", "purpose");

-- CreateIndex
CREATE INDEX "CustomerOtp_expiresAt_idx" ON "CustomerOtp"("expiresAt");

-- CreateIndex
CREATE UNIQUE INDEX "WebsiteProduct_productId_key" ON "WebsiteProduct"("productId");

-- CreateIndex
CREATE UNIQUE INDEX "WebsiteProduct_slug_key" ON "WebsiteProduct"("slug");

-- CreateIndex
CREATE INDEX "WebsiteProduct_status_idx" ON "WebsiteProduct"("status");

-- CreateIndex
CREATE INDEX "WebsiteProduct_marketVn_status_idx" ON "WebsiteProduct"("marketVn", "status");

-- CreateIndex
CREATE INDEX "WebsiteProduct_marketInternational_status_idx" ON "WebsiteProduct"("marketInternational", "status");

-- CreateIndex
CREATE INDEX "WebsiteProduct_featured_sortOrder_idx" ON "WebsiteProduct"("featured", "sortOrder");

-- CreateIndex
CREATE INDEX "WebsiteProductImage_websiteProductId_sortOrder_idx" ON "WebsiteProductImage"("websiteProductId", "sortOrder");

-- CreateIndex
CREATE INDEX "AuditLog_userId_idx" ON "AuditLog"("userId");

-- CreateIndex
CREATE INDEX "AuditLog_entity_idx" ON "AuditLog"("entity");

-- CreateIndex
CREATE INDEX "AuditLog_entityId_idx" ON "AuditLog"("entityId");

-- CreateIndex
CREATE INDEX "Customer_legacyCode_idx" ON "Customer"("legacyCode");

-- CreateIndex
CREATE INDEX "Customer_customerGroup_idx" ON "Customer"("customerGroup");

-- CreateIndex
CREATE INDEX "CustomerAddress_customerId_idx" ON "CustomerAddress"("customerId");

-- CreateIndex
CREATE INDEX "CustomerAddress_province_idx" ON "CustomerAddress"("province");

-- CreateIndex
CREATE INDEX "CustomerAddress_ward_idx" ON "CustomerAddress"("ward");

-- CreateIndex
CREATE INDEX "CustomerAddress_ghnDistrictId_idx" ON "CustomerAddress"("ghnDistrictId");

-- CreateIndex
CREATE INDEX "CustomerAddress_ghnWardCode_idx" ON "CustomerAddress"("ghnWardCode");

-- CreateIndex
CREATE INDEX "InventoryItem_branchId_idx" ON "InventoryItem"("branchId");

-- CreateIndex
CREATE INDEX "InventoryItem_variantId_idx" ON "InventoryItem"("variantId");

-- CreateIndex
CREATE UNIQUE INDEX "InventoryItem_variantId_branchId_key" ON "InventoryItem"("variantId", "branchId");

-- CreateIndex
CREATE INDEX "InventoryMovement_variantId_idx" ON "InventoryMovement"("variantId");

-- CreateIndex
CREATE INDEX "InventoryMovement_branchId_idx" ON "InventoryMovement"("branchId");

-- CreateIndex
CREATE INDEX "InventoryMovement_type_idx" ON "InventoryMovement"("type");

-- CreateIndex
CREATE INDEX "InventoryMovement_refType_refId_idx" ON "InventoryMovement"("refType", "refId");

-- CreateIndex
CREATE INDEX "InventoryMovement_createdAt_idx" ON "InventoryMovement"("createdAt");

-- CreateIndex
CREATE UNIQUE INDEX "Order_quickOrderRequestId_key" ON "Order"("quickOrderRequestId");

-- CreateIndex
CREATE INDEX "Order_omniConversationId_idx" ON "Order"("omniConversationId");

-- CreateIndex
CREATE INDEX "Order_customerPhone_idx" ON "Order"("customerPhone");

-- CreateIndex
CREATE INDEX "Order_branchId_idx" ON "Order"("branchId");

-- CreateIndex
CREATE INDEX "Order_customerId_idx" ON "Order"("customerId");

-- CreateIndex
CREATE INDEX "Order_customerAddressId_idx" ON "Order"("customerAddressId");

-- CreateIndex
CREATE INDEX "Order_shippingProvince_idx" ON "Order"("shippingProvince");

-- CreateIndex
CREATE INDEX "Order_shippingWard_idx" ON "Order"("shippingWard");

-- CreateIndex
CREATE INDEX "Order_shippingGhnDistrictId_idx" ON "Order"("shippingGhnDistrictId");

-- CreateIndex
CREATE INDEX "Order_shippingGhnWardCode_idx" ON "Order"("shippingGhnWardCode");

-- CreateIndex
CREATE INDEX "Order_status_idx" ON "Order"("status");

-- CreateIndex
CREATE INDEX "Order_createdByStaffId_idx" ON "Order"("createdByStaffId");

-- CreateIndex
CREATE INDEX "Order_soldAt_idx" ON "Order"("soldAt");

-- CreateIndex
CREATE INDEX "OrderItem_orderId_idx" ON "OrderItem"("orderId");

-- CreateIndex
CREATE INDEX "OrderItem_variantId_idx" ON "OrderItem"("variantId");

-- CreateIndex
CREATE INDEX "Payment_orderId_idx" ON "Payment"("orderId");

-- CreateIndex
CREATE INDEX "Payment_status_idx" ON "Payment"("status");

-- CreateIndex
CREATE INDEX "Product_categoryId_idx" ON "Product"("categoryId");

-- CreateIndex
CREATE UNIQUE INDEX "Shipment_orderId_key" ON "Shipment"("orderId");

-- CreateIndex
CREATE INDEX "Shipment_trackingCode_idx" ON "Shipment"("trackingCode");

-- CreateIndex
CREATE INDEX "Shipment_shippingStatus_idx" ON "Shipment"("shippingStatus");

-- CreateIndex
CREATE INDEX "Shipment_partnerStatus_idx" ON "Shipment"("partnerStatus");

-- CreateIndex
CREATE INDEX "Shipment_lastSyncedAt_idx" ON "Shipment"("lastSyncedAt");

-- CreateIndex
CREATE INDEX "Shipment_ahamoveOrderId_idx" ON "Shipment"("ahamoveOrderId");

-- CreateIndex
CREATE INDEX "Shipment_ahamoveStatus_idx" ON "Shipment"("ahamoveStatus");

-- AddForeignKey
ALTER TABLE "Product" ADD CONSTRAINT "Product_categoryId_fkey" FOREIGN KEY ("categoryId") REFERENCES "Category"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "InventoryMovement" ADD CONSTRAINT "InventoryMovement_branchId_fkey" FOREIGN KEY ("branchId") REFERENCES "Branch"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "Order" ADD CONSTRAINT "Order_customerAddressId_fkey" FOREIGN KEY ("customerAddressId") REFERENCES "CustomerAddress"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "Order" ADD CONSTRAINT "Order_omniConversationId_fkey" FOREIGN KEY ("omniConversationId") REFERENCES "OmniConversation"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "Payment" ADD CONSTRAINT "Payment_paymentSourceId_fkey" FOREIGN KEY ("paymentSourceId") REFERENCES "PaymentSource"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ShipmentTimelineEvent" ADD CONSTRAINT "ShipmentTimelineEvent_shipmentId_fkey" FOREIGN KEY ("shipmentId") REFERENCES "Shipment"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "AhamoveShipment" ADD CONSTRAINT "AhamoveShipment_shipmentId_fkey" FOREIGN KEY ("shipmentId") REFERENCES "Shipment"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "StaffUser" ADD CONSTRAINT "StaffUser_branchId_fkey" FOREIGN KEY ("branchId") REFERENCES "Branch"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "StaffSession" ADD CONSTRAINT "StaffSession_staffId_fkey" FOREIGN KEY ("staffId") REFERENCES "StaffUser"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "StaffUserRole" ADD CONSTRAINT "StaffUserRole_staffId_fkey" FOREIGN KEY ("staffId") REFERENCES "StaffUser"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "StaffBranchPermission" ADD CONSTRAINT "StaffBranchPermission_staffId_fkey" FOREIGN KEY ("staffId") REFERENCES "StaffUser"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "StaffBranchPermission" ADD CONSTRAINT "StaffBranchPermission_branchId_fkey" FOREIGN KEY ("branchId") REFERENCES "Branch"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ImportJob" ADD CONSTRAINT "ImportJob_defaultBranchId_fkey" FOREIGN KEY ("defaultBranchId") REFERENCES "Branch"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ImportErrorLog" ADD CONSTRAINT "ImportErrorLog_jobId_fkey" FOREIGN KEY ("jobId") REFERENCES "ImportJob"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "AdministrativeWard" ADD CONSTRAINT "AdministrativeWard_provinceCode_fkey" FOREIGN KEY ("provinceCode") REFERENCES "AdministrativeProvince"("code") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PurchaseReceipt" ADD CONSTRAINT "PurchaseReceipt_adminUserId_fkey" FOREIGN KEY ("adminUserId") REFERENCES "AdminUser"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PurchaseReceipt" ADD CONSTRAINT "PurchaseReceipt_branchId_fkey" FOREIGN KEY ("branchId") REFERENCES "Branch"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PurchaseReceipt" ADD CONSTRAINT "PurchaseReceipt_createdById_fkey" FOREIGN KEY ("createdById") REFERENCES "StaffUser"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PurchaseReceipt" ADD CONSTRAINT "PurchaseReceipt_supplierId_fkey" FOREIGN KEY ("supplierId") REFERENCES "Supplier"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PurchaseReceiptItem" ADD CONSTRAINT "PurchaseReceiptItem_productId_fkey" FOREIGN KEY ("productId") REFERENCES "Product"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PurchaseReceiptItem" ADD CONSTRAINT "PurchaseReceiptItem_receiptId_fkey" FOREIGN KEY ("receiptId") REFERENCES "PurchaseReceipt"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PurchaseReceiptItem" ADD CONSTRAINT "PurchaseReceiptItem_variantId_fkey" FOREIGN KEY ("variantId") REFERENCES "ProductVariant"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "StockTransfer" ADD CONSTRAINT "StockTransfer_fromBranchId_fkey" FOREIGN KEY ("fromBranchId") REFERENCES "Branch"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "StockTransfer" ADD CONSTRAINT "StockTransfer_toBranchId_fkey" FOREIGN KEY ("toBranchId") REFERENCES "Branch"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "StockTransferItem" ADD CONSTRAINT "StockTransferItem_transferId_fkey" FOREIGN KEY ("transferId") REFERENCES "StockTransfer"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "StockTransferItem" ADD CONSTRAINT "StockTransferItem_variantId_fkey" FOREIGN KEY ("variantId") REFERENCES "ProductVariant"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "BranchNotification" ADD CONSTRAINT "BranchNotification_transferId_fkey" FOREIGN KEY ("transferId") REFERENCES "StockTransfer"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ShipmentTrackingCache" ADD CONSTRAINT "ShipmentTrackingCache_shipmentId_fkey" FOREIGN KEY ("shipmentId") REFERENCES "Shipment"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PartialDeliveryRecord" ADD CONSTRAINT "PartialDeliveryRecord_orderId_fkey" FOREIGN KEY ("orderId") REFERENCES "Order"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PartialDeliveryRecord" ADD CONSTRAINT "PartialDeliveryRecord_returnOrderId_fkey" FOREIGN KEY ("returnOrderId") REFERENCES "Order"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PartialDeliveryItem" ADD CONSTRAINT "PartialDeliveryItem_recordId_fkey" FOREIGN KEY ("recordId") REFERENCES "PartialDeliveryRecord"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "GhnCodReconciliationRow" ADD CONSTRAINT "GhnCodReconciliationRow_batchId_fkey" FOREIGN KEY ("batchId") REFERENCES "GhnCodReconciliationBatch"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "StocktakeWorker" ADD CONSTRAINT "StocktakeWorker_sessionId_fkey" FOREIGN KEY ("sessionId") REFERENCES "StocktakeSession"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "StocktakeScanEvent" ADD CONSTRAINT "StocktakeScanEvent_sessionId_fkey" FOREIGN KEY ("sessionId") REFERENCES "StocktakeSession"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "StocktakeScanEvent" ADD CONSTRAINT "StocktakeScanEvent_workerId_fkey" FOREIGN KEY ("workerId") REFERENCES "StocktakeWorker"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "WarehouseRack" ADD CONSTRAINT "WarehouseRack_mapId_fkey" FOREIGN KEY ("mapId") REFERENCES "WarehouseMap"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "WarehouseShelf" ADD CONSTRAINT "WarehouseShelf_rackId_fkey" FOREIGN KEY ("rackId") REFERENCES "WarehouseRack"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ProductVariantLocation" ADD CONSTRAINT "ProductVariantLocation_variantId_fkey" FOREIGN KEY ("variantId") REFERENCES "ProductVariant"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ProductVariantLocation" ADD CONSTRAINT "ProductVariantLocation_branchId_fkey" FOREIGN KEY ("branchId") REFERENCES "Branch"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ProductVariantLocation" ADD CONSTRAINT "ProductVariantLocation_areaId_fkey" FOREIGN KEY ("areaId") REFERENCES "StocktakeArea"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ProductVariantLocation" ADD CONSTRAINT "ProductVariantLocation_rackId_fkey" FOREIGN KEY ("rackId") REFERENCES "WarehouseRack"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ProductVariantLocation" ADD CONSTRAINT "ProductVariantLocation_shelfId_fkey" FOREIGN KEY ("shelfId") REFERENCES "WarehouseShelf"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "StocktakeArea" ADD CONSTRAINT "StocktakeArea_sessionId_fkey" FOREIGN KEY ("sessionId") REFERENCES "StocktakeSession"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "WarehouseFloor" ADD CONSTRAINT "WarehouseFloor_mapId_fkey" FOREIGN KEY ("mapId") REFERENCES "WarehouseMap"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "WarehouseZone" ADD CONSTRAINT "WarehouseZone_mapId_fkey" FOREIGN KEY ("mapId") REFERENCES "WarehouseMap"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "WarehouseZone" ADD CONSTRAINT "WarehouseZone_floorId_fkey" FOREIGN KEY ("floorId") REFERENCES "WarehouseFloor"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "WarehouseDoor" ADD CONSTRAINT "WarehouseDoor_mapId_fkey" FOREIGN KEY ("mapId") REFERENCES "WarehouseMap"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "WarehouseDoor" ADD CONSTRAINT "WarehouseDoor_floorId_fkey" FOREIGN KEY ("floorId") REFERENCES "WarehouseFloor"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "StocktakeResult" ADD CONSTRAINT "StocktakeResult_sessionId_fkey" FOREIGN KEY ("sessionId") REFERENCES "StocktakeSession"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "StocktakeResultItem" ADD CONSTRAINT "StocktakeResultItem_resultId_fkey" FOREIGN KEY ("resultId") REFERENCES "StocktakeResult"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ReturnExchangeItem" ADD CONSTRAINT "ReturnExchangeItem_returnExchangeId_fkey" FOREIGN KEY ("returnExchangeId") REFERENCES "ReturnExchange"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "CashVoucher" ADD CONSTRAINT "CashVoucher_refId_fkey" FOREIGN KEY ("refId") REFERENCES "ReturnExchange"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "Promotion" ADD CONSTRAINT "Promotion_branchId_fkey" FOREIGN KEY ("branchId") REFERENCES "Branch"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PromotionProduct" ADD CONSTRAINT "PromotionProduct_promotionId_fkey" FOREIGN KEY ("promotionId") REFERENCES "Promotion"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PromotionProduct" ADD CONSTRAINT "PromotionProduct_productId_fkey" FOREIGN KEY ("productId") REFERENCES "Product"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PromotionProduct" ADD CONSTRAINT "PromotionProduct_variantId_fkey" FOREIGN KEY ("variantId") REFERENCES "ProductVariant"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PurchaseReceiptPayment" ADD CONSTRAINT "PurchaseReceiptPayment_receiptId_fkey" FOREIGN KEY ("receiptId") REFERENCES "PurchaseReceipt"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PurchaseReceiptPayment" ADD CONSTRAINT "PurchaseReceiptPayment_paymentSourceId_fkey" FOREIGN KEY ("paymentSourceId") REFERENCES "PaymentSource"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "StaffBranchRole" ADD CONSTRAINT "StaffBranchRole_staffId_fkey" FOREIGN KEY ("staffId") REFERENCES "StaffUser"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "StaffBranchRole" ADD CONSTRAINT "StaffBranchRole_branchId_fkey" FOREIGN KEY ("branchId") REFERENCES "Branch"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "StaffDepartment" ADD CONSTRAINT "StaffDepartment_staffId_fkey" FOREIGN KEY ("staffId") REFERENCES "StaffUser"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "StaffDepartment" ADD CONSTRAINT "StaffDepartment_departmentId_fkey" FOREIGN KEY ("departmentId") REFERENCES "Department"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PayrollLine" ADD CONSTRAINT "PayrollLine_periodId_fkey" FOREIGN KEY ("periodId") REFERENCES "PayrollPeriod"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PayrollOrderLink" ADD CONSTRAINT "PayrollOrderLink_payrollLineId_fkey" FOREIGN KEY ("payrollLineId") REFERENCES "PayrollLine"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PayrollAttendanceImport" ADD CONSTRAINT "PayrollAttendanceImport_periodId_fkey" FOREIGN KEY ("periodId") REFERENCES "PayrollPeriod"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PayrollAdjustment" ADD CONSTRAINT "PayrollAdjustment_payrollLineId_fkey" FOREIGN KEY ("payrollLineId") REFERENCES "PayrollLine"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "MetaCampaign" ADD CONSTRAINT "MetaCampaign_metaAccountId_fkey" FOREIGN KEY ("metaAccountId") REFERENCES "MetaAdAccount"("metaAccountId") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "MetaAdSet" ADD CONSTRAINT "MetaAdSet_metaAccountId_fkey" FOREIGN KEY ("metaAccountId") REFERENCES "MetaAdAccount"("metaAccountId") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "MetaAdSet" ADD CONSTRAINT "MetaAdSet_metaCampaignId_fkey" FOREIGN KEY ("metaCampaignId") REFERENCES "MetaCampaign"("metaCampaignId") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "MetaAd" ADD CONSTRAINT "MetaAd_metaAccountId_fkey" FOREIGN KEY ("metaAccountId") REFERENCES "MetaAdAccount"("metaAccountId") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "MetaAd" ADD CONSTRAINT "MetaAd_metaCampaignId_fkey" FOREIGN KEY ("metaCampaignId") REFERENCES "MetaCampaign"("metaCampaignId") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "MetaAd" ADD CONSTRAINT "MetaAd_metaAdSetId_fkey" FOREIGN KEY ("metaAdSetId") REFERENCES "MetaAdSet"("metaAdSetId") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "MetaAdInsightDaily" ADD CONSTRAINT "MetaAdInsightDaily_metaAccountId_fkey" FOREIGN KEY ("metaAccountId") REFERENCES "MetaAdAccount"("metaAccountId") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "MetaAdInsightDaily" ADD CONSTRAINT "MetaAdInsightDaily_metaCampaignId_fkey" FOREIGN KEY ("metaCampaignId") REFERENCES "MetaCampaign"("metaCampaignId") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "MetaAdInsightDaily" ADD CONSTRAINT "MetaAdInsightDaily_metaAdSetId_fkey" FOREIGN KEY ("metaAdSetId") REFERENCES "MetaAdSet"("metaAdSetId") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "MetaAdInsightDaily" ADD CONSTRAINT "MetaAdInsightDaily_metaAdId_fkey" FOREIGN KEY ("metaAdId") REFERENCES "MetaAd"("metaAdId") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "OmniAssignmentMember" ADD CONSTRAINT "OmniAssignmentMember_settingId_fkey" FOREIGN KEY ("settingId") REFERENCES "OmniAssignmentSetting"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "OmniConversation" ADD CONSTRAINT "OmniConversation_pageId_fkey" FOREIGN KEY ("pageId") REFERENCES "OmniInboxPage"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "OmniConversation" ADD CONSTRAINT "OmniConversation_customerId_fkey" FOREIGN KEY ("customerId") REFERENCES "OmniCustomer"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "OmniMessage" ADD CONSTRAINT "OmniMessage_conversationId_fkey" FOREIGN KEY ("conversationId") REFERENCES "OmniConversation"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "OmniConversationTag" ADD CONSTRAINT "OmniConversationTag_conversationId_fkey" FOREIGN KEY ("conversationId") REFERENCES "OmniConversation"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "OmniConversationNote" ADD CONSTRAINT "OmniConversationNote_conversationId_fkey" FOREIGN KEY ("conversationId") REFERENCES "OmniConversation"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "OmniConversationNote" ADD CONSTRAINT "OmniConversationNote_templateId_fkey" FOREIGN KEY ("templateId") REFERENCES "OmniNoteTemplate"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricBoard" ADD CONSTRAINT "FabricBoard_supplierId_fkey" FOREIGN KEY ("supplierId") REFERENCES "FabricSupplier"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricBoardColor" ADD CONSTRAINT "FabricBoardColor_fabricBoardId_fkey" FOREIGN KEY ("fabricBoardId") REFERENCES "FabricBoard"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricBoardImage" ADD CONSTRAINT "FabricBoardImage_fabricBoardId_fkey" FOREIGN KEY ("fabricBoardId") REFERENCES "FabricBoard"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricOrder" ADD CONSTRAINT "FabricOrder_supplierId_fkey" FOREIGN KEY ("supplierId") REFERENCES "FabricSupplier"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricOrderItem" ADD CONSTRAINT "FabricOrderItem_fabricOrderId_fkey" FOREIGN KEY ("fabricOrderId") REFERENCES "FabricOrder"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricOrderItem" ADD CONSTRAINT "FabricOrderItem_fabricBoardId_fkey" FOREIGN KEY ("fabricBoardId") REFERENCES "FabricBoard"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricOrderItem" ADD CONSTRAINT "FabricOrderItem_fabricColorId_fkey" FOREIGN KEY ("fabricColorId") REFERENCES "FabricBoardColor"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricOrderItem" ADD CONSTRAINT "FabricOrderItem_designSampleId_fkey" FOREIGN KEY ("designSampleId") REFERENCES "DesignSample"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricSampleDispatch" ADD CONSTRAINT "FabricSampleDispatch_designSampleId_fkey" FOREIGN KEY ("designSampleId") REFERENCES "DesignSample"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricSampleDispatch" ADD CONSTRAINT "FabricSampleDispatch_fabricBoardId_fkey" FOREIGN KEY ("fabricBoardId") REFERENCES "FabricBoard"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricSampleDispatch" ADD CONSTRAINT "FabricSampleDispatch_fabricColorId_fkey" FOREIGN KEY ("fabricColorId") REFERENCES "FabricBoardColor"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DesignSample" ADD CONSTRAINT "DesignSample_supplierId_fkey" FOREIGN KEY ("supplierId") REFERENCES "FabricSupplier"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DesignSample" ADD CONSTRAINT "DesignSample_fabricBoardId_fkey" FOREIGN KEY ("fabricBoardId") REFERENCES "FabricBoard"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DesignSample" ADD CONSTRAINT "DesignSample_fabricColorId_fkey" FOREIGN KEY ("fabricColorId") REFERENCES "FabricBoardColor"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DesignSample" ADD CONSTRAINT "DesignSample_producedProductId_fkey" FOREIGN KEY ("producedProductId") REFERENCES "Product"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DesignSample" ADD CONSTRAINT "DesignSample_sampleMakerId_fkey" FOREIGN KEY ("sampleMakerId") REFERENCES "SampleTechnicalPerson"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DesignSample" ADD CONSTRAINT "DesignSample_patternMakerId_fkey" FOREIGN KEY ("patternMakerId") REFERENCES "SampleTechnicalPerson"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DesignSampleIdeaBoardItem" ADD CONSTRAINT "DesignSampleIdeaBoardItem_boardId_fkey" FOREIGN KEY ("boardId") REFERENCES "DesignSampleIdeaBoard"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DesignSampleIdeaBoardItem" ADD CONSTRAINT "DesignSampleIdeaBoardItem_designSampleId_fkey" FOREIGN KEY ("designSampleId") REFERENCES "DesignSample"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "MeasurementTemplateSize" ADD CONSTRAINT "MeasurementTemplateSize_templateId_fkey" FOREIGN KEY ("templateId") REFERENCES "MeasurementTemplate"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "MeasurementTemplateRow" ADD CONSTRAINT "MeasurementTemplateRow_templateId_fkey" FOREIGN KEY ("templateId") REFERENCES "MeasurementTemplate"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "MeasurementTemplateValue" ADD CONSTRAINT "MeasurementTemplateValue_rowId_fkey" FOREIGN KEY ("rowId") REFERENCES "MeasurementTemplateRow"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "MeasurementTemplateValue" ADD CONSTRAINT "MeasurementTemplateValue_sizeId_fkey" FOREIGN KEY ("sizeId") REFERENCES "MeasurementTemplateSize"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DesignSampleMeasurementProfile" ADD CONSTRAINT "DesignSampleMeasurementProfile_designSampleId_fkey" FOREIGN KEY ("designSampleId") REFERENCES "DesignSample"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DesignSampleMeasurementProfile" ADD CONSTRAINT "DesignSampleMeasurementProfile_sourceTemplateId_fkey" FOREIGN KEY ("sourceTemplateId") REFERENCES "MeasurementTemplate"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DesignSampleMeasurementSize" ADD CONSTRAINT "DesignSampleMeasurementSize_profileId_fkey" FOREIGN KEY ("profileId") REFERENCES "DesignSampleMeasurementProfile"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DesignSampleMeasurementRow" ADD CONSTRAINT "DesignSampleMeasurementRow_profileId_fkey" FOREIGN KEY ("profileId") REFERENCES "DesignSampleMeasurementProfile"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DesignSampleMeasurementValue" ADD CONSTRAINT "DesignSampleMeasurementValue_rowId_fkey" FOREIGN KEY ("rowId") REFERENCES "DesignSampleMeasurementRow"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DesignSampleMeasurementValue" ADD CONSTRAINT "DesignSampleMeasurementValue_sizeId_fkey" FOREIGN KEY ("sizeId") REFERENCES "DesignSampleMeasurementSize"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DesignSampleColor" ADD CONSTRAINT "DesignSampleColor_designSampleId_fkey" FOREIGN KEY ("designSampleId") REFERENCES "DesignSample"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DesignSampleImage" ADD CONSTRAINT "DesignSampleImage_designSampleId_fkey" FOREIGN KEY ("designSampleId") REFERENCES "DesignSample"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DesignSampleProgressLog" ADD CONSTRAINT "DesignSampleProgressLog_designSampleId_fkey" FOREIGN KEY ("designSampleId") REFERENCES "DesignSample"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricReceipt" ADD CONSTRAINT "FabricReceipt_designSampleId_fkey" FOREIGN KEY ("designSampleId") REFERENCES "DesignSample"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricReceipt" ADD CONSTRAINT "FabricReceipt_productId_fkey" FOREIGN KEY ("productId") REFERENCES "Product"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricReceipt" ADD CONSTRAINT "FabricReceipt_fabricBoardId_fkey" FOREIGN KEY ("fabricBoardId") REFERENCES "FabricBoard"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricReceipt" ADD CONSTRAINT "FabricReceipt_fabricColorId_fkey" FOREIGN KEY ("fabricColorId") REFERENCES "FabricBoardColor"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricReceipt" ADD CONSTRAINT "FabricReceipt_supplierId_fkey" FOREIGN KEY ("supplierId") REFERENCES "FabricSupplier"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricReceipt" ADD CONSTRAINT "FabricReceipt_branchId_fkey" FOREIGN KEY ("branchId") REFERENCES "Branch"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricReceiptRoll" ADD CONSTRAINT "FabricReceiptRoll_fabricReceiptId_fkey" FOREIGN KEY ("fabricReceiptId") REFERENCES "FabricReceipt"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricReceiptFabricConfig" ADD CONSTRAINT "FabricReceiptFabricConfig_fabricReceiptId_fkey" FOREIGN KEY ("fabricReceiptId") REFERENCES "FabricReceipt"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricReceiptFabricConfig" ADD CONSTRAINT "FabricReceiptFabricConfig_supplierId_fkey" FOREIGN KEY ("supplierId") REFERENCES "FabricSupplier"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricReceiptFabricConfig" ADD CONSTRAINT "FabricReceiptFabricConfig_productId_fkey" FOREIGN KEY ("productId") REFERENCES "Product"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricReceiptFabricConfig" ADD CONSTRAINT "FabricReceiptFabricConfig_designSampleId_fkey" FOREIGN KEY ("designSampleId") REFERENCES "DesignSample"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricReceiptFabricCost" ADD CONSTRAINT "FabricReceiptFabricCost_fabricReceiptId_fkey" FOREIGN KEY ("fabricReceiptId") REFERENCES "FabricReceipt"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricReceiptColorMap" ADD CONSTRAINT "FabricReceiptColorMap_fabricReceiptId_fkey" FOREIGN KEY ("fabricReceiptId") REFERENCES "FabricReceipt"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricMeasurement" ADD CONSTRAINT "FabricMeasurement_fabricReceiptId_fkey" FOREIGN KEY ("fabricReceiptId") REFERENCES "FabricReceipt"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricMeasurement" ADD CONSTRAINT "FabricMeasurement_rollId_fkey" FOREIGN KEY ("rollId") REFERENCES "FabricReceiptRoll"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricReceiptImage" ADD CONSTRAINT "FabricReceiptImage_fabricReceiptId_fkey" FOREIGN KEY ("fabricReceiptId") REFERENCES "FabricReceipt"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "FabricReceiptImage" ADD CONSTRAINT "FabricReceiptImage_rollId_fkey" FOREIGN KEY ("rollId") REFERENCES "FabricReceiptRoll"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ProductionAccessoryReceiptItem" ADD CONSTRAINT "ProductionAccessoryReceiptItem_receiptId_fkey" FOREIGN KEY ("receiptId") REFERENCES "ProductionAccessoryReceipt"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ProductionAccessoryTemplateItem" ADD CONSTRAINT "ProductionAccessoryTemplateItem_templateId_fkey" FOREIGN KEY ("templateId") REFERENCES "ProductionAccessoryTemplate"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "CustomerAccount" ADD CONSTRAINT "CustomerAccount_customerId_fkey" FOREIGN KEY ("customerId") REFERENCES "Customer"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "CustomerSession" ADD CONSTRAINT "CustomerSession_accountId_fkey" FOREIGN KEY ("accountId") REFERENCES "CustomerAccount"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "CustomerOtp" ADD CONSTRAINT "CustomerOtp_accountId_fkey" FOREIGN KEY ("accountId") REFERENCES "CustomerAccount"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "WebsiteProduct" ADD CONSTRAINT "WebsiteProduct_productId_fkey" FOREIGN KEY ("productId") REFERENCES "Product"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "WebsiteProductImage" ADD CONSTRAINT "WebsiteProductImage_websiteProductId_fkey" FOREIGN KEY ("websiteProductId") REFERENCES "WebsiteProduct"("id") ON DELETE CASCADE ON UPDATE CASCADE;

