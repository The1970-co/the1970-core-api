-- AlterTable
ALTER TABLE "WebsiteProduct" ADD COLUMN     "websiteCategorySlugs" TEXT[] DEFAULT ARRAY[]::TEXT[];

-- CreateTable
CREATE TABLE "WebsiteSettings" (
    "id" TEXT NOT NULL,
    "market" TEXT NOT NULL,
    "cartEnabled" BOOLEAN NOT NULL DEFAULT false,
    "checkoutEnabled" BOOLEAN NOT NULL DEFAULT false,
    "newCustomerDiscountEnabled" BOOLEAN NOT NULL DEFAULT false,
    "newCustomerDiscountPercent" DECIMAL(5,2) NOT NULL DEFAULT 0,
    "messengerEnabled" BOOLEAN NOT NULL DEFAULT false,
    "messengerUrl" TEXT,
    "zaloEnabled" BOOLEAN NOT NULL DEFAULT false,
    "zaloUrl" TEXT,
    "defaultBranchId" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "WebsiteSettings_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "WebsiteSettings_market_key" ON "WebsiteSettings"("market");

-- CreateIndex
CREATE INDEX "WebsiteSettings_market_idx" ON "WebsiteSettings"("market");
