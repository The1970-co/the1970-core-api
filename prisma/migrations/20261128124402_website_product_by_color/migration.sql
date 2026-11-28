/*
  Warnings:

  - A unique constraint covering the columns `[productId,colorKey]` on the table `WebsiteProduct` will be added. If there are existing duplicate values, this will fail.

*/
-- DropIndex
DROP INDEX "WebsiteProduct_productId_key";

-- AlterTable
ALTER TABLE "WebsiteProduct" ADD COLUMN     "colorKey" TEXT NOT NULL DEFAULT '';

-- CreateIndex
CREATE UNIQUE INDEX "WebsiteProduct_productId_colorKey_key" ON "WebsiteProduct"("productId", "colorKey");
