-- AlterTable
ALTER TABLE "DesignSampleIdeaBoardItem" ADD COLUMN     "isActive" BOOLEAN NOT NULL DEFAULT true;

-- AlterTable
ALTER TABLE "DesignSampleMaterialBoardItem" ADD COLUMN     "isActive" BOOLEAN NOT NULL DEFAULT true;

-- CreateTable
CREATE TABLE "DesignSampleFabricColor" (
    "id" TEXT NOT NULL,
    "fabricSampleId" TEXT NOT NULL,
    "colorName" TEXT NOT NULL,
    "colorCode" TEXT,
    "receivedMeters" DECIMAL(12,3) NOT NULL DEFAULT 0,
    "sortOrder" INTEGER NOT NULL DEFAULT 0,
    "isActive" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "DesignSampleFabricColor_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "DesignSampleFabricAllocation" (
    "id" TEXT NOT NULL,
    "designSampleId" TEXT NOT NULL,
    "fabricSampleColorId" TEXT NOT NULL,
    "meters" DECIMAL(12,3) NOT NULL,
    "assignedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "releasedAt" TIMESTAMP(3),
    "createdById" TEXT,
    "createdByName" TEXT,
    "note" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "DesignSampleFabricAllocation_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "DesignSampleFabricColor_fabricSampleId_sortOrder_idx" ON "DesignSampleFabricColor"("fabricSampleId", "sortOrder");

-- CreateIndex
CREATE INDEX "DesignSampleFabricColor_colorCode_idx" ON "DesignSampleFabricColor"("colorCode");

-- CreateIndex
CREATE INDEX "DesignSampleFabricAllocation_designSampleId_releasedAt_idx" ON "DesignSampleFabricAllocation"("designSampleId", "releasedAt");

-- CreateIndex
CREATE INDEX "DesignSampleFabricAllocation_fabricSampleColorId_releasedAt_idx" ON "DesignSampleFabricAllocation"("fabricSampleColorId", "releasedAt");

-- CreateIndex
CREATE INDEX "DesignSampleFabricAllocation_assignedAt_idx" ON "DesignSampleFabricAllocation"("assignedAt");

-- AddForeignKey
ALTER TABLE "DesignSampleFabricColor" ADD CONSTRAINT "DesignSampleFabricColor_fabricSampleId_fkey" FOREIGN KEY ("fabricSampleId") REFERENCES "DesignSample"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DesignSampleFabricAllocation" ADD CONSTRAINT "DesignSampleFabricAllocation_designSampleId_fkey" FOREIGN KEY ("designSampleId") REFERENCES "DesignSample"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "DesignSampleFabricAllocation" ADD CONSTRAINT "DesignSampleFabricAllocation_fabricSampleColorId_fkey" FOREIGN KEY ("fabricSampleColorId") REFERENCES "DesignSampleFabricColor"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
