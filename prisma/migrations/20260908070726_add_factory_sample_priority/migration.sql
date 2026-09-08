/*
  Warnings:

  - A unique constraint covering the columns `[sampleFactoryId,factoryPriorityRank]` on the table `DesignSample` will be added. If there are existing duplicate values, this will fail.

*/
-- AlterTable
ALTER TABLE "DesignSample" ADD COLUMN     "factoryPriorityRank" INTEGER;

-- CreateIndex
CREATE INDEX "DesignSample_sampleFactoryId_idx" ON "DesignSample"("sampleFactoryId");

-- CreateIndex
CREATE UNIQUE INDEX "DesignSample_sampleFactoryId_factoryPriorityRank_key" ON "DesignSample"("sampleFactoryId", "factoryPriorityRank");
