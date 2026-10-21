-- CreateTable
CREATE TABLE "PosPaymentRule" (
    "id" TEXT NOT NULL,
    "branchId" TEXT NOT NULL,
    "kind" TEXT NOT NULL,
    "operator" TEXT NOT NULL DEFAULT 'gte',
    "threshold" INTEGER NOT NULL DEFAULT 0,
    "paymentSourceId" TEXT NOT NULL,
    "qrAccountId" TEXT NOT NULL DEFAULT '',
    "priority" INTEGER NOT NULL DEFAULT 0,
    "enabled" BOOLEAN NOT NULL DEFAULT true,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "PosPaymentRule_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "PosPaymentRule_branchId_enabled_priority_idx" ON "PosPaymentRule"("branchId", "enabled", "priority");

