CREATE TABLE "PosBankAccount" (
  "id" TEXT NOT NULL,
  "slot" INTEGER NOT NULL,
  "label" TEXT NOT NULL,
  "bankCode" TEXT NOT NULL,
  "bankName" TEXT,
  "accountNumber" TEXT NOT NULL,
  "accountName" TEXT NOT NULL,
  "isActive" BOOLEAN NOT NULL DEFAULT true,
  "sortOrder" INTEGER NOT NULL DEFAULT 0,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "PosBankAccount_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "PosBankAccount_slot_key"
ON "PosBankAccount"("slot");
