-- CreateTable
CREATE TABLE "WebsiteHomepage" (
    "id" TEXT NOT NULL,
    "market" TEXT NOT NULL,
    "draftConfig" JSONB NOT NULL,
    "publishedConfig" JSONB,
    "publishedAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "WebsiteHomepage_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "WebsiteHomepage_market_key" ON "WebsiteHomepage"("market");

