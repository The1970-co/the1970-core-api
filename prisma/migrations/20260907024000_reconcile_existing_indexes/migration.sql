-- CreateIndex
CREATE INDEX "CashVoucher_branchId_createdAt_idx" ON "public"."CashVoucher"("branchId" ASC, "createdAt" ASC);

-- CreateIndex
CREATE INDEX "CashVoucher_paymentSourceId_createdAt_idx" ON "public"."CashVoucher"("paymentSourceId" ASC, "createdAt" ASC);

-- CreateIndex
CREATE INDEX "CashVoucher_type_status_createdAt_idx" ON "public"."CashVoucher"("type" ASC, "status" ASC, "createdAt" ASC);

-- CreateIndex
CREATE INDEX "Order_branchId_createdAt_desc_idx" ON "public"."Order"("branchId" ASC, "createdAt" DESC);

-- CreateIndex
CREATE INDEX "Order_createdAt_desc_idx" ON "public"."Order"("createdAt" DESC);

-- CreateIndex
CREATE INDEX "Order_createdByStaffName_trgm_idx" ON "public"."Order" USING GIN ("createdByStaffName" gin_trgm_ops);

-- CreateIndex
CREATE INDEX "Order_customerName_trgm_idx" ON "public"."Order" USING GIN ("customerName" gin_trgm_ops);

-- CreateIndex
CREATE INDEX "Order_orderCode_idx" ON "public"."Order"("orderCode" ASC);

-- CreateIndex
CREATE INDEX "Order_paymentStatus_createdAt_desc_idx" ON "public"."Order"("paymentStatus" ASC, "createdAt" DESC);

-- CreateIndex
CREATE INDEX "Order_shippingAddressLine1_trgm_idx" ON "public"."Order" USING GIN ("shippingAddressLine1" gin_trgm_ops);

-- CreateIndex
CREATE INDEX "Order_shippingPhone_idx" ON "public"."Order"("shippingPhone" ASC);

-- CreateIndex
CREATE INDEX "Order_status_createdAt_desc_idx" ON "public"."Order"("status" ASC, "createdAt" DESC);

-- CreateIndex
CREATE INDEX "OrderItem_productName_trgm_idx" ON "public"."OrderItem" USING GIN ("productName" gin_trgm_ops);

-- CreateIndex
CREATE INDEX "OrderItem_sku_idx" ON "public"."OrderItem"("sku" ASC);

-- CreateIndex
CREATE INDEX "OrderItem_sku_trgm_idx" ON "public"."OrderItem" USING GIN ("sku" gin_trgm_ops);

-- CreateIndex
CREATE INDEX "Shipment_carrier_idx" ON "public"."Shipment"("carrier" ASC);

-- CreateIndex
CREATE INDEX "Shipment_trackingCode_trgm_idx" ON "public"."Shipment" USING GIN ("trackingCode" gin_trgm_ops);

