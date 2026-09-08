CREATE INDEX "CashVoucher_branchId_createdAt_idx"
ON "CashVoucher"("branchId", "createdAt");

CREATE INDEX "CashVoucher_paymentSourceId_createdAt_idx"
ON "CashVoucher"("paymentSourceId", "createdAt");

CREATE INDEX "CashVoucher_type_status_createdAt_idx"
ON "CashVoucher"("type", "status", "createdAt");

CREATE INDEX "Order_branchId_createdAt_desc_idx"
ON "Order"("branchId", "createdAt" DESC);

CREATE INDEX "Order_createdAt_desc_idx"
ON "Order"("createdAt" DESC);

CREATE INDEX "Order_createdByStaffName_trgm_idx"
ON "Order" USING GIN ("createdByStaffName" gin_trgm_ops);

CREATE INDEX "Order_customerName_trgm_idx"
ON "Order" USING GIN ("customerName" gin_trgm_ops);

CREATE INDEX "Order_orderCode_idx"
ON "Order"("orderCode");

CREATE INDEX "Order_paymentStatus_createdAt_desc_idx"
ON "Order"("paymentStatus", "createdAt" DESC);

CREATE INDEX "Order_shippingAddressLine1_trgm_idx"
ON "Order" USING GIN ("shippingAddressLine1" gin_trgm_ops);

CREATE INDEX "Order_shippingPhone_idx"
ON "Order"("shippingPhone");

CREATE INDEX "Order_status_createdAt_desc_idx"
ON "Order"("status", "createdAt" DESC);

CREATE INDEX "OrderItem_productName_trgm_idx"
ON "OrderItem" USING GIN ("productName" gin_trgm_ops);

CREATE INDEX "OrderItem_sku_idx"
ON "OrderItem"("sku");

CREATE INDEX "OrderItem_sku_trgm_idx"
ON "OrderItem" USING GIN ("sku" gin_trgm_ops);

CREATE INDEX "Shipment_carrier_idx"
ON "Shipment"("carrier");

CREATE INDEX "Shipment_trackingCode_trgm_idx"
ON "Shipment" USING GIN ("trackingCode" gin_trgm_ops);
