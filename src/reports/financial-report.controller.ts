import { Controller, Get, Query } from "@nestjs/common";
import { FinancialReportService } from "./financial-report.service";

@Controller("reports/financial")
export class FinancialReportController {
  constructor(private readonly service: FinancialReportService) {}

  @Get()
  getReport(
    @Query("fromDate") fromDate?: string,
    @Query("toDate") toDate?: string,
    @Query("dateField") dateField?: "createdAt" | "soldAt",
    @Query("branchIds") branchIds?: string,
    @Query("createdByStaffIds") createdByStaffIds?: string,
    @Query("assignedStaffIds") assignedStaffIds?: string,
    @Query("orderStatuses") orderStatuses?: string,
    @Query("paymentStatuses") paymentStatuses?: string,
    @Query("fulfillmentStatuses") fulfillmentStatuses?: string,
    @Query("deliveryStatuses") deliveryStatuses?: string,
    @Query("salesChannels") salesChannels?: string,
    @Query("shippingModes") shippingModes?: string,
    @Query("carriers") carriers?: string,
    @Query("paymentSourceIds") paymentSourceIds?: string,
    @Query("trackingFilter") trackingFilter?: string,
    @Query("codFilter") codFilter?: string,
    @Query("codReconciliationStatuses") codReconciliationStatuses?: string,
    @Query("amountDueFilter") amountDueFilter?: string,
    @Query("itemCountFilter") itemCountFilter?: string,
  ) {
    return this.service.getFinancialReport({
      fromDate,
      toDate,
      dateField,
      branchIds,
      createdByStaffIds,
      assignedStaffIds,
      orderStatuses,
      paymentStatuses,
      fulfillmentStatuses,
      deliveryStatuses,
      salesChannels,
      shippingModes,
      carriers,
      paymentSourceIds,
      trackingFilter,
      codFilter,
      codReconciliationStatuses,
      amountDueFilter,
      itemCountFilter,
    });
  }
}
