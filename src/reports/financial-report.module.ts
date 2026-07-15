import { Module } from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";
import { FinancialReportController } from "./financial-report.controller";
import { FinancialReportService } from "./financial-report.service";

@Module({
  controllers: [FinancialReportController],
  providers: [FinancialReportService, PrismaService],
  exports: [FinancialReportService],
})
export class FinancialReportModule {}