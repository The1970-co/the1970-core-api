import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { DashboardModule } from '../dashboard/dashboard.module';
import { MobilePushModule } from '../mobile-push/mobile-push.module';
import { ShipmentModule } from '../shipment/shipment.module';
import { BusinessReportController } from './business-report.controller';
import { BusinessReportService } from './business-report.service';

@Module({
  imports: [PrismaModule, DashboardModule, MobilePushModule, ShipmentModule],
  controllers: [BusinessReportController],
  providers: [BusinessReportService],
})
export class BusinessReportModule {}
