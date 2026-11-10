import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { DashboardModule } from '../dashboard/dashboard.module';
import { MobilePushModule } from '../mobile-push/mobile-push.module';
import { BusinessReportController } from './business-report.controller';
import { BusinessReportService } from './business-report.service';

@Module({imports:[PrismaModule,DashboardModule,MobilePushModule],controllers:[BusinessReportController],providers:[BusinessReportService]})
export class BusinessReportModule {}
