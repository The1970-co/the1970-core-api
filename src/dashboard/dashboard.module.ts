import { Module } from '@nestjs/common';
import { DashboardController } from './dashboard.controller';
import { DashboardService } from './dashboard.service';
import { PrismaModule } from '../prisma/prisma.module';
import { MetaAdsModule } from '../meta-ads/meta-ads.module';

@Module({
  imports: [PrismaModule, MetaAdsModule],
  controllers: [DashboardController],
  providers: [DashboardService],
})
export class DashboardModule {}