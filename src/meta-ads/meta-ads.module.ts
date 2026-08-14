import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { MetaAdsController } from './meta-ads.controller';
import { MetaAdsService } from './meta-ads.service';
import { MetaAdsSyncService } from './meta-ads-sync.service';
import { MetaAdsOrderAttributionService } from './meta-ads-order-attribution.service';
import { MetaAdsInventoryAutopilotService } from './meta-ads-inventory-autopilot.service';
import { MetaAdsPerformanceAutopilotService } from './meta-ads-performance-autopilot.service';

@Module({
  imports: [PrismaModule],
  controllers: [MetaAdsController],
  providers: [
    MetaAdsService,
    MetaAdsSyncService,
    MetaAdsOrderAttributionService,
    MetaAdsInventoryAutopilotService,
    MetaAdsPerformanceAutopilotService,
  ],
  exports: [
    MetaAdsService,
    MetaAdsSyncService,
    MetaAdsOrderAttributionService,
    MetaAdsInventoryAutopilotService,
    MetaAdsPerformanceAutopilotService,
  ],
})
export class MetaAdsModule {}
