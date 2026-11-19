import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { MobilePushModule } from '../mobile-push/mobile-push.module';
import { MetaAdsController } from './meta-ads.controller';
import { MetaAdsService } from './meta-ads.service';
import { MetaAdsSyncService } from './meta-ads-sync.service';
import { MetaAdsOrderAttributionService } from './meta-ads-order-attribution.service';
import { MetaAdsInventoryAutopilotService } from './meta-ads-inventory-autopilot.service';
import { MetaAdsPerformanceAutopilotService } from './meta-ads-performance-autopilot.service';
import { MetaAdsPostLaunchAutopilotService } from './meta-ads-post-launch-autopilot.service';

@Module({
  imports: [PrismaModule, MobilePushModule],
  controllers: [MetaAdsController],
  providers: [
    MetaAdsService,
    MetaAdsSyncService,
    MetaAdsOrderAttributionService,
    MetaAdsInventoryAutopilotService,
    MetaAdsPerformanceAutopilotService,
    MetaAdsPostLaunchAutopilotService,
  ],
  exports: [
    MetaAdsService,
    MetaAdsSyncService,
    MetaAdsOrderAttributionService,
    MetaAdsInventoryAutopilotService,
    MetaAdsPerformanceAutopilotService,
    MetaAdsPostLaunchAutopilotService,
  ],
})
export class MetaAdsModule {}
