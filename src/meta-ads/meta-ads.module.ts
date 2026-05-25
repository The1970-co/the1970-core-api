import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { MetaAdsController } from './meta-ads.controller';
import { MetaAdsService } from './meta-ads.service';
import { MetaAdsSyncService } from './meta-ads-sync.service';

@Module({
  imports: [PrismaModule],
  controllers: [MetaAdsController],
  providers: [MetaAdsService, MetaAdsSyncService],
  exports: [MetaAdsService, MetaAdsSyncService],
})
export class MetaAdsModule {}
