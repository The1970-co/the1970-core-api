import { Module } from '@nestjs/common';
import { PrismaModule } from './prisma/prisma.module';
import { AuthModule } from './auth/auth.module';
import { ProductModule } from './product/product.module';
import { OrderModule } from './order/order.module';
import { ShipmentModule } from './shipment/shipment.module';

@Module({
  imports: [PrismaModule, AuthModule, ProductModule, OrderModule, ShipmentModule],
})
export class AppModule {}