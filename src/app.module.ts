import { Module } from "@nestjs/common";
import { PrismaModule } from "./prisma/prisma.module";
import { AuthModule } from "./auth/auth.module";
import { ProductModule } from "./product/product.module";
import { OrderModule } from "./order/order.module";
import { ShipmentModule } from "./shipment/shipment.module";
import { CustomerModule } from "./customer/customer.module";
import { StocktakeModule } from "./stocktake/stocktake.module";
import { StaffModule } from "./staff/staff.module";
import { InventoryModule } from "./inventory/inventory.module";
import { BranchesModule } from "./branches/branches.module";
import { ImportsModule } from "./imports/imports.module";
import { CategoriesModule } from './categories/categories.module';
import { AddressModule } from "./address/address.module";
import { SuppliersModule } from './suppliers/suppliers.module';
import { PurchaseReceiptsModule } from './purchase-receipts/purchase-receipts.module';
import { StockTransferModule } from './stock-transfer/stock-transfer.module';
import { AuthTotpModule } from "./auth-totp/auth-totp.module";
import { ShippingAddressesModule } from "./shipping-addresses/shipping-addresses.module";
import { MobileModule } from "./mobile/mobile.module";
import { PartialDeliveryModule } from "./partial-delivery/partial-delivery.module";
import { BranchNotificationsModule } from "./notifications/branch-notifications.module";
import { PaymentSourcesModule } from "./payment-sources/payment-sources.module";
import { ScheduleModule } from "@nestjs/schedule";
import { FinanceModule } from "./finance/finance.module";
@Module({
  imports: [
    PrismaModule,
    AuthModule,
    ProductModule,
    OrderModule,
    ShipmentModule,
    CustomerModule,
    StocktakeModule,
    StaffModule,
    InventoryModule,
    BranchesModule,
    ImportsModule,
    CategoriesModule,
    AddressModule,
    SuppliersModule,
    PurchaseReceiptsModule,
    StockTransferModule,
    AuthTotpModule,
    ShippingAddressesModule,
    MobileModule,
    PartialDeliveryModule,
    BranchNotificationsModule,
    PaymentSourcesModule,
    ScheduleModule.forRoot(),
    FinanceModule,
  ],
})
export class AppModule {}