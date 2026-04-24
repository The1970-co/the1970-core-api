import {
  Body,
  Controller,
  Get,
  Param,
  Patch,
  Post,
  Query,
} from "@nestjs/common";
import { CustomerService } from "./customer.service";

@Controller("customers")
export class CustomerController {
  constructor(private readonly customerService: CustomerService) {}

  @Get()
  findAll() {
    return this.customerService.findAll();
  }

  @Get("search")
  search(@Query("phone") phone: string) {
    return this.customerService.findByPhone(phone);
  }

  @Get(":id")
  findOne(@Param("id") id: string) {
    return this.customerService.findOne(id);
  }

  @Get(":id/import-history")
  getImportHistory(@Param("id") id: string) {
    return this.customerService.getImportHistory(id);
  }

  @Get(":id/addresses")
  getAddresses(@Param("id") id: string) {
    return this.customerService.getAddresses(id);
  }

  @Post(":id/addresses")
  createAddress(
    @Param("id") id: string,
    @Body()
    body: {
      label?: string;
      recipientName?: string;
      phone?: string;
      email?: string;
      addressLine1: string;
      addressLine2?: string;
      ward?: string;
      district?: string;
      city?: string;
      province?: string;
      country?: string;
      postalCode?: string;
      isDefault?: boolean;
    }
  ) {
    return this.customerService.createAddress(id, body);
  }

  @Patch(":id/addresses/:addressId")
  updateAddress(
    @Param("id") id: string,
    @Param("addressId") addressId: string,
    @Body()
    body: {
      label?: string;
      recipientName?: string;
      phone?: string;
      email?: string;
      addressLine1?: string;
      addressLine2?: string;
      ward?: string;
      district?: string;
      city?: string;
      province?: string;
      country?: string;
      postalCode?: string;
      isDefault?: boolean;
    }
  ) {
    return this.customerService.updateAddress(id, addressId, body);
  }

  @Post(":id/addresses/:addressId/set-default")
  setDefaultAddress(
    @Param("id") id: string,
    @Param("addressId") addressId: string
  ) {
    return this.customerService.setDefaultAddress(id, addressId);
  }

  @Post()
  create(
    @Body()
    body: {
      legacyCode?: string;
      fullName: string;
      phone?: string;
      email?: string;
      source?: string;
      customerGroup?: string;
      gender?: string;
      birthDate?: string;
      points?: number;

      totalOrders?: number;
      totalSpent?: number;
      lastOrderAt?: string;

      defaultDiscountPercent?: number;
      pricePolicyName?: string;
      customerNote?: string;

      addressLine1?: string;
      addressLine2?: string;
      ward?: string;
      district?: string;
      city?: string;
      province?: string;
      country?: string;
      postalCode?: string;
      label?: string;
      recipientName?: string;
      isDefaultAddress?: boolean;
    }
  ) {
    return this.customerService.createCustomer(body);
  }

  @Patch(":id")
  update(
    @Param("id") id: string,
    @Body()
    body: {
      legacyCode?: string;
      fullName?: string;
      phone?: string;
      email?: string;
      source?: string;
      customerGroup?: string;
      gender?: string;
      birthDate?: string;
      points?: number;
      totalOrders?: number;
      totalSpent?: number;
      lastOrderAt?: string;

      defaultDiscountPercent?: number;
      pricePolicyName?: string;
      customerNote?: string;

      addressLine1?: string;
      addressLine2?: string;
      ward?: string;
      district?: string;
      city?: string;
      province?: string;
      country?: string;
      postalCode?: string;
      label?: string;
      recipientName?: string;
      isDefaultAddress?: boolean;
    }
  ) {
    return this.customerService.updateCustomer(id, body);
  }
}