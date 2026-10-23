import { Controller, Get, Query } from "@nestjs/common";
import { WebsiteNavigationService } from "./website-navigation.service";

@Controller("storefront/navigation")
export class WebsiteNavigationPublicController {
  constructor(private readonly service: WebsiteNavigationService) {}

  @Get()
  get(@Query("market") market?: string) {
    return this.service.publicConfig(market);
  }
}
