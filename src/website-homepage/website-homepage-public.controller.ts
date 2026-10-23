import { Controller, Get, Query } from "@nestjs/common";
import { WebsiteHomepageService } from "./website-homepage.service";

@Controller("storefront/homepage")
export class WebsiteHomepagePublicController {
  constructor(private readonly service: WebsiteHomepageService) {}

  @Get()
  get(@Query("market") market?: string) {
    return this.service.publicConfig(market);
  }
}
