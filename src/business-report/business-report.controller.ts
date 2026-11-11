import { Body, Controller, Get, Post, Req, UseGuards } from '@nestjs/common';
import { Request } from 'express';
import { JwtGuard } from '../auth/jwt.guard';
import { BusinessReportService } from './business-report.service';

@UseGuards(JwtGuard)
@Controller('business-reports')
export class BusinessReportController {
  constructor(private readonly reports:BusinessReportService){}
  @Get('settings') settings(@Req() req:Request & {user?:any}) {return this.reports.settings(req.user);}
  @Post('settings') save(@Req() req:Request & {user?:any},@Body() body:any){return this.reports.save(req.user,body);}
  @Get('preview') preview(@Req() req:Request & {user?:any}){return this.reports.preview(req.user);}
  @Get('history') history(@Req() req:Request & {user?:any}){return this.reports.history(req.user);}
  @Get('devices') devices(@Req() req:Request & {user?:any}){return this.reports.devices(req.user);}
  @Get('scheduler-status') schedulerStatus(@Req() req:Request & {user?:any}){return this.reports.schedulerStatus(req.user);}
  @Post('scheduler-check') schedulerCheck(@Req() req:Request & {user?:any}){return this.reports.runSchedulerCheck(req.user);}
  @Post('test') test(@Req() req:Request & {user?:any},@Body() body:any){return this.reports.test(req.user,body?.tokenId,body?.kind==='growth'?'growth':'daily');}
}
