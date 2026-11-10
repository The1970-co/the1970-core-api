import { Injectable, OnModuleInit, OnModuleDestroy, ForbiddenException, BadRequestException } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { DashboardService } from '../dashboard/dashboard.service';
import { MobilePushService } from '../mobile-push/mobile-push.service';

const TZ = 'Asia/Ho_Chi_Minh';
const KEY_PREFIX = 'business_report_settings:';
const METRICS = ['orders','pos','cod','createdRevenue','successRevenue','actualProfit','estimatedProfit','ads'] as const;
type Metric = typeof METRICS[number];
type AlertName = 'pos' | 'cod' | 'revenue' | 'growth' | 'record' | 'decline';
type ReportSettings = {
  enabled: boolean; slots: { time: string; enabled: boolean }[];
  metrics: Record<Metric, boolean>;
  alerts: Record<AlertName, boolean>;
  posTarget: number; codTarget: number; revenueTarget: number;
  growthPct: number; declinePct: number; recordDays: 7 | 14 | 30;
  operatingCostEnabled: boolean; dailyOperatingCost: number;
};
const defaults: ReportSettings = {
  enabled: true,
  slots: [{time:'12:00',enabled:true},{time:'20:00',enabled:true},{time:'22:00',enabled:true}],
  metrics: {orders:true,pos:true,cod:true,createdRevenue:true,successRevenue:true,actualProfit:false,estimatedProfit:false,ads:false},
  alerts: {pos:false,cod:false,revenue:false,growth:true,record:true,decline:false},
  posTarget: 20_000_000, codTarget: 30_000_000, revenueTarget: 50_000_000,
  growthPct: 10, declinePct: 20, recordDays: 7,
  operatingCostEnabled: true, dailyOperatingCost: 7_000_000,
};

// Convert a local wall-clock date/time to UTC; no dependence on Railway's TZ.
function vnParts(d = new Date()) {
  const p = new Intl.DateTimeFormat('en-GB', {timeZone:TZ,year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).formatToParts(d);
  const v = (k:string) => p.find(x=>x.type===k)?.value || '00';
  return {day:`${v('year')}-${v('month')}-${v('day')}`,time:`${v('hour')}:${v('minute')}`, minutes:Number(v('hour'))*60+Number(v('minute'))};
}
function vnUtc(day: string, mins=0) {
  const [y,m,d]=day.split('-').map(Number);
  return new Date(Date.UTC(y,m-1,d,Math.floor(mins/60)-7,mins%60));
}
function shiftDay(day:string, days:number) {
  const d=vnUtc(day); d.setUTCDate(d.getUTCDate()+days);
  return vnParts(d).day;
}
const fmt = (n:number) => new Intl.NumberFormat('vi-VN').format(Math.round(n));
const num = (n:any) => Number.isFinite(Number(n)) ? Number(n) : 0;

@Injectable()
export class BusinessReportService implements OnModuleInit, OnModuleDestroy {
  private timer?: ReturnType<typeof setInterval>;
  private running=false;
  constructor(private prisma:PrismaService, private dashboard:DashboardService, private push:MobilePushService) {}
  onModuleInit() {
    // Railway backend must be a persistent instance; no browser/phone required.
    this.timer=setInterval(()=>void this.tick().catch(e=>console.error('[REPORT_CRON]',e)), 60_000);
    void this.tick().catch(e=>console.error('[REPORT_BOOT]',e));
  }
  onModuleDestroy() { if(this.timer) clearInterval(this.timer); }

  private owner(user:any) {
    const id=String(user?.id || user?.userId || '');
    if(!id) throw new ForbiddenException('Không xác định tài khoản');
    if(!['OWNER','ADMIN'].includes(String(user?.role || '').toUpperCase())) throw new ForbiddenException('Chỉ owner/admin được cấu hình báo cáo doanh thu và lợi nhuận');
    return id;
  }
  async settings(user:any) {
    const id=this.owner(user);
    const row=await this.prisma.systemSetting.findUnique({where:{key:KEY_PREFIX+id}});
    const v=(row?.value || {}) as Partial<ReportSettings>;
    return {
      ...defaults,...v,
      slots:Array.isArray(v.slots)?v.slots:defaults.slots,
      metrics:{...defaults.metrics,...v.metrics},
      alerts:{...defaults.alerts,...v.alerts},
    } as ReportSettings;
  }
  async save(user:any, input:Partial<ReportSettings>) {
    const id=this.owner(user), prev=await this.settings(user);
    const slots=Array.isArray(input.slots) ? input.slots.map(x=>({time:String(x.time),enabled:Boolean(x.enabled)})) : prev.slots;
    if(slots.length>12 || slots.some(x=>!/^(?:[01]\d|2[0-3]):[0-5]\d$/.test(x.time))) throw new BadRequestException('Giờ gửi phải là HH:mm; tối đa 12 mốc');
    const safeNum=(v:any,f:number,max=1e12)=>v===undefined?f:Math.max(0,Math.min(max,num(v)));
    const next:ReportSettings={
      enabled:input.enabled===undefined?prev.enabled:Boolean(input.enabled),
      slots,
      metrics:Object.fromEntries(METRICS.map(k=>[k,input.metrics?.[k]===undefined?prev.metrics[k]:Boolean(input.metrics[k])])) as Record<Metric,boolean>,
      alerts:Object.fromEntries((Object.keys(defaults.alerts) as AlertName[]).map(k=>[k,input.alerts?.[k]===undefined?prev.alerts[k]:Boolean(input.alerts[k])])) as Record<AlertName,boolean>,
      posTarget:safeNum(input.posTarget,prev.posTarget),codTarget:safeNum(input.codTarget,prev.codTarget),revenueTarget:safeNum(input.revenueTarget,prev.revenueTarget),
      growthPct:safeNum(input.growthPct,prev.growthPct,10000),declinePct:safeNum(input.declinePct,prev.declinePct,100),
      recordDays:[7,14,30].includes(Number(input.recordDays))?Number(input.recordDays) as 7|14|30:prev.recordDays,
      operatingCostEnabled:input.operatingCostEnabled===undefined?prev.operatingCostEnabled:Boolean(input.operatingCostEnabled),
      dailyOperatingCost:safeNum(input.dailyOperatingCost,prev.dailyOperatingCost),
    };
    await this.prisma.systemSetting.upsert({where:{key:KEY_PREFIX+id},create:{key:KEY_PREFIX+id,value:next as any},update:{value:next as any}});
    return next;
  }
  async history(user:any) {const id=this.owner(user);return this.prisma.businessReportDelivery.findMany({where:{userId:id},orderBy:{createdAt:'desc'},take:50});}

  // Use DashboardService as one source for revenue, ads & cost. Augmented dailyRows
  // from that same service expose created orders by channel for web and mobile.
  async snapshot(settings:ReportSettings, now=new Date()) {
    const p=vnParts(now);
    const overview=await this.dashboard.getOverview();
    const daily=(overview.dailyRows as any[]);
    const today=daily.find(r=>r.date===p.day)?.raw || {};
    const operating=settings.operatingCostEnabled?settings.dailyOperatingCost:0;
    const revenue=num(today.revenue),cost=num(today.cost),ads=num(today.adsCost);
    const createdRevenue=num(today.posCreatedAmount)+num(today.facebookCreatedAmount);
    const createdCost=num(today.posCreatedCostEstimate)+num(today.facebookCreatedCostEstimate);
    const fallbackCost=createdRevenue*(revenue>0&&cost>0?cost/revenue:0.42);
    const estCost=createdCost>0?createdCost:fallbackCost;
    return {day:p.day,time:p.time, minutes:p.minutes,
      orders:num(today.createdOrders),pos:num(today.posCreatedOrders),cod:num(today.facebookCreatedOrders),
      createdRevenue:num(today.createdAmount),posRevenue:num(today.posCreatedAmount),codRevenue:num(today.facebookCreatedAmount),
      successRevenue:revenue,actualProfit:revenue-cost-ads-operating,
      estimatedProfit:createdRevenue-estCost-ads-operating,ads,operating,
      historical:daily,
    };
  }
  private format(s:any,c:ReportSettings) {
    const entries:[Metric,string][]=[
      ['orders',`Đơn tạo ${fmt(s.orders)}`],['pos',`POS ${fmt(s.pos)}`],['cod',`COD ${fmt(s.cod)}`],
      ['createdRevenue',`Doanh thu tạo ${fmt(s.createdRevenue)}đ`],
      ['successRevenue',`Doanh thu thành công ${fmt(s.successRevenue)}đ`],
      ['actualProfit',`Lợi nhuận thực tế ${fmt(s.actualProfit)}đ`],
      ['estimatedProfit',`Lợi nhuận ước tính ${fmt(s.estimatedProfit)}đ`],
      ['ads',`Ads ${fmt(s.ads)}đ`],
    ];
    return entries.filter(([key])=>c.metrics[key]).map(([,v])=>v).join(' · ') || 'Không có chỉ số được bật';
  }
  async preview(user:any) { const c=await this.settings(user);const s=await this.snapshot(c);return {snapshot:s, title:`Báo cáo kinh doanh ${s.time}`,body:this.format(s,c)}; }
  async test(user:any) {
    const id=this.owner(user),c=await this.settings(user),s=await this.snapshot(c);
    return this.push.notifyReport(id,'[Thử] Báo cáo kinh doanh',this.format(s,c),{kind:'test',day:s.day});
  }
  private async deliver(userId:string,key:string,kind:string,title:string,body:string,day:string) {
    // Unique key provides deduplication even if Railway runs >1 worker.
    try {await this.prisma.businessReportDelivery.create({data:{key,userId,kind,title,body}});}
    catch(e:any){if(e?.code==='P2002')return;throw e;}
    try {
      const result=await this.push.notifyReport(userId,title,body,{kind,day});
      await this.prisma.businessReportDelivery.update({where:{key},data:{status:result.sent?'SENT':result.failed?'FAILED':'NO_DEVICE',sent:result.sent,failed:result.failed}});
    } catch(e:any) {await this.prisma.businessReportDelivery.update({where:{key},data:{status:'FAILED',error:String(e?.message||e).slice(0,500)}});}
  }
  private async tick(){
    if(this.running)return; this.running=true;
    try {
      const p=vnParts();
      // Active owner/admin devices get default schedule even before first settings visit.
      const [savedRows, admins, activeTokens] = await Promise.all([
        this.prisma.systemSetting.findMany({where:{key:{startsWith:KEY_PREFIX}}}),
        this.prisma.adminUser.findMany({where:{isActive:true,role:{in:['OWNER','ADMIN']}},select:{id:true}}),
        this.prisma.mobilePushToken.findMany({where:{isActive:true,platform:'ios',provider:'apns',userId:{not:null}},select:{userId:true}}),
      ]);
      const authorized = new Set(admins.map(u=>u.id));
      const ids = new Set(savedRows.map(r=>r.key.slice(KEY_PREFIX.length)));
      for(const device of activeTokens) if(device.userId&&authorized.has(device.userId)) ids.add(device.userId);
      const rows=[...ids].map(id=>({key:KEY_PREFIX+id}));
      for(const row of rows){
        const userId=row.key.slice(KEY_PREFIX.length);
        if(!authorized.has(userId)) continue;
        const c=await this.settings({id:userId,role:'OWNER'});
        if(!c.enabled)continue;
        const due=c.slots.some(x=>x.enabled&&x.time===p.time);
        // Alert evaluation every 5 minutes, not every minute.
        if(!due&&p.minutes%5!==0)continue;
        const s=await this.snapshot(c);
        if(due)await this.deliver(userId,`daily:${userId}:${p.day}:${p.time}`,'daily',`Báo cáo kinh doanh ${p.time}`,this.format(s,c),p.day);
        if(p.minutes%5!==0)continue;
        for(const [name,value,threshold,label] of [
          ['pos',s.posRevenue,c.posTarget,'POS'],['cod',s.codRevenue,c.codTarget,'COD'],['revenue',s.createdRevenue,c.revenueTarget,'Tổng doanh thu tạo'],
        ] as const) {
          if(c.alerts[name]&&threshold>0&&value>=threshold) await this.deliver(userId,`threshold:${name}:${userId}:${p.day}:${threshold}`,name,`${label} đạt mục tiêu`,`${label} hôm nay đạt ${fmt(value)}đ, vượt mốc ${fmt(threshold)}đ.`,p.day);
        }
        // Historical daily comparison: same cut-off needs historical snapshots;
        // use exact time window revenue aggregate, never compare full yesterday to partial today.
        if(c.alerts.growth||c.alerts.decline){
          const previous=shiftDay(p.day,-1);
          const [a,b]=await Promise.all([
            this.prisma.order.aggregate({where:{status:{not:'CANCELLED'},soldAt:{gte:vnUtc(previous),lt:vnUtc(previous,p.minutes)}},_sum:{finalAmount:true}}),
            this.prisma.order.aggregate({where:{status:{not:'CANCELLED'},soldAt:{gte:vnUtc(p.day),lt:new Date()}},_sum:{finalAmount:true}}),
          ]);
          const before=num(a._sum.finalAmount),current=num(b._sum.finalAmount);
          if(before>0){
            const pct=100*(current-before)/before;
            if(c.alerts.growth&&pct>=c.growthPct)await this.deliver(userId,`growth:${userId}:${p.day}:${c.growthPct}`,'growth','Doanh thu tăng trưởng',`Doanh thu thành công tăng ${pct.toFixed(1)}% so với hôm qua cùng giờ (${fmt(current)}đ).`,p.day);
            if(c.alerts.decline&&pct<=-c.declinePct)await this.deliver(userId,`decline:${userId}:${p.day}:${c.declinePct}`,'decline','Doanh thu giảm',`Doanh thu thành công giảm ${Math.abs(pct).toFixed(1)}% so với hôm qua cùng giờ.`,p.day);
          }
        }
        if(c.alerts.record){
          const past=s.historical.filter((r:any)=>r.date<p.day&&r.date>=shiftDay(p.day,-c.recordDays));
          if(past.length===c.recordDays&&s.createdRevenue>0){
            const highest=Math.max(...past.map((r:any)=>num(r.raw?.createdAmount)));
            if(s.createdRevenue>highest)await this.deliver(userId,`record:${userId}:${p.day}:${c.recordDays}`,'record',`Kỷ lục doanh thu ${c.recordDays} ngày`,`Doanh thu tạo đơn ${fmt(s.createdRevenue)}đ, vượt mức cao nhất ${fmt(highest)}đ.`,p.day);
          }
        }
      }
    } finally {this.running=false;}
  }
}
