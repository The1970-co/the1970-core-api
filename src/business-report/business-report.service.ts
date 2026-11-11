import { Injectable, OnModuleInit, OnModuleDestroy, ForbiddenException, BadRequestException } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { DashboardService } from '../dashboard/dashboard.service';
import { MobilePushService } from '../mobile-push/mobile-push.service';
import { ShipmentService } from '../shipment/shipment.service';

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
  growthPct: 0, declinePct: 20, recordDays: 7,
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
  private lastTickAt: Date | null = null;
  private lastTickError: string | null = null;
  private lastTickSummary: Record<string, unknown> | null = null;
  constructor(private prisma:PrismaService, private dashboard:DashboardService, private push:MobilePushService, private shipment:ShipmentService) {}
  onModuleInit() {
    // Railway backend must be a persistent instance; no browser/phone required.
    console.log('[REPORT_SCHEDULER_START] timezone=Asia/Ho_Chi_Minh interval=60s');
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
  async devices(user:any) {
    const userId=this.owner(user);
    const devices=await this.prisma.mobilePushToken.findMany({
      where:{userId,isActive:true,platform:'ios',provider:'apns'},
      select:{id:true,deviceId:true,appVersion:true,lastSeenAt:true,createdAt:true,token:true},
      orderBy:{lastSeenAt:'desc'},take:50,
    });
    return devices.map((d,i)=>({id:d.id,label:`iPhone ${i+1} · ${d.token.slice(-6)}`,deviceId:d.deviceId,appVersion:d.appVersion,lastSeenAt:d.lastSeenAt}));
  }

  // Dashboard Web currently combines DashboardService (daily ads) with
  // ShipmentService.getWarRoomDeliveryRevenue (actual delivered sales and created orders).
  // Keep the same two independent sources here. Never infer delivered sales from
  // overview.dailyRows.raw.revenue, which includes non-delivered orders.
  async snapshot(settings:ReportSettings, now=new Date()) {
    const p=vnParts(now);
    const [overview, warRoom] = await Promise.all([
      this.dashboard.getOverview(),
      this.shipment.getWarRoomDeliveryRevenue({range:'today',fromDate:p.day,toDate:p.day}),
    ]);
    if (!warRoom || !Array.isArray(warRoom.dailySuccessRows) || !Array.isArray(warRoom.createdOrders)) {
      throw new Error('War Room chưa trả đủ dữ liệu giao thành công và đơn tạo; ngừng gửi báo cáo để tránh số liệu sai');
    }
    const daily=(overview.dailyRows || []) as any[];
    const today=daily.find(r=>String(r.date || '').slice(0,10)===p.day)?.raw || {};
    const delivered=(warRoom.dailySuccessRows as any[]).find(r=>String(r.date || '').slice(0,10)===p.day);
    // No delivered row today means zero delivered revenue, NOT overview daily revenue.
    const revenue=num(delivered?.successAmount);
    const cost=num(delivered?.successCost);
    const ads=num(today.adsCost);
    const operating=settings.operatingCostEnabled?settings.dailyOperatingCost:0;

    const created=(warRoom.createdOrders as any[]).filter(order=>{
      const status=String(order?.status || '').trim().toUpperCase();
      return status!=='CANCELLED' && status!=='CANCELED';
    });
    const channelOf=(order:any):'pos'|'facebook'|'other'=>{
      const value=String([order.salesChannel,order.channel,order.orderType,order.paymentMethod,order.paymentType].filter(Boolean).join(' '))
        .toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'').replace(/đ/g,'d');
      if(/pos|ban le|retail|quay/.test(value))return 'pos';
      if(/facebook|\bfb\b|meta|cod|giao hang|ship|delivery/.test(value))return 'facebook';
      return 'other';
    };
    const amountOf=(order:any)=>num(order.finalAmount ?? order.totalAmount ?? order.amount);
    const costOf=(order:any)=>{
      const items=Array.isArray(order.items)?order.items:Array.isArray(order.orderItems)?order.orderItems:[];
      return items.reduce((sum:number,item:any)=>{
        const direct=num(item.lineCost ?? item.totalCost ?? item.costAmount ?? item.totalCostAmount ?? item.costTotal);
        if(direct>0)return sum+direct;
        const unit=num(item.costPrice ?? item.unitCost ?? item.cost ?? item.variant?.costPrice ?? item.productVariant?.costPrice ?? item.inventoryCost ?? item.importPrice);
        return sum+unit*num(item.qty ?? item.quantity);
      },0);
    };
    const pos=created.filter(x=>channelOf(x)==='pos');
    const cod=created.filter(x=>channelOf(x)==='facebook');
    const sum=(rows:any[], f:(x:any)=>number)=>rows.reduce((total,x)=>total+f(x),0);
    const posRevenue=sum(pos,amountOf),codRevenue=sum(cod,amountOf);
    const createdRevenue=posRevenue+codRevenue;
    const directCreatedCost=sum([...pos,...cod],costOf);
    // Match DashboardPage.tsx: direct cost if available, otherwise today's actual
    // cost ratio, falling back to 42% when there is no usable actual cost ratio.
    const successCostRate=revenue>0&&cost>0?cost/revenue:0;
    const estimatedCost=directCreatedCost>0?directCreatedCost:
      createdRevenue>0?Math.round(createdRevenue*(successCostRate || 0.42)):0;
    const actualProfit=revenue-cost-ads-operating;
    const estimatedProfit=createdRevenue-estimatedCost-ads-operating;
    return {day:p.day,time:p.time,minutes:p.minutes,
      orders:created.length,pos:pos.length,cod:cod.length,
      createdRevenue:sum(created,amountOf),posRevenue,codRevenue,
      successRevenue:revenue,actualProfit,estimatedProfit,
      ads,operating,historical:daily,
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
  async test(user:any, tokenId?:string, kind:'daily'|'growth'='daily') {
    const id=this.owner(user),c=await this.settings(user),s=await this.snapshot(c);
    const title=kind==='growth'?'[Thử] Cảnh báo tăng trưởng':'[Thử] Báo cáo kinh doanh';
    const body=kind==='growth'?`Doanh thu thành công hiện tại: ${fmt(s.successRevenue)}đ. Đây là thông báo thử.`:this.format(s,c);
    // Only tokens belonging to the authenticated owner/admin may be addressed.
    return this.push.notifyReport(id,title,body,{kind:kind==='growth'?'growth':'test',day:s.day},tokenId);
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
  // Diagnoses whether this API process is actually running the scheduler.
  async schedulerStatus(user:any) {
    const userId=this.owner(user);
    const c=await this.settings(user);
    const p=vnParts();
    const histories=await this.prisma.businessReportDelivery.findMany({
      where:{userId,kind:'daily',key:{startsWith:`daily:${userId}:${p.day}:`}},
      select:{key:true,status:true,createdAt:true,sent:true,failed:true,error:true},
      orderBy:{createdAt:'desc'},take:20,
    });
    return {
      serverNow:new Date().toISOString(),localNow:p, schedulerStarted:Boolean(this.timer),
      isRunning:this.running,lastTickAt:this.lastTickAt,lastTickError:this.lastTickError,
      lastTickSummary:this.lastTickSummary,enabled:c.enabled,slots:c.slots,
      // Values that have passed should have delivery rows; otherwise scheduler is not delivering.
      dueToday:c.slots.filter(x=>x.enabled&&x.time<=p.time),histories,
    };
  }

  async runSchedulerCheck(user:any) {
    this.owner(user);
    await this.tick();
    return this.schedulerStatus(user);
  }

  private async tick(){
    if(this.running)return; this.running=true;
    const stats={owners:0, dueSlots:0, sentChecks:0, skipped:0, errors:0};
    this.lastTickAt=new Date();
    this.lastTickError=null;
    try {
      const p=vnParts();
      // Both AdminUser (legacy) and StaffUser (current multi-role accounts) exist.
      // Only active Owner/Admin users can receive financial report pushes.
      // Push tokens are registered per device, so a user with two iPhones receives both.
      const [savedRows, legacyAdmins, staffAdmins, activeTokens] = await Promise.all([
        this.prisma.systemSetting.findMany({where:{key:{startsWith:KEY_PREFIX}},select:{key:true}}),
        this.prisma.adminUser.findMany({where:{isActive:true,role:{in:['OWNER','ADMIN']}},select:{id:true}}),
        this.prisma.staffUser.findMany({
          where:{
            isActive:true,
            OR:[
              {role:{in:['OWNER','ADMIN','owner','admin']}},
              {roles:{some:{roleCode:{in:['OWNER','ADMIN','owner','admin']}}}},
            ],
          },
          select:{id:true},
        }),
        this.prisma.mobilePushToken.findMany({
          where:{isActive:true,platform:'ios',provider:'apns'},
          select:{userId:true,staffId:true},
        }),
      ]);
      const authorized = new Set([...legacyAdmins,...staffAdmins].map(u=>u.id));
      const configuredIds = new Set(savedRows.map(r=>r.key.slice(KEY_PREFIX.length)));
      const ids = new Set<string>();
      // Every logged-in iPhone should already have an active APNs token. Only allow
      // valid Owner/Admin identities, regardless of which user table stores them.
      for(const device of activeTokens){
        if(device.userId && authorized.has(device.userId)) ids.add(device.userId);
        // If staffId is populated but userId differs, this is a registration
        // mismatch. Do not send to a possibly unrelated userId.
      }
      for(const id of configuredIds) if(authorized.has(id)) ids.add(id);
      console.log('[REPORT_SCHEDULER_RECIPIENTS]',{
        savedSettings:savedRows.length,
        legacyAdmins:legacyAdmins.length,
        staffAdmins:staffAdmins.length,
        activeDevices:activeTokens.length,
        staffIdOnlyDevices:activeTokens.filter(t=>t.staffId && authorized.has(t.staffId) && t.userId !== t.staffId).length,
        authorizedRecipients:ids.size,
      });
      for(const userId of ids){
        stats.owners++;
        try {
        const c=await this.settings({id:userId,role:'OWNER'});
        if(!c.enabled)continue;
        // Catch up any due slot earlier today, including after restarts or slow queries.
        // Unique delivery key prevents duplicates; never send tomorrow's slots early.
        const dueSlots=c.slots.filter(x=>x.enabled && x.time<=p.time);
        stats.dueSlots+=dueSlots.length;
        // Avoid expensive Dashboard/Shipment queries every minute after noon.
        const dueKeys=dueSlots.map(slot=>`daily:${userId}:${p.day}:${slot.time}`);
        const existing=dueKeys.length?await this.prisma.businessReportDelivery.findMany({
          where:{key:{in:dueKeys}},select:{key:true},
        }):[];
        const existingKeys=new Set(existing.map(x=>x.key));
        const pendingSlots=dueSlots.filter(slot=>!existingKeys.has(`daily:${userId}:${p.day}:${slot.time}`));
        if(!pendingSlots.length&&p.minutes%5!==0)continue;
        const s=await this.snapshot(c);
        for(const slot of pendingSlots){
          stats.sentChecks++;
          await this.deliver(userId,`daily:${userId}:${p.day}:${slot.time}`,'daily',`Báo cáo kinh doanh ${slot.time}`,this.format(s,c),p.day);
        }
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
            if(c.alerts.growth&&pct>0&&pct>=c.growthPct)await this.deliver(userId,`growth:${userId}:${p.day}:${c.growthPct}`,'growth','Doanh thu tăng trưởng',`Doanh thu thành công tăng ${pct.toFixed(1)}% so với hôm qua cùng giờ (${fmt(current)}đ).`,p.day);
            if(c.alerts.decline&&pct<0&&pct<=-c.declinePct)await this.deliver(userId,`decline:${userId}:${p.day}:${c.declinePct}`,'decline','Doanh thu giảm',`Doanh thu thành công giảm ${Math.abs(pct).toFixed(1)}% so với hôm qua cùng giờ.`,p.day);
          }
        }
        if(c.alerts.record){
          const past=s.historical.filter((r:any)=>r.date<p.day&&r.date>=shiftDay(p.day,-c.recordDays));
          if(past.length===c.recordDays&&s.createdRevenue>0){
            const highest=Math.max(...past.map((r:any)=>num(r.raw?.createdAmount)));
            if(s.createdRevenue>highest)await this.deliver(userId,`record:${userId}:${p.day}:${c.recordDays}`,'record',`Kỷ lục doanh thu ${c.recordDays} ngày`,`Doanh thu tạo đơn ${fmt(s.createdRevenue)}đ, vượt mức cao nhất ${fmt(highest)}đ.`,p.day);
          }
        }
        } catch (error:any) {
          stats.errors++;
          console.error(`[REPORT_SCHEDULER_USER_FAILED] user=${userId}`,error);
        }
      }
    } catch(error:any) {
      this.lastTickError=String(error?.message || error);
      console.error('[REPORT_SCHEDULER_TICK_FAILED]',error);
      throw error;
    } finally {
      this.lastTickSummary=stats;
      this.running=false;
      console.log('[REPORT_SCHEDULER_TICK]',{at:this.lastTickAt?.toISOString(),...stats});
    }
  }
}
