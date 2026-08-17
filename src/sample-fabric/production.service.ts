import { BadRequestException, Injectable, NotFoundException } from "@nestjs/common";
import { PrismaService } from "../prisma/prisma.service";

type Actor = { id?: string; sub?: string; name?: string; fullName?: string; email?: string };

@Injectable()
export class ProductionService {
  constructor(private readonly prisma: PrismaService) {}

  private actor(user?: Actor) {
    return { id: String(user?.id || user?.sub || "") || null, name: String(user?.name || user?.fullName || user?.email || "Hệ thống") };
  }

  private n(value: any) {
    if (value === null || value === undefined || value === "") return null;
    const clean = typeof value === "string" ? value.replace(/[^\d,.\-]/g, "").replace(",", ".") : value;
    const n = Number(clean);
    return Number.isFinite(n) ? n : null;
  }

  private initial(name: string) {
    const raw = String(name || "").normalize("NFD").replace(/[\u0300-\u036f]/g, "").replace(/đ/g, "d").replace(/Đ/g, "D");
    return (raw.match(/[A-Za-z0-9]/)?.[0] || "X").toUpperCase();
  }

  private async nextSimpleCode(model: "factory"|"supplier"|"item", name: string) {
    const delegate: any =
      model === "factory" ? this.prisma.productionPartner :
      model === "supplier" ? this.prisma.productionAccessorySupplier :
      this.prisma.productionAccessoryItem;
    const rows = await delegate.findMany({ select: { code: true } });
    const max = rows.reduce((m: number, r: any) => Math.max(m, Number(String(r.code || "").match(/\d+/)?.[0] || 0)), 0);
    return `${String(max + 1).padStart(3, "0")}-${this.initial(name)}`;
  }

  private async nextOrderCode() {
    const d = new Date();
    const suffix = `${String(d.getDate()).padStart(2,"0")}${String(d.getMonth()+1).padStart(2,"0")}${d.getFullYear()}`;
    const rows = await this.prisma.productionOrder.findMany({ where: { code: { endsWith: suffix } }, select: { code: true } });
    const max = rows.reduce((m: number, r: any) => Math.max(m, Number(String(r.code).match(/^SX-(\d+)-/)?.[1] || 0)), 0);
    return `SX-${String(max + 1).padStart(3, "0")}-${suffix}`;
  }

  async meta() {
    const [samples, factories, accessories, rolls] = await Promise.all([
      this.prisma.designSample.findMany({
        where: { status: { not: "ON_HOLD" } },
        select: { id: true, code: true, name: true, year: true, season: true, category: true, coverImageUrl: true },
        orderBy: { updatedAt: "desc" },
      }),
      this.prisma.productionPartner.findMany({ where: { isActive: true }, orderBy: { name: "asc" } }),
      this.prisma.productionAccessoryItem.findMany({ where: { isActive: true }, orderBy: [{ typeName: "asc" }, { name: "asc" }] }),
      this.availableFabricRolls(),
    ]);
    return { samples, factories, accessories, rolls };
  }

  async availableFabricRolls() {
    const rows = await this.prisma.fabricReceiptRoll.findMany({
      where: { fabricReceipt: { status: { in: ["RECEIVING","INSPECTING","COMPLETED"] } } },
      include: {
        fabricReceipt: { select: { id: true, receiptCode: true, fabricName: true, colorName: true, colorCode: true } },
        images: { orderBy: { createdAt: "desc" }, take: 1 },
      },
      orderBy: { createdAt: "desc" },
    });
    const allocated = await this.prisma.productionOrderRoll.groupBy({
      by: ["fabricReceiptRollId"],
      _sum: { allocatedM: true, allocatedKg: true },
    });
    const used = new Map(allocated.map((x:any) => [x.fabricReceiptRollId, x._sum]));
    return rows.map((r:any) => {
      const sum:any = used.get(r.id) || {};
      const actualM = Number(r.actualM || 0), actualKg = Number(r.actualKg || 0);
      return {
        id: r.id,
        fabricReceiptId: r.fabricReceiptId,
        receiptCode: r.fabricReceipt?.receiptCode,
        fabricName: r.fabricReceipt?.fabricName,
        rollCode: r.rollCode,
        colorName: r.colorName || r.fabricReceipt?.colorName || null,
        colorCode: r.colorCode || r.fabricReceipt?.colorCode || null,
        actualM, actualKg,
        remainingM: Math.max(0, actualM - Number(sum.allocatedM || 0)),
        remainingKg: Math.max(0, actualKg - Number(sum.allocatedKg || 0)),
        imageUrl: r.images?.[0]?.url || null,
      };
    }).filter((r:any) => r.remainingM > 0 || r.remainingKg > 0);
  }

  listFactories() {
    return this.prisma.productionPartner.findMany({ where: { isActive: true }, orderBy: { name: "asc" } });
  }
  async createFactory(body:any) {
    const name=String(body?.name||"").trim(); if(!name) throw new BadRequestException("Thiếu tên nhà may.");
    return this.prisma.productionPartner.create({data:{
      code:String(body?.code||"").trim().toUpperCase() || await this.nextSimpleCode("factory",name),
      name,contactName:body?.contactName||null,phone:body?.phone||null,email:body?.email||null,address:body?.address||null,note:body?.note||null
    }});
  }
  async updateFactory(id:string,body:any){
    return this.prisma.productionPartner.update({where:{id},data:{
      ...(body?.code!==undefined?{code:String(body.code||"").trim().toUpperCase()}:{ }),
      ...(body?.name!==undefined?{name:String(body.name||"").trim()}:{ }),
      ...(body?.contactName!==undefined?{contactName:body.contactName||null}:{ }),
      ...(body?.phone!==undefined?{phone:body.phone||null}:{ }),
      ...(body?.email!==undefined?{email:body.email||null}:{ }),
      ...(body?.address!==undefined?{address:body.address||null}:{ }),
      ...(body?.note!==undefined?{note:body.note||null}:{ }),
    }});
  }
  deactivateFactory(id:string){return this.prisma.productionPartner.update({where:{id},data:{isActive:false}})}

  listAccessorySuppliers(){return this.prisma.productionAccessorySupplier.findMany({where:{isActive:true},orderBy:{name:"asc"}})}
  async createAccessorySupplier(body:any){
    const name=String(body?.name||"").trim(); if(!name) throw new BadRequestException("Thiếu tên NCC NPL.");
    return this.prisma.productionAccessorySupplier.create({data:{
      code:String(body?.code||"").trim().toUpperCase() || await this.nextSimpleCode("supplier",name),
      name,phone:body?.phone||null,email:body?.email||null,address:body?.address||null,note:body?.note||null
    }});
  }
  async updateAccessorySupplier(id:string,body:any){return this.prisma.productionAccessorySupplier.update({where:{id},data:{
    ...(body?.code!==undefined?{code:String(body.code||"").trim().toUpperCase()}:{ }),
    ...(body?.name!==undefined?{name:String(body.name||"").trim()}:{ }),
    ...(body?.phone!==undefined?{phone:body.phone||null}:{ }),
    ...(body?.email!==undefined?{email:body.email||null}:{ }),
    ...(body?.address!==undefined?{address:body.address||null}:{ }),
    ...(body?.note!==undefined?{note:body.note||null}:{ }),
  }})}
  deactivateAccessorySupplier(id:string){return this.prisma.productionAccessorySupplier.update({where:{id},data:{isActive:false}})}

  listAccessories(query?:any){
    const q=String(query?.q||"").trim();
    return this.prisma.productionAccessoryItem.findMany({where:{
      isActive:true,
      ...(query?.type?{typeName:query.type}:{ }),
      ...(q?{OR:[{code:{contains:q,mode:"insensitive"}},{name:{contains:q,mode:"insensitive"}},{typeName:{contains:q,mode:"insensitive"}}]}:{ }),
    },orderBy:[{typeName:"asc"},{name:"asc"}]});
  }
  async createAccessory(body:any){
    const name=String(body?.name||"").trim(), typeName=String(body?.typeName||"").trim();
    if(!name||!typeName) throw new BadRequestException("Thiếu tên hoặc loại NPL.");
    return this.prisma.productionAccessoryItem.create({data:{
      code:String(body?.code||"").trim().toUpperCase() || await this.nextSimpleCode("item",name),
      name,typeName,imageUrl:body?.imageUrl||null,unit:body?.unit||"PIECE",
      stockQty:this.n(body?.stockQty)||0,unitPrice:this.n(body?.unitPrice),supplierId:body?.supplierId||null,note:body?.note||null
    }});
  }
  async updateAccessory(id:string,body:any){return this.prisma.productionAccessoryItem.update({where:{id},data:{
    ...(body?.code!==undefined?{code:String(body.code||"").trim().toUpperCase()}:{ }),
    ...(body?.name!==undefined?{name:String(body.name||"").trim()}:{ }),
    ...(body?.typeName!==undefined?{typeName:String(body.typeName||"").trim()}:{ }),
    ...(body?.imageUrl!==undefined?{imageUrl:body.imageUrl||null}:{ }),
    ...(body?.unit!==undefined?{unit:body.unit}:{ }),
    ...(body?.stockQty!==undefined?{stockQty:this.n(body.stockQty)||0}:{ }),
    ...(body?.unitPrice!==undefined?{unitPrice:this.n(body.unitPrice)}:{ }),
    ...(body?.supplierId!==undefined?{supplierId:body.supplierId||null}:{ }),
    ...(body?.note!==undefined?{note:body.note||null}:{ }),
  }})}
  async adjustAccessoryStock(id:string,body:any){
    const item=await this.prisma.productionAccessoryItem.findUnique({where:{id}}); if(!item) throw new NotFoundException("Không tìm thấy NPL.");
    const qty=Number(this.n(body?.qty)||0), current=Number(item.stockQty||0), mode=String(body?.mode||"ADD").toUpperCase();
    const next=mode==="SET"?qty:mode==="SUBTRACT"?current-qty:current+qty;
    if(next<0) throw new BadRequestException("Tồn NPL không thể âm.");
    return this.prisma.productionAccessoryItem.update({where:{id},data:{stockQty:next}});
  }

  async getSampleSpec(designSampleId:string){
    const [spec,materials]=await Promise.all([
      this.prisma.sampleProductionSpec.findUnique({where:{designSampleId}}),
      this.prisma.sampleAccessorySpec.findMany({where:{designSampleId},orderBy:{createdAt:"asc"}})
    ]);
    return {spec,materials};
  }
  async saveSampleSpec(designSampleId:string,body:any){
    if(!await this.prisma.designSample.findUnique({where:{id:designSampleId},select:{id:true}})) throw new NotFoundException("Không tìm thấy mẫu.");
    await this.prisma.sampleProductionSpec.upsert({
      where:{designSampleId},
      create:{designSampleId,productKind:body?.productKind||"OTHER",fabricWidthCm:this.n(body?.fabricWidthCm),fabricConsumptionM:this.n(body?.fabricConsumptionM),fabricWastePercent:this.n(body?.fabricWastePercent)||0,sizeSet:body?.sizeSet||null,defaultSizeRatio:body?.defaultSizeRatio||null,note:body?.note||null},
      update:{productKind:body?.productKind||"OTHER",fabricWidthCm:this.n(body?.fabricWidthCm),fabricConsumptionM:this.n(body?.fabricConsumptionM),fabricWastePercent:this.n(body?.fabricWastePercent)||0,sizeSet:body?.sizeSet||null,defaultSizeRatio:body?.defaultSizeRatio||null,note:body?.note||null},
    });
    if(Array.isArray(body?.materials)){
      await this.prisma.$transaction(async(tx:any)=>{
        await tx.sampleAccessorySpec.deleteMany({where:{designSampleId}});
        if(body.materials.length) await tx.sampleAccessorySpec.createMany({data:body.materials.filter((x:any)=>x.accessoryItemId).map((x:any)=>({
          designSampleId,accessoryItemId:x.accessoryItemId,qtyPerProduct:Number(this.n(x.qtyPerProduct)||0),wastePercent:Number(this.n(x.wastePercent)||0),sizeScoped:x.sizeScoped===true,note:x.note||null
        }))});
      });
    }
    return this.getSampleSpec(designSampleId);
  }

  async listOrders(query?:any){
    const q=String(query?.q||"").trim();
    const rows=await this.prisma.productionOrder.findMany({where:{...(query?.status?{status:query.status}:{ }),...(q?{code:{contains:q,mode:"insensitive"}}:{ })},orderBy:{updatedAt:"desc"}});
    const sampleIds=[...new Set(rows.map((x:any)=>x.designSampleId))], factoryIds=[...new Set(rows.map((x:any)=>x.productionPartnerId))];
    const [samples,factories]=await Promise.all([
      sampleIds.length?this.prisma.designSample.findMany({where:{id:{in:sampleIds}},select:{id:true,code:true,name:true,coverImageUrl:true}}):[],
      factoryIds.length?this.prisma.productionPartner.findMany({where:{id:{in:factoryIds}},select:{id:true,code:true,name:true}}):[]
    ]);
    return rows.map((r:any)=>({...r,sample:samples.find((x:any)=>x.id===r.designSampleId)||null,factory:factories.find((x:any)=>x.id===r.productionPartnerId)||null}));
  }

  async getOrder(id:string){
    const order=await this.prisma.productionOrder.findUnique({where:{id}}); if(!order) throw new NotFoundException("Không tìm thấy lệnh SX.");
    const [sample,factory,rolls,sizes,materials]=await Promise.all([
      this.prisma.designSample.findUnique({where:{id:order.designSampleId},select:{id:true,code:true,name:true,category:true,coverImageUrl:true}}),
      this.prisma.productionPartner.findUnique({where:{id:order.productionPartnerId}}),
      this.prisma.productionOrderRoll.findMany({where:{productionOrderId:id},orderBy:{createdAt:"asc"}}),
      this.prisma.productionSizePlan.findMany({where:{productionOrderId:id},orderBy:[{colorName:"asc"},{size:"asc"}]}),
      this.prisma.productionMaterialCalc.findMany({where:{productionOrderId:id},orderBy:[{accessoryName:"asc"},{sizeLabel:"asc"}]})
    ]);
    return {...order,sample,factory,rolls,sizes,materials};
  }

  async createOrder(body:any,user?:Actor){
    const designSampleId=String(body?.designSampleId||""), productionPartnerId=String(body?.productionPartnerId||"");
    if(!designSampleId||!productionPartnerId) throw new BadRequestException("Chưa chọn mẫu hoặc nhà may.");
    const [sample,factory,spec]=await Promise.all([
      this.prisma.designSample.findUnique({where:{id:designSampleId}}),
      this.prisma.productionPartner.findUnique({where:{id:productionPartnerId}}),
      this.prisma.sampleProductionSpec.findUnique({where:{designSampleId}})
    ]);
    if(!sample) throw new NotFoundException("Không tìm thấy mẫu."); if(!factory) throw new NotFoundException("Không tìm thấy nhà may.");
    const actor=this.actor(user);
    return this.prisma.productionOrder.create({data:{
      code:String(body?.code||"").trim().toUpperCase() || await this.nextOrderCode(),
      designSampleId,productionPartnerId,status:body?.status||"DRAFT",
      plannedStartAt:body?.plannedStartAt?new Date(body.plannedStartAt):null,dueDate:body?.dueDate?new Date(body.dueDate):null,
      productKind:body?.productKind||spec?.productKind||"OTHER",fabricWidthCm:this.n(body?.fabricWidthCm)??spec?.fabricWidthCm??null,
      fabricConsumptionM:this.n(body?.fabricConsumptionM)??spec?.fabricConsumptionM??null,fabricWastePercent:this.n(body?.fabricWastePercent)??spec?.fabricWastePercent??0,
      sizeSet:body?.sizeSet||spec?.sizeSet||null,sizeRatio:body?.sizeRatio||spec?.defaultSizeRatio||null,plannedQtyOverride:body?.plannedQtyOverride?Number(body.plannedQtyOverride):null,
      note:body?.note||null,createdById:actor.id,createdByName:actor.name
    }});
  }

  async updateOrder(id:string,body:any){return this.prisma.productionOrder.update({where:{id},data:{
    ...(body?.productionPartnerId!==undefined?{productionPartnerId:body.productionPartnerId}:{ }),
    ...(body?.status!==undefined?{status:body.status}:{ }),
    ...(body?.plannedStartAt!==undefined?{plannedStartAt:body.plannedStartAt?new Date(body.plannedStartAt):null}:{ }),
    ...(body?.dueDate!==undefined?{dueDate:body.dueDate?new Date(body.dueDate):null}:{ }),
    ...(body?.productKind!==undefined?{productKind:body.productKind}:{ }),
    ...(body?.fabricWidthCm!==undefined?{fabricWidthCm:this.n(body.fabricWidthCm)}:{ }),
    ...(body?.fabricConsumptionM!==undefined?{fabricConsumptionM:this.n(body.fabricConsumptionM)}:{ }),
    ...(body?.fabricWastePercent!==undefined?{fabricWastePercent:this.n(body.fabricWastePercent)||0}:{ }),
    ...(body?.sizeSet!==undefined?{sizeSet:body.sizeSet||null}:{ }),
    ...(body?.sizeRatio!==undefined?{sizeRatio:body.sizeRatio||null}:{ }),
    ...(body?.plannedQtyOverride!==undefined?{plannedQtyOverride:body.plannedQtyOverride?Number(body.plannedQtyOverride):null}:{ }),
    ...(body?.note!==undefined?{note:body.note||null}:{ }),
  }})}

  async setOrderRolls(id:string,body:any){
    if(!await this.prisma.productionOrder.findUnique({where:{id},select:{id:true}})) throw new NotFoundException("Không tìm thấy lệnh SX.");
    const rows=Array.isArray(body?.rolls)?body.rolls:[], ids=rows.map((x:any)=>x.fabricReceiptRollId).filter(Boolean);
    const src=ids.length?await this.prisma.fabricReceiptRoll.findMany({where:{id:{in:ids}},include:{fabricReceipt:true,images:{orderBy:{createdAt:"desc"},take:1}}}):[];
    await this.prisma.$transaction(async(tx:any)=>{
      await tx.productionOrderRoll.deleteMany({where:{productionOrderId:id}});
      if(rows.length) await tx.productionOrderRoll.createMany({data:rows.map((x:any)=>{
        const r:any=src.find((y:any)=>y.id===x.fabricReceiptRollId); if(!r) throw new BadRequestException("Cây vải không tồn tại.");
        const availableM=Number(r.actualM||0),availableKg=Number(r.actualKg||0),allocatedM=Number(this.n(x.allocatedM)??availableM),allocatedKg=Number(this.n(x.allocatedKg)??availableKg);
        if(allocatedM>availableM+0.0001||allocatedKg>availableKg+0.0001) throw new BadRequestException(`Phân bổ vượt cây ${r.rollCode||r.id}.`);
        return {productionOrderId:id,fabricReceiptRollId:r.id,fabricReceiptId:r.fabricReceiptId,rollCode:r.rollCode,colorName:r.colorName||r.fabricReceipt?.colorName||null,colorCode:r.colorCode||r.fabricReceipt?.colorCode||null,availableM,availableKg,allocatedM,allocatedKg,imageUrl:r.images?.[0]?.url||null};
      })});
    });
    return this.getOrder(id);
  }

  private distribute(total:number,ratio:Record<string,number>){
    const entries=Object.entries(ratio).filter(([,v])=>Number(v)>0), sum=entries.reduce((s,[,v])=>s+Number(v),0);
    if(!sum||total<=0)return Object.fromEntries(entries.map(([k])=>[k,0]));
    const exact=entries.map(([k,v])=>({k,x:total*Number(v)/sum})), result:Record<string,number>=Object.fromEntries(exact.map(({k,x})=>[k,Math.floor(x)]));
    let remain=total-Object.values(result).reduce((a,b)=>a+b,0);
    exact.sort((a,b)=>(b.x-Math.floor(b.x))-(a.x-Math.floor(a.x)));
    for(let i=0;remain>0;i=(i+1)%exact.length,remain--)result[exact[i].k]+=1;
    return result;
  }

  async calculateOrder(id:string){
    const order=await this.prisma.productionOrder.findUnique({where:{id}});if(!order)throw new NotFoundException("Không tìm thấy lệnh SX.");
    const rolls=await this.prisma.productionOrderRoll.findMany({where:{productionOrderId:id}});
    const consumption=Number(order.fabricConsumptionM||0);if(consumption<=0)throw new BadRequestException("Chưa nhập định mức vải / sản phẩm.");
    const effective=consumption*(1+Number(order.fabricWastePercent||0)/100), grouped=new Map<string,any>();
    for(const r of rolls as any[]){const key=`${r.colorName||"Không màu"}|||${r.colorCode||""}`,x=grouped.get(key)||{colorName:r.colorName||"Không màu",colorCode:r.colorCode,meters:0};x.meters+=Number(r.allocatedM||0);grouped.set(key,x)}
    const ratio=(order.sizeRatio&&typeof order.sizeRatio==="object"?order.sizeRatio:{}) as Record<string,number>;
    const colors=[...grouped.values()].map((x:any)=>{const plannedQty=Math.floor(x.meters/effective);return {...x,plannedQty,sizes:this.distribute(plannedQty,ratio)}});
    const totalQty=order.plannedQtyOverride||colors.reduce((s:number,x:any)=>s+x.plannedQty,0), totalsBySize:Record<string,number>={};
    colors.forEach((c:any)=>Object.entries(c.sizes).forEach(([size,qty])=>totalsBySize[size]=(totalsBySize[size]||0)+Number(qty)));
    const specs=await this.prisma.sampleAccessorySpec.findMany({where:{designSampleId:order.designSampleId}}),ids=specs.map((x:any)=>x.accessoryItemId);
    const items=ids.length?await this.prisma.productionAccessoryItem.findMany({where:{id:{in:ids}}}):[], materials:any[]=[];
    for(const s of specs as any[]){const item:any=items.find((x:any)=>x.id===s.accessoryItemId);if(!item)continue;const per=Number(s.qtyPerProduct||0),w=Number(s.wastePercent||0),stock=Number(item.stockQty||0);
      if(s.sizeScoped&&Object.keys(totalsBySize).length){for(const [size,qty] of Object.entries(totalsBySize)){const base=Number(qty)*per,required=Math.ceil(base*(1+w/100)*1000)/1000;materials.push({accessoryItemId:item.id,accessoryCode:item.code,accessoryName:item.name,unit:item.unit,sizeLabel:size,qtyPerProduct:per,wastePercent:w,baseQty:base,requiredQty:required,stockQtySnapshot:stock,shortageQty:Math.max(0,required-stock)})}}
      else{const base=totalQty*per,required=Math.ceil(base*(1+w/100)*1000)/1000;materials.push({accessoryItemId:item.id,accessoryCode:item.code,accessoryName:item.name,unit:item.unit,sizeLabel:null,qtyPerProduct:per,wastePercent:w,baseQty:base,requiredQty:required,stockQtySnapshot:stock,shortageQty:Math.max(0,required-stock)})}
    }
    await this.prisma.$transaction(async(tx:any)=>{
      await tx.productionSizePlan.deleteMany({where:{productionOrderId:id}});
      const sizeRows=colors.flatMap((c:any)=>Object.entries(c.sizes).map(([size,qty])=>({productionOrderId:id,colorName:c.colorName,colorCode:c.colorCode||null,size,ratio:Number(ratio[size]||0),plannedQty:Number(qty)})));
      if(sizeRows.length)await tx.productionSizePlan.createMany({data:sizeRows});
      await tx.productionMaterialCalc.deleteMany({where:{productionOrderId:id}});
      if(materials.length)await tx.productionMaterialCalc.createMany({data:materials.map(x=>({productionOrderId:id,...x}))});
      if(order.status==="DRAFT")await tx.productionOrder.update({where:{id},data:{status:"PLANNING"}});
    });
    return {totalQty,effectiveConsumptionM:effective,colors,materials};
  }

  async printPayload(id:string){return {...await this.getOrder(id),generatedAt:new Date().toISOString(),confirmation:{the1970:"The 1970 xác nhận",factory:"Nhà may / xưởng xác nhận"}}}
}
