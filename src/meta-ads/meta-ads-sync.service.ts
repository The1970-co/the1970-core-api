import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { SyncMetaAdsDto, MetaInsightLevel } from './dto/sync-meta-ads.dto';

type GraphListResponse<T> = {
  data?: T[];
  paging?: { next?: string };
  error?: {
    message?: string;
    type?: string;
    code?: number;
    error_subcode?: number;
    fbtrace_id?: string;
  };
};

type MetaDateRange = { since: string; until: string };

@Injectable()
export class MetaAdsSyncService {
  private readonly logger = new Logger(MetaAdsSyncService.name);

  constructor(private readonly prisma: PrismaService) {}

  private get version() {
    return process.env.META_API_VERSION || process.env.META_GRAPH_API_VERSION || 'v25.0';
  }

  private get accessToken() {
    return process.env.META_ACCESS_TOKEN || '';
  }

  private get defaultAdAccountId() {
    return process.env.META_AD_ACCOUNT_ID || 'act_474042859768081';
  }

  private get defaultAccountName() {
    return process.env.META_AD_ACCOUNT_NAME || 'Nam Nguyen';
  }

  private n(value: any) {
    const parsed = Number(value || 0);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  private parseDate(value?: string | null) {
    if (!value) return null;
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? null : date;
  }

  private toDateInput(date: Date) {
    const yyyy = date.getFullYear();
    const mm = String(date.getMonth() + 1).padStart(2, '0');
    const dd = String(date.getDate()).padStart(2, '0');
    return `${yyyy}-${mm}-${dd}`;
  }

  private getDateRange(input?: SyncMetaAdsDto): MetaDateRange {
    if (input?.range === 'custom' && input.fromDate && input.toDate) {
      return { since: input.fromDate, until: input.toDate };
    }

    const range = input?.range || '7d';
    const end = new Date();
    const start = new Date();

    if (range === 'yesterday') {
      start.setDate(start.getDate() - 1);
      end.setDate(end.getDate() - 1);
    } else if (range === '7d') {
      start.setDate(start.getDate() - 6);
    } else if (range === '10d') {
      start.setDate(start.getDate() - 9);
    } else if (range === '30d') {
      start.setDate(start.getDate() - 29);
    }

    return { since: this.toDateInput(start), until: this.toDateInput(end) };
  }

  private async graphGet<T>(path: string, params: Record<string, string>) {
    if (!this.accessToken) {
      throw new Error('META_ACCESS_TOKEN is missing');
    }

    const url = new URL(`https://graph.facebook.com/${this.version}${path}`);
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== '') {
        url.searchParams.set(key, value);
      }
    });
    url.searchParams.set('access_token', this.accessToken);

    const res = await fetch(url.toString(), { method: 'GET' });
    const json = (await res.json()) as T & { error?: any };

    if (!res.ok || (json as any).error) {
      this.logger.error(`[MetaAdsSync] API error: ${JSON.stringify((json as any).error || json)}`);
      throw new Error((json as any).error?.message || 'Meta Ads API error');
    }

    return json;
  }

  private async graphList<T>(path: string, params: Record<string, string>, maxPages = 20) {
    const first = new URL(`https://graph.facebook.com/${this.version}${path}`);
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== '') first.searchParams.set(key, value);
    });
    first.searchParams.set('access_token', this.accessToken);

    const rows: T[] = [];
    let nextUrl: string | undefined = first.toString();
    let page = 0;

    while (nextUrl && page < maxPages) {
      page += 1;
      const res = await fetch(nextUrl, { method: 'GET' });
      const json = (await res.json()) as GraphListResponse<T>;
      if (!res.ok || json.error) {
        this.logger.error(`[MetaAdsSync] API list error: ${JSON.stringify(json.error || json)}`);
        throw new Error(json.error?.message || 'Meta Ads API error');
      }
      if (Array.isArray(json.data)) rows.push(...json.data);
      nextUrl = json.paging?.next;
    }

    return rows;
  }

  private normalizeAccountId(value?: string | null) {
    const raw = String(value || this.defaultAdAccountId || '').trim();
    if (!raw) return '';
    return raw.startsWith('act_') ? raw : `act_${raw}`;
  }

  private pickActionCount(actions: any[] | undefined, names: string[]) {
    if (!Array.isArray(actions)) return 0;
    const wanted = names.map((name) => name.toLowerCase());
    return actions.reduce((sum, action) => {
      const type = String(action?.action_type || '').toLowerCase();
      return wanted.includes(type) ? sum + this.n(action?.value) : sum;
    }, 0);
  }

  private pickActionValue(actions: any[] | undefined, names: string[]) {
    return this.pickActionCount(actions, names);
  }

  private calcRoas(spend: number, purchaseValue: number) {
    return spend > 0 ? purchaseValue / spend : 0;
  }

  async listMetaAccounts() {
    const result = await this.graphGet<{ data: any[] }>('/me/adaccounts', {
      fields: 'id,name,account_id,currency,account_status,timezone_name,business{id,name}',
      limit: '100',
    });
    return result.data || [];
  }

  async syncAccount(metaAccountId = this.defaultAdAccountId) {
    const accountId = this.normalizeAccountId(metaAccountId);
    const row = await this.graphGet<any>(`/${accountId}`, {
      fields: 'id,name,account_id,currency,account_status,timezone_name,business{id,name}',
    });

    return this.prisma.metaAdAccount.upsert({
      where: { metaAccountId: row.id || accountId },
      update: {
        accountId: row.account_id || null,
        name: row.name || this.defaultAccountName,
        currency: row.currency || null,
        timezoneName: row.timezone_name || null,
        accountStatus: row.account_status != null ? String(row.account_status) : null,
        businessId: row.business?.id || null,
        businessName: row.business?.name || null,
        rawJson: row,
        lastSyncedAt: new Date(),
      },
      create: {
        metaAccountId: row.id || accountId,
        accountId: row.account_id || null,
        name: row.name || this.defaultAccountName,
        currency: row.currency || null,
        timezoneName: row.timezone_name || null,
        accountStatus: row.account_status != null ? String(row.account_status) : null,
        businessId: row.business?.id || null,
        businessName: row.business?.name || null,
        rawJson: row,
        lastSyncedAt: new Date(),
      },
    } as any);
  }

  async syncStructure(metaAccountId = this.defaultAdAccountId, limit = 500) {
    const account = await this.syncAccount(metaAccountId);
    const accountId = this.normalizeAccountId(account.metaAccountId);
    const now = new Date();

    const campaigns = await this.graphList<any>(`/${accountId}/campaigns`, {
      fields: 'id,name,status,effective_status,objective,buying_type,daily_budget,lifetime_budget,start_time,stop_time,updated_time,created_time',
      limit: String(Math.min(Math.max(limit, 50), 1000)),
    });

    let campaignCount = 0;
    for (const row of campaigns) {
      await this.prisma.metaCampaign.upsert({
        where: { metaCampaignId: row.id },
        update: {
          metaAccountId: account.metaAccountId,
          name: row.name || null,
          status: row.status || null,
          effectiveStatus: row.effective_status || null,
          objective: row.objective || null,
          buyingType: row.buying_type || null,
          dailyBudget: row.daily_budget != null ? this.n(row.daily_budget) : null,
          lifetimeBudget: row.lifetime_budget != null ? this.n(row.lifetime_budget) : null,
          startTime: this.parseDate(row.start_time),
          stopTime: this.parseDate(row.stop_time),
          rawJson: row,
          lastSyncedAt: now,
        },
        create: {
          metaAccountId: account.metaAccountId,
          metaCampaignId: row.id,
          name: row.name || null,
          status: row.status || null,
          effectiveStatus: row.effective_status || null,
          objective: row.objective || null,
          buyingType: row.buying_type || null,
          dailyBudget: row.daily_budget != null ? this.n(row.daily_budget) : null,
          lifetimeBudget: row.lifetime_budget != null ? this.n(row.lifetime_budget) : null,
          startTime: this.parseDate(row.start_time),
          stopTime: this.parseDate(row.stop_time),
          rawJson: row,
          lastSyncedAt: now,
        },
      } as any);
      campaignCount += 1;
    }

    const adSets = await this.graphList<any>(`/${accountId}/adsets`, {
      fields: 'id,name,campaign_id,status,effective_status,optimization_goal,billing_event,bid_strategy,daily_budget,lifetime_budget,start_time,end_time,targeting,updated_time,created_time',
      limit: String(Math.min(Math.max(limit, 50), 1000)),
    });

    let adSetCount = 0;
    for (const row of adSets) {
      await this.prisma.metaAdSet.upsert({
        where: { metaAdSetId: row.id },
        update: {
          metaAccountId: account.metaAccountId,
          metaCampaignId: row.campaign_id || null,
          name: row.name || null,
          status: row.status || null,
          effectiveStatus: row.effective_status || null,
          optimizationGoal: row.optimization_goal || null,
          billingEvent: row.billing_event || null,
          bidStrategy: row.bid_strategy || null,
          dailyBudget: row.daily_budget != null ? this.n(row.daily_budget) : null,
          lifetimeBudget: row.lifetime_budget != null ? this.n(row.lifetime_budget) : null,
          startTime: this.parseDate(row.start_time),
          endTime: this.parseDate(row.end_time),
          targetingJson: row.targeting || undefined,
          rawJson: row,
          lastSyncedAt: now,
        },
        create: {
          metaAccountId: account.metaAccountId,
          metaCampaignId: row.campaign_id || null,
          metaAdSetId: row.id,
          name: row.name || null,
          status: row.status || null,
          effectiveStatus: row.effective_status || null,
          optimizationGoal: row.optimization_goal || null,
          billingEvent: row.billing_event || null,
          bidStrategy: row.bid_strategy || null,
          dailyBudget: row.daily_budget != null ? this.n(row.daily_budget) : null,
          lifetimeBudget: row.lifetime_budget != null ? this.n(row.lifetime_budget) : null,
          startTime: this.parseDate(row.start_time),
          endTime: this.parseDate(row.end_time),
          targetingJson: row.targeting || undefined,
          rawJson: row,
          lastSyncedAt: now,
        },
      } as any);
      adSetCount += 1;
    }

    const ads = await this.graphList<any>(`/${accountId}/ads`, {
      fields: 'id,name,campaign_id,adset_id,status,effective_status,creative{id,name,thumbnail_url,image_url,video_id,object_story_id,object_story_spec,call_to_action_type},preview_shareable_link,updated_time,created_time',
      limit: String(Math.min(Math.max(limit, 50), 1000)),
    });

    let adCount = 0;
    for (const row of ads) {
      const creative = row.creative || {};
      const storySpec = creative.object_story_spec || {};
      await this.prisma.metaAd.upsert({
        where: { metaAdId: row.id },
        update: {
          metaAccountId: account.metaAccountId,
          metaCampaignId: row.campaign_id || null,
          metaAdSetId: row.adset_id || null,
          metaCreativeId: creative.id || null,
          name: row.name || null,
          status: row.status || null,
          effectiveStatus: row.effective_status || null,
          previewShareableLink: row.preview_shareable_link || null,
          thumbnailUrl: creative.thumbnail_url || null,
          imageUrl: creative.image_url || null,
          videoId: creative.video_id || storySpec?.video_data?.video_id || null,
          postId: creative.object_story_id || null,
          pageId: storySpec?.page_id || null,
          callToActionType: creative.call_to_action_type || storySpec?.link_data?.call_to_action?.type || null,
          creativeJson: creative || undefined,
          rawJson: row,
          lastSyncedAt: now,
        },
        create: {
          metaAccountId: account.metaAccountId,
          metaCampaignId: row.campaign_id || null,
          metaAdSetId: row.adset_id || null,
          metaAdId: row.id,
          metaCreativeId: creative.id || null,
          name: row.name || null,
          status: row.status || null,
          effectiveStatus: row.effective_status || null,
          previewShareableLink: row.preview_shareable_link || null,
          thumbnailUrl: creative.thumbnail_url || null,
          imageUrl: creative.image_url || null,
          videoId: creative.video_id || storySpec?.video_data?.video_id || null,
          postId: creative.object_story_id || null,
          pageId: storySpec?.page_id || null,
          callToActionType: creative.call_to_action_type || storySpec?.link_data?.call_to_action?.type || null,
          creativeJson: creative || undefined,
          rawJson: row,
          lastSyncedAt: now,
        },
      } as any);
      adCount += 1;
    }

    return { account, campaignCount, adSetCount, adCount };
  }

  async syncInsights(input: SyncMetaAdsDto = {}) {
    const account = await this.syncAccount();
    const accountId = this.normalizeAccountId(account.metaAccountId);
    const dateRange = this.getDateRange(input);
    const levels: MetaInsightLevel[] = input.levels?.length ? input.levels : ['campaign', 'adset', 'ad'];
    const now = new Date();
    let totalRows = 0;

    const fieldsByLevel: Record<MetaInsightLevel, string> = {
      campaign: 'date_start,date_stop,campaign_id,campaign_name,spend,impressions,reach,clicks,inline_link_clicks,cpc,cpm,ctr,actions,action_values,purchase_roas',
      adset: 'date_start,date_stop,campaign_id,campaign_name,adset_id,adset_name,spend,impressions,reach,clicks,inline_link_clicks,cpc,cpm,ctr,actions,action_values,purchase_roas',
      ad: 'date_start,date_stop,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,spend,impressions,reach,clicks,inline_link_clicks,cpc,cpm,ctr,actions,action_values,purchase_roas',
    };

    for (const level of levels) {
      const rows = await this.graphList<any>(`/${accountId}/insights`, {
        fields: fieldsByLevel[level],
        level,
        time_increment: '1',
        time_range: JSON.stringify(dateRange),
        limit: String(Math.min(Math.max(Number(input.limit || 500), 50), 1000)),
      }, 30);

      for (const row of rows) {
        if (row.campaign_id) {
          await this.prisma.metaCampaign.upsert({
            where: { metaCampaignId: row.campaign_id },
            update: { metaAccountId: account.metaAccountId, name: row.campaign_name || undefined, lastSyncedAt: now },
            create: { metaAccountId: account.metaAccountId, metaCampaignId: row.campaign_id, name: row.campaign_name || null, lastSyncedAt: now },
          } as any);
        }
        if (row.adset_id) {
          await this.prisma.metaAdSet.upsert({
            where: { metaAdSetId: row.adset_id },
            update: { metaAccountId: account.metaAccountId, metaCampaignId: row.campaign_id || null, name: row.adset_name || undefined, lastSyncedAt: now },
            create: { metaAccountId: account.metaAccountId, metaCampaignId: row.campaign_id || null, metaAdSetId: row.adset_id, name: row.adset_name || null, lastSyncedAt: now },
          } as any);
        }
        if (row.ad_id) {
          await this.prisma.metaAd.upsert({
            where: { metaAdId: row.ad_id },
            update: { metaAccountId: account.metaAccountId, metaCampaignId: row.campaign_id || null, metaAdSetId: row.adset_id || null, name: row.ad_name || undefined, lastSyncedAt: now },
            create: { metaAccountId: account.metaAccountId, metaCampaignId: row.campaign_id || null, metaAdSetId: row.adset_id || null, metaAdId: row.ad_id, name: row.ad_name || null, lastSyncedAt: now },
          } as any);
        }

        const dateStart = new Date(`${String(row.date_start).slice(0, 10)}T00:00:00.000Z`);
        const dateStop = new Date(`${String(row.date_stop || row.date_start).slice(0, 10)}T00:00:00.000Z`);
        const actions = Array.isArray(row.actions) ? row.actions : [];
        const actionValues = Array.isArray(row.action_values) ? row.action_values : [];
        const spend = this.n(row.spend);
        const purchases = this.pickActionCount(actions, ['purchase', 'omni_purchase', 'offsite_conversion.fb_pixel_purchase']);
        const purchaseValue = this.pickActionValue(actionValues, ['purchase', 'omni_purchase', 'offsite_conversion.fb_pixel_purchase']);
        const purchaseRoas = Array.isArray(row.purchase_roas) ? this.n(row.purchase_roas?.[0]?.value) : 0;
        const costPerPurchase = purchases > 0 ? spend / purchases : 0;
        const roas = purchaseRoas || this.calcRoas(spend, purchaseValue);

        const existing = await this.prisma.metaAdInsightDaily.findFirst({
          where: {
            metaAccountId: account.metaAccountId,
            level,
            dateStart,
            metaCampaignId: row.campaign_id || null,
            metaAdSetId: row.adset_id || null,
            metaAdId: row.ad_id || null,
          },
          select: { id: true },
        });

        const data = {
          metaAccountId: account.metaAccountId,
          level,
          dateStart,
          dateStop,
          metaCampaignId: row.campaign_id || null,
          metaAdSetId: row.adset_id || null,
          metaAdId: row.ad_id || null,
          campaignName: row.campaign_name || null,
          adSetName: row.adset_name || null,
          adName: row.ad_name || null,
          spend,
          impressions: Math.round(this.n(row.impressions)),
          reach: Math.round(this.n(row.reach)),
          clicks: Math.round(this.n(row.clicks)),
          inlineLinkClicks: Math.round(this.n(row.inline_link_clicks)),
          cpc: this.n(row.cpc),
          cpm: this.n(row.cpm),
          ctr: this.n(row.ctr),
          purchases: Math.round(purchases),
          purchaseValue,
          costPerPurchase,
          roas,
          actionsJson: row.actions || undefined,
          actionValuesJson: row.action_values || undefined,
          rawJson: row,
          syncedAt: now,
        };

        if (existing) {
          await this.prisma.metaAdInsightDaily.update({ where: { id: existing.id }, data } as any);
        } else {
          await this.prisma.metaAdInsightDaily.create({ data } as any);
        }
        totalRows += 1;
      }
    }

    return { account, range: dateRange, levels, insightRows: totalRows };
  }

  async syncAll(input: SyncMetaAdsDto = {}, user?: any) {
    const started = Date.now();
    const range = this.getDateRange(input);
    const log = await this.prisma.metaSyncLog.create({
      data: {
        metaAccountId: this.normalizeAccountId(this.defaultAdAccountId),
        syncType: 'META_ADS_BRAIN_CENTER',
        status: 'RUNNING',
        range: input.range || '7d',
        fromDate: new Date(`${range.since}T00:00:00.000Z`),
        toDate: new Date(`${range.until}T00:00:00.000Z`),
        createdById: user?.id || user?.sub || null,
        createdByName: user?.name || user?.code || null,
      },
    } as any);

    try {
      const structure = input.includeStructure === false ? null : await this.syncStructure(undefined, Number(input.limit || 500));
      const insights = input.includeInsights === false ? null : await this.syncInsights(input);
      const scanned = (structure?.campaignCount || 0) + (structure?.adSetCount || 0) + (structure?.adCount || 0) + (insights?.insightRows || 0);

      await this.prisma.metaSyncLog.update({
        where: { id: log.id },
        data: {
          status: 'SUCCESS',
          finishedAt: new Date(),
          durationMs: Date.now() - started,
          scanned,
          upserted: scanned,
          message: 'Sync Meta Ads Brain Center thành công',
        },
      } as any);

      return { ok: true, logId: log.id, structure, insights, durationMs: Date.now() - started };
    } catch (error: any) {
      await this.prisma.metaSyncLog.update({
        where: { id: log.id },
        data: {
          status: 'FAILED',
          finishedAt: new Date(),
          durationMs: Date.now() - started,
          failed: 1,
          message: error?.message || String(error),
          errorJson: { message: error?.message || String(error), stack: error?.stack || null },
        },
      } as any);
      throw error;
    }
  }

  private pagination(query: { page?: string; limit?: string }) {
    const page = Math.max(1, Number(query.page || 1));
    const limit = Math.min(Math.max(Number(query.limit || 50), 1), 200);
    return { page, limit, skip: (page - 1) * limit };
  }

  async getAccounts() {
    return this.prisma.metaAdAccount.findMany({ orderBy: { updatedAt: 'desc' } });
  }

  async getCampaigns(query: any = {}) {
    const { page, limit, skip } = this.pagination(query);
    const where: any = {};
    if (query.status) where.effectiveStatus = query.status;
    if (query.search) where.name = { contains: query.search, mode: 'insensitive' };
    const [items, total] = await Promise.all([
      this.prisma.metaCampaign.findMany({ where, orderBy: { updatedAt: 'desc' }, skip, take: limit }),
      this.prisma.metaCampaign.count({ where }),
    ]);
    return { items, total, page, limit };
  }

  async getAdSets(query: any = {}) {
    const { page, limit, skip } = this.pagination(query);
    const where: any = {};
    if (query.status) where.effectiveStatus = query.status;
    if (query.search) where.name = { contains: query.search, mode: 'insensitive' };
    const [items, total] = await Promise.all([
      this.prisma.metaAdSet.findMany({ where, orderBy: { updatedAt: 'desc' }, skip, take: limit }),
      this.prisma.metaAdSet.count({ where }),
    ]);
    return { items, total, page, limit };
  }

  async getAds(query: any = {}) {
    const { page, limit, skip } = this.pagination(query);
    const where: any = {};
    if (query.status) where.effectiveStatus = query.status;
    if (query.search) where.name = { contains: query.search, mode: 'insensitive' };
    const [items, total] = await Promise.all([
      this.prisma.metaAd.findMany({ where, orderBy: { updatedAt: 'desc' }, skip, take: limit }),
      this.prisma.metaAd.count({ where }),
    ]);
    return { items, total, page, limit };
  }

  async getInsights(query: any = {}) {
    const { page, limit, skip } = this.pagination(query);
    const range = this.getDateRange(query);
    const where: any = {
      dateStart: {
        gte: new Date(`${range.since}T00:00:00.000Z`),
        lte: new Date(`${range.until}T23:59:59.999Z`),
      },
    };
    if (query.level) where.level = query.level;
    if (query.search) {
      where.OR = [
        { campaignName: { contains: query.search, mode: 'insensitive' } },
        { adSetName: { contains: query.search, mode: 'insensitive' } },
        { adName: { contains: query.search, mode: 'insensitive' } },
      ];
    }

    const [items, total, summary] = await Promise.all([
      this.prisma.metaAdInsightDaily.findMany({ where, orderBy: [{ dateStart: 'desc' }, { spend: 'desc' }], skip, take: limit }),
      this.prisma.metaAdInsightDaily.count({ where }),
      this.prisma.metaAdInsightDaily.aggregate({ where, _sum: { spend: true, impressions: true, reach: true, clicks: true, purchases: true, purchaseValue: true } }),
    ]);

    const spend = this.n(summary._sum.spend);
    const purchaseValue = this.n(summary._sum.purchaseValue);
    return {
      items,
      total,
      page,
      limit,
      summary: {
        spend,
        impressions: this.n(summary._sum.impressions),
        reach: this.n(summary._sum.reach),
        clicks: this.n(summary._sum.clicks),
        purchases: this.n(summary._sum.purchases),
        purchaseValue,
        roas: spend > 0 ? purchaseValue / spend : 0,
      },
    };
  }

  async getSyncLogs(query: any = {}) {
    const { page, limit, skip } = this.pagination(query);
    const [items, total] = await Promise.all([
      this.prisma.metaSyncLog.findMany({ orderBy: { startedAt: 'desc' }, skip, take: limit }),
      this.prisma.metaSyncLog.count(),
    ]);
    return { items, total, page, limit };
  }

  private buildInsightWhere(query: any = {}, level?: 'campaign' | 'adset' | 'ad') {
    const range = this.getDateRange(query);
    const where: any = {
      dateStart: {
        gte: new Date(`${range.since}T00:00:00.000Z`),
        lte: new Date(`${range.until}T23:59:59.999Z`),
      },
    };

    if (level || query.level) where.level = level || query.level;
    if (query.metaAccountId) where.metaAccountId = this.normalizeAccountId(query.metaAccountId);
    if (query.metaCampaignId) where.metaCampaignId = String(query.metaCampaignId);
    if (query.metaAdSetId) where.metaAdSetId = String(query.metaAdSetId);
    if (query.metaAdId) where.metaAdId = String(query.metaAdId);
    if (query.search) {
      where.OR = [
        { campaignName: { contains: String(query.search), mode: 'insensitive' } },
        { adSetName: { contains: String(query.search), mode: 'insensitive' } },
        { adName: { contains: String(query.search), mode: 'insensitive' } },
      ];
    }

    return { where, range };
  }

  private metricsFromSum(sum: any) {
    const spend = this.n(sum?.spend);
    const impressions = this.n(sum?.impressions);
    const reach = this.n(sum?.reach);
    const clicks = this.n(sum?.clicks);
    const inlineLinkClicks = this.n(sum?.inlineLinkClicks);
    const purchases = this.n(sum?.purchases);
    const purchaseValue = this.n(sum?.purchaseValue);
    return {
      spend,
      impressions,
      reach,
      clicks,
      inlineLinkClicks,
      purchases,
      purchaseValue,
      ctr: impressions > 0 ? (clicks / impressions) * 100 : 0,
      cpc: clicks > 0 ? spend / clicks : 0,
      cpm: impressions > 0 ? (spend / impressions) * 1000 : 0,
      costPerPurchase: purchases > 0 ? spend / purchases : 0,
      roas: spend > 0 ? purchaseValue / spend : 0,
    };
  }

  private async getTopInsightGroups(level: 'campaign' | 'adset' | 'ad', query: any = {}, take = 20) {
    const { where } = this.buildInsightWhere(query, level);
    const by = level === 'campaign'
      ? ['metaCampaignId', 'campaignName']
      : level === 'adset'
        ? ['metaAdSetId', 'adSetName', 'metaCampaignId', 'campaignName']
        : ['metaAdId', 'adName', 'metaAdSetId', 'adSetName', 'metaCampaignId', 'campaignName'];

    if (level === 'campaign') where.metaCampaignId = { not: null };
    if (level === 'adset') where.metaAdSetId = { not: null };
    if (level === 'ad') where.metaAdId = { not: null };

    const rows = await (this.prisma as any).metaAdInsightDaily.groupBy({
      by,
      where,
      _sum: {
        spend: true,
        impressions: true,
        reach: true,
        clicks: true,
        inlineLinkClicks: true,
        purchases: true,
        purchaseValue: true,
      },
      orderBy: { _sum: { spend: 'desc' } },
      take,
    });

    const ids = rows
      .map((row: any) => level === 'campaign' ? row.metaCampaignId : level === 'adset' ? row.metaAdSetId : row.metaAdId)
      .filter(Boolean);

    const structureRows = level === 'campaign'
      ? await (this.prisma as any).metaCampaign.findMany({ where: { metaCampaignId: { in: ids } } })
      : level === 'adset'
        ? await (this.prisma as any).metaAdSet.findMany({ where: { metaAdSetId: { in: ids } }, include: { campaign: true } })
        : await (this.prisma as any).metaAd.findMany({ where: { metaAdId: { in: ids } }, include: { campaign: true, adSet: true } });

    const structureMap = new Map(
      structureRows.map((row: any) => [level === 'campaign' ? row.metaCampaignId : level === 'adset' ? row.metaAdSetId : row.metaAdId, row]),
    );

    return rows.map((row: any) => {
      const id = level === 'campaign' ? row.metaCampaignId : level === 'adset' ? row.metaAdSetId : row.metaAdId;
      const structure: any = structureMap.get(id) || {};
      return {
        id,
        level,
        name:
          level === 'campaign'
            ? row.campaignName || structure?.name || id
            : level === 'adset'
              ? row.adSetName || structure?.name || id
              : row.adName || structure?.name || id,
        campaignName: row.campaignName || structure?.campaign?.name || structure?.campaignName || null,
        adSetName: row.adSetName || structure?.adSet?.name || structure?.adSetName || null,
        status: structure?.status || null,
        effectiveStatus: structure?.effectiveStatus || null,
        thumbnailUrl: structure?.thumbnailUrl || structure?.imageUrl || null,
        previewShareableLink: structure?.previewShareableLink || null,
        metrics: this.metricsFromSum(row._sum),
      };
    });
  }


  private metricsFromMetaInsightRow(row: any) {
    const actions = Array.isArray(row?.actions) ? row.actions : [];
    const actionValues = Array.isArray(row?.action_values) ? row.action_values : [];
    const spend = this.n(row?.spend);
    const impressions = Math.round(this.n(row?.impressions));
    const reach = Math.round(this.n(row?.reach));
    const clicks = Math.round(this.n(row?.clicks));
    const inlineLinkClicks = Math.round(this.n(row?.inline_link_clicks));
    const purchases = Math.round(this.pickActionCount(actions, ['purchase', 'omni_purchase', 'offsite_conversion.fb_pixel_purchase']));
    const purchaseValue = this.pickActionValue(actionValues, ['purchase', 'omni_purchase', 'offsite_conversion.fb_pixel_purchase']);
    const purchaseRoas = Array.isArray(row?.purchase_roas) ? this.n(row.purchase_roas?.[0]?.value) : 0;

    return {
      spend,
      impressions,
      reach,
      clicks,
      inlineLinkClicks,
      purchases,
      purchaseValue,
      ctr: impressions > 0 ? (clicks / impressions) * 100 : this.n(row?.ctr),
      cpc: clicks > 0 ? spend / clicks : this.n(row?.cpc),
      cpm: impressions > 0 ? (spend / impressions) * 1000 : this.n(row?.cpm),
      costPerPurchase: purchases > 0 ? spend / purchases : 0,
      roas: purchaseRoas || (spend > 0 ? purchaseValue / spend : 0),
    };
  }

  private mergeMetricRows(rows: any[]) {
    return this.metricsFromSum({
      spend: rows.reduce((sum, row) => sum + this.n(row?.spend), 0),
      impressions: rows.reduce((sum, row) => sum + this.n(row?.impressions), 0),
      reach: rows.reduce((sum, row) => sum + this.n(row?.reach), 0),
      clicks: rows.reduce((sum, row) => sum + this.n(row?.clicks), 0),
      inlineLinkClicks: rows.reduce((sum, row) => sum + this.n(row?.inlineLinkClicks), 0),
      purchases: rows.reduce((sum, row) => sum + this.n(row?.purchases), 0),
      purchaseValue: rows.reduce((sum, row) => sum + this.n(row?.purchaseValue), 0),
    });
  }

  private async fetchOfficialAccountDailyRows(range: MetaDateRange, query: any = {}) {
    // V4 Accuracy Layer:
    // Meta Ads Manager total spend is safest at account-level insights.
    // DB rows remain used for drilldown/creative table, but KPI total should reconcile with Meta official.
    const accountId = this.normalizeAccountId(query.metaAccountId || this.defaultAdAccountId);
    const rows = await this.graphList<any>(`/${accountId}/insights`, {
      fields: 'date_start,date_stop,spend,impressions,reach,clicks,inline_link_clicks,cpc,cpm,ctr,actions,action_values,purchase_roas',
      time_increment: '1',
      time_range: JSON.stringify(range),
      limit: '1000',
    }, 10);

    const dailyRows = rows.map((row: any) => ({
      date: String(row.date_start || row.date_stop || '').slice(0, 10),
      metrics: this.metricsFromMetaInsightRow(row),
    })).filter((row: any) => row.date);

    return {
      source: 'meta_account_live',
      dailyRows,
      summary: this.mergeMetricRows(dailyRows.map((row: any) => row.metrics)),
      fetchedAt: new Date().toISOString(),
    };
  }


  async getBrainOverview(query: any = {}) {
    // Big Data V2: tuyệt đối không cộng lẫn campaign + adset + ad.
    // Spend là cùng một dòng tiền ở 3 layer, cộng tất cả sẽ bị double/triple count.
    // Summary/daily dùng ad-level làm nguồn chuẩn cho Creative/Attribution; nếu cần có thể truyền summaryLevel=campaign/adset/ad.
    const summaryLevel = ['campaign', 'adset', 'ad'].includes(String(query.summaryLevel || '').toLowerCase())
      ? String(query.summaryLevel).toLowerCase() as 'campaign' | 'adset' | 'ad'
      : 'ad';
    const { where, range } = this.buildInsightWhere({ ...query, level: summaryLevel }, summaryLevel);
    const accountWhere = query.metaAccountId ? { metaAccountId: this.normalizeAccountId(query.metaAccountId) } : {};

    const [
      accounts,
      campaignCount,
      adSetCount,
      adCount,
      activeCampaignCount,
      activeAdSetCount,
      activeAdCount,
      summaryAgg,
      dailyRowsRaw,
      latestLogs,
    ] = await Promise.all([
      (this.prisma as any).metaAdAccount.findMany({ where: accountWhere, orderBy: { updatedAt: 'desc' } }),
      (this.prisma as any).metaCampaign.count({ where: accountWhere }),
      (this.prisma as any).metaAdSet.count({ where: accountWhere }),
      (this.prisma as any).metaAd.count({ where: accountWhere }),
      (this.prisma as any).metaCampaign.count({ where: { ...accountWhere, effectiveStatus: 'ACTIVE' } }),
      (this.prisma as any).metaAdSet.count({ where: { ...accountWhere, effectiveStatus: 'ACTIVE' } }),
      (this.prisma as any).metaAd.count({ where: { ...accountWhere, effectiveStatus: 'ACTIVE' } }),
      (this.prisma as any).metaAdInsightDaily.aggregate({
        where,
        _sum: { spend: true, impressions: true, reach: true, clicks: true, inlineLinkClicks: true, purchases: true, purchaseValue: true },
      }),
      (this.prisma as any).metaAdInsightDaily.groupBy({
        by: ['dateStart'],
        where,
        _sum: { spend: true, impressions: true, reach: true, clicks: true, inlineLinkClicks: true, purchases: true, purchaseValue: true },
        orderBy: { dateStart: 'asc' },
      }),
      (this.prisma as any).metaSyncLog.findMany({ orderBy: { startedAt: 'desc' }, take: 8 }),
    ]);

    const dbSummary = this.metricsFromSum(summaryAgg._sum);
    const dbDailyRows = dailyRowsRaw.map((row: any) => ({
      date: this.toDateInput(new Date(row.dateStart)),
      metrics: this.metricsFromSum(row._sum),
    }));

    let officialMeta: any = null;
    if (String(query.skipOfficialMeta || query.skipOfficial || '') !== '1') {
      try {
        officialMeta = await this.fetchOfficialAccountDailyRows(range, query);
      } catch (error: any) {
        this.logger.warn(`[MetaAdsBrain] Official account-level reconcile failed: ${error?.message || error}`);
      }
    }

    // KPI tổng ưu tiên số official account-level để khớp Meta Ads Manager.
    // DB ad-level vẫn giữ cho bảng creative/drilldown, tránh query nặng và vẫn phục vụ attribution.
    const summary = officialMeta?.summary || dbSummary;
    const dailyRows = officialMeta?.dailyRows?.length ? officialMeta.dailyRows : dbDailyRows;

    const [topCampaigns, topAdSets, topAds] = await Promise.all([
      this.getTopInsightGroups('campaign', query, 12),
      this.getTopInsightGroups('adset', query, 12),
      this.getTopInsightGroups('ad', query, 20),
    ]);

    const statusBreakdown = {
      campaigns: { total: campaignCount, active: activeCampaignCount, inactive: Math.max(campaignCount - activeCampaignCount, 0) },
      adSets: { total: adSetCount, active: activeAdSetCount, inactive: Math.max(adSetCount - activeAdSetCount, 0) },
      ads: { total: adCount, active: activeAdCount, inactive: Math.max(adCount - activeAdCount, 0) },
    };

    const warnings = [] as Array<{ id: string; title: string; desc: string; tone: 'safe' | 'warning' | 'critical' }>;
    const highSpendNoPurchase = topAds
      .filter((row: any) => row.metrics.spend >= 100000 && row.metrics.purchases <= 0)
      .slice(0, 5);
    for (const row of highSpendNoPurchase) {
      warnings.push({
        id: `waste-${row.id}`,
        title: `Ads đốt tiền chưa ra đơn: ${row.name}`,
        desc: `Spend ${Math.round(row.metrics.spend).toLocaleString('vi-VN')}₫ nhưng chưa có purchase trong khoảng đang xem.`,
        tone: 'warning',
      });
    }
    const highCpa = topAds
      .filter((row: any) => row.metrics.purchases > 0 && row.metrics.costPerPurchase >= 50000)
      .slice(0, 5);
    for (const row of highCpa) {
      warnings.push({
        id: `cpa-${row.id}`,
        title: `CPA cao: ${row.name}`,
        desc: `CPA ${Math.round(row.metrics.costPerPurchase).toLocaleString('vi-VN')}₫/purchase. Cần so với biên lợi nhuận thật trước khi scale.`,
        tone: 'warning',
      });
    }

    return {
      ok: true,
      range,
      summaryLevel,
      generatedAt: new Date().toISOString(),
      accounts,
      summary,
      dbSummary,
      metaOfficialSummary: officialMeta?.summary || null,
      reconciliation: {
        source: officialMeta?.source || 'db_ad_level',
        officialFetchedAt: officialMeta?.fetchedAt || null,
        dbSpend: dbSummary.spend,
        officialSpend: officialMeta?.summary?.spend || null,
        diffSpend: officialMeta?.summary ? officialMeta.summary.spend - dbSummary.spend : 0,
        diffPercent: officialMeta?.summary?.spend ? ((officialMeta.summary.spend - dbSummary.spend) / officialMeta.summary.spend) * 100 : 0,
        note: officialMeta?.summary
          ? 'KPI dùng Meta account-level live để khớp Ads Manager; bảng chi tiết vẫn dùng DB ad-level đã sync.'
          : 'KPI đang dùng DB ad-level vì không lấy được official account-level.',
      },
      statusBreakdown,
      dailyRows,
      topCampaigns,
      topAdSets,
      topAds,
      warnings: warnings.slice(0, 8),
      latestLogs,
    };
  }

  async getEntityDetail(query: any = {}) {
    const type = String(query.type || query.level || 'ad');
    const id = String(query.id || query.metaId || '').trim();
    if (!id) return { item: null, insights: [], children: [] };

    const { where } = this.buildInsightWhere(query, type as any);
    let item: any = null;
    const childPayload: any = {};

    if (type === 'campaign') {
      item = await (this.prisma as any).metaCampaign.findFirst({
        where: { OR: [{ id }, { metaCampaignId: id }] },
      });
      if (item?.metaCampaignId) {
        where.metaCampaignId = item.metaCampaignId;
        childPayload.adSets = await (this.prisma as any).metaAdSet.findMany({ where: { metaCampaignId: item.metaCampaignId }, take: 100, orderBy: { updatedAt: 'desc' } });
        childPayload.ads = await (this.prisma as any).metaAd.findMany({ where: { metaCampaignId: item.metaCampaignId }, take: 100, orderBy: { updatedAt: 'desc' } });
      }
    } else if (type === 'adset') {
      item = await (this.prisma as any).metaAdSet.findFirst({
        where: { OR: [{ id }, { metaAdSetId: id }] },
        include: { campaign: true },
      });
      if (item?.metaAdSetId) {
        where.metaAdSetId = item.metaAdSetId;
        childPayload.ads = await (this.prisma as any).metaAd.findMany({ where: { metaAdSetId: item.metaAdSetId }, take: 100, orderBy: { updatedAt: 'desc' } });
      }
    } else {
      item = await (this.prisma as any).metaAd.findFirst({
        where: { OR: [{ id }, { metaAdId: id }] },
        include: { campaign: true, adSet: true },
      });
      if (item?.metaAdId) where.metaAdId = item.metaAdId;
    }

    const insights = await (this.prisma as any).metaAdInsightDaily.findMany({
      where,
      orderBy: { dateStart: 'asc' },
      take: 120,
    });

    const aggregate = await (this.prisma as any).metaAdInsightDaily.aggregate({
      where,
      _sum: { spend: true, impressions: true, reach: true, clicks: true, inlineLinkClicks: true, purchases: true, purchaseValue: true },
    });

    return {
      item,
      insights,
      summary: this.metricsFromSum(aggregate._sum),
      ...childPayload,
    };
  }

}
