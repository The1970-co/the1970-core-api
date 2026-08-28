
function getMetaActionValueExact(actions: any, actionTypes: string[]): number {
  if (!Array.isArray(actions)) return 0;
  const allowed = new Set(actionTypes.map((x) => String(x).toLowerCase()));
  return actions.reduce((sum, item) => {
    const type = String(item?.action_type || item?.actionType || '').toLowerCase();
    if (!allowed.has(type)) return sum;
    return sum + (Number(item?.value || 0) || 0);
  }, 0);
}

function getMetaCostValueExact(costs: any, actionTypes: string[]): number {
  if (!Array.isArray(costs)) return 0;
  const allowed = new Set(actionTypes.map((x) => String(x).toLowerCase()));
  const found = costs.find((item) => allowed.has(String(item?.action_type || item?.actionType || '').toLowerCase()));
  return Number(found?.value || 0) || 0;
}

function applyLiveMatchedMetaMetrics(row: any, metrics: any = {}) {
  const raw = row?.rawJson || row || {};
  const actions = row?.actionsJson || raw?.actions || row?.actions || [];
  const costActions = raw?.cost_per_action_type || row?.cost_per_action_type || row?.costPerActionType || [];
  const spend = Number(metrics?.spend ?? row?.spend ?? raw?.spend ?? 0) || 0;

  // Đã đối chiếu live ngày 26/05: khớp Meta Ads Manager.
  // Kết quả = "Lượt bắt đầu cuộc trò chuyện qua tin nhắn"
  const resultStartedChat = getMetaActionValueExact(actions, [
    'onsite_conversion.messaging_conversation_started_7d',
    'messaging_conversation_started_7d',
  ]);

  // Cột Meta: "Tổng số người liên hệ nhắn tin"
  const totalMessagingContact = getMetaActionValueExact(actions, [
    'onsite_conversion.total_messaging_connection',
    'total_messaging_connection',
  ]);

  // Cột Meta: "Người liên hệ nhắn tin"
  const messagingContact = getMetaActionValueExact(actions, [
    'onsite_conversion.messaging_conversation_replied_7d',
    'messaging_conversation_replied_7d',
    'onsite_conversion.messaging_first_reply',
    'messaging_first_reply',
  ]);

  // Cột Meta: "Bình luận về bài viết"
  const postComment = getMetaActionValueExact(actions, [
    'comment',
    'post_comment',
  ]);

  const costPerResultFromMeta = getMetaCostValueExact(costActions, [
    'onsite_conversion.messaging_conversation_started_7d',
    'messaging_conversation_started_7d',
  ]);

  const costPerResult = costPerResultFromMeta || (resultStartedChat > 0 ? spend / resultStartedChat : 0);

  return {
    ...metrics,

    // Giữ key cũ để FE không vỡ, nhưng ý nghĩa đã map đúng cột Meta.
    purchases: resultStartedChat,
    result: resultStartedChat,
    messages: totalMessagingContact,
    conversationStarts: resultStartedChat,
    comments: postComment,

    metaResultStartedChat: resultStartedChat,
    metaTotalMessagingContact: totalMessagingContact,
    metaMessagingContact: messagingContact,
    metaPostComment: postComment,

    costPerResult,
    costPerMessage: totalMessagingContact > 0 ? spend / totalMessagingContact : costPerResult,
    costPerConversation: costPerResult,
  };
}



function getActionCountByExactTypes(actions: any, types: string[]): number {
  if (!Array.isArray(actions)) return 0;
  const set = new Set(types.map((x) => String(x).toLowerCase()));
  let total = 0;
  for (const item of actions) {
    const t = String(item?.action_type || item?.actionType || '').toLowerCase();
    if (set.has(t)) total += Number(item?.value || 0) || 0;
  }
  return total;
}

function getCostPerActionByExactTypes(costs: any, types: string[]): number {
  if (!Array.isArray(costs)) return 0;
  const set = new Set(types.map((x) => String(x).toLowerCase()));
  for (const item of costs) {
    const t = String(item?.action_type || item?.actionType || '').toLowerCase();
    if (set.has(t)) return Number(item?.value || 0) || 0;
  }
  return 0;
}

function mapMetaAdsManagerMetrics(row: any, metrics: any = {}) {
  return applyLiveMatchedMetaMetrics(row, metrics);
}


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
  private autopilotActiveAdsCache: { at: number; rows: any[] } | null = null;
  private autopilotActiveAdsInFlight: Promise<any[]> | null = null;
  private autopilotTemplateAdSetsCache: { at: number; rows: any[] } | null = null;

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

  private async graphPost<T>(path: string, params: Record<string, string>) {
    if (!this.accessToken) {
      throw new Error('META_ACCESS_TOKEN is missing');
    }

    const url = new URL(`https://graph.facebook.com/${this.version}${path}`);
    const body = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined && value !== null && value !== '') body.set(key, value);
    });
    body.set('access_token', this.accessToken);

    const res = await fetch(url.toString(), {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: body.toString(),
    });
    const json = (await res.json()) as T & { error?: any };

    if (!res.ok || (json as any).error) {
      this.logger.error(`[MetaAdsSync] POST API error: ${JSON.stringify((json as any).error || json)}`);
      throw new Error((json as any).error?.message || 'Meta Ads API write error');
    }

    return json;
  }

  private get pageAccessToken() {
    return process.env.META_INBOX_PAGE_ACCESS_TOKEN || process.env.META_PAGE_ACCESS_TOKEN || process.env.META_INBOX || this.accessToken;
  }

  async resolvePageIdForAutoLaunch() {
    const fromEnv = String(process.env.META_PAGE_ID || process.env.META_INBOX_PAGE_ID || '').trim();
    if (fromEnv) return fromEnv;

    try {
      const row = await (this.prisma as any).metaAd.findFirst({
        where: { OR: [{ pageId: { not: null } }, { postId: { not: null } }] },
        orderBy: { updatedAt: 'desc' },
        select: { pageId: true, postId: true },
      });
      const direct = String(row?.pageId || '').trim();
      if (direct) return direct;
      const postId = String(row?.postId || '').trim();
      if (postId.includes('_')) return postId.split('_')[0];
    } catch {}

    throw new Error('Thiếu META_PAGE_ID và chưa suy ra được Page ID từ Meta Ads cache');
  }

  async getPublishedPagePostsForAutoLaunch(pageId: string, limit = 100) {
    const id = String(pageId || '').trim();
    if (!id) throw new Error('Thiếu Page ID');
    const token = String(this.pageAccessToken || '').trim();
    if (!token) throw new Error('Thiếu Page access token để đọc bài viết Page');

    const url = new URL(`https://graph.facebook.com/${this.version}/${id}/published_posts`);
    url.searchParams.set('fields', 'id,message,created_time,permalink_url,full_picture,attachments{media,type,url,title,subattachments}');
    url.searchParams.set('limit', String(Math.min(Math.max(Number(limit || 100), 1), 100)));
    url.searchParams.set('access_token', token);
    const res = await fetch(url.toString(), { method: 'GET' });
    const json = (await res.json()) as any;
    if (!res.ok || json?.error) {
      this.logger.error(`[AUTO_LAUNCH_PAGE_POSTS] ${JSON.stringify(json?.error || json)}`);
      throw new Error(json?.error?.message || 'Không đọc được published posts của Page');
    }
    return Array.isArray(json?.data) ? json.data : [];
  }

  async createCreativeFromPagePostAutoLaunch(input: { pageId: string; postId: string; name: string }) {
    const accountId = this.normalizeAccountId(this.defaultAdAccountId);
    const pageId = String(input.pageId || '').trim();
    const postId = String(input.postId || '').trim();
    if (!pageId || !postId) throw new Error('Thiếu pageId/postId để tạo creative');

    const result = await this.graphPost<{ id?: string }>(`/${accountId}/adcreatives`, {
      name: String(input.name || `Auto Launch ${postId}`).slice(0, 200),
      object_story_id: postId,
      page_id: pageId,
    });
    const id = String(result?.id || '').trim();
    if (!id) throw new Error('Meta không trả metaCreativeId');
    return { ok: true, metaCreativeId: id, pageId, postId };
  }

  async createAdFromCreativeAutoLaunch(input: { adSetId: string; creativeId: string; name: string; status?: 'PAUSED' | 'ACTIVE' }) {
    const accountId = this.normalizeAccountId(this.defaultAdAccountId);
    const adSetId = String(input.adSetId || '').trim();
    const creativeId = String(input.creativeId || '').trim();
    if (!adSetId || !creativeId) throw new Error('Thiếu adSetId/creativeId để tạo Ad');

    const result = await this.graphPost<{ id?: string }>(`/${accountId}/ads`, {
      name: String(input.name || 'Auto Launch Ad').slice(0, 200),
      adset_id: adSetId,
      status: input.status === 'ACTIVE' ? 'ACTIVE' : 'PAUSED',
      creative: JSON.stringify({ creative_id: creativeId }),
    });
    const id = String(result?.id || '').trim();
    if (!id) throw new Error('Meta không trả metaAdId');
    return { ok: true, metaAdId: id, metaAdSetId: adSetId, metaCreativeId: creativeId, status: input.status === 'ACTIVE' ? 'ACTIVE' : 'PAUSED' };
  }

  private detectAutoLaunchBudgetPlacement(templateAdSet: any, templateCampaign: any) {
    const adSetDaily = this.n(templateAdSet?.daily_budget);
    const adSetLifetime = this.n(templateAdSet?.lifetime_budget);
    const campaignDaily = this.n(templateCampaign?.daily_budget);
    const campaignLifetime = this.n(templateCampaign?.lifetime_budget);

    if (adSetDaily > 0 || adSetLifetime > 0) {
      return {
        level: 'ADSET' as const,
        mode: 'ABO' as const,
        sourceField: adSetDaily > 0 ? 'daily_budget' : 'lifetime_budget',
        templateAmount: adSetDaily || adSetLifetime,
      };
    }

    if (campaignDaily > 0 || campaignLifetime > 0) {
      return {
        level: 'CAMPAIGN' as const,
        mode: 'CBO' as const,
        sourceField: campaignDaily > 0 ? 'daily_budget' : 'lifetime_budget',
        templateAmount: campaignDaily || campaignLifetime,
      };
    }

    // Fail-safe: nếu Meta không trả budget ở cả 2 level, mặc định dùng Ad Set
    // vì API create Ad Set có budget rõ ràng, tránh tạo Campaign budget ngoài ý muốn.
    return {
      level: 'ADSET' as const,
      mode: 'ABO' as const,
      sourceField: 'daily_budget',
      templateAmount: 0,
    };
  }

  private copyBidFieldsFromTemplate(target: Record<string, string>, source: any) {
    const bidStrategy = String(source?.bid_strategy || '').trim();
    const bidAmount = this.n(source?.bid_amount);

    if (bidStrategy) target.bid_strategy = bidStrategy;
    else delete target.bid_strategy;

    if (bidAmount > 0) target.bid_amount = String(Math.round(bidAmount));
    else delete target.bid_amount;

    if (source?.bid_constraints && typeof source.bid_constraints === 'object') {
      target.bid_constraints = JSON.stringify(source.bid_constraints);
    } else {
      delete target.bid_constraints;
    }

    const costGoal = this.n(source?.cost_per_result_goal);
    if (costGoal > 0) target.cost_per_result_goal = String(Math.round(costGoal));
    else delete target.cost_per_result_goal;
  }

  private buildAutoLaunchCampaignParams(input: {
    name: string;
    desiredBudget: number;
    budgetPlacement: { level: 'ADSET' | 'CAMPAIGN'; mode: 'ABO' | 'CBO'; sourceField: string; templateAmount: number };
    templateCampaign: any;
  }) {
    const params: Record<string, string> = {
      name: String(input.name || 'Auto Launch').slice(0, 200),
      objective: String(input.templateCampaign?.objective || 'OUTCOME_ENGAGEMENT'),
      buying_type: String(input.templateCampaign?.buying_type || 'AUCTION'),
      special_ad_categories: JSON.stringify([]),
      status: 'PAUSED',
    };

    if (input.budgetPlacement.level === 'CAMPAIGN') {
      // CBO: ngân sách nằm ở Campaign.
      params.daily_budget = String(Math.max(1, Math.round(input.desiredBudget)));
      delete params.is_adset_budget_sharing_enabled;
    } else {
      // ABO: ngân sách nằm ở từng Ad Set.
      // Meta hiện yêu cầu khai báo rõ có cho các Ad Set chia sẻ ngân sách hay không.
      // Flow của mình muốn mỗi Ad Set giữ budget riêng => false.
      delete params.daily_budget;
      delete params.lifetime_budget;
      params.is_adset_budget_sharing_enabled = 'false';
    }

    // Campaign raw của Meta hỗ trợ bid_strategy, nhưng bid_amount / bid_constraints
    // là cấu hình Ad Set. Không query/copy các field đó ở Campaign để tránh Graph API 500/400.
    const campaignBidStrategy = String(input.templateCampaign?.bid_strategy || '').trim();
    if (campaignBidStrategy) params.bid_strategy = campaignBidStrategy;
    else delete params.bid_strategy;

    delete params.bid_amount;
    delete params.bid_constraints;
    delete params.cost_per_result_goal;
    return params;
  }

  async createCampaignForPagePostAutoLaunch(input: {
    name: string;
    dailyBudget?: number;
    budgetPlacement: { level: 'ADSET' | 'CAMPAIGN'; mode: 'ABO' | 'CBO'; sourceField: string; templateAmount: number };
    templateCampaign: any;
  }) {
    const accountId = this.normalizeAccountId(this.defaultAdAccountId);
    const desiredBudget = Math.max(1, Math.round(Number(input.dailyBudget || 1_000_000)));
    const params = this.buildAutoLaunchCampaignParams({
      name: input.name,
      desiredBudget,
      budgetPlacement: input.budgetPlacement,
      templateCampaign: input.templateCampaign,
    });

    this.logger.warn(
      `[META_AUTO_LAUNCH_BUDGET] detected=${input.budgetPlacement.mode} level=${input.budgetPlacement.level} templateAmount=${input.budgetPlacement.templateAmount} desiredBudget=${desiredBudget}`,
    );

    const result = await this.graphPost<{ id?: string }>(`/${accountId}/campaigns`, params);
    const id = String(result?.id || '').trim();
    if (!id) throw new Error('Meta không trả metaCampaignId');

    return {
      ok: true,
      metaCampaignId: id,
      name: params.name,
      objective: params.objective,
      dailyBudget: params.daily_budget ? desiredBudget : null,
      budgetLevel: input.budgetPlacement.level,
      budgetMode: input.budgetPlacement.mode,
      status: params.status,
    };
  }


  private buildAutoLaunchAdSetParams(input: {
    template: any;
    campaignId: string;
    desiredBudget: number;
    budgetPlacement: { level: 'ADSET' | 'CAMPAIGN'; mode: 'ABO' | 'CBO'; sourceField: string; templateAmount: number };
    name: string;
  }) {
    const { template, campaignId, desiredBudget, budgetPlacement } = input;

    const params: Record<string, string> = {
      campaign_id: campaignId,
      name: String(input.name || 'Auto Launch').slice(0, 200),
      status: 'PAUSED',
    };

    params.optimization_goal = String(template?.optimization_goal || 'CONVERSATIONS');
    params.destination_type = String(template?.destination_type || 'MESSENGER');
    params.billing_event = String(template?.billing_event || 'IMPRESSIONS');

    if (template?.targeting) params.targeting = JSON.stringify(template.targeting);
    if (template?.promoted_object) params.promoted_object = JSON.stringify(template.promoted_object);
    if (Array.isArray(template?.attribution_spec)) {
      // Với OUTCOME_ENGAGEMENT + CONVERSATIONS, Meta hiện chỉ chấp nhận
      // attribution click-through 1 ngày. Template cũ có thể trả 7 ngày.
      params.attribution_spec = JSON.stringify(
        template.attribution_spec.map((item: any) => {
          const eventType = String(item?.event_type || '').toUpperCase();
          if (eventType === 'CLICK_THROUGH') {
            return {
              ...item,
              window_days: 1,
            };
          }
          return item;
        }),
      );
    }

    // Bidding copy đúng raw Ad Set mẫu. Raw null => OMIT.
    this.copyBidFieldsFromTemplate(params, template);

    // Tự phát hiện budget ở đâu từ mẫu:
    // ABO => budget ở Ad Set; CBO => budget ở Campaign.
    if (budgetPlacement.level === 'ADSET') {
      params.daily_budget = String(Math.max(1, Math.round(desiredBudget)));
    } else {
      delete params.daily_budget;
      delete params.lifetime_budget;
    }

    return params;
  }

  async getAutoLaunchAdSetTemplates(input: { q?: string; limit?: number } = {}) {
    const q = String(input.q || '').trim().toLowerCase();
    const limit = Math.min(200, Math.max(20, Number(input.limit || 100)));
    const now = Date.now();

    let rows = this.autopilotTemplateAdSetsCache?.rows || [];
    const cacheFresh = Boolean(
      this.autopilotTemplateAdSetsCache &&
      now - this.autopilotTemplateAdSetsCache.at < 5 * 60_000,
    );

    if (!cacheFresh) {
      const accountId = this.normalizeAccountId(this.defaultAdAccountId);

      // Chỉ lấy structure nhẹ, không insights/creative.
      // 3 page x 100 = tối đa khoảng 300 Ad Set.
      const raw = await this.graphList<any>(
        `/${accountId}/adsets`,
        {
          fields: 'id,name,status,effective_status,updated_time,campaign{id,name,status,effective_status}',
          limit: '100',
        },
        3,
      );

      rows = raw
        .map((x: any) => ({
          id: String(x?.id || ''),
          name: String(x?.name || x?.id || ''),
          status: String(x?.status || ''),
          effectiveStatus: String(x?.effective_status || ''),
          updatedTime: x?.updated_time || null,
          campaignId: String(x?.campaign?.id || ''),
          campaignName: String(x?.campaign?.name || ''),
          campaignStatus: String(x?.campaign?.status || ''),
          campaignEffectiveStatus: String(x?.campaign?.effective_status || ''),
        }))
        .filter((x: any) => x.id);

      rows.sort((a: any, b: any) => {
        const ta = Date.parse(String(a.updatedTime || '')) || 0;
        const tb = Date.parse(String(b.updatedTime || '')) || 0;
        return tb - ta;
      });

      this.autopilotTemplateAdSetsCache = { at: now, rows };
    }

    const filtered = q
      ? rows.filter((x: any) =>
          `${x.name} ${x.campaignName} ${x.id}`.toLowerCase().includes(q),
        )
      : rows;

    return {
      ok: true,
      query: q,
      cached: cacheFresh,
      totalScanned: rows.length,
      items: filtered.slice(0, limit),
    };
  }

  private async getAutoLaunchTemplateRaw(templateId: string) {
    return this.graphGet<any>(`/${templateId}`, {
      fields: [
        'id',
        'name',
        'campaign_id',
        'status',
        'effective_status',
        'optimization_goal',
        'billing_event',
        'bid_strategy',
        'bid_amount',
        'bid_constraints',
        'daily_budget',
        'lifetime_budget',
        'targeting',
        'promoted_object',
        'destination_type',
        'attribution_spec',
        'start_time',
        'end_time',
      ].join(','),
    });
  }

  async getAutoLaunchTemplateComparison(input: {
    launchMode?: 'EXISTING_ADSET' | 'CLONE_ADSET' | 'NEW_CAMPAIGN';
    targetAdSetId?: string;
    templateAdSetId?: string;
    targetCampaignId?: string;
    dailyBudget?: number;
    name?: string;
  }) {
    this.logger.warn(
      `[META_AUTO_LAUNCH_PREVIEW] ENTER ${JSON.stringify({
        launchMode: input.launchMode || 'NEW_CAMPAIGN',
        templateAdSetId: input.templateAdSetId || input.targetAdSetId || null,
        dailyBudget: input.dailyBudget || null,
        name: input.name || null,
      })}`,
    );

    const mode = String(input.launchMode || 'NEW_CAMPAIGN').toUpperCase();
    const templateId = String(input.templateAdSetId || input.targetAdSetId || '').trim();

    if (!templateId) throw new Error('Chưa cấu hình templateAdSetId');

    let template: any;
    try {
      template = await this.getAutoLaunchTemplateRaw(templateId);
    } catch (error: any) {
      this.logger.error(`[META_AUTO_LAUNCH_PREVIEW] stage=adset_raw templateAdSetId=${templateId} error=${error?.message || error}`);
      throw error;
    }
    const desiredBudget = Math.round(Number(input.dailyBudget || 0)) || 1_000_000;

    const templateCampaignId = String(template?.campaign_id || input.targetCampaignId || '').trim();
    let campaignRaw: any = null;

    if (templateCampaignId) {
      try {
        campaignRaw = await this.graphGet<any>(`/${templateCampaignId}`, {
          fields: [
            'id',
            'name',
            'objective',
            'buying_type',
            'daily_budget',
            'lifetime_budget',
            'bid_strategy',
            'status',
            'effective_status',
          ].join(','),
        });
      } catch (error: any) {
        this.logger.error(`[META_AUTO_LAUNCH_PREVIEW] stage=campaign_raw templateCampaignId=${templateCampaignId} error=${error?.message || error}`);
        throw error;
      }
    }

    const budgetPlacement = this.detectAutoLaunchBudgetPlacement(template, campaignRaw);
    const previewCampaignId = mode === 'NEW_CAMPAIGN'
      ? '<NEW_CAMPAIGN_ID>'
      : String(input.targetCampaignId || templateCampaignId || '').trim();

    const campaignPayload =
      mode === 'NEW_CAMPAIGN'
        ? this.buildAutoLaunchCampaignParams({
            name: String(input.name || 'Auto Launch'),
            desiredBudget,
            budgetPlacement,
            templateCampaign: campaignRaw,
          })
        : null;

    const adSetParams = this.buildAutoLaunchAdSetParams({
      template,
      campaignId: previewCampaignId,
      desiredBudget,
      budgetPlacement,
      name: String(input.name || 'Auto Launch'),
    });

    const compareField = (field: string, rawValue: any, sendValue: any, note?: string) => {
      const rawMissing = rawValue === undefined || rawValue === null || rawValue === '';
      const sendMissing = sendValue === undefined || sendValue === null || sendValue === '';
      let state: 'SAME' | 'NORMALIZED' | 'OMITTED' | 'ADDED' = 'SAME';

      if (!rawMissing && sendMissing) state = 'OMITTED';
      else if (rawMissing && !sendMissing) state = 'ADDED';
      else if (JSON.stringify(rawValue) !== JSON.stringify(sendValue)) state = 'NORMALIZED';

      return {
        field,
        raw: rawMissing ? null : rawValue,
        send: sendMissing ? null : sendValue,
        state,
        note: note || null,
      };
    };

    const campaignComparison = campaignPayload ? [
      compareField('objective', campaignRaw?.objective, campaignPayload.objective),
      compareField('buying_type', campaignRaw?.buying_type, campaignPayload.buying_type),
      compareField(
        'daily_budget',
        campaignRaw?.daily_budget,
        campaignPayload.daily_budget,
        budgetPlacement.level === 'CAMPAIGN'
          ? 'Phát hiện CBO: ngân sách sẽ đặt ở Campaign.'
          : 'Phát hiện ABO: Campaign không gửi ngân sách.',
      ),
      compareField(
        'is_adset_budget_sharing_enabled',
        campaignRaw?.is_adset_budget_sharing_enabled,
        campaignPayload.is_adset_budget_sharing_enabled,
        budgetPlacement.level === 'ADSET'
          ? 'ABO: đặt false để mỗi Ad Set dùng ngân sách riêng, không chia sẻ ngân sách giữa các Ad Set.'
          : 'CBO: không cần gửi field chia sẻ ngân sách Ad Set.',
      ),
      compareField('bid_strategy', campaignRaw?.bid_strategy, campaignPayload.bid_strategy, 'Copy đúng raw Campaign mẫu; raw null thì OMIT.'),
      compareField('status', campaignRaw?.status, campaignPayload.status, 'Tạo mới ở PAUSED để duyệt an toàn.'),
    ] : [];

    const adSetComparison = [
      compareField('optimization_goal', template?.optimization_goal, adSetParams.optimization_goal),
      compareField('destination_type', template?.destination_type, adSetParams.destination_type),
      compareField('billing_event', template?.billing_event, adSetParams.billing_event),
      compareField('bid_strategy', template?.bid_strategy, adSetParams.bid_strategy, 'Copy đúng raw Ad Set mẫu; raw null thì OMIT.'),
      compareField('bid_amount', template?.bid_amount, adSetParams.bid_amount),
      compareField('bid_constraints', template?.bid_constraints, adSetParams.bid_constraints),
      compareField('cost_per_result_goal', null, adSetParams.cost_per_result_goal, 'Meta không trả field này trong raw Ad Set; Auto Launch không tự thêm nếu không có dữ liệu.'),
      compareField(
        'daily_budget',
        template?.daily_budget,
        adSetParams.daily_budget,
        budgetPlacement.level === 'ADSET'
          ? 'Phát hiện ABO: ngân sách sẽ đặt ở Ad Set.'
          : 'Phát hiện CBO: ngân sách nằm ở Campaign nên Ad Set OMIT.',
      ),
      compareField('targeting', template?.targeting, template?.targeting ? JSON.parse(adSetParams.targeting || '{}') : null),
      compareField('promoted_object', template?.promoted_object, template?.promoted_object ? JSON.parse(adSetParams.promoted_object || '{}') : null),
      compareField(
        'attribution_spec',
        template?.attribution_spec,
        adSetParams.attribution_spec ? JSON.parse(adSetParams.attribution_spec) : null,
        'Meta yêu cầu CLICK_THROUGH window_days = 1 cho objective/optimization hiện tại; template 7 ngày sẽ được NORMALIZED về 1 ngày.',
      ),
      compareField('status', template?.status, adSetParams.status, 'Ads mới tạo ở PAUSED để an toàn.'),
    ];

    return {
      ok: true,
      mode,
      templateAdSetId: templateId,
      budgetDetection: {
        mode: budgetPlacement.mode,
        level: budgetPlacement.level,
        label: budgetPlacement.level === 'CAMPAIGN'
          ? 'CBO · ngân sách ở Campaign'
          : 'ABO · ngân sách ở Ad Set',
        sourceField: budgetPlacement.sourceField,
        templateAmount: budgetPlacement.templateAmount,
        desiredBudget,
      },
      templateRaw: template,
      templateCampaignRaw: campaignRaw,
      willSend: {
        campaign: campaignPayload,
        adSet: adSetParams,
      },
      comparison: {
        campaign: campaignComparison,
        adSet: adSetComparison,
      },
      validation: {
        valid:
          Boolean(adSetParams.targeting) &&
          Boolean(adSetParams.promoted_object) &&
          (
            (budgetPlacement.level === 'ADSET' && this.n(adSetParams.daily_budget) > 0 && !campaignPayload?.daily_budget) ||
            (budgetPlacement.level === 'CAMPAIGN' && this.n(campaignPayload?.daily_budget) > 0 && !adSetParams.daily_budget)
          ),
        checks: [
          {
            key: 'budget_level',
            ok: true,
            message: `Phát hiện ${budgetPlacement.mode}: ngân sách ở ${budgetPlacement.level === 'CAMPAIGN' ? 'Campaign' : 'Ad Set'}`,
          },
          {
            key: 'budget_value',
            ok: desiredBudget > 0,
            message: `Ngân sách sẽ dùng: ${desiredBudget.toLocaleString('vi-VN')}đ/ngày`,
          },
          {
            key: 'targeting',
            ok: Boolean(adSetParams.targeting),
            message: 'Có targeting từ Ad Set mẫu',
          },
          {
            key: 'promoted_object',
            ok: Boolean(adSetParams.promoted_object),
            message: 'Có promoted_object / tài sản messaging từ mẫu',
          },
        ],
      },
    };
  }


  async prepareAdSetForPagePostAutoLaunch(input: {
    launchMode?: 'EXISTING_ADSET' | 'CLONE_ADSET' | 'NEW_CAMPAIGN';
    targetAdSetId?: string;
    templateAdSetId?: string;
    targetCampaignId?: string;
    dailyBudget?: number;
    name: string;
  }) {
    const mode = String(input.launchMode || 'NEW_CAMPAIGN').toUpperCase();

    if (mode === 'EXISTING_ADSET') {
      const adSetId = String(input.targetAdSetId || '').trim();
      if (!adSetId) throw new Error('Auto Launch chưa cấu hình targetAdSetId');
      const row = await this.graphGet<any>(`/${adSetId}`, {
        fields: 'id,name,campaign_id,status,effective_status',
      });
      return {
        ok: true,
        mode: 'EXISTING_ADSET',
        metaAdSetId: adSetId,
        metaCampaignId: row?.campaign_id || null,
      };
    }

    const templateId = String(input.templateAdSetId || input.targetAdSetId || '').trim();
    if (!templateId) throw new Error('Auto Launch chưa cấu hình templateAdSetId');

    const template = await this.getAutoLaunchTemplateRaw(templateId);
    const desiredBudget = Math.round(Number(input.dailyBudget || 0)) || 1_000_000;

    const templateCampaignId = String(template?.campaign_id || '').trim();
    let templateCampaign: any = null;

    if (templateCampaignId) {
      templateCampaign = await this.graphGet<any>(`/${templateCampaignId}`, {
        fields: [
          'id',
          'name',
          'objective',
          'buying_type',
          'daily_budget',
          'lifetime_budget',
          'bid_strategy',
          'is_adset_budget_sharing_enabled',
          'status',
          'effective_status',
        ].join(','),
      });
    }

    const budgetPlacement = this.detectAutoLaunchBudgetPlacement(template, templateCampaign);

    let campaignId = String(input.targetCampaignId || templateCampaignId || '').trim();

    if (mode === 'NEW_CAMPAIGN') {
      const createdCampaign = await this.createCampaignForPagePostAutoLaunch({
        name: String(input.name || 'Auto Launch'),
        dailyBudget: desiredBudget,
        budgetPlacement,
        templateCampaign,
      });
      campaignId = String(createdCampaign.metaCampaignId);
    } else if (!campaignId) {
      throw new Error('Không xác định được Campaign cho Ad Set mới');
    }

    const params = this.buildAutoLaunchAdSetParams({
      template,
      campaignId,
      desiredBudget,
      budgetPlacement,
      name: String(input.name || 'Auto Launch'),
    });

    this.logger.warn(
      `[META_AUTO_LAUNCH_ADSET_PAYLOAD] ${JSON.stringify({
        templateAdSetId: templateId,
        budgetMode: budgetPlacement.mode,
        budgetLevel: budgetPlacement.level,
        templateBudget: budgetPlacement.templateAmount,
        desiredBudget,
        campaign_id: params.campaign_id,
        optimization_goal: params.optimization_goal,
        destination_type: params.destination_type,
        billing_event: params.billing_event,
        bid_strategy: params.bid_strategy ?? null,
        bid_amount: params.bid_amount ?? null,
        bid_constraints: params.bid_constraints ?? null,
        cost_per_result_goal: params.cost_per_result_goal ?? null,
        daily_budget: params.daily_budget ?? null,
        hasTargeting: Boolean(params.targeting),
        hasPromotedObject: Boolean(params.promoted_object),
        hasAttributionSpec: Boolean(params.attribution_spec),
      })}`,
    );

    const accountId = this.normalizeAccountId(this.defaultAdAccountId);
    const result = await this.graphPost<{ id?: string }>(`/${accountId}/adsets`, params);
    const id = String(result?.id || '').trim();
    if (!id) throw new Error('Meta không trả metaAdSetId');

    return {
      ok: true,
      mode: mode === 'NEW_CAMPAIGN' ? 'NEW_CAMPAIGN' : 'CLONE_ADSET',
      metaAdSetId: id,
      metaCampaignId: campaignId,
      budgetMode: budgetPlacement.mode,
      budgetLevel: budgetPlacement.level,
      dailyBudget: desiredBudget,
    };
  }

  async setAdStatus(metaAdId: string, status: 'PAUSED' | 'ACTIVE') {
    const adId = String(metaAdId || '').trim();
    if (!adId) throw new Error('Thiếu metaAdId');
    if (status !== 'PAUSED' && status !== 'ACTIVE') throw new Error('Meta ad status không hợp lệ');

    const result = await this.graphPost<{ success?: boolean }>(`/${adId}`, { status });

    try {
      await (this.prisma as any).metaAd.updateMany({
        where: { metaAdId: adId },
        data: {
          status,
          effectiveStatus: status,
          lastSyncedAt: new Date(),
        },
      });
    } catch (error: any) {
      // Graph đã nhận lệnh; lỗi cập nhật cache DB không được làm rollback lệnh Meta.
      this.logger.warn(`[META_AD_STATUS_DB_CACHE] ${adId}: ${error?.message || error}`);
    }

    return { ok: result?.success !== false, metaAdId: adId, status };
  }

  async setAutopilotAdManualMapping(input: { metaAdId: string; productCode: string; color?: string }) {
    const metaAdId = String(input?.metaAdId || '').trim();
    const productCode = String(input?.productCode || '').trim().toUpperCase();
    const color = String(input?.color || '').trim();

    if (!metaAdId) throw new Error('Thiếu metaAdId');
    if (!productCode) throw new Error('Thiếu productCode');

    const current = await (this.prisma as any).metaAd.findFirst({
      where: { metaAdId },
      select: { rawJson: true },
    });

    const currentRaw =
      current?.rawJson && typeof current.rawJson === 'object' && !Array.isArray(current.rawJson)
        ? current.rawJson
        : {};

    const mapping = {
      productCode,
      color: color || null,
      updatedAt: new Date().toISOString(),
      source: 'mobile_manual',
    };

    let saved = await (this.prisma as any).metaAd.updateMany({
      where: { metaAdId },
      data: {
        rawJson: {
          ...currentRaw,
          _autopilotMapping: mapping,
        },
      },
    });

    // Một số Ads đang chạy được lấy trực tiếp từ Meta nhưng chưa từng có row trong metaAd cache DB.
    // updateMany khi đó trả count=0 nhưng trước đây vẫn trả ok=true => UI tưởng đã lưu,
    // còn inventory/control-center load lại thì không có manualMapping nên vẫn UNMAPPED.
    if (Number(saved?.count || 0) === 0) {
      await this.syncStructure(this.defaultAdAccountId, 500);

      const synced = await (this.prisma as any).metaAd.findFirst({
        where: { metaAdId },
        select: { rawJson: true },
      });

      const syncedRaw =
        synced?.rawJson && typeof synced.rawJson === 'object' && !Array.isArray(synced.rawJson)
          ? synced.rawJson
          : {};

      saved = await (this.prisma as any).metaAd.updateMany({
        where: { metaAdId },
        data: {
          rawJson: {
            ...syncedRaw,
            _autopilotMapping: mapping,
          },
        },
      });
    }

    if (Number(saved?.count || 0) === 0) {
      throw new Error(`Không lưu được mapping cho Meta Ad ${metaAdId}: chưa có Ads này trong cache DB sau khi sync`);
    }

    // Đọc lại để bảo đảm mapping thực sự đã persist trước khi báo ok.
    const persisted = await (this.prisma as any).metaAd.findFirst({
      where: { metaAdId },
      select: { rawJson: true },
    });
    const persistedMapping =
      persisted?.rawJson && typeof persisted.rawJson === 'object'
        ? persisted.rawJson?._autopilotMapping || null
        : null;

    if (
      String(persistedMapping?.productCode || '').trim().toUpperCase() !== productCode ||
      String(persistedMapping?.color || '').trim() !== String(mapping.color || '').trim()
    ) {
      throw new Error(`Mapping Meta Ad ${metaAdId} chưa được lưu chắc chắn vào DB`);
    }

    // Lưu thêm một bản mapping độc lập trong MetaSyncLog.
    // rawJson của MetaAd vẫn là nguồn chính; log này là fallback nếu một luồng sync khác
    // vô tình ghi đè rawJson về dữ liệu Meta thuần ở tương lai.
    try {
      await (this.prisma as any).metaSyncLog.create({
        data: {
          metaAccountId: null,
          syncType: 'META_ADS_AUTOPILOT_AD_MAPPING',
          status: 'SUCCESS',
          range: 'manual_mapping',
          startedAt: new Date(),
          finishedAt: new Date(),
          durationMs: 0,
          scanned: 1,
          upserted: 1,
          failed: 0,
          message: `Saved manual mapping ${metaAdId} -> ${productCode}${mapping.color ? ` · ${mapping.color}` : ''}`,
          errorJson: {
            metaAdId,
            mapping: persistedMapping,
          },
        },
      });
    } catch (error: any) {
      // Mapping trong MetaAd đã lưu thành công; lỗi audit fallback không được làm fail thao tác.
      this.logger.warn(`[META_AD_MAPPING_AUDIT] ${metaAdId}: ${error?.message || error}`);
    }

    // Bắt lần load sau lấy DB mới, không giữ cache 60s cũ.
    this.autopilotActiveAdsCache = null;
    this.autopilotActiveAdsInFlight = null;

    return {
      ok: true,
      metaAdId,
      mapping: persistedMapping,
      persisted: true,
    };
  }

  async getLiveAdsForAutopilot(limit = 500) {
    const requested = Math.min(Math.max(Number(limit || 500), 1), 500);
    const now = Date.now();

    // Cache 60s + share request đang chạy để web/mobile/control-center/inventory
    // không bắn trùng Meta cùng lúc.
    if (this.autopilotActiveAdsCache && now - this.autopilotActiveAdsCache.at < 60_000) {
      return this.autopilotActiveAdsCache.rows.slice(0, requested);
    }
    if (this.autopilotActiveAdsInFlight) {
      const rows = await this.autopilotActiveAdsInFlight;
      return rows.slice(0, requested);
    }

    this.autopilotActiveAdsInFlight = (async () => {
      const accountId = this.normalizeAccountId(this.defaultAdAccountId);

      // CALL 1: chỉ lấy Ads ACTIVE. Không lấy creative, không tải lịch sử ads.
      const ads = await this.graphList<any>(`/${accountId}/ads`, {
        fields: 'id,name,campaign_id,adset_id,status,effective_status,configured_status,created_time,updated_time,creative{id,thumbnail_url,image_url}',
        filtering: JSON.stringify([
          { field: 'effective_status', operator: 'IN', value: ['ACTIVE'] },
        ]),
        limit: '100',
      }, 5);

      const activeAds = (ads || [])
        .filter((row: any) =>
          String(row?.effective_status || row?.status || row?.configured_status || '').toUpperCase() === 'ACTIVE',
        )
        .slice(0, requested);

      if (!activeAds.length) {
        const empty: any[] = [];
        this.autopilotActiveAdsCache = { at: Date.now(), rows: empty };
        this.logger.log(`[META_AUTOPILOT_LIVE_ADS] account=${accountId} source=META_ACTIVE_BULK ads=0 metaCalls=1`);
        return empty;
      }

      const adSetIds = Array.from(new Set(activeAds.map((x: any) => String(x?.adset_id || '')).filter(Boolean)));
      const campaignIdsFromAds = Array.from(new Set(activeAds.map((x: any) => String(x?.campaign_id || '')).filter(Boolean)));

      // CALL 2: lấy TẤT CẢ Ad Set liên quan trong 1 bulk call.
      // Không còn graphGet từng adset.
      let liveAdSetsById: Record<string, any> = {};
      if (adSetIds.length) {
        try {
          liveAdSetsById = await this.graphGet<Record<string, any>>('/', {
            ids: adSetIds.join(','),
            fields: 'id,name,campaign_id,status,effective_status,daily_budget,lifetime_budget,start_time,updated_time',
          });
        } catch (error: any) {
          this.logger.warn(`[META_AUTOPILOT_BULK_ADSETS] ${error?.message || error}`);
        }
      }

      const campaignIds = Array.from(new Set([
        ...campaignIdsFromAds,
        ...Object.values(liveAdSetsById || {}).map((x: any) => String(x?.campaign_id || '')).filter(Boolean),
      ]));

      // CALL 3: lấy TẤT CẢ Campaign liên quan trong 1 bulk call.
      // Đây là nguồn budget CBO đáng tin cậy cho các campaign như QSK925/QJ943.
      let liveCampaignsById: Record<string, any> = {};
      if (campaignIds.length) {
        try {
          liveCampaignsById = await this.graphGet<Record<string, any>>('/', {
            ids: campaignIds.join(','),
            fields: 'id,name,daily_budget,lifetime_budget,status,effective_status',
          });
        } catch (error: any) {
          this.logger.warn(`[META_AUTOPILOT_BULK_CAMPAIGNS] ${error?.message || error}`);
        }
      }

      // CALL 4: chỉ giữ Ads đang THỰC SỰ PHÂN PHỐI hôm nay.
      // effective_status=ACTIVE chưa đủ vì Meta có thể để lại ads/test shell ở trạng thái ACTIVE.
      // Một ad chỉ được coi là đang phân phối khi insights hôm nay có impressions hoặc spend > 0.
      const deliveringAdIds = new Set<string>();
      try {
        const insightRows = await this.graphList<any>(`/${accountId}/insights`, {
          level: 'ad',
          fields: 'ad_id,impressions,spend',
          date_preset: 'today',
          filtering: JSON.stringify([
            { field: 'ad.id', operator: 'IN', value: activeAds.map((x: any) => String(x?.id || '')).filter(Boolean) },
          ]),
          limit: '100',
        }, 5);

        for (const row of insightRows || []) {
          const id = String(row?.ad_id || '').trim();
          const impressions = Number(row?.impressions || 0);
          const spend = Number(row?.spend || 0);
          if (id && (impressions > 0 || spend > 0)) deliveringAdIds.add(id);
        }
      } catch (error: any) {
        this.logger.warn(`[META_AUTOPILOT_DELIVERY_INSIGHTS] ${error?.message || error}`);
      }

      // DB cache chỉ để thumbnail và fallback nếu bulk call thiếu field.
      const adIds = activeAds.map((x: any) => String(x?.id || '')).filter(Boolean);
      const [cachedAds, cachedAdSets, cachedCampaigns, mappingLogs] = await Promise.all([
        adIds.length
          ? (this.prisma as any).metaAd.findMany({
              where: { metaAdId: { in: adIds } },
              select: { metaAdId: true, thumbnailUrl: true, imageUrl: true, rawJson: true },
            })
          : Promise.resolve([]),
        adSetIds.length
          ? (this.prisma as any).metaAdSet.findMany({
              where: { metaAdSetId: { in: adSetIds } },
              select: { metaAdSetId: true, metaCampaignId: true, name: true, dailyBudget: true, lifetimeBudget: true, startTime: true, updatedAt: true },
            })
          : Promise.resolve([]),
        campaignIds.length
          ? (this.prisma as any).metaCampaign.findMany({
              where: { metaCampaignId: { in: campaignIds } },
              select: { metaCampaignId: true, name: true, dailyBudget: true, lifetimeBudget: true },
            })
          : Promise.resolve([]),
        adIds.length
          ? (this.prisma as any).metaSyncLog.findMany({
              where: {
                syncType: 'META_ADS_AUTOPILOT_AD_MAPPING',
                status: 'SUCCESS',
              },
              orderBy: { startedAt: 'desc' },
              take: 2000,
              select: { startedAt: true, errorJson: true },
            })
          : Promise.resolve([]),
      ]);

      const fallbackMappingByAd = new Map<string, any>();
      for (const log of mappingLogs || []) {
        const payload = log?.errorJson && typeof log.errorJson === 'object' ? log.errorJson : {};
        const logAdId = String(payload?.metaAdId || '').trim();
        if (!logAdId || !adIds.includes(logAdId) || fallbackMappingByAd.has(logAdId)) continue;
        const logMapping = payload?.mapping && typeof payload.mapping === 'object' ? payload.mapping : null;
        if (logMapping?.productCode) fallbackMappingByAd.set(logAdId, logMapping);
      }

      const cachedAdMap = new Map((cachedAds || []).map((x: any) => [String(x.metaAdId), x]));
      const thumbMap = new Map((cachedAds || []).map((x: any) => [String(x.metaAdId), x.thumbnailUrl || x.imageUrl || null]));
      const cachedAdSetMap = new Map((cachedAdSets || []).map((x: any) => [String(x.metaAdSetId), x]));
      const cachedCampaignMap = new Map((cachedCampaigns || []).map((x: any) => [String(x.metaCampaignId), x]));

      const rows = activeAds.map((row: any) => {
        const metaAdId = String(row.id || '');
        const adSetId = String(row.adset_id || '');
        const campaignId = String(row.campaign_id || '');
        const liveAdSet = liveAdSetsById?.[adSetId] || {};
        const liveCampaign = liveCampaignsById?.[campaignId] || {};
        const cachedAd = cachedAdMap.get(metaAdId) as any;
        const rawManualMapping =
          cachedAd?.rawJson && typeof cachedAd.rawJson === 'object'
            ? cachedAd.rawJson?._autopilotMapping || null
            : null;
        const manualMapping = rawManualMapping || fallbackMappingByAd.get(metaAdId) || null;
        const cachedAdSet = cachedAdSetMap.get(adSetId) as any;
        const cachedCampaign = cachedCampaignMap.get(campaignId) as any;

        const adSetDailyBudget = this.n(liveAdSet?.daily_budget ?? cachedAdSet?.dailyBudget);
        const campaignDailyBudget = this.n(liveCampaign?.daily_budget ?? cachedCampaign?.dailyBudget);
        const currentBudget = adSetDailyBudget > 0 ? adSetDailyBudget : campaignDailyBudget > 0 ? campaignDailyBudget : null;
        const budgetLevel = adSetDailyBudget > 0 ? 'ADSET' : campaignDailyBudget > 0 ? 'CAMPAIGN' : null;

        return {
          id: metaAdId,
          metaAdId,
          name: row.name || '',
          adName: row.name || '',
          campaignId: campaignId || null,
          metaCampaignId: campaignId || null,
          campaignName: liveCampaign?.name || cachedCampaign?.name || null,
          campaignDailyBudget: campaignDailyBudget || null,
          campaignLifetimeBudget: this.n(liveCampaign?.lifetime_budget ?? cachedCampaign?.lifetimeBudget) || null,
          adSetId: adSetId || null,
          metaAdSetId: adSetId || null,
          adSetName: liveAdSet?.name || cachedAdSet?.name || null,
          adSetDailyBudget: adSetDailyBudget || null,
          adSetLifetimeBudget: this.n(liveAdSet?.lifetime_budget ?? cachedAdSet?.lifetimeBudget) || null,
          adSetStatus: liveAdSet?.status || null,
          adSetEffectiveStatus: liveAdSet?.effective_status || liveAdSet?.status || null,
          campaignStatus: liveCampaign?.status || null,
          campaignEffectiveStatus: liveCampaign?.effective_status || liveCampaign?.status || null,
          budgetLevel,
          currentBudget,
          status: row.status || row.configured_status || null,
          effectiveStatus: row.effective_status || row.status || row.configured_status || null,
          createdTime: row.created_time || null,
          updatedTime: row.updated_time || null,
          liveThumbnailUrl: row?.creative?.thumbnail_url || row?.creative?.image_url || null,
          thumbnailUrl: row?.creative?.thumbnail_url || row?.creative?.image_url || thumbMap.get(metaAdId) || null,
          adSetStartTime: liveAdSet?.start_time || cachedAdSet?.startTime || null,
          adSetUpdatedTime: liveAdSet?.updated_time || cachedAdSet?.updatedAt || null,
          manualProductCode: manualMapping?.productCode || null,
          manualColor: manualMapping?.color || null,
          manualMapping,
          source: 'META_ACTIVE_BULK',
        };
      });

      // Autopilot vận hành chỉ hiển thị Ads ACTIVE có creative thật.
      // Các shell/test ads kiểu “Thử nghiệm phân tách” thường không có thumbnail và không phải Ads bán hàng cần vận hành.
      const rejectedRows: any[] = [];
      const operationalRows = rows.filter((row: any) => {
        const adStatus = String(row?.effectiveStatus || row?.status || '').toUpperCase();
        const adSetStatus = String(row?.adSetEffectiveStatus || row?.adSetStatus || '').toUpperCase();
        const campaignStatus = String(row?.campaignEffectiveStatus || row?.campaignStatus || '').toUpperCase();
        const hasLiveCreative = Boolean(String(row?.liveThumbnailUrl || '').trim());
        const isDeliveringToday = deliveringAdIds.has(String(row?.metaAdId || ''));

        // Chỉ vận hành khi:
        // 1) Ad ACTIVE
        // 2) Ad Set ACTIVE
        // 3) Campaign ACTIVE
        // 4) Có creative live
        // 5) Có delivery thật hôm nay (impressions/spend > 0)
        const trulyActive = adStatus === 'ACTIVE' && adSetStatus === 'ACTIVE' && campaignStatus === 'ACTIVE';
        const keep = trulyActive && hasLiveCreative && isDeliveringToday;
        if (!keep) {
          rejectedRows.push({
            metaAdId: row?.metaAdId,
            name: row?.adName || row?.name,
            adStatus,
            adSetStatus,
            campaignStatus,
            hasLiveCreative,
            isDeliveringToday,
          });
        }
        return keep;
      });

      if (rejectedRows.length) {
        this.logger.log(`[META_AUTOPILOT_FILTERED_OUT] ${JSON.stringify(rejectedRows.slice(0, 20))}`);
      }

      this.autopilotActiveAdsCache = { at: Date.now(), rows: operationalRows };
      this.logger.log(
        `[META_AUTOPILOT_LIVE_ADS] account=${accountId} source=META_ACTIVE_BULK rawActive=${rows.length} operational=${operationalRows.length} adsets=${adSetIds.length} campaigns=${campaignIds.length} metaCalls<=4 delivery=today cacheTtlSec=60`,
      );
      return operationalRows;
    })();

    try {
      const rows = await this.autopilotActiveAdsInFlight;
      return rows.slice(0, requested);
    } finally {
      this.autopilotActiveAdsInFlight = null;
    }
  }

  async getAdSetForAutopilot(metaAdSetId: string) {
    const adSetId = String(metaAdSetId || '').trim();
    if (!adSetId) throw new Error('Thiếu metaAdSetId');
    return this.graphGet<any>(`/${adSetId}`, {
      fields: 'id,name,status,effective_status,daily_budget,lifetime_budget,start_time,end_time,updated_time,campaign_id',
    });
  }

  async getBudgetSnapshot(input: { metaAdSetIds?: string[]; metaCampaignIds?: string[] } = {}) {
    const adSetIds = Array.from(new Set((input.metaAdSetIds || []).map((id) => String(id || '').trim()).filter(Boolean))).slice(0, 500);
    const campaignIds = Array.from(new Set((input.metaCampaignIds || []).map((id) => String(id || '').trim()).filter(Boolean))).slice(0, 500);

    // UI snapshot chỉ đọc DB cache. Không gọi Meta từng ID.
    const [adSets, campaigns] = await Promise.all([
      adSetIds.length
        ? (this.prisma as any).metaAdSet.findMany({
            where: { metaAdSetId: { in: adSetIds } },
            select: { metaAdSetId: true, metaCampaignId: true, dailyBudget: true, lifetimeBudget: true },
          })
        : Promise.resolve([]),
      campaignIds.length
        ? (this.prisma as any).metaCampaign.findMany({
            where: { metaCampaignId: { in: campaignIds } },
            select: { metaCampaignId: true, dailyBudget: true, lifetimeBudget: true },
          })
        : Promise.resolve([]),
    ]);

    return {
      ok: true,
      adSets: (adSets || []).map((row: any) => ({ ...row, source: 'DB_CACHE' })),
      campaigns: (campaigns || []).map((row: any) => ({ ...row, source: 'DB_CACHE' })),
      generatedAt: new Date().toISOString(),
      source: 'DB_CACHE_ONLY',
    };
  }

  async getCampaignForAutopilot(metaCampaignId: string) {
    const campaignId = String(metaCampaignId || '').trim();
    if (!campaignId) throw new Error('Thiếu metaCampaignId');
    return this.graphGet<any>(`/${campaignId}`, {
      fields: 'id,name,status,effective_status,daily_budget,lifetime_budget,start_time,stop_time,updated_time',
    });
  }

  async setCampaignDailyBudget(metaCampaignId: string, dailyBudget: number) {
    const campaignId = String(metaCampaignId || '').trim();
    const budget = Math.round(Number(dailyBudget || 0));
    if (!campaignId) throw new Error('Thiếu metaCampaignId');
    if (!Number.isFinite(budget) || budget <= 0) throw new Error('daily_budget campaign không hợp lệ');

    const result = await this.graphPost<{ success?: boolean }>(`/${campaignId}`, {
      daily_budget: String(budget),
    });

    try {
      await (this.prisma as any).metaCampaign.updateMany({
        where: { metaCampaignId: campaignId },
        data: { dailyBudget: budget, lastSyncedAt: new Date() },
      });
    } catch (error: any) {
      this.logger.warn(`[META_CAMPAIGN_BUDGET_DB_CACHE] ${campaignId}: ${error?.message || error}`);
    }

    return { ok: result?.success !== false, metaCampaignId: campaignId, dailyBudget: budget };
  }

  async setAdSetDailyBudget(metaAdSetId: string, dailyBudget: number) {
    const adSetId = String(metaAdSetId || '').trim();
    const budget = Math.round(Number(dailyBudget || 0));
    if (!adSetId) throw new Error('Thiếu metaAdSetId');
    if (!Number.isFinite(budget) || budget <= 0) throw new Error('daily_budget không hợp lệ');

    const result = await this.graphPost<{ success?: boolean }>(`/${adSetId}`, {
      daily_budget: String(budget),
    });

    try {
      await (this.prisma as any).metaAdSet.updateMany({
        where: { metaAdSetId: adSetId },
        data: { dailyBudget: budget, lastSyncedAt: new Date() },
      });
    } catch (error: any) {
      this.logger.warn(`[META_ADSET_BUDGET_DB_CACHE] ${adSetId}: ${error?.message || error}`);
    }

    return { ok: result?.success !== false, metaAdSetId: adSetId, dailyBudget: budget };
  }

  private hcmParts(date: Date) {
    const parts = new Intl.DateTimeFormat('en-CA', {
      timeZone: 'Asia/Ho_Chi_Minh',
      year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', hourCycle: 'h23',
    }).formatToParts(date);
    const get = (type: string) => parts.find((p) => p.type === type)?.value || '';
    return { ymd: `${get('year')}-${get('month')}-${get('day')}`, hour: Number(get('hour') || 0) };
  }

  private hourlyBucketStart(dateStart: string, label: string) {
    const match = String(label || '').match(/(\d{1,2}):/);
    if (!dateStart || !match) return null;
    const hour = String(Number(match[1])).padStart(2, '0');
    const d = new Date(`${dateStart}T${hour}:00:00.000+07:00`);
    return Number.isNaN(d.getTime()) ? null : d;
  }

  async getRolling24hAdInsights(limit = 1000) {
    const accountId = this.normalizeAccountId(this.defaultAdAccountId);
    const now = new Date();
    const since = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    const sinceYmd = this.hcmParts(since).ymd;
    const untilYmd = this.hcmParts(now).ymd;

    try {
      const rows = await this.graphList<any>(`/${accountId}/insights`, {
        fields: 'date_start,date_stop,account_id,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,spend,impressions,reach,frequency,clicks,inline_link_clicks,cpc,cpm,ctr,actions,cost_per_action_type,action_values,purchase_roas,website_purchase_roas',
        level: 'ad',
        time_increment: '1',
        time_range: JSON.stringify({ since: sinceYmd, until: untilYmd }),
        breakdowns: 'hourly_stats_aggregated_by_advertiser_time_zone',
        action_report_time: 'conversion',
        use_unified_attribution_setting: 'true',
        limit: String(Math.min(Math.max(Number(limit || 1000), 50), 1000)),
      }, 100);

      const filtered = rows.filter((row: any) => {
        const label = row?.hourly_stats_aggregated_by_advertiser_time_zone;
        const bucket = this.hourlyBucketStart(String(row?.date_start || ''), String(label || ''));
        if (!bucket) return false;
        const t = bucket.getTime();
        return t >= since.getTime() && t <= now.getTime();
      });

      const byAd = new Map<string, any>();
      for (const row of filtered) {
        const id = String(row?.ad_id || '').trim();
        if (!id) continue;
        const metrics = this.metricsFromMetaInsightRow(row);
        const old = byAd.get(id) || {
          id,
          level: 'ad',
          name: row?.ad_name || '',
          campaignName: row?.campaign_name || null,
          adSetName: row?.adset_name || null,
          metaCampaignId: row?.campaign_id || null,
          metaAdSetId: row?.adset_id || null,
          metaAdId: id,
          status: null,
          effectiveStatus: null,
          metricRows: [],
          rawRows: [],
        };
        old.metricRows.push(metrics);
        old.rawRows.push(row);
        byAd.set(id, old);
      }

      const normalized = Array.from(byAd.values()).map((row: any) => ({
        ...row,
        metrics: this.mergeMetricRows(row.metricRows || []),
        rawJson: { rolling24h: true, rows: row.rawRows || [] },
        metricRows: undefined,
        rawRows: undefined,
      }));
      const enriched = await this.enrichLiveRowsWithStructure(normalized, 'ad');

      return {
        ok: true,
        source: 'meta_live_hourly_rolling_24h',
        exactRolling24h: true,
        generatedAt: now.toISOString(),
        window: { since: since.toISOString(), until: now.toISOString() },
        count: enriched.length,
        topAds: enriched,
        summary: this.mergeMetricRows(enriched.map((row: any) => row.metrics || {})),
      };
    } catch (error: any) {
      // Fail closed cho auto-scale: vẫn trả dữ liệu để UI xem, nhưng exactRolling24h=false nên engine không tự scale.
      this.logger.warn(`[META_ROLLING_24H] hourly breakdown unavailable: ${error?.message || error}`);
      const fallback = await this.getLiveInsights({ range: 'today', level: 'ad', limit });
      return {
        ...fallback,
        exactRolling24h: false,
        fallbackReason: error?.message || String(error),
        window: { since: since.toISOString(), until: now.toISOString() },
      };
    }
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

  private pickActionCountLoose(actions: any[] | undefined, aliases: string[]) {
    if (!Array.isArray(actions)) return 0;
    const wanted = aliases.map((name) => String(name || '').toLowerCase());
    return actions.reduce((sum, action) => {
      const type = String(action?.action_type || action?.actionType || '').toLowerCase();
      if (!type) return sum;
      const matched = wanted.some((alias) => type === alias || type === alias);
      return matched ? sum + this.n(action?.value) : sum;
    }, 0);
  }

  private pickCostPerAction(actions: any[] | undefined, aliases: string[]) {
    if (!Array.isArray(actions)) return 0;
    const wanted = aliases.map((name) => String(name || '').toLowerCase());
    for (const action of actions) {
      const type = String(action?.action_type || action?.actionType || '').toLowerCase();
      if (!type) continue;
      const matched = wanted.some((alias) => type === alias || type === alias);
      if (matched) return this.n(action?.value);
    }
    return 0;
  }

  private metaMessagingAliases() {
    return [
      'onsite_conversion.total_messaging_connection',
      'total_messaging_connection',
    ];
  }

  private metaConversationStartAliases() {
    return [
      'onsite_conversion.messaging_conversation_started_7d',
      'messaging_conversation_started_7d',
    ];
  }

  private metaCommentAliases() {
    return ['comment', 'post_comment'];
  }

  private actionPayloadFromInsight(row: any) {
    const raw = row?.rawJson || row?.raw_json || row || {};
    return {
      actions: Array.isArray(row?.actionsJson)
        ? row.actionsJson
        : Array.isArray(row?.actions)
          ? row.actions
          : Array.isArray(raw?.actions)
            ? raw.actions
            : [],
      actionValues: Array.isArray(row?.actionValuesJson)
        ? row.actionValuesJson
        : Array.isArray(row?.action_values)
          ? row.action_values
          : Array.isArray(raw?.action_values)
            ? raw.action_values
            : [],
      costPerActionType: Array.isArray(raw?.cost_per_action_type)
        ? raw.cost_per_action_type
        : Array.isArray(row?.costPerActionTypeJson)
          ? row.costPerActionTypeJson
          : Array.isArray(row?.cost_per_action_type)
            ? row.cost_per_action_type
            : [],
    };
  }

  private metaActionMetrics(row: any, spendInput?: number, purchasesInput?: number, purchaseValueInput?: number) {
    const payload = this.actionPayloadFromInsight(row);
    const spend = this.n(spendInput ?? row?.spend);
    const purchases = this.n(purchasesInput ?? row?.purchases);
    const purchaseValue = this.n(purchaseValueInput ?? row?.purchaseValue);

    // Đã đối chiếu live với Meta Ads Manager ngày 26/05:
    // Kết quả = Lượt bắt đầu cuộc trò chuyện qua tin nhắn.
    const messages = Math.round(this.pickActionCount(payload.actions, this.metaMessagingAliases()));
    const conversationStarts = Math.round(this.pickActionCount(payload.actions, this.metaConversationStartAliases()));
    const comments = Math.round(this.pickActionCount(payload.actions, this.metaCommentAliases()));

    const metaPurchases = Math.round(this.pickActionCount(payload.actions, [
      'purchase',
      'omni_purchase',
      'offsite_conversion.fb_pixel_purchase',
    ]));
    const metaPurchaseValue = this.pickActionValue(payload.actionValues, [
      'purchase',
      'omni_purchase',
      'offsite_conversion.fb_pixel_purchase',
    ]);

    const costPerMessageFromMeta = this.pickCostPerAction(payload.costPerActionType, this.metaMessagingAliases());
    const costPerConversationFromMeta = this.pickCostPerAction(payload.costPerActionType, this.metaConversationStartAliases());
    const costPerMetaPurchaseFromMeta = this.pickCostPerAction(payload.costPerActionType, [
      'purchase',
      'omni_purchase',
      'offsite_conversion.fb_pixel_purchase',
    ]);
    const costPerPurchaseFromMeta = this.pickCostPerAction(payload.costPerActionType, ['purchase', 'omni_purchase', 'offsite_conversion.fb_pixel_purchase']);

    return {
      messages,
      conversationStarts,
      comments,
      costPerMessage: costPerMessageFromMeta || (messages > 0 ? spend / messages : 0),
      costPerConversation: costPerConversationFromMeta || (conversationStarts > 0 ? spend / conversationStarts : 0),
      costPerPurchase: costPerPurchaseFromMeta || (purchases > 0 ? spend / purchases : 0),
      costPerResult: costPerConversationFromMeta || (conversationStarts > 0 ? spend / conversationStarts : 0),
      averagePurchaseValue: purchases > 0 ? purchaseValue / purchases : 0,
    };
  }

  private mergeActionMetrics(rows: any[], sum: any = {}) {
    const spend = this.n(sum?.spend ?? rows.reduce((acc, row) => acc + this.n(row?.spend), 0));
    const purchases = this.n(sum?.purchases ?? rows.reduce((acc, row) => acc + this.n(row?.purchases), 0));
    const purchaseValue = this.n(sum?.purchaseValue ?? rows.reduce((acc, row) => acc + this.n(row?.purchaseValue), 0));

    const messages = rows.reduce((acc, row) => acc + this.metaActionMetrics(row).messages, 0);
    const conversationStarts = rows.reduce((acc, row) => acc + this.metaActionMetrics(row).conversationStarts, 0);
    const comments = rows.reduce((acc, row) => acc + this.metaActionMetrics(row).comments, 0);

    return {
      messages,
      conversationStarts,
      comments,
      costPerMessage: this.n(sum?.messages) > 0 ? spend / this.n(sum?.messages) : 0,
      costPerConversation: this.n(sum?.conversationStarts) > 0 ? spend / this.n(sum?.conversationStarts) : 0,
      costPerResult: this.n(sum?.conversationStarts) > 0 ? spend / this.n(sum?.conversationStarts) : 0,
      averagePurchaseValue: purchases > 0 ? purchaseValue / purchases : 0,
    };
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

      const existingAd = await (this.prisma as any).metaAd.findFirst({
        where: { metaAdId: row.id },
        select: { rawJson: true },
      });
      const preservedMapping =
        existingAd?.rawJson && typeof existingAd.rawJson === 'object'
          ? existingAd.rawJson?._autopilotMapping || null
          : null;
      const mergedRawJson = preservedMapping
        ? { ...row, _autopilotMapping: preservedMapping }
        : row;

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
          rawJson: mergedRawJson,
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
          rawJson: mergedRawJson,
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
      campaign: 'date_start,date_stop,account_id,campaign_id,campaign_name,spend,impressions,reach,frequency,clicks,inline_link_clicks,cpc,cpm,ctr,actions,cost_per_action_type,action_values,purchase_roas,website_purchase_roas',
      adset: 'date_start,date_stop,account_id,campaign_id,campaign_name,adset_id,adset_name,spend,impressions,reach,frequency,clicks,inline_link_clicks,cpc,cpm,ctr,actions,cost_per_action_type,action_values,purchase_roas,website_purchase_roas',
      ad: 'date_start,date_stop,account_id,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,spend,impressions,reach,frequency,clicks,inline_link_clicks,cpc,cpm,ctr,actions,cost_per_action_type,action_values,purchase_roas,website_purchase_roas',
    };

    for (const level of levels) {
      const rows = await this.graphList<any>(`/${accountId}/insights`, {
        fields: fieldsByLevel[level],
        level,
        time_increment: '1',
        action_report_time: 'conversion',
        use_unified_attribution_setting: 'true',
        time_range: JSON.stringify(dateRange),
        limit: String(Math.min(Math.max(Number(input.limit || 500), 50), 1000)),
      }, 100);

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
        const actionMetrics = this.metaActionMetrics(row, spend, purchases, purchaseValue);
        const metaResult = actionMetrics.conversationStarts;
        const costPerPurchase = actionMetrics.costPerResult || (metaResult > 0 ? spend / metaResult : 0);
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
          purchases: Math.round(metaResult),
          purchaseValue,
          costPerPurchase,
          roas,
          actionsJson: row.actions || undefined,
          actionValuesJson: row.action_values || undefined,
          rawJson: {
            ...row,
            metaActionMetrics: actionMetrics,
          },
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
      const insightOnlyAdSync =
        Array.isArray(input.levels) &&
        input.levels.length === 1 &&
        String(input.levels[0]).toLowerCase() === 'ad' &&
        input.includeInsights !== false;

      // Khi chỉ sync ad insights, tuyệt đối không kéo structure/creative để tránh đơ server.
      const structure = input.includeStructure === false || insightOnlyAdSync
        ? null
        : await this.syncStructure(undefined, Number(input.limit || 500));
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
    const metaPurchases = this.n(sum?.metaPurchases);
    const purchaseValue = this.n(sum?.purchaseValue);
    const metaPurchaseValue = this.n(sum?.metaPurchaseValue);
    const conversationStarts = this.n(sum?.conversationStarts);
    const messages = this.n(sum?.messages);
    return {
      spend,
      impressions,
      reach,
      clicks,
      inlineLinkClicks,

      // Legacy key dùng cho cột "Kết quả" trong Ads Center: giữ là số bắt đầu chat để không phá UI hiện tại.
      purchases: conversationStarts,
      purchaseValue,
      ctr: impressions > 0 ? (clicks / impressions) * 100 : 0,
      cpc: clicks > 0 ? spend / clicks : 0,
      cpm: impressions > 0 ? (spend / impressions) * 1000 : 0,
      costPerPurchase: purchases > 0 ? spend / purchases : 0,
      roas: spend > 0 ? purchaseValue / spend : 0,
      messages,
      conversationStarts,
      comments: this.n(sum?.comments),
      costPerMessage: messages > 0 ? spend / messages : 0,
      costPerConversation: conversationStarts > 0 ? spend / conversationStarts : 0,
      costPerResult: conversationStarts > 0 ? spend / conversationStarts : 0,
      averagePurchaseValue: purchases > 0 ? purchaseValue / purchases : 0,

      // Meta purchase thật để card "Lượt mua Meta" không bị lấy nhầm 870 tin nhắn.
      metaPurchases,
      metaPurchaseValue,
      costPerMetaPurchase: metaPurchases > 0 ? spend / metaPurchases : 0,
      metaAveragePurchaseValue: metaPurchases > 0 ? metaPurchaseValue / metaPurchases : 0,
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

    const actionWhere: any = { ...where };
    if (level === 'campaign') actionWhere.metaCampaignId = { in: ids };
    if (level === 'adset') actionWhere.metaAdSetId = { in: ids };
    if (level === 'ad') actionWhere.metaAdId = { in: ids };

    const actionRows = ids.length
      ? await (this.prisma as any).metaAdInsightDaily.findMany({
          where: actionWhere,
          select: {
            metaCampaignId: true,
            metaAdSetId: true,
            metaAdId: true,
            spend: true,
            purchases: true,
            purchaseValue: true,
            actionsJson: true,
            actionValuesJson: true,
            rawJson: true,
          },
          take: 5000,
        })
      : [];

    const actionMap = new Map<string, any[]>();
    for (const item of actionRows) {
      const key = level === 'campaign' ? item.metaCampaignId : level === 'adset' ? item.metaAdSetId : item.metaAdId;
      if (!key) continue;
      const list = actionMap.get(key) || [];
      list.push(item);
      actionMap.set(key, list);
    }

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
        metrics: {
          ...this.metricsFromSum(row._sum),
          ...this.mergeActionMetrics(actionMap.get(id) || [], row._sum),
        },
      };
    });
  }


  private metricsFromMetaInsightRow(row: any) {
    const spend = this.n(row?.spend);
    const impressions = Math.round(this.n(row?.impressions));
    const reach = Math.round(this.n(row?.reach));
    const clicks = Math.round(this.n(row?.clicks));
    const inlineLinkClicks = Math.round(this.n(row?.inline_link_clicks ?? row?.inlineLinkClicks));
    const cpc = this.n(row?.cpc);
    const cpm = this.n(row?.cpm);
    const ctr = this.n(row?.ctr);

    const payload = this.actionPayloadFromInsight(row);
    const messages = Math.round(this.pickActionCount(payload.actions, this.metaMessagingAliases()));
    const conversationStarts = Math.round(this.pickActionCount(payload.actions, this.metaConversationStartAliases()));
    const comments = Math.round(this.pickActionCount(payload.actions, this.metaCommentAliases()));

    const metaPurchases = Math.round(this.pickActionCount(payload.actions, [
      'purchase',
      'omni_purchase',
      'offsite_conversion.fb_pixel_purchase',
    ]));
    const metaPurchaseValue = this.pickActionValue(payload.actionValues, [
      'purchase',
      'omni_purchase',
      'offsite_conversion.fb_pixel_purchase',
    ]);

    const costPerMessageFromMeta = this.pickCostPerAction(payload.costPerActionType, this.metaMessagingAliases());
    const costPerConversationFromMeta = this.pickCostPerAction(payload.costPerActionType, this.metaConversationStartAliases());
    const costPerMetaPurchaseFromMeta = this.pickCostPerAction(payload.costPerActionType, [
      'purchase',
      'omni_purchase',
      'offsite_conversion.fb_pixel_purchase',
    ]);

    return {
      spend,
      impressions,
      reach,
      clicks,
      inlineLinkClicks,
      cpc,
      cpm,
      ctr,

      // Meta Ads Manager: Kết quả = Lượt bắt đầu cuộc trò chuyện qua tin nhắn.
      purchases: conversationStarts,
      purchaseValue: 0,
      roas: 0,

      messages,
      conversationStarts,
      comments,
      costPerMessage: costPerMessageFromMeta || (messages > 0 ? spend / messages : 0),
      costPerConversation: costPerConversationFromMeta || (conversationStarts > 0 ? spend / conversationStarts : 0),
      costPerResult: costPerConversationFromMeta || (conversationStarts > 0 ? spend / conversationStarts : 0),
      averagePurchaseValue: 0,
      metaPurchases,
      metaPurchaseValue,
      costPerMetaPurchase: costPerMetaPurchaseFromMeta || (metaPurchases > 0 ? spend / metaPurchases : 0),
      metaAveragePurchaseValue: metaPurchases > 0 ? metaPurchaseValue / metaPurchases : 0,
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
      metaPurchases: rows.reduce((sum, row) => sum + this.n(row?.metaPurchases), 0),
      metaPurchaseValue: rows.reduce((sum, row) => sum + this.n(row?.metaPurchaseValue), 0),
      messages: rows.reduce((sum, row) => sum + this.n(row?.messages), 0),
      conversationStarts: rows.reduce((sum, row) => sum + this.n(row?.conversationStarts), 0),
      comments: rows.reduce((sum, row) => sum + this.n(row?.comments), 0),
    });
  }

  private async fetchOfficialAccountDailyRows(range: MetaDateRange, query: any = {}) {
    // V4 Accuracy Layer:
    // Meta Ads Manager total spend is safest at account-level insights.
    // DB rows remain used for drilldown/creative table, but KPI total should reconcile with Meta official.
    const accountId = this.normalizeAccountId(query.metaAccountId || this.defaultAdAccountId);
    const rows = await this.graphList<any>(`/${accountId}/insights`, {
      fields: 'date_start,date_stop,account_id,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,spend,impressions,reach,frequency,clicks,inline_link_clicks,cpc,cpm,ctr,actions,cost_per_action_type,action_values,purchase_roas,website_purchase_roas',
      time_increment: '1',
      time_range: JSON.stringify(range),
      action_report_time: 'conversion',
        use_unified_attribution_setting: 'true',
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

  async getLiveInsights(query: {
    range?: string;
    fromDate?: string;
    toDate?: string;
    level?: MetaInsightLevel;
    limit?: number;
  }) {
    const rawRange = String(query.range || 'today');
    const dtoRange =
      rawRange === 'last_7d' ? '7d' :
      rawRange === '7days' ? '7d' :
      rawRange;

    const dateRange = this.getMetaLiveDateRange({
      range: dtoRange,
      fromDate: query.fromDate,
      toDate: query.toDate,
    });

    const level = (query.level || 'ad') as MetaInsightLevel;
    const accountId = this.normalizeAccountId(this.defaultAdAccountId);

    const fieldsByLevel: Record<MetaInsightLevel, string> = {
      campaign:
        'date_start,date_stop,account_id,campaign_id,campaign_name,spend,impressions,reach,frequency,clicks,inline_link_clicks,cpc,cpm,ctr,actions,cost_per_action_type,action_values,purchase_roas,website_purchase_roas',
      adset:
        'date_start,date_stop,account_id,campaign_id,campaign_name,adset_id,adset_name,spend,impressions,reach,frequency,clicks,inline_link_clicks,cpc,cpm,ctr,actions,cost_per_action_type,action_values,purchase_roas,website_purchase_roas',
      ad:
        'date_start,date_stop,account_id,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,spend,impressions,reach,frequency,clicks,inline_link_clicks,cpc,cpm,ctr,actions,cost_per_action_type,action_values,purchase_roas,website_purchase_roas',
    };

    // Quan trọng: lấy theo insight live của Meta trong range.
    // Insight API chỉ trả những entity có data trong range, kể cả hiện đã tắt.
    const rows = await this.graphList<any>(`/${accountId}/insights`, {
      fields: fieldsByLevel[level],
      level,
      time_increment: 'all_days',
      time_range: JSON.stringify(dateRange),
      action_report_time: 'conversion',
      use_unified_attribution_setting: 'true',
      limit: String(Math.min(Math.max(Number(query.limit || 1000), 50), 1000)),
    }, 100);

    const normalized = rows.map((row) => {
      const metrics = this.metricsFromMetaInsightRow(row);
      
    return {
        id:
          level === 'campaign'
            ? String(row.campaign_id || row.campaign_name || '')
            : level === 'adset'
              ? String(row.adset_id || row.adset_name || '')
              : String(row.ad_id || row.ad_name || ''),
        level,
        name:
          level === 'campaign'
            ? String(row.campaign_name || '')
            : level === 'adset'
              ? String(row.adset_name || '')
              : String(row.ad_name || ''),
        campaignName: row.campaign_name || null,
        adSetName: row.adset_name || null,
        metaCampaignId: row.campaign_id || null,
        metaAdSetId: row.adset_id || null,
        metaAdId: row.ad_id || null,
        status: null,
        effectiveStatus: null,
        metrics,
        rawJson: row,
      };
    });

    const enrichedNormalized = await this.enrichLiveRowsWithStructure(normalized, level);

    const summary = this.mergeMetricRows(enrichedNormalized.map((row) => row.metrics));

    return {
      ok: true,
      source: 'meta_live',
      generatedAt: new Date().toISOString(),
      range: dateRange,
      level,
      count: enrichedNormalized.length,
      summary,
      officialSummary: summary,
      dbSummary: null,
      reconciliation: {
        officialSpend: summary.spend,
        dbSpend: 0,
        spendDiff: 0,
        spendDiffPercent: 0,
      },
      statusBreakdown: {
        campaigns: { total: level === 'campaign' ? enrichedNormalized.length : 0, active: 0, inactive: 0 },
        adSets: { total: level === 'adset' ? enrichedNormalized.length : 0, active: 0, inactive: 0 },
        ads: { total: level === 'ad' ? enrichedNormalized.length : 0, active: 0, inactive: 0 },
      },
      dailyRows: [],
      topCampaigns: level === 'campaign' ? enrichedNormalized : [],
      topAdSets: level === 'adset' ? enrichedNormalized : [],
      topAds: level === 'ad' ? enrichedNormalized : [],
      warnings: [],
      latestLogs: [],
      attribution: {
        enabled: true,
        mode: 'meta_live_first',
        note: 'Meta metrics lấy live từ Graph Insights theo range; DB chỉ dùng để ghép đơn nội bộ.',
      },
    };
  }

  private getMetaLiveDateRange(query: {
    range?: string;
    fromDate?: string;
    toDate?: string;
  }) {
    if (query.fromDate && query.toDate) {
      return { since: query.fromDate, until: query.toDate };
    }

    const now = new Date();
    const pad = (n: number) => String(n).padStart(2, '0');
    const fmt = (d: Date) =>
      `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;

    const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    const yesterday = new Date(today);
    yesterday.setDate(yesterday.getDate() - 1);

    const range = String(query.range || 'today');

    if (range === 'today') {
      return { since: fmt(today), until: fmt(today) };
    }

    if (range === 'yesterday') {
      return { since: fmt(yesterday), until: fmt(yesterday) };
    }

    const rollingMap: Record<string, number> = {
      '7d': 7,
      '7days': 7,
      'last_7d': 7,
      '10d': 10,
      '10days': 10,
      'last_10d': 10,
      '30d': 30,
      '30days': 30,
      'last_30d': 30,
    };

    const days = rollingMap[range];
    if (days) {
      // Meta Ads Manager "7 ngày qua" là 7 ngày đã hoàn tất,
      // ví dụ ngày 27/05 thì range phải là 20/05 - 26/05, không lấy ngày 27/05.
      const since = new Date(yesterday);
      since.setDate(since.getDate() - days + 1);
      return { since: fmt(since), until: fmt(yesterday) };
    }

    return this.getDateRange({
      range: range as any,
      fromDate: query.fromDate,
      toDate: query.toDate,
    } as SyncMetaAdsDto);
  }

  private pickFirstString(...values: any[]): string | null {
    for (const value of values) {
      if (typeof value === 'string' && value.trim()) return value.trim();
    }
    return null;
  }

  private pickMetaThumbnail(entity: any): string | null {
    if (!entity) return null;
    const raw = entity.rawJson || entity.raw || entity.creativeJson || {};
    return this.pickFirstString(
      entity.thumbnailUrl,
      entity.thumbnail_url,
      entity.imageUrl,
      entity.image_url,
      entity.creativeThumbnailUrl,
      entity.creative?.thumbnailUrl,
      entity.creative?.thumbnail_url,
      entity.creative?.imageUrl,
      entity.creative?.image_url,
      raw.thumbnail_url,
      raw.image_url,
      raw.creative?.thumbnail_url,
      raw.creative?.image_url,
      raw.object_story_spec?.link_data?.picture,
      raw.object_story_spec?.video_data?.image_url,
    );
  }

  private pickMetaStatus(entity: any): string | null {
    if (!entity) return null;
    const raw = entity.rawJson || entity.raw || {};
    return this.pickFirstString(
      entity.effectiveStatus,
      entity.effective_status,
      entity.status,
      entity.configuredStatus,
      entity.configured_status,
      raw.effective_status,
      raw.status,
      raw.configured_status,
    );
  }

  private async enrichLiveRowsWithStructure(rows: any[], level: string) {
    if (!Array.isArray(rows) || !rows.length) return rows;

    // Quan trọng: insight live chỉ có số liệu, không có ảnh/trạng thái.
    // Với level ad, gọi trực tiếp Graph /?ids=... để lấy effective_status + creative thumbnail.
    if (level === 'ad') {
      const adIds = Array.from(
        new Set(
          rows
            .map((row) => String(row?.metaAdId || row?.id || '').trim())
            .filter(Boolean),
        ),
      );

      if (!adIds.length) return rows;

      const enrichMap = new Map<string, any>();

      for (let i = 0; i < adIds.length; i += 50) {
        const chunk = adIds.slice(i, i + 50);
        try {
          const data = await this.graphGet<Record<string, any>>('/', {
            ids: chunk.join(','),
            fields:
              'id,name,status,effective_status,configured_status,creative{id,thumbnail_url,image_url,object_story_spec}',
          });

          for (const id of chunk) {
            const item = (data as any)?.[id];
            if (!item) continue;
            const creative = item.creative || {};
            const objectStory = creative.object_story_spec || {};
            enrichMap.set(id, {
              status: item.status || item.configured_status || item.effective_status || null,
              effectiveStatus: item.effective_status || item.status || item.configured_status || null,
              thumbnailUrl:
                creative.thumbnail_url ||
                creative.image_url ||
                objectStory?.link_data?.picture ||
                objectStory?.video_data?.image_url ||
                null,
            });
          }
        } catch (error) {
          this.logger.warn(`[META_LIVE_ENRICH_GRAPH] skip ad chunk: ${error?.message || error}`);
        }
      }

      return rows.map((row) => {
        const id = String(row?.metaAdId || row?.id || '').trim();
        const extra = enrichMap.get(id);
        if (!extra) return row;
        return {
          ...row,
          status: row.status || extra.status,
          effectiveStatus: row.effectiveStatus || extra.effectiveStatus,
          thumbnailUrl: row.thumbnailUrl || extra.thumbnailUrl,
          imageUrl: row.imageUrl || extra.thumbnailUrl,
        };
      });
    }

    // Campaign/adset: chỉ cần trạng thái; nếu không lấy được thì để nguyên.
    const ids = Array.from(
      new Set(
        rows
          .map((row) =>
            String(
              level === 'campaign'
                ? row?.metaCampaignId || row?.id || ''
                : row?.metaAdSetId || row?.id || '',
            ).trim(),
          )
          .filter(Boolean),
      ),
    );

    if (!ids.length) return rows;

    const enrichMap = new Map<string, any>();
    for (let i = 0; i < ids.length; i += 50) {
      const chunk = ids.slice(i, i + 50);
      try {
        const data = await this.graphGet<Record<string, any>>('/', {
          ids: chunk.join(','),
          fields: 'id,name,status,effective_status,configured_status',
        });

        for (const id of chunk) {
          const item = (data as any)?.[id];
          if (!item) continue;
          enrichMap.set(id, {
            status: item.status || item.configured_status || item.effective_status || null,
            effectiveStatus: item.effective_status || item.status || item.configured_status || null,
          });
        }
      } catch (error) {
        this.logger.warn(`[META_LIVE_ENRICH_GRAPH] skip ${level} chunk: ${error?.message || error}`);
      }
    }

    return rows.map((row) => {
      const id = String(
        level === 'campaign' ? row?.metaCampaignId || row?.id || '' : row?.metaAdSetId || row?.id || '',
      ).trim();
      const extra = enrichMap.get(id);
      if (!extra) return row;
      return {
        ...row,
        status: row.status || extra.status,
        effectiveStatus: row.effectiveStatus || extra.effectiveStatus,
      };
    });
  }

}
