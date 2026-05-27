
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

type MetaRange = 'today' | 'yesterday' | '7d' | '10d' | '30d' | 'custom';

type MetaAdsInsightRow = {
  date_start?: string;
  date_stop?: string;
  campaign_id?: string;
  campaign_name?: string;
  spend?: string;
  impressions?: string;
  clicks?: string;
};

type MetaGraphListResponse<T> = {
  data?: T[];
  paging?: {
    next?: string;
  };
  error?: {
    message?: string;
    type?: string;
    code?: number;
    error_subcode?: number;
    fbtrace_id?: string;
  };
};

@Injectable()
export class MetaAdsService {
  private readonly logger = new Logger(MetaAdsService.name);

  private get version() {
    return process.env.META_API_VERSION || 'v25.0';
  }

  private get accessToken() {
    return process.env.META_ACCESS_TOKEN || '';
  }

  private get adAccountId() {
    return process.env.META_AD_ACCOUNT_ID || 'act_474042859768081';
  }

  private get accountName() {
    return process.env.META_AD_ACCOUNT_NAME || 'Nam Nguyen';
  }

  private toDateInput(date: Date) {
    const yyyy = date.getFullYear();
    const mm = String(date.getMonth() + 1).padStart(2, '0');
    const dd = String(date.getDate()).padStart(2, '0');
    return `${yyyy}-${mm}-${dd}`;
  }

  private getDateRange(range?: MetaRange, fromDate?: string, toDate?: string) {
    if (range === 'custom' && fromDate && toDate) {
      return { since: fromDate, until: toDate };
    }

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

    return {
      since: this.toDateInput(start),
      until: this.toDateInput(end),
    };
  }

  private async graphGet<T>(path: string, params: Record<string, string>) {
    if (!this.accessToken) {
      throw new Error('META_ACCESS_TOKEN is missing');
    }

    const url = new URL(`https://graph.facebook.com/${this.version}${path}`);
    Object.entries(params).forEach(([key, value]) => {
      if (value != null && value !== '') url.searchParams.set(key, value);
    });
    url.searchParams.set('access_token', this.accessToken);

    const res = await fetch(url.toString(), { method: 'GET' });
    const json = (await res.json()) as T & { error?: any };

    if (!res.ok || (json as any).error) {
      this.logger.error(`Meta Ads API error: ${JSON.stringify((json as any).error || json)}`);
      throw new Error((json as any).error?.message || 'Meta Ads API error');
    }

    return json;
  }

  async testConnection() {
    return this.graphGet<{ data: any[] }>('/me/adaccounts', {
      fields: 'id,name,account_id,currency,account_status',
    });
  }

  async getCampaignInsights(params: {
    range?: MetaRange;
    fromDate?: string;
    toDate?: string;
  }) {
    if (!this.accessToken) {
      throw new Error('META_ACCESS_TOKEN is missing');
    }

    const { since, until } = this.getDateRange(
      params.range || 'today',
      params.fromDate,
      params.toDate,
    );

    const firstUrl = new URL(
      `https://graph.facebook.com/${this.version}/${this.adAccountId}/insights`,
    );
    firstUrl.searchParams.set(
      'fields',
      'date_start,date_stop,campaign_id,campaign_name,spend,impressions,clicks,actions,action_values,cost_per_action_type',
    );
    firstUrl.searchParams.set('level', 'campaign');
    firstUrl.searchParams.set('time_increment', '1');
    firstUrl.searchParams.set('time_range', JSON.stringify({ since, until }));
    firstUrl.searchParams.set('limit', '500');
    firstUrl.searchParams.set('access_token', this.accessToken);

    const rows: MetaAdsInsightRow[] = [];
    let nextUrl: string | undefined = firstUrl.toString();
    let page = 0;

    while (nextUrl && page < 20) {
      page += 1;

      const res = await fetch(nextUrl, { method: 'GET' });
      const json = (await res.json()) as MetaGraphListResponse<MetaAdsInsightRow>;

      if (!res.ok || json.error) {
        this.logger.error(`Meta Ads API error: ${JSON.stringify(json.error || json)}`);
        throw new Error(json.error?.message || 'Meta Ads API error');
      }

      if (Array.isArray(json.data)) {
        rows.push(...json.data);
      }

      nextUrl = json.paging?.next;
    }

    return rows;
  }

  async getSummary(params: {
    range?: MetaRange;
    fromDate?: string;
    toDate?: string;
  }) {
    const rows = await this.getCampaignInsights(params);
    const totalAdsCost = rows.reduce((sum, row) => sum + Number(row.spend || 0), 0);
    const impressions = rows.reduce((sum, row) => sum + Number(row.impressions || 0), 0);
    const clicks = rows.reduce((sum, row) => sum + Number(row.clicks || 0), 0);

    return {
      connected: true,
      adAccountId: this.adAccountId,
      accountName: this.accountName,
      totalAdsCost,
      impressions,
      clicks,
      campaigns: rows,
    };
  }
}
