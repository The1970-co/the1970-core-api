
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

  private hcmYmd(date: Date) {
    return new Intl.DateTimeFormat('en-CA', {
      timeZone: 'Asia/Ho_Chi_Minh',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    }).format(date);
  }

  private hcmBoundary(ymd: string, end = false) {
    return new Date(`${ymd}${end ? 'T23:59:59.999+07:00' : 'T00:00:00.000+07:00'}`);
  }

  private addDays(ymd: string, days: number) {
    const d = this.hcmBoundary(ymd, false);
    d.setUTCDate(d.getUTCDate() + days);
    return this.hcmYmd(d);
  }

  private getDateRange(range?: MetaRange, fromDate?: string, toDate?: string) {
    if (range === 'custom' && fromDate && toDate) {
      return { since: fromDate.slice(0, 10), until: toDate.slice(0, 10) };
    }

    const today = this.hcmYmd(new Date());
    let since = today;
    let until = today;

    if (range === 'yesterday') {
      since = this.addDays(today, -1);
      until = since;
    } else if (range === '7d') {
      // Giống Meta "7 ngày qua": 7 ngày đã kết thúc, không lấy hôm nay.
      until = this.addDays(today, -1);
      since = this.addDays(until, -6);
    } else if (range === '10d') {
      // Dashboard War Room đang xem 10 ngày gồm hôm nay + 9 ngày trước.
      since = this.addDays(today, -9);
      until = today;
    } else if (range === '30d') {
      since = this.addDays(today, -29);
      until = today;
    }

    return { since, until };
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

  /**
   * Summary endpoint cũ của Dashboard.
   *
   * Lỗi cũ: sum theo level=campaign + paging riêng nên có lúc lệch với Meta Ads Manager.
   * Fix an toàn: query account-level insights theo ngày, time_increment=1.
   * Đây là tổng chi tiêu chính thức của account trong range, không phụ thuộc trạng thái,
   * không phụ thuộc danh sách ads đang hiển thị, không phụ thuộc filter FE.
   */
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
      'date_start,date_stop,spend,impressions,clicks,reach,inline_link_clicks',
    );
    firstUrl.searchParams.set('time_increment', '1');
    firstUrl.searchParams.set('time_range', JSON.stringify({ since, until }));
    firstUrl.searchParams.set('action_report_time', 'conversion');
    firstUrl.searchParams.set('use_unified_attribution_setting', 'true');
    firstUrl.searchParams.set('limit', '100');
    firstUrl.searchParams.set('access_token', this.accessToken);

    const rows: MetaAdsInsightRow[] = [];
    let nextUrl: string | undefined = firstUrl.toString();
    let page = 0;

    while (nextUrl && page < 10) {
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
      // Giữ key campaigns để không vỡ Dashboard cũ, nhưng dữ liệu là account daily rows.
      campaigns: rows,
      daily: rows,
      source: 'meta_account_insights_daily',
      range: this.getDateRange(params.range || 'today', params.fromDate, params.toDate),
    };
  }
}
