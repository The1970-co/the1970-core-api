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
