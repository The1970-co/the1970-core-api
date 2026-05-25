export type MetaInsightLevel = 'campaign' | 'adset' | 'ad';
export type MetaSyncRange = 'today' | 'yesterday' | '7d' | '10d' | '30d' | 'custom';

export class SyncMetaAdsDto {
  range?: MetaSyncRange;
  fromDate?: string;
  toDate?: string;
  levels?: MetaInsightLevel[];
  includeStructure?: boolean;
  includeInsights?: boolean;
  limit?: number;
}
