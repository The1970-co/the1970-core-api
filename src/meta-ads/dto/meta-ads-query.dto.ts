export class MetaAdsQueryDto {
  range?: 'today' | 'yesterday' | '7d' | '10d' | '30d' | 'custom';
  fromDate?: string;
  toDate?: string;
  level?: 'campaign' | 'adset' | 'ad';
  status?: string;
  search?: string;
  page?: string;
  limit?: string;
}
