export class QuoteAhamoveDto {
  serviceId?: string;
  fromName?: string;
  fromPhone?: string;
  fromAddress?: string;
  toName!: string;
  toPhone!: string;
  toAddress!: string;
  codAmount?: number;
  note?: string;
  items?: any[];
}
