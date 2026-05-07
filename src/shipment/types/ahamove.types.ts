export type AhamoveWebhookPayload = {
  order_id?: string;
  id?: string;
  status?: string;
  sub_status?: string;
  service_id?: string;
  shared_link?: string;
  tracking_url?: string;
  fee?: number;
  total_fee?: number;
  cod_amount?: number;
  path?: any[];
  [key: string]: any;
};

export type AhamoveQuotePayload = {
  serviceId?: string;
  fromName?: string;
  fromPhone?: string;
  fromAddress?: string;
  toName: string;
  toPhone: string;
  toAddress: string;
  codAmount?: number;
  note?: string;
  items?: any[];
};

export type CreateAhamoveShipmentPayload = AhamoveQuotePayload & {
  orderCode?: string;
};
