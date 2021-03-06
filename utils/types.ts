import { Kind } from "@zetamarkets/sdk/dist/types";

export interface EventQueueHeader {
  head: number;
  count: number;
  seqNum: number;
}

export interface Trade {
  seq_num: number;
  client_order_id: string;
  order_id: string;
  timestamp: number;
  owner_pub_key: string;
  underlying: string;
  market_index: number;
  expiry_timestamp: number;
  strike: number;
  kind: Kind;
  is_maker: boolean;
  is_bid: boolean;
  price: number;
  size: number;
}

export interface Pricing {
  timestamp: number;
  slot?: number;
  expiry_series_index: number;
  expiry_timestamp: number;
  market_index: number;
  strike: number;
  kind: Kind;
  theo: number;
  delta: number;
  sigma: number;
  vega: number;
}

export interface Surface {
  timestamp: number;
  slot?: number;
  expiry_series_index: number;
  expiry_timestamp: number;
  vol_surface: number[];
  nodes: number[];
  interest_rate: number;
}
