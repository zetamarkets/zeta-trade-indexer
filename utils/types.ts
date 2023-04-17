import { PublicKey } from "@solana/web3.js";
import { Kind } from "@zetamarkets/sdk/dist/types";
import { BN } from "@zetamarkets/anchor";

export interface EventQueueHeader {
  head: number;
  count: number;
  seqNum: number;
}

export interface EventQueueLayout {
  eventFlags: {
    fill: boolean;
    out: boolean;
    bid: boolean;
    maker: boolean;
  };
  openOrdersSlot: number;
  feeTier: number;
  nativeQuantityReleased: BN;
  nativeQuantityPaid: BN;
  nativeFeeOrRebate: BN;
  orderId: BN;
  openOrders: PublicKey;
  clientOrderId: BN;
}

export interface Trade {
  seq_num: number;
  timestamp: number;
  authority: string;
  asset: string;
  is_maker: boolean;
  is_bid: boolean;
  price: number;
  size: number;
  client_order_id: string;
  order_id: string;
}

export interface Pricing {
  timestamp: number;
  slot?: number;
  market_index: number;
  kind: Kind;
  theo: number;
}
