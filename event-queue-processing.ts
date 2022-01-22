import {
  constants,
  Exchange,
  Market,
  programTypes,
  utils,
} from "@zetamarkets/sdk";
import { SYSVAR_CLOCK_PUBKEY } from "@solana/web3.js";
import { Trade } from "./utils/types";
import { putFirehoseBatch } from "./utils/firehose";
import { decodeRecentEvents } from "./utils";
import { PublicKey } from "@solana/web3.js";
import { putDynamo } from "./utils/dynamodb";
import { FETCH_INTERVAL } from "./utils/constants";

let fetchingMarkets: boolean[];
fetchingMarkets = new Array(constants.ACTIVE_MARKETS).fill(false);

export async function collectMarketData(lastSeqNum?: Record<number, number>) {
  const numberOfExpirySeries = Exchange.zetaGroup.expirySeries.length;

  let accountInfo = await this._provider.connection.getAccountInfo(
    SYSVAR_CLOCK_PUBKEY
  );
  let clockData = utils.getClockData(accountInfo);
  let timestamp = clockData.timestamp;

  for (var i = 0; i < numberOfExpirySeries; i++) {
    let expiryIndex = i;
    let expirySeries = Exchange.markets.expirySeries[expiryIndex];

    // Fetch trades if the market is active or
    // the market expired < 60 seconds ago.
    // 60 second buffer to handle trades that happened right as expiry occurred.
    if (
      expirySeries.activeTs > timestamp ||
      expirySeries.expiryTs + 60 < timestamp
    ) {
      continue;
    }

    let markets = Exchange.markets.getMarketsByExpiryIndex(expiryIndex);
    for (var j = 0; j < markets.length; j++) {
      let market = markets[j];
      let marketIndex = market.marketIndex;
      if (!fetchingMarkets[marketIndex]) {
        fetchingMarkets[marketIndex] = true;
        collectEventQueue(markets[j], lastSeqNum);
        fetchingMarkets[marketIndex] = false;
      }
    }
  }
}

async function fetchTrades(
  market: Market,
  lastSeqNum?: number
): Promise<[Trade[], number]> {
  const accountInfo = await Exchange.provider.connection.getAccountInfo(
    market.serumMarket.decoded.eventQueue
  );

  const { header, events } = decodeRecentEvents(accountInfo.data, lastSeqNum);
  lastSeqNum = header.seqNum;
  let trades: Trade[] = [];

  for (let i = 0; i < events.length; i++) {
    const openOrdersMap = await utils.getOpenOrdersMap(
      new PublicKey(process.env.PROGRAM_ID),
      events[i].openOrders
    );
    const { userKey } = (await Exchange.program.account.openOrdersMap.fetch(
      openOrdersMap[0]
    )) as programTypes.OpenOrdersMap;
    let price, size;
    // Trade has occured
    if (events[i].eventFlags.fill) {
      if (events[i].eventFlags.maker) {
        if (events[i].eventFlags.bid) {
          price =
            events[i].nativeQuantityPaid.toNumber() /
            events[i].nativeQuantityReleased.toNumber();
          size = events[i].nativeQuantityReleased.toNumber();
        } else {
          price =
            events[i].nativeQuantityReleased.toNumber() /
            events[i].nativeQuantityPaid.toNumber();
          size = events[i].nativeQuantityPaid.toNumber();
        }
      } else {
        if (events[i].eventFlags.bid) {
          price =
            events[i].nativeQuantityPaid.toNumber() /
            events[i].nativeQuantityReleased.toNumber();
          size = events[i].nativeQuantityReleased.toNumber();
        } else {
          price =
            events[i].nativeQuantityReleased.toNumber() /
            events[i].nativeQuantityPaid.toNumber();
          size = events[i].nativeQuantityPaid.toNumber();
        }
      }
    } else {
      continue;
    }

    let expirySeries = market.expirySeries;

    let newTradeObject: Trade = {
      seq_num: lastSeqNum - events.length + i + 1,
      timestamp: Math.floor(Date.now() / 1000),
      owner_pub_key: userKey.toString(),
      expiry_series_index: expirySeries.expiryIndex,
      expiry_timestamp: expirySeries.expiryTs,
      market_index: market.marketIndex,
      strike: market.strike,
      kind: market.kind,
      is_maker: events[i].eventFlags.maker,
      is_bid: events[i].eventFlags.bid,
      price: utils.convertNativeIntegerToDecimal(price),
      size: utils.convertNativeLotSizeToDecimal(size),
    };
    trades.push(newTradeObject);
  }
  return [trades, lastSeqNum];
}

async function collectEventQueue(
  market: Market,
  lastSeqNum?: Record<number, number>
) {
  try {
    const [trades, currentSeqNum] = await fetchTrades(
      market,
      lastSeqNum[market.marketIndex]
    );
    lastSeqNum[market.marketIndex] = currentSeqNum;
    if (trades.length > 0) {
      putDynamo(trades, process.env.DYNAMO_TABLE_NAME);
      putFirehoseBatch(trades, process.env.FIREHOSE_DS_NAME);
    }
  } catch (e) {
    console.warn("Unable to fetch event queue: ", e);
  }
}
