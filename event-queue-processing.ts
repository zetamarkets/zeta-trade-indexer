import {
  constants,
  Exchange,
  Market,
  programTypes,
  utils,
} from "@zetamarkets/sdk";
import { SYSVAR_CLOCK_PUBKEY } from "@solana/web3.js";
import { Trade } from "./utils/types";
import { decodeRecentEvents } from "./utils";
import { PublicKey } from "@solana/web3.js";
import { putFirehoseBatch } from "./utils/firehose";
import { putDynamo } from "./utils/dynamodb";
import { putLastSeqNumMetadata } from "./utils/s3";
import { alert } from "./utils/telegram";

let fetchingMarkets: boolean[];
fetchingMarkets = new Array(constants.ACTIVE_MARKETS).fill(false);

export async function collectMarketData(lastSeqNum?: Record<number, number>) {
  let accountInfo = await Exchange.connection.getAccountInfo(
    SYSVAR_CLOCK_PUBKEY
  );
  let clockData = utils.getClockData(accountInfo);
  let timestamp = clockData.timestamp;

  await Promise.all(
    Exchange.markets.markets.map(async (market) => {
      let expirySeries = market.expirySeries;

      // Fetch trades if the market is active or
      // the market expired < 60 seconds ago.
      // 60 second buffer to handle trades that happened right as expiry occurred.
      if (
        expirySeries.activeTs > timestamp ||
        expirySeries.expiryTs + 60 < timestamp
      ) {
        return;
      }

      let marketIndex = market.marketIndex;
      if (!fetchingMarkets[marketIndex]) {
        fetchingMarkets[marketIndex] = true;
        await collectEventQueue(market, lastSeqNum);
        fetchingMarkets[marketIndex] = false;
      }
    })
  );
}

async function fetchTrades(
  market: Market,
  lastSeqNum?: number
): Promise<[Trade[], number]> {
  let accountInfo;
  try {
    accountInfo = await Exchange.provider.connection.getAccountInfo(
      market.serumMarket.decoded.eventQueue
    );
  } catch (e) {
    alert(`Failed to get event queue account info: ${e}`, true);
    return [[], lastSeqNum];
  }

  const { header, events } = decodeRecentEvents(accountInfo.data, lastSeqNum);
  lastSeqNum = header.seqNum;
  let trades: Trade[] = [];

  for (let i = 0; i < events.length; i++) {
    let userKey;
    try {
      const openOrdersMap = await utils.getOpenOrdersMap(
        new PublicKey(process.env.PROGRAM_ID),
        events[i].openOrders
      );
      userKey = ((await Exchange.program.account.openOrdersMap.fetch(
        openOrdersMap[0]
      )) as programTypes.OpenOrdersMap).userKey;
    } catch (e) {
      alert(`Failed to get user key info: ${e}`, true);
      return [[], lastSeqNum];
    }

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
      order_id: events[i].orderId.toString(),
      client_order_id: events[i].clientOrderId.toString(),
      timestamp: Math.floor(Date.now() / 1000),
      owner_pub_key: userKey.toString(),
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
  const [trades, currentSeqNum] = await fetchTrades(
    market,
    lastSeqNum[market.marketIndex]
  );
  lastSeqNum[market.marketIndex] = currentSeqNum;
  if (trades.length > 0) {
    await putLastSeqNumMetadata(process.env.BUCKET_NAME, lastSeqNum);
    putDynamo(trades, process.env.DYNAMO_TABLE_NAME);
    putFirehoseBatch(trades, process.env.FIREHOSE_DS_NAME);
  }
}
