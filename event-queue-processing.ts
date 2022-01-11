import {
  constants,
  Exchange,
  Market,
  programTypes,
  utils,
} from "@zetamarkets/sdk";
import { Trade } from "./utils/types";
import { putFirehoseBatch } from "./utils/firehose";
import { decodeRecentEvents } from "./utils";
import { connection } from ".";
import { PublicKey } from "@solana/web3.js";
import { putDynamo } from "./utils/dynamodb";
import { FETCH_INTERVAL } from "./utils/constants";

let fetchingMarkets: boolean[];
fetchingMarkets = new Array(constants.ACTIVE_MARKETS).fill(false);

export const collectMarketData = (lastSeqNum?: Record<number, number>) => {
  const numberOfExpirySeries = Exchange.zetaGroup.expirySeries.length;
  for (var i = 0; i < numberOfExpirySeries; i++) {
    let expiryIndex = i;
    let expirySeries = Exchange.markets.expirySeries[expiryIndex];

    // If expirySeries isn't live, do not go through inactive markets
    if (!expirySeries.isLive()) continue;

    let markets = Exchange.markets.getMarketsByExpiryIndex(expiryIndex);
    for (var j = 0; j < markets.length; j++) {
      collectEventQueue(markets[j], lastSeqNum);
    }
  }
};

const collectEventQueue = (
  market: Market,
  lastSeqNum?: Record<number, number>
) => {
  const eventQueuePk = market.serumMarket.decoded.eventQueue;
  const { expiryIndex, expiryTs } = market.expirySeries;
  const { marketIndex, strike, kind } = market;

  const fetchTrades = async (
    lastSeqNum?: number
  ): Promise<[Trade[], number]> => {
    const accountInfo = await connection.getAccountInfo(eventQueuePk);
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
        // console.log(`Trade occurred: Market ${marketIndex} ${strike} ${kind} ${utils.convertNativeIntegerToDecimal(price)} ${utils.convertNativeLotSizeToDecimal(size)}`)
        // Could be cancel or insert here
      } else {
        if (events[i].eventFlags.bid) {
          // Bid cancel?
          if (
            events[i].nativeQuantityReleased.toNumber() > 0 &&
            events[i].nativeQuantityPaid.toNumber() === 0
          ) {
            price = events[i].orderId.iushrn(64).toNumber();
            size = events[i].nativeQuantityReleased.toNumber() / price;
            // console.log(`Non fill bid case: ${convertNativeIntegerToDecimal(price)} ${utils.convertNativeLotSizeToDecimal(size)}`);
          } else {
            // Overlap between cancel and insert?
            continue;
          }
        } else {
          // Ask cancel?
          if (
            events[i].nativeQuantityReleased.toNumber() > 0 &&
            events[i].nativeQuantityPaid.toNumber() === 0
          ) {
            price = events[i].orderId.iushrn(64).toNumber();
            size = events[i].nativeQuantityReleased.toNumber();
            // console.log(`Non fill ask case: ${convertNativeIntegerToDecimal(price)} ${utils.convertNativeLotSizeToDecimal(size)}`);
          } else {
            // Overlap between cancel and insert?
            continue;
          }
        }
      }
      let newTradeObject: Trade = {
        seq_num: lastSeqNum - events.length + i + 1,
        timestamp: Math.floor(Date.now() / 1000),
        owner_pub_key: userKey.toString(),
        expiry_series_index: expiryIndex,
        expiry_timestamp: expiryTs,
        market_index: marketIndex,
        strike: strike,
        kind: kind,
        is_fill: events[i].eventFlags.fill,
        is_maker: events[i].eventFlags.maker,
        is_bid: events[i].eventFlags.bid,
        price: utils.convertNativeIntegerToDecimal(price),
        size: utils.convertNativeLotSizeToDecimal(size),
      };
      trades.push(newTradeObject);
    }
    return [trades, lastSeqNum];
  };

  setInterval(async () => {
    try {
      if (!fetchingMarkets[marketIndex]) {
        fetchingMarkets[marketIndex] = true;
        const [trades, currentSeqNum] = await fetchTrades(
          lastSeqNum[marketIndex]
        );
        lastSeqNum[marketIndex] = currentSeqNum;
        if (trades.length > 0) {
          putDynamo(trades);
          putFirehoseBatch(trades, process.env.DS_NAME_EVENT_QUEUE);
        }
        fetchingMarkets[marketIndex] = false;
      }
    } catch (e) {
      console.warn("Unable to fetch event queue: ", e);
    }
  }, FETCH_INTERVAL);
};
