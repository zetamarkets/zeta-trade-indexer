import {
  constants,
  Exchange,
  Market,
  programTypes,
  utils,
  assets,
} from "@zetamarkets/sdk";
import { Trade } from "./utils/types";
import { decodeRecentEvents } from "./utils";
import { PublicKey } from "@solana/web3.js";
import { putFirehoseBatch } from "./utils/firehose";
import { putDynamo } from "./utils/dynamodb";
import { putLastSeqNumMetadata } from "./utils/s3";
import { logger } from "./utils/logging";

export async function collectMarketData(
  asset: assets.Asset,
  lastSeqNum?: Record<number, Record<number, number>>,
  fetchingState?: Map<assets.Asset, Array<boolean>>
) {
  // logger.info("Collecting market data", { asset });
  let timestamp = Math.floor(Date.now() / 1000);

  await Promise.all(
    Exchange.getMarkets(asset).map(async (market) => {
      let expirySeries = market.expirySeries;

      // Fetch trades if the market is active or
      // the market expired < 60 seconds ago.
      // 60 second buffer to handle trades that happened right as expiry occurred.
      // If market is a perp market can always fetch trades in the case where its a perp market
      // expiry series == udefined
      if (
        market.marketIndex != constants.PERP_INDEX &&
        expirySeries != undefined &&
        (expirySeries.activeTs > timestamp ||
          expirySeries.expiryTs + 60 < timestamp)
      ) {
        return;
      }
      let marketIndex = market.marketIndex;
      if (!fetchingState || !fetchingState.get(asset)[marketIndex]) {
        fetchingState.get(asset)[marketIndex] = true;
        await collectEventQueue(asset, market, lastSeqNum);
        fetchingState.get(asset)[marketIndex] = false;
      } else {
        // logger.info("Market already in fetching state", { asset });
      }
    })
  );
}

async function collectEventQueue(
  asset: assets.Asset,
  market: Market,
  lastSeqNum?: Record<number, Record<number, number>>
) {
  // logger.info("Collecting event queue", {
  //   asset,
  //   marketIndex: market.marketIndex,
  // });
  const [trades, currentSeqNum] = await fetchTrades(
    asset,
    market,
    lastSeqNum[asset][market.marketIndex]
  );
  let lastSeqNumCache = lastSeqNum[asset][market.marketIndex];
  lastSeqNum[asset][market.marketIndex] = currentSeqNum;
  if (trades.length > 0) {
    logger.info(`${trades.length} trades indexed`, {
      asset,
      marketIndex: market.marketIndex,
      lastSeqNum: lastSeqNumCache,
      currentSeqNum,
      tradeData: trades,
    });
    putDynamo(trades, process.env.DYNAMO_TABLE_NAME);
    putFirehoseBatch(trades, process.env.FIREHOSE_DS_NAME);
    // Newest sequence number should only be written after the data has been written
    await putLastSeqNumMetadata(process.env.BUCKET_NAME, lastSeqNum);
  }
}

async function fetchTrades(
  asset: assets.Asset,
  market: Market,
  lastSeqNum?: number
): Promise<[Trade[], number]> {
  // logger.info("Fetching trades", { asset, marketIndex: market.marketIndex });
  let accountInfo;
  try {
    accountInfo = await Exchange.provider.connection.getAccountInfo(
      market.serumMarket.decoded.eventQueue
    );
  } catch (e) {
    logger.warn("Failed to get event queue account info", {
      asset,
      error: (e as Error).message,
    });
    // Return empty list for trades, so no data is written to AWS
    return [[], lastSeqNum];
  }

  const { header, events } = decodeRecentEvents(accountInfo.data, lastSeqNum);
  const newLastSeqNum = header.seqNum;

  // Since we're polling on finalized commitment, any reversion in event queue sequence number has to be the result of caching.
  // i.e. If we are directed to a backup RPC server due to an upgrade or other incident.
  if (lastSeqNum > newLastSeqNum) {
    logger.warn(
      `Market index: ${market.marketIndex}, header sequence number (${header.seqNum}) < last sequence number (${lastSeqNum})`,
      { asset }
    );

    return [[], lastSeqNum];
  }

  let trades: Trade[] = [];

  await Promise.all(
    events.map(async (event) => {
      let userKey;
      try {
        const openOrdersMap = await utils.getOpenOrdersMap(
          new PublicKey(process.env.PROGRAM_ID),
          event.openOrders
        );
        userKey = (
          (await Exchange.program.account.openOrdersMap.fetch(
            openOrdersMap[0]
          )) as programTypes.OpenOrdersMap
        ).userKey;
      } catch (e) {
        logger.warn("Failed to get user key info", {
          asset,
          error: (e as Error).message,
        });
        return [[], lastSeqNum];
      }
      let priceBN, sizeBN;
      // Trade has occured
      if (event.eventFlags.fill) {
        if (event.eventFlags.maker) {
          if (event.eventFlags.bid) {
            priceBN = event.nativeQuantityPaid.div(
              event.nativeQuantityReleased
            );
            sizeBN = event.nativeQuantityReleased;
          } else {
            priceBN = event.nativeQuantityReleased.div(
              event.nativeQuantityPaid
            );
            sizeBN = event.nativeQuantityPaid;
          }
        } else {
          if (event.eventFlags.bid) {
            priceBN = event.nativeQuantityPaid.div(
              event.nativeQuantityReleased
            );
            sizeBN = event.nativeQuantityReleased;
          } else {
            priceBN = event.nativeQuantityReleased.div(
              event.nativeQuantityPaid
            );
            sizeBN = event.nativeQuantityPaid;
          }
        }
      } else {
        return;
      }

      let expirySeries = market.expirySeries;
      let expiry_timestamp = expirySeries == null ? 0 : expirySeries.expiryTs;
      let underlying = assets.assetToName(asset);

      let newTradeObject: Trade = {
        seq_num: newLastSeqNum - events.length + events.indexOf(event) + 1,
        order_id: event.orderId.toString(),
        client_order_id: event.clientOrderId.toString(),
        timestamp: Math.floor(Date.now() / 1000),
        underlying: underlying,
        owner_pub_key: userKey.toString(),
        expiry_timestamp: expiry_timestamp,
        market_index: market.marketIndex,
        strike: market.strike,
        kind: market.kind,
        is_maker: event.eventFlags.maker,
        is_bid: event.eventFlags.bid,
        price: utils.convertNativeBNToDecimal(priceBN),
        size: utils.convertNativeBNToDecimal(
          sizeBN,
          constants.POSITION_PRECISION
        ),
      };
      trades.push(newTradeObject);
    })
  );

  trades.sort((a, b) => a.seq_num - b.seq_num);

  return [trades, newLastSeqNum];
}
