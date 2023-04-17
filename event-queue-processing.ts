import {
  Exchange,
  programTypes,
  utils,
  assets,
} from "@zetamarkets/sdk";
import { EventQueueHeader, EventQueueLayout, Trade } from "./utils/types";
import { decodeRecentEvents } from "./utils/decode";
import { PublicKey } from "@solana/web3.js";
import { batchWriteFirehose } from "./utils/firehose";
import { putDynamo } from "./utils/dynamodb";
import { logger } from "./utils/logging";

const DEBUG = process.env.DEBUG == "true";

export const collectEventQueue = async (
  asset: assets.Asset,
  lastSeqNum?: Record<number, Record<number, number>>
) => {
  const market = Exchange.getPerpMarket(asset);
  const [trades, currentSeqNum] = await fetchTrades(
    asset,
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

    if (!DEBUG) {
      putDynamo(trades, process.env.DYNAMO_TABLE_NAME);
      batchWriteFirehose(trades, process.env.FIREHOSE_DS_NAME);
      // Newest sequence number should only be written after the data has been written
      await writeCheckpoint(process.env.DYNAMO_CHECKPOINT_TABLE, lastSeqNum);
    } else {
      logger.warn(
        "Debug mode enabled, results and checkpoints will not be written out"
      );
    }
  }
};

const formatTrade = async (
  asset: assets.Asset,
  event: EventQueueLayout,
  header: EventQueueHeader
) => {
  // TODO: get counterparty
  let authority;
  try {
    const openOrdersMap = await utils.getOpenOrdersMap(
      new PublicKey(Exchange.programId),
      event.openOrders
    );
    authority = (
      (await Exchange.program.account.openOrdersMap.fetch(
        openOrdersMap[0]
      )) as programTypes.OpenOrdersMap
    ).userKey;
  } catch (e) {
    logger.warn("Failed to get open orders account", {
      account: event.openOrders.toString(),
      error: (e as Error).message,
    });
    throw e;
  }
  let priceBN, sizeBN;
  // Trade has occured
  if (event.eventFlags.fill) {
    if (event.eventFlags.maker) {
      if (event.eventFlags.bid) {
        priceBN = event.nativeQuantityPaid / event.nativeQuantityReleased;
        sizeBN = event.nativeQuantityReleased;
      } else {
        priceBN = event.nativeQuantityReleased / event.nativeQuantityPaid;
        sizeBN = event.nativeQuantityPaid;
      }
    } else {
      if (event.eventFlags.bid) {
        priceBN = event.nativeQuantityPaid / event.nativeQuantityReleased;
        sizeBN = event.nativeQuantityReleased;
      } else {
        priceBN = event.nativeQuantityReleased / event.nativeQuantityPaid;
        sizeBN = event.nativeQuantityPaid;
      }
    }
  } else {
    return;
  }

  let newTradeObject: Trade = {
    // seq_num: newLastSeqNum - events.length + events.indexOf(event) + 1,
    seq_num: header.seqNum,
    timestamp: Math.floor(Date.now() / 1000),
    asset: asset,
    authority: authority.toString(),
    is_maker: event.eventFlags.maker,
    is_bid: event.eventFlags.bid,
    price: utils.convertNativeBNToDecimal(priceBN),
    size: utils.convertNativeLotSizeToDecimal(sizeBN),
    order_id: event.orderId.toString(),
    client_order_id: event.clientOrderId.toString(),
  };
  return newTradeObject;
};

const fetchTrades = async (
  asset: assets.Asset,
  lastSeqNum?: number
): Promise<[Trade[], number]> => {
  // logger.info("Fetching trades", { asset, marketIndex: market.marketIndex });
  const market = Exchange.getPerpMarket(asset);
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

  const trades = await Promise.all(
    events.map((event) => formatTrade(asset, event, header))
  );

  // Order by sequence number
  trades.sort((a, b) => a.seq_num - b.seq_num);

  return [trades, newLastSeqNum];
};
