import {
  constants,
  Exchange,
  Market,
  programTypes,
  utils,
} from "@zetamarkets/sdk";
import { Network, utils as FlexUtils } from "@zetamarkets/flex-sdk";
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

const network =
  process.env.NETWORK === "mainnet"
    ? Network.MAINNET
    : process.env.NETWORK === "devnet"
    ? Network.DEVNET
    : Network.LOCALNET;

export async function collectMarketData(lastSeqNum?: Record<number, number>) {
  let accountInfo;
  try {
    accountInfo = await Exchange.connection.getAccountInfo(SYSVAR_CLOCK_PUBKEY);
  } catch (e) {
    alert(`Failed to get clock account info: ${e}`, true);
    return;
  }
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
    // Return empty list for trades, so no data is written to AWS
    return [[], lastSeqNum];
  }

  const { header, events } = decodeRecentEvents(accountInfo.data, lastSeqNum);
  const newLastSeqNum = header.seqNum;

  // Since we're polling on finalized commitment, any reversion in event queue sequence number has to be the result of caching.
  // i.e. If we are directed to a backup RPC server due to an upgrade or other incident.
  if (lastSeqNum > newLastSeqNum) {
    alert(
      `Market index: ${market.marketIndex}, header sequence number (${header.seqNum}) < last sequence number (${lastSeqNum})`,
      true
    );

    return [[], lastSeqNum];
  }

  let trades: Trade[] = [];

  for (let i = 0; i < events.length; i++) {
    let userKey;
    try {
      const openOrdersMap = await utils.getOpenOrdersMap(
        new PublicKey(process.env.PROGRAM_ID),
        events[i].openOrders
      );
      userKey = (
        (await Exchange.program.account.openOrdersMap.fetch(
          openOrdersMap[0]
        )) as programTypes.OpenOrdersMap
      ).userKey;
    } catch (e) {
      alert(`Failed to get user key info: ${e}`, true);
      return [[], lastSeqNum];
    }
    let priceBN, sizeBN;
    // Trade has occured
    if (events[i].eventFlags.fill) {
      if (events[i].eventFlags.maker) {
        if (events[i].eventFlags.bid) {
          priceBN = events[i].nativeQuantityPaid.div(
            events[i].nativeQuantityReleased
          );
          sizeBN = events[i].nativeQuantityReleased;
        } else {
          priceBN = events[i].nativeQuantityReleased.div(
            events[i].nativeQuantityPaid
          );
          sizeBN = events[i].nativeQuantityPaid;
        }
      } else {
        if (events[i].eventFlags.bid) {
          priceBN = events[i].nativeQuantityPaid.div(
            events[i].nativeQuantityReleased
          );
          sizeBN = events[i].nativeQuantityReleased;
        } else {
          priceBN = events[i].nativeQuantityReleased.div(
            events[i].nativeQuantityPaid
          );
          sizeBN = events[i].nativeQuantityPaid;
        }
      }
    } else {
      continue;
    }

    let expirySeries = market.expirySeries;

    let underlying = FlexUtils.getUnderlyingMapping(
      network,
      Exchange.zetaGroup.underlyingMint
    );

    let newTradeObject: Trade = {
      seq_num: newLastSeqNum - events.length + i + 1,
      order_id: events[i].orderId.toString(),
      client_order_id: events[i].clientOrderId.toString(),
      timestamp: Math.floor(Date.now() / 1000),
      underlying: underlying,
      owner_pub_key: userKey.toString(),
      expiry_timestamp: expirySeries.expiryTs,
      market_index: market.marketIndex,
      strike: market.strike,
      kind: market.kind,
      is_maker: events[i].eventFlags.maker,
      is_bid: events[i].eventFlags.bid,
      price: utils.convertNativeBNToDecimal(priceBN),
      size: utils.convertNativeBNToDecimal(
        sizeBN,
        constants.POSITION_PRECISION
      ),
    };
    trades.push(newTradeObject);
  }
  return [trades, newLastSeqNum];
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
    putDynamo(trades, process.env.DYNAMO_TABLE_NAME);
    putFirehoseBatch(trades, process.env.FIREHOSE_DS_NAME);
    // Newest sequence number should only be written after the data has been written
    await putLastSeqNumMetadata(process.env.BUCKET_NAME, lastSeqNum);
  }
}
