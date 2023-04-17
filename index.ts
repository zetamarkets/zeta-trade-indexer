import {
  Exchange,
  Network,
  utils,
  assets,
  constants,
  types,
} from "@zetamarkets/sdk";
import { Connection } from "@solana/web3.js";
import { collectEventQueue, collectMarketData } from "./event-queue-processing";
import { logger } from "./utils/logging";

let reloadingState = false;
let fetchingState: Map<assets.Asset, Array<boolean>>;
const FETCH_INTERVAL = Number(process.env.FETCH_INTERVAL) | 1000;

const NETWORK =
  process.env!.NETWORK === "mainnet"
    ? Network.MAINNET
    : process.env!.NETWORK === "devnet"
    ? Network.DEVNET
    : Network.LOCALNET;
const COMMITMENT = "confirmed";

console.log(`DEBUG: ${process.env.DEBUG == "true"}`);

export const loadExchange = async (
  allAssets: assets.Asset[],
  reload?: boolean
) => {
  reloadingState = true;
  try {
    logger.info(`${reload ? "Reloading" : "Loading"} exchange...`, {
      asset: allAssets,
    });
    const connection = new Connection(process.env.RPC_URL, {
      commitment: COMMITMENT,
      wsEndpoint: process.env.RPC_WS_URL,
    });

    const LOAD_CONFIG: types.LoadExchangeConfig = {
      assets: allAssets,
      network: NETWORK,
      connection: connection,
      opts: utils.commitmentConfig(COMMITMENT),
      throttleMs: 0,
      loadFromStore: true,
    };
    await Exchange.load(LOAD_CONFIG);
    logger.info(`${reload ? "Reloaded" : "Loaded"} exchange.`, {
      asset: allAssets,
    });
    // Close to reduce websocket strain
    await Exchange.close();
  } catch (e) {
    logger.error(`Failed to ${reload ? "reload" : "load"} exchange`, {
      error: (e as Error).message,
    });
    loadExchange(allAssets, true);
  }
  reloadingState = false;
};

const main = async () => {
  await loadExchange(assets.allAssets());

  // Set the fetching state to false initially
  fetchingState = new Map(
    assets.allAssets().map((asset) => {
      return [asset, new Array(constants.ACTIVE_MARKETS).fill(false)];
    })
  );

  // Each asset/market has it own sequence number
  let { lastSeqNum } = await getLastSeqNumMetadata(process.env.BUCKET_NAME);
  // logger.info("Loaded checkpoint", { lastSeqNum });
  if (!lastSeqNum) {
    lastSeqNum = {};
  }

  // for (const asset of assets.allAssets()) {
  //   if (!(asset in lastSeqNum)) {
  //     lastSeqNum[asset] = {};
  //   }
  // }

  // Refresh connection every 3hr
  setInterval(async () => {
    loadExchange(assets.allAssets(), true);
  }, 10_800_000);

  // Fetch trade data from the event queue
  setInterval(async () => {
    if (!reloadingState) {
      const marketPromises = assets.allAssets().map(async (asset) => {
        // let market = Exchange.getPerpMarket(asset);

        // Fetch and process the event queue if not already fetching
        if (!fetchingState || !fetchingState.get(asset)) {
          fetchingState[asset] = true;
          await collectEventQueue(asset, lastSeqNum);
          fetchingState[asset] = false;
        } else {
          // logger.info("Market already in fetching state", { asset });
        }
      });
      Promise.all(marketPromises);
    }
  }, FETCH_INTERVAL);

  // Update exchange state every 60s
  setInterval(async () => {
    try {
      await Exchange.updateExchangeState();
    } catch (e) {
      logger.error("Failed to update exchange state", {
        error: (e as Error).message,
      });
    }
  }, 60_000);
};

main().catch(console.error.bind(logger));
