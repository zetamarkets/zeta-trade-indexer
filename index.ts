import { Exchange, Network, utils, assets, constants } from "@zetamarkets/sdk";
import { PublicKey, Connection } from "@solana/web3.js";
import { collectMarketData } from "./event-queue-processing";
import { getLastSeqNumMetadata } from "./utils/s3";
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
const COMMITMENT = "finalized";

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

    await Exchange.load(
      allAssets,
      new PublicKey(process.env.PROGRAM_ID),
      NETWORK,
      connection,
      utils.commitmentConfig(COMMITMENT),
      undefined,
      undefined,
      undefined
    );
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
  let assetsJson = process.env.ASSETS!;
  if (assetsJson[0] != "[" && assetsJson[-1] != "]") {
    assetsJson = "[" + assetsJson + "]";
  }
  let assetsStrings: string[] = JSON.parse(assetsJson);
  let allAssets = assetsStrings.map((assetStr) => {
    return assets.nameToAsset(assetStr);
  });
  await loadExchange(allAssets);

  // Set the fetching state to false initially
  fetchingState = new Map(
    allAssets.map((asset) => {
      return [asset, new Array(constants.ACTIVE_MARKETS).fill(false)];
    })
  );

  // Each asset/market has it own sequence number
  let { lastSeqNum } = await getLastSeqNumMetadata(process.env.BUCKET_NAME);
  // logger.info("Loaded checkpoint", { lastSeqNum });
  if (!lastSeqNum) {
    lastSeqNum = {};
  }

  for (const asset of allAssets) {
    if (!(asset in lastSeqNum)) {
      lastSeqNum[asset] = {};
    }
  }

  setInterval(async () => {
    loadExchange(allAssets, true);
  }, 10_800_000); // Refresh connection every 3hr

  setInterval(async () => {
    allAssets.map((asset) => {
      if (!reloadingState) {
        collectMarketData(asset, lastSeqNum, fetchingState);
      }
    });
  }, FETCH_INTERVAL);

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
