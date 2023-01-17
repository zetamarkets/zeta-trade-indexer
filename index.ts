import { Exchange, Network, utils, assets } from "@zetamarkets/sdk";
import { PublicKey, Connection } from "@solana/web3.js";
import { collectMarketData } from "./event-queue-processing";
import { FETCH_INTERVAL } from "./utils/constants";
import { getLastSeqNumMetadata } from "./utils/s3";
import { logger } from "./utils/logging";

let reloading = false;

const network =
  process.env!.NETWORK === "mainnet"
    ? Network.MAINNET
    : process.env!.NETWORK === "devnet"
    ? Network.DEVNET
    : Network.LOCALNET;

export const loadExchange = async (
  allAssets: assets.Asset[],
  reload?: boolean
) => {
  reloading = true;
  try {
    logger.info(`${reload ? "Reloading" : "Loading"} exchange...`);
    const connection = new Connection(process.env.RPC_URL, "finalized");

    await Exchange.load(
      allAssets,
      new PublicKey(process.env.PROGRAM_ID),
      network,
      connection,
      utils.commitmentConfig("finalized"),
      undefined,
      undefined,
      undefined
    );
    logger.info(`${reload ? "Reloaded" : "Loaded"} exchange.`);
    // Close to reduce websocket strain
    await Exchange.close();
  } catch (e) {
    logger.error(`Failed to ${reload ? "reload" : "load"} exchange: ${e}`);
    loadExchange(allAssets, true);
  }
  reloading = false;
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

  // Each asset/market has it own sequence number
  let { lastSeqNum } = await getLastSeqNumMetadata(process.env.BUCKET_NAME);
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
      if (!reloading) {
        collectMarketData(asset, lastSeqNum);
      }
    });
  }, FETCH_INTERVAL);

  setInterval(async () => {
    try {
      await Exchange.updateExchangeState();
    } catch (e) {
      logger.error(`Failed to update exchange state: ${e}`);
    }
  }, 60_000);
};

main().catch(console.error.bind(logger));
