import { Exchange, Network, utils, assets } from "@zetamarkets/sdk";
import { PublicKey, Connection } from "@solana/web3.js";
import { collectMarketData } from "./event-queue-processing";
import { FETCH_INTERVAL } from "./utils/constants";
import { getLastSeqNumMetadata } from "./utils/s3";
import { alert } from "./utils/telegram";

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
  try {
    alert(`${reload ? "Reloading" : "Loading"} exchange...`, false);
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
    alert(`${reload ? "Reloaded" : "Loaded"} exchange.`, false);
    // Close to reduce websocket strain
    await Exchange.close();
  } catch (e) {
    alert(`Failed to ${reload ? "reload" : "load"} exchange: ${e}`, true);
    loadExchange(allAssets, true);
  }
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
  }, 10_800_000); // Refresh every 3 hours

  setInterval(async () => {
    allAssets.map((asset) => {
      collectMarketData(asset, lastSeqNum);
    });
  }, FETCH_INTERVAL);

  setInterval(async () => {
    try {
      await Exchange.updateExchangeState();
    } catch (e) {
      alert(`Failed to update exchange state: ${e}`, true);
    }
  }, 60_000);
};

// 0 active => 0 active => 0 inactive
// 1 inactive => 1 active => 1 active

main().catch(console.error.bind(console));
