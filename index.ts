import { Exchange, Network, utils } from "@zetamarkets/sdk";
import { PublicKey, Connection } from "@solana/web3.js";
import { collectMarketData } from "./event-queue-processing";
import { FETCH_INTERVAL } from "./utils/constants";
import { getLastSeqNumMetadata} from "./utils/s3";
import { alert } from "./utils/telegram";

export const connection = new Connection(process.env.RPC_URL, "finalized");

const network =
  process.env!.NETWORK === "mainnet"
    ? Network.MAINNET
    : process.env!.NETWORK === "devnet"
    ? Network.DEVNET
    : Network.LOCALNET;

export const reloadExchange = async () => {
  const newConnection = new Connection(process.env.RPC_URL, "finalized");
  alert("Reloading exchange...", false);
  await Exchange.load(
    new PublicKey(process.env.PROGRAM_ID),
    network,
    newConnection,
    utils.defaultCommitment(),
    undefined,
    undefined,
    undefined
  );
  alert("Reloaded exchange.", false);
  await Exchange.close();
};

const main = async () => {
  alert("Loading exchange...", false);
  await Exchange.load(
    new PublicKey(process.env.PROGRAM_ID),
    network,
    connection,
    utils.defaultCommitment(),
    undefined,
    undefined,
    undefined
  );
  alert("Loaded exchange.", false);
  // Close to reduce websocket strain.
  await Exchange.close();

  // Each market has it own sequence number
  let { lastSeqNum } = await getLastSeqNumMetadata(process.env.BUCKET_NAME);
  if (!lastSeqNum) {
    lastSeqNum = {};
  }

  setInterval(async () => {
    reloadExchange();
  }, 10_800_000); // Refresh every 3 hours

  setInterval(async () => {
    collectMarketData(lastSeqNum);
  }, FETCH_INTERVAL);

  setInterval(async () => {
    try {
      await Exchange.updateExchangeState();
    } catch (e) {
      alert(`Failed to update exchange state: ${e}`, true)
    }
  }, 60_000);
};

// 0 active => 0 active => 0 inactive
// 1 inactive => 1 active => 1 active

main().catch(console.error.bind(console));
