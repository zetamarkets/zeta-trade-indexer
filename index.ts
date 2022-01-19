// require("dotenv").config();
import { Exchange, Network, utils } from "@zetamarkets/sdk";
import { PublicKey, Connection } from "@solana/web3.js";
import { collectMarketData } from "./event-queue-processing";

export const connection = new Connection(process.env.RPC_URL, "finalized");

const network =
  process.env!.NETWORK === "mainnet"
    ? Network.MAINNET
    : process.env!.NETWORK === "devnet"
    ? Network.DEVNET
    : Network.LOCALNET;

const main = async () => {
  await Exchange.load(
    new PublicKey(process.env.PROGRAM_ID),
    network,
    connection,
    utils.defaultCommitment(),
    undefined,
    undefined,
    undefined
  );
  // Each market has it own sequence number
  let lastSeqNum: Record<number, number> = {};

  collectMarketData(lastSeqNum);

  const refreshExchange = async () => {
    await Exchange.close().then(async () => {
      await Exchange.load(
        new PublicKey(process.env.PROGRAM_ID),
        network,
        connection,
        utils.defaultCommitment(),
        undefined,
        undefined,
        undefined
      );
    }).catch((error) => {
      console.log("Failed to close Exchange:", error);
    })

  }
  setInterval(async () => {
    console.log("Refreshing Exchange");
    refreshExchange();
  }, 21600000); // Refresh every 6 hours
};

main().catch(console.error.bind(console));
