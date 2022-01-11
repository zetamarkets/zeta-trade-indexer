// require("dotenv").config();
import { Exchange, Network, utils } from "@zetamarkets/sdk";
import { PublicKey, Connection } from "@solana/web3.js";
import { collectMarketData } from "./event-queue-processing";

export const connection = new Connection(
  process.env.RPC_URL,
  utils.defaultCommitment()
);

console.log(process.env.PROGRAM_ID);

const main = async () => {
  await Exchange.load(
    new PublicKey(process.env.PROGRAM_ID),
    Network.DEVNET,
    connection,
    utils.defaultCommitment(),
    undefined,
    undefined,
    undefined
  );
  // Each market has it own sequence number
  let lastSeqNum: Record<number, number> = {};

  collectMarketData(lastSeqNum);
};

main().catch(console.error.bind(console));
