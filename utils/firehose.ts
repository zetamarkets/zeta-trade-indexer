import {
  FirehoseClient,
  PutRecordBatchCommand,
  PutRecordBatchCommandInput,
} from "@aws-sdk/client-firehose";
import { logger } from "./logging";
import { AWSOptions } from "./aws-config";
import { Pricing, Surface, Trade } from "./types";
import { sleep } from ".";

const FIREHOSE_MAX_BATCH_SIZE = 500;

// Set up the Firehose client
const client = new FirehoseClient(AWSOptions);

export const batchWriteFirehose = async (
  records: any[],
  deliveryStreamName: string
): Promise<any> => {
  // Split the records array into chunks of FIREHOSE_MAX_BATCH_SIZE
  const chunks = [];
  for (let i = 0; i < records.length; i += FIREHOSE_MAX_BATCH_SIZE) {
    chunks.push(records.slice(i, i + FIREHOSE_MAX_BATCH_SIZE));
  }

  // Send each chunk as a separate batch write request
  let responses = [];
  for (const chunk of chunks) {
    const params = {
      DeliveryStreamName: deliveryStreamName,
      Records: chunk.map((r) => ({
        Data: Buffer.from(JSON.stringify(r).concat("\n")),
      })),
    };
    const response = await _batchWriteFirehose(params);
    responses.push(response);

    // Add a delay between requests to avoid throttling (adjust as needed)
    await sleep(100);
  }
  return responses;
};

async function _batchWriteFirehose(params: PutRecordBatchCommandInput) {
  try {
    let response = await client.send(new PutRecordBatchCommand(params));
    logger.info("Batch write successful:", { response });
    return response;
  } catch (error: any) {
    logger.error("Error in batch write:", { error });
    return error;
  }
}
