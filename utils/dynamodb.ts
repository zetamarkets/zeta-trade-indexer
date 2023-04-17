import { AWSOptions } from "./aws-config";
import { Trade } from "./types";
import { logger } from "./logging";
import { DynamoDBClient, PutItemCommand } from "@aws-sdk/client-dynamodb";
import {
  BatchWriteCommand,
  BatchWriteCommandInput,
} from "@aws-sdk/lib-dynamodb";

export const putDynamo = (data: Trade[], dynamoTableName: string) => {
  if (!data.length) return;
  const dynamoData = data.map((d) => {
    return {
      PutRequest: {
        Item: {
          id: d.underlying + "#" + d.market_index + "#" + d.seq_num,
          seq_num: d.seq_num,
          order_id: d.order_id,
          client_order_id: d.client_order_id,
          timestamp: new Date(d.timestamp * 1000).toISOString(),
          expiry_timestamp: new Date(d.expiry_timestamp * 1000).toISOString(),
          is_bid: d.is_bid,
          is_maker: d.is_maker,
          kind: d.kind,
          market_index: d.market_index,
          owner_pub_key: d.owner_pub_key,
          price: d.price,
          size: d.size,
          strike: d.strike,
          underlying: d.underlying,
        },
      },
    };
  });
  // Max size of one dynamo write batch is 25
  if (dynamoData.length > 25) {
    const batches = Math.ceil(dynamoData.length / 25);
    for (let i = 0; i < batches; i++) {
      let indexShift = i * 25;
      let startIndex = 0 + indexShift;
      let endIndex = 25 + indexShift;
      const slicedBatch = dynamoData.slice(startIndex, endIndex);
      putDynamoBatch(slicedBatch, dynamoTableName);
    }
  } else {
    putDynamoBatch(dynamoData, dynamoTableName);
  }
};

const MAX_DYNAMO_BATCH_SIZE = 25;

const client = new DynamoDBClient(AWSOptions);

export async function batchWriteDynamo(params: BatchWriteCommandInput) {
  const maxBatchSize = 25;
  const requestItemsKeys = Object.keys(
    params.RequestItems
  ) as (keyof typeof params.RequestItems)[];

  for (const key of requestItemsKeys) {
    const items = params.RequestItems[key];

    // Split the items array into chunks of maxBatchSize
    const chunks = [];
    for (let i = 0; i < items.length; i += maxBatchSize) {
      chunks.push(items.slice(i, i + maxBatchSize));
    }

    // Send each chunk as a separate batch write request
    for (const chunk of chunks) {
      const chunkParams: BatchWriteCommandInput = {
        RequestItems: {
          [key]: chunk,
        },
      };

      return _batchWriteDynamo(chunkParams);
    }
  }
}

const _batchWriteDynamo = async (params: BatchWriteCommandInput) => {
  try {
    const response = await client.send(new BatchWriteCommand(params));
    console.log("Batch write successful:", response);
    return response;
  } catch (error) {
    console.error("Error in batch write:", error);
  }
};

export const writeCheckpoint = async (
  table: string,
  lastSeqNum: Record<number, number> | undefined
) => {
  let data = JSON.stringify({ lastSeqNum });
  // await s3
  //   .putObject({
  //     Bucket: bucketName,
  //     Key: `trades/checkpoint.json`,
  //     Body: data,
  //     ContentType: "application/json",
  //   })
  //   .promise();
  // logger.info("Successfully wrote indices to S3", { lastSeqNum });

  const params = {
    TableName: table,
    Item: {
      SEQ_NUM: { N: "001" },
    },
  };

  const response = await client.send(new PutItemCommand(params));
};

export const readCheckpoint = async (table: string) => {
  try {
    const data = await s3
      .getObject({
        Bucket: bucketName,
        Key: `trades/checkpoint.json`,
      })
      .promise();
    return JSON.parse(data.Body.toString("utf-8"));
  } catch (e) {
    logger.error("Failed to fetch last seqnum", {
      error: (e as Error).message,
    });
    return { lastSeqNum: undefined };
  }
};
