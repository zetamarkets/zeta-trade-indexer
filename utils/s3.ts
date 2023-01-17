import AWS from "aws-sdk";
import { AWSOptions } from "./aws-config";
import { logger } from "./logging";

let s3 = new AWS.S3(AWSOptions);

export const putLastSeqNumMetadata = async (
  bucketName: string,
  lastSeqNum: Record<number, Record<number, number>> | undefined
) => {
  let data = JSON.stringify({ lastSeqNum });
  await s3
    .putObject({
      Bucket: bucketName,
      Key: `trades/checkpoint.json`,
      Body: data,
      ContentType: "application/json",
    })
    .promise();
  logger.info("Successfully wrote indices to S3", data);
};

export const getLastSeqNumMetadata = async (bucketName: string) => {
  try {
    const data = await s3
      .getObject({
        Bucket: bucketName,
        Key: `trades/checkpoint.json`,
      })
      .promise();
    return JSON.parse(data.Body.toString("utf-8"));
  } catch (error) {
    logger.error(`Failed to fetch last seqnum: ${error}`);
    return { lastSeqNum: undefined };
  }
};
