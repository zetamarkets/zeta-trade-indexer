import AWS from "aws-sdk";
import { AWSOptions } from "./aws-config";
import { alert } from "./telegram";

let s3 = new AWS.S3(AWSOptions);

export const putLastSeqNumMetadata = async (
  bucketName: string,
  lastSeqNum: Record<number, number> | undefined
) => {
  let data = JSON.stringify({ lastSeqNum });
  await s3
    .putObject({
      Bucket: bucketName,
      Key: `metadata/last-sequence-numbers.json`,
      Body: data,
      ContentType: "application/json",
    })
    .promise();
  console.log("Successfully wrote indices to S3", data);
};

export const getLastSeqNumMetadata = async (bucketName: string) => {
  try {
    const data = await s3
      .getObject({
        Bucket: bucketName,
        Key: `metadata/last-sequence-numbers.json`,
      })
      .promise();
    return JSON.parse(data.Body.toString("utf-8"));
  } catch (error) {
    alert(`Failed to fetch last seqnum: ${error}`, true)
    return { lastSeqNum: undefined };
  }
};
