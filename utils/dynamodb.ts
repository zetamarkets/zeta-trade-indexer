import AWS from "aws-sdk";
import { AWSOptions } from "./aws-config";
import { Trade } from "./types";

let dynamodb = new AWS.DynamoDB(AWSOptions);
let docClient = new AWS.DynamoDB.DocumentClient(AWSOptions);

dynamodb.listTables({}, function (err, data) {
  if (err) console.log(err, err.stack);
  // an error occurred
  else console.log(data); // successful response
});

export const putDynamo = (data: Trade[], dynamoTableName: string) => {
  if (!data.length) return;
  const dynamoData = data.map((d) => {
    return {
      PutRequest: {
        Item: {
          seq_num: d.seq_num,
          order_id: d.order_id,
          client_order_id: d.client_order_id,
          timestamp: (new Date(d.timestamp * 1000)).toISOString(),
          expiry_timestamp: (new Date(d.expiry_timestamp * 1000)).toISOString(),
          is_bid: d.is_bid,
          is_maker: d.is_maker,
          kind: d.kind,
          market_index: d.market_index,
          owner_pub_key: d.owner_pub_key,
          price: d.price,
          size: d.size,
          strike: d.strike,
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

const putDynamoBatch = (dynamoData, dynamoTableName: string) => {
  let requestItems = {};
  requestItems[dynamoTableName] = dynamoData;
  const params = {
    RequestItems: requestItems,
  };

  docClient.batchWrite(params, function (err, d) {
    if (err) {
      console.log("DynamoDB BatchWrite Error", err);
    } else {
      console.log("DynamoDB BatchWrite Success", d);
    }
  });
};
