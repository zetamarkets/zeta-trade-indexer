import AWS from "aws-sdk";
import { AWSOptions } from "./aws-config";
import { Pricing, Surface, Trade } from "./types";
import { logger } from "./logging";

let firehose = new AWS.Firehose(AWSOptions);

export const putFirehoseBatch = (
  data: Trade[] | Pricing[] | Surface[],
  deliveryStreamName: string
) => {
  if (!data.length) return;
  const records = data.map((d) => {
    return { Data: JSON.stringify(d).concat("\n") };
  });
  var params = {
    DeliveryStreamName: deliveryStreamName /* required */,
    Records: records,
  };
  firehose.putRecordBatch(params, function (err, data) {
    if (err) {
      logger.info("Firehose putRecordBatch Error", JSON.stringify(err));
    } else {
      logger.info("Firehose putRecordBatch Success", JSON.stringify(data));
    }
  });
};
