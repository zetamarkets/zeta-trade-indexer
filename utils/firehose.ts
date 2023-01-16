import AWS from "aws-sdk";
import { AWSOptions } from "./aws-config";
import { Pricing, Surface, Trade } from "./types";

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
      console.log("[LOG] Firehose putRecordBatch Error", JSON.stringify(err));
    } else {
      console.log("[LOG] Firehose putRecordBatch Success", JSON.stringify(data));
    }
  });
};
