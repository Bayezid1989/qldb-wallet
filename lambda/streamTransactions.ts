import { UserRecord, deaggregateSync } from "aws-kinesis-agg";
import { load, dumpPrettyText } from "ion-js";
import { DynamoDBClient, PutItemCommand } from "@aws-sdk/client-dynamodb";
import {
  Handler,
  KinesisStreamRecord,
  KinesisStreamRecordPayload,
} from "aws-lambda";
import { ionString, parseIonRecord } from "./util/util";
import { marshall } from "@aws-sdk/util-dynamodb";

const QLDB_TABLE_NAME = process.env.QLDB_TABLE_NAME || "";
const client = new DynamoDBClient();
const TABLE_NAME = process.env.DDB_TX_TABLE_NAME || "";
const REVISION_DETAILS_RECORD_TYPE = "REVISION_DETAILS";
const computeChecksums = true;

// Ref: https://github.com/AWS-South-Wales-User-Group/qldb-simple-demo/blob/master/streams-dynamodb/functions/qldb-streams-dynamodb.js
/**
 * Promisified function to deaggregate Kinesis record
 * @param record An individual Kinesis record from the aggregated records
 * @returns The resolved Promise object containing the deaggregated records
 */
const promiseDeaggregate = (
  record: KinesisStreamRecordPayload,
): Promise<UserRecord[] | undefined> =>
  new Promise((resolve, reject) => {
    deaggregateSync(record, computeChecksums, (err, responseObject) => {
      if (err) {
        // handle/report error
        return reject(err);
      }
      return resolve(responseObject);
    });
  });

const getRevisionDetailsPayload = (
  record: UserRecord,
  tableNames?: string[],
) => {
  // Kinesis data in Node.js Lambdas is base64 encoded
  const kinesisPayload = Buffer.from(record.data, "base64");
  // payload is the actual ion binary record published by QLDB to the stream
  const ionRecord = load(kinesisPayload);
  console.info(`Ion record: ${dumpPrettyText(ionRecord)}}`);

  if (ionString(ionRecord, "recordType") === REVISION_DETAILS_RECORD_TYPE) {
    const payload = parseIonRecord(ionRecord);
    const tableInfo = payload?.tableInfo;

    if (
      !tableNames ||
      (tableInfo?.tableName && tableNames.includes(tableInfo.tableName))
    ) {
      return payload;
    }
  }
  return null;
};

export const handler: Handler = async (event) => {
  const rawKinesisRecords: KinesisStreamRecord[] = event.Records;

  // Deaggregate all records in one call
  const userRecords: UserRecord[] = [];
  await Promise.all(
    rawKinesisRecords.map(async (kinesisRecord) => {
      const records = await promiseDeaggregate(kinesisRecord.kinesis);
      if (records) userRecords.push(...records);
    }),
  );

  // Iterate through deaggregated records
  for (const record of userRecords) {
    const payload = getRevisionDetailsPayload(record, [QLDB_TABLE_NAME]);
    if (!payload?.revision || !payload?.tableInfo) continue;

    const { data, metadata } = payload.revision;
    if (data && metadata && payload.tableInfo.tableName === QLDB_TABLE_NAME) {
      const txDate = metadata.txTime?.getDate();

      const ddbItem: typeof data & {
        txId: string | undefined | null;
        txTime: string | undefined | null;
      } = {
        ...data,
        txId: metadata.txId,
        txTime: txDate?.toISOString(),
      };

      const putCommand = new PutItemCommand({
        TableName: TABLE_NAME,
        Item: marshall(ddbItem),
      });

      try {
        await client.send(putCommand);
      } catch (error) {
        console.error(`Error processing record ${JSON.stringify(ddbItem)}`);
        throw error;
      }
    }
  }
  return {
    statusCode: 200,
  };
};
