import { UserRecord, deaggregateSync } from "aws-kinesis-agg";
import { load, dumpPrettyText } from "ion-js";
import {
  DynamoDBClient,
  PutItemCommand,
  UpdateItemCommand,
} from "@aws-sdk/client-dynamodb";
import {
  Handler,
  KinesisStreamRecord,
  KinesisStreamRecordPayload,
} from "aws-lambda";
import { ionString, parseIonRecord } from "./util/util";
import { marshall } from "@aws-sdk/util-dynamodb";
import { config } from "../config";
import { TX_STATUS } from "./util/constant";

const { QLDB_TABLE_NAME, DDB_TABLE_NAME } = config;

const client = new DynamoDBClient();
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
    if (
      data?.accountId && // Omitting delete account record
      metadata &&
      payload.tableInfo.tableName === QLDB_TABLE_NAME
    ) {
      const txDate = metadata.txTime?.getDate();

      const { lastTx, pendingTxs, ...rest } = data;

      const revisionObj = {
        txId: metadata.txId,
        txTime: txDate?.toISOString(),
      };

      if (
        lastTx.status === TX_STATUS.IMMEDIATE ||
        lastTx.status === TX_STATUS.REQUESTED
      ) {
        const ddbItem = {
          ...rest,
          ...lastTx,
          revisions: {
            [lastTx.status || TX_STATUS.IMMEDIATE]: revisionObj,
          },
          createdAt: txDate?.toISOString(),
          // expireTimestamp: Date.now() + daysToSeconds(Number(EXPIRE_AFTER_DAYS)),
        };
        const command = new PutItemCommand({
          TableName: DDB_TABLE_NAME,
          Item: marshall(ddbItem),
        });
        try {
          await client.send(command);
        } catch (error) {
          console.error(`Error Putting record ${JSON.stringify(ddbItem)}`);
          throw error;
        }
      } else {
        const key = marshall({
          accountId: data.accountId,
          requestId: data.lastTx.requestId,
        });
        const values = marshall({
          ":balance": data.balance,
          ":status": data.lastTx.status,
          ":revisionObj": revisionObj,
        });
        const command = new UpdateItemCommand({
          TableName: DDB_TABLE_NAME,
          Key: key,
          UpdateExpression: `SET balance = :balance, status = :status, revisions.${lastTx.status} = :revisionObj, `,
          ExpressionAttributeValues: values,
        });
        try {
          await client.send(command);
        } catch (error) {
          console.error(
            `Error Updating record ${JSON.stringify(key)}, ${JSON.stringify(
              values,
            )}`,
          );
          throw error;
        }
      }
    }
  }
  return {
    statusCode: 200,
  };
};
