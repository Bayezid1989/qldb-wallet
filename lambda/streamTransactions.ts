import { UserRecord, deaggregateSync } from "aws-kinesis-agg";
import { load, dumpText, dumpPrettyText, dom } from "ion-js";
import { DynamoDBClient, PutItemCommand } from "@aws-sdk/client-dynamodb";
import { KinesisStreamRecord, KinesisStreamRecordPayload } from "aws-lambda";

const QLDB_TABLE_NAME = process.env.QLDB_TABLE_NAME || "";
const client = new DynamoDBClient();
const TABLE_NAME = process.env.DDB_TABLE_NAME || "";
const EXPIRE_AFTER_DAYS = process.env.EXPIRE_AFTER_DAYS;
const TTL_ATTRIBUTE = process.env.TTL_ATTRIBUTE;

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

// Ref: ion-js DOM API struct example: https://github.com/amazon-ion/ion-js/blob/master/src/dom/README.md#struct-example-1
const getTableInfoFromRevisionRecord = (revisionRecord: dom.Value) => {
  //  Retrieves the table information block from revision Revision Record
  //  Table information contains the table name and table id
  //  Parameters:
  //    revision_record (string): The ion representation of Revision record from QLDB Streams
  const tableInfo = revisionRecord.get("payload", "tableInfo");
  if (tableInfo) {
    return tableInfo;
  }
  return null;
};

const getDataMetadataFromRevisionRecord = (revisionRecord: dom.Value) => {
  let revisionData: dom.Value | null = null;
  let revisionMetadata: dom.Value | null = null;

  const revision = revisionRecord.get("payload", "revision");
  if (revision) {
    if (revision.get("data")) {
      revisionData = revision.get("data");
    }
    if (revision.get("metadata")) {
      revisionMetadata = revision.get("metadata");
    }
  }

  return [revisionData, revisionMetadata];
};

const filteredRecordsGenerator = (
  kinesisDeaggregateRecords: UserRecord[],
  tableNames?: string[],
) =>
  kinesisDeaggregateRecords.reduce((acc, record) => {
    // Kinesis data in Node.js Lambdas is base64 encoded
    const payload = Buffer.from(record.data, "base64");
    // payload is the actual ion binary record published by QLDB to the stream
    const ionRecord = load(payload);
    console.info(`Ion record: ${dumpPrettyText(ionRecord)}}`);

    if (
      ionRecord &&
      ionRecord.get("recordType")?.stringValue() ===
        REVISION_DETAILS_RECORD_TYPE
    ) {
      const tableInfo = getTableInfoFromRevisionRecord(ionRecord);
      const tableName = tableInfo?.get("tableName")?.stringValue();

      if (
        !tableNames ||
        (tableInfo && tableName && tableNames.includes(tableName))
      ) {
        const [revisionData, revisionMetadata] =
          getDataMetadataFromRevisionRecord(ionRecord);

        acc.push({
          tableInfo,
          revisionData,
          revisionMetadata,
        });
      }
    }

    return acc;
  }, [] as { tableInfo: dom.Value | null; revisionData: dom.Value | null; revisionMetadata: dom.Value | null }[]);

const daysToSeconds = (days: number) => Math.floor(days) * 24 * 60 * 60;

export async function lambdaHandler(event: any, context: any): Promise<any> {
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
  for (const record of filteredRecordsGenerator(userRecords, [
    QLDB_TABLE_NAME,
  ])) {
    const tableName = record.tableInfo?.get("tableName")?.stringValue();
    const revisionData = record.revisionData;
    const revisionMetadata = record.revisionMetadata;

    if (revisionData) {
      if (tableName === QLDB_TABLE_NAME) {
        // Ref: dumpText: https://github.com/amazon-ion/ion-js/blob/master/src/dom/README.md#iondumpbinary-iondumptext-and-iondumpprettytext
        // Or Down-converting to JSON? https://amazon-ion.github.io/ion-docs/guides/cookbook.html
        const ddbItem = JSON.parse(dumpText(revisionData), (key, value) =>
          typeof value === "string" && /^[\d-]+T[\d:.]+Z$/.test(value)
            ? new Date(value)
            : value,
        );
        const stringDatetime = dumpText(revisionMetadata?.get("txTime"))?.split(
          " ",
        )?.[1];
        const parsedDatetime = new Date(stringDatetime);
        const unixTime = Math.floor(parsedDatetime.getTime() / 1000);
        ddbItem.txTime = stringDatetime;
        ddbItem.txId = revisionMetadata?.get("txId");
        ddbItem.timestamp = unixTime;
        if (TTL_ATTRIBUTE && EXPIRE_AFTER_DAYS) {
          ddbItem[TTL_ATTRIBUTE] =
            unixTime + daysToSeconds(Number(EXPIRE_AFTER_DAYS));
        }

        const putCommand = new PutItemCommand({
          TableName: TABLE_NAME,
          Item: ddbItem,
        });

        try {
          await client.send(putCommand);
        } catch (error) {
          console.error(`Error processing record ${dumpPrettyText(ddbItem)}`);
          throw error;
        }
      }
    }
  }
  return {
    statusCode: 200,
  };
}
