import {
  QldbDriver,
  RetryConfig,
  TransactionExecutor,
} from "amazon-qldb-driver-nodejs";
import { APIGatewayProxyResult } from "aws-lambda";
import type { dom } from "ion-js";

import { config } from "../../config";

const QLDB_TABLE_NAME = process.env.QLDB_TABLE_NAME || "";

export const daysToSeconds = (days: number) => Math.floor(days) * 24 * 60 * 60;

export const returnError = (
  message: string,
  httpStatusCode: number = 500,
): APIGatewayProxyResult => {
  const returnMessage = { status: "Error", message };
  const returnObject = {
    statusCode: httpStatusCode,
    body: JSON.stringify(returnMessage),
    isBase64Encoded: false,
  };
  console.error(returnMessage);
  return returnObject;
};

export const returnResponse = (body: Record<string, any>) => ({
  statusCode: 200,
  body: JSON.stringify({ ...body, status: "OK" }),
  isBase64Encoded: false,
});

export const initQldbDriver = (ledgerName: string) => {
  const retryLimit = 3;

  const retryConfig = new RetryConfig(retryLimit);

  // Initialize the driver
  return new QldbDriver(ledgerName, retryConfig);
};

const isIonNull = (ion: dom.Value | null | undefined, key: string) =>
  !ion || !ion.get(key) || ion.get(key)?.isNull();

export const ionString = (ion: dom.Value | null | undefined, key: string) =>
  isIonNull(ion, key) ? null : ion?.get(key)?.stringValue();

export const ionNumber = (ion: dom.Value | null | undefined, key: string) =>
  isIonNull(ion, key) ? null : ion?.get(key)?.numberValue();

export const ionTimestamp = (ion: dom.Value | null | undefined, key: string) =>
  isIonNull(ion, key) ? null : ion?.get(key)?.timestampValue();

export const checkAccountBalance = async (
  accountId: string,
  txRequestId: string,
  executor: TransactionExecutor,
): Promise<number | APIGatewayProxyResult> => {
  console.info(`Retrieving account for id ${accountId}`);
  const res = await executor.execute(
    `SELECT accountId, balance, txRequestId FROM "${QLDB_TABLE_NAME}" WHERE accountId = ?`,
    accountId,
  );
  const records: dom.Value[] = res.getResultList();

  if (!records.length) {
    return returnError(`Account ${accountId} not found`, 400);
  }
  if (records.length > 1) {
    return returnError(`More than one account with user id ${accountId}`, 500);
  }
  const record = records[0];
  if (ionString(record, "txRequestId") === txRequestId) {
    return returnError(
      `Transaction Request ${txRequestId} is already processed`,
      400,
    );
  }
  return ionNumber(record, "balance") || 0;
};

export const parseIonRecord = (ionRecord: dom.Value | null) => {
  const payload = ionRecord?.get("payload");
  const tableInfo = payload?.get("tableInfo");
  const revision = payload?.get("revision");
  const data = revision?.get("data");
  const metadata = revision?.get("metadata");

  return {
    tableInfo: {
      tableName: ionString(tableInfo, "tableName"),
      tableId: ionString(tableInfo, "tableId"),
    },
    revision: {
      data: {
        accountId: ionString(data, "accountId"),
        balance: ionNumber(data, "balance"),

        // Last transaction data
        txAmount: ionNumber(data, "txAmount"),
        txFrom: ionString(data, "txFrom"),
        txTo: ionString(data, "txTo"),
        txType: ionString(data, "txType"),
        txRequestId: ionString(data, "txRequestId"),
      },
      metadata: {
        txTime: ionTimestamp(metadata, "txTime"),
        txId: ionString(metadata, "txId"),
      },
    },
  };
};

// Ion record: {
//   qldbStreamArn: "arn:aws:qldb:ap-northeast-1:670756400362:stream/test-wallet/0FyCS5aYSysK7aD7h8wvp3",
//   recordType: "REVISION_DETAILS",
//   payload: {
//     tableInfo: {
//       tableName: "Wallet",
//       tableId: "AYj94Ipn0re4Fr2PDgadpl"
//     },
//     revision: {
//       blockAddress: {
//         strandId: "A2mzwAutFNnJm2ho9nls4q",
//         sequenceNo: 711
//       },
//       hash: {{j5yn00sQj4clTrEN4vka5BrbVjQ+v41wBSqjj9AFveA=}},
//       data: {
//         accountId: "user1",
//         balance: 3000,
//         txAmount: 500,
//         txFrom: null,
//         txTo: "user1",
//         txType: "DEPOSIT",
//         txRequestId: "req5"
//       },
//       metadata: {
//         id: "59NbC9MoyMw4vsTsoNROxX",
//         version: 15,
//         txTime: 2023-09-25T06:34:30.619Z,
//         txId: "0007t6JMyzMK2LEscgWJ28"
//       }
//     }
//   }
// }}
