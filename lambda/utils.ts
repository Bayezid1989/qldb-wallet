import {
  QldbDriver,
  RetryConfig,
  TransactionExecutor,
} from "amazon-qldb-driver-nodejs";
import { APIGatewayProxyResult } from "aws-lambda";
import type { dom } from "ion-js";

import { config } from "../config";

const QLDB_TABLE_NAME = process.env.QLDB_TABLE_NAME || "";

export const returnError = (
  message: string,
  httpStatusCode: number = 500,
): APIGatewayProxyResult => {
  const returnMessage = { status: "error", message };
  const returnObject = {
    statusCode: httpStatusCode,
    body: JSON.stringify(returnMessage),
    isBase64Encoded: false,
  };
  console.error(returnMessage);
  return returnObject;
};

export const initQldbDriver = () => {
  const LEDGER_NAME = config.ledgerName;
  const retryLimit = 3;

  const retryConfig = new RetryConfig(retryLimit);

  // Initialize the driver
  return new QldbDriver(LEDGER_NAME, retryConfig);
};

export const getQldbAccountBalance = async (
  accountId: string,
  executor: TransactionExecutor,
): Promise<number | APIGatewayProxyResult> => {
  console.info(`Retrieving number of accounts for id ${accountId}`);
  const res1 = await executor.execute(
    `SELECT count(accountId) as numberOfAccounts FROM "${QLDB_TABLE_NAME}" WHERE accountId = ?`,
    accountId,
  );

  const firstDoc1: dom.Value = res1.getResultList()[0];

  if (firstDoc1) {
    const numOfAccounts = firstDoc1.get("numberOfAccounts")?.numberValue();
    if (numOfAccounts && numOfAccounts > 1) {
      return returnError(
        `More than one account with user id ${accountId}`,
        500,
      );
    }
    if (numOfAccounts === 0) {
      return returnError(`Account ${accountId} not found`, 400);
    }
  }

  console.info(`Retrieving balance for UPDATE... for ${accountId}`);
  const res2 = await executor.execute(
    `SELECT balance FROM "${QLDB_TABLE_NAME}" WHERE accountId = ?`,
    accountId,
  );

  const firstDoc2 = res2.getResultList()[0];
  return firstDoc2.get("balance")?.numberValue() || 0;
};

export const parseRevisionDetails = (ionRecord: dom.Value) => {
  const payload = ionRecord.get("payload");
  const tableInfo = payload?.get("tableInfo");
  const revision = payload?.get("revision");
  const blockAddress = revision?.get("blockAddress");
  const data = revision?.get("data");
  const metadata = revision?.get("metadata");

  return {
    qldbStreamArn: ionRecord.get("qldbStreamArn")?.stringValue(),
    recordType: ionRecord.get("recordType")?.stringValue(),
    payload: {
      tableInfo: {
        tableName: tableInfo?.get("tableName")?.stringValue(),
        tableId: tableInfo?.get("tableId")?.stringValue(),
      },
      revision: {
        blockAddress: {
          strandId: blockAddress?.get("strandId")?.stringValue(),
          sequenceNo: blockAddress?.get("sequenceNo")?.numberValue(),
        },
        hash: revision?.get("hash")?.stringValue(),
        data: {
          accountId: data?.get("accountId")?.stringValue(),
          balance: data?.get("balance")?.numberValue(),
        },
        metadata: {
          id: metadata?.get("id")?.stringValue(),
          version: metadata?.get("version")?.numberValue(),
          txTime: metadata?.get("txTime")?.timestampValue(),
          txId: metadata?.get("txId")?.stringValue(),
        },
      },
    },
  };
};

//  Ion record: {
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
//         sequenceNo: 26
//       },
//       hash: {{KGQhnyXb/BltXwCY9WPc0y1B8JLk/ok6w5YiaKweOvc=}},
//       data: {
//         accountId: "user1",
//         balance: 0
//       },
//       metadata: {
//         id: "59NbC9MoyMw4vsTsoNROxX",
//         version: 0,
//         txTime: 2023-09-23T07:45:10.121Z,
//         txId: "GE2lVViZmSY6QHl0OmvsmN"
//       }
//     }
//   }
// }}
