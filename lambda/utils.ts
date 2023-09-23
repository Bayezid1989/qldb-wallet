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
