import {
  QldbDriver,
  RetryConfig,
  TransactionExecutor,
} from "amazon-qldb-driver-nodejs";
import type { Handler } from "aws-lambda";
import type { dom } from "ion-js";
import { config } from "../config";

const LEDGER_NAME = config.ledgerName;
const QLDB_TABLE_NAME = config.qldbTableName;
const retryLimit = 3;

const retryConfig = new RetryConfig(retryLimit);

// Initialize the driver
const qldbDriver = new QldbDriver(LEDGER_NAME, retryConfig);

let returnObject: {
  statusCode?: number;
  body?: string;
  isBase64Encoded?: boolean;
} = {};

const setError = (message: string, httpStatusCode: number = 500): void => {
  const returnMessage = { status: "error", message };
  returnObject = {
    statusCode: httpStatusCode,
    body: JSON.stringify(returnMessage),
    isBase64Encoded: false,
  };
  console.error(returnMessage);
};

// Ref: QLDB driver NodeJs CRUD: https://docs.aws.amazon.com/qldb/latest/developerguide/driver-cookbook-nodejs.html
const addFunds = async (
  accountId: string,
  amount: number,
  executor: TransactionExecutor,
): Promise<void> => {
  const returnMessage: any = {};

  console.info(`Retrieving number of accounts for id ${accountId}`);
  const res1 = await executor.execute(
    `SELECT count(accountId) as numberOfAccounts FROM "${QLDB_TABLE_NAME}" WHERE accountId = ?`,
    accountId,
  );

  const firstDoc1: dom.Value = res1.getResultList()[0];

  if (firstDoc1) {
    const numOfAccounts = firstDoc1.get("numberOfAccounts")?.numberValue();
    if (numOfAccounts && numOfAccounts > 1) {
      setError(`More than one account with user id ${accountId}`, 500);
      return;
    }
    if (numOfAccounts === 0) {
      setError(`Account ${accountId} not found`, 400);
      return;
    }
  }

  console.info(`Retrieving balance for UPDATE... for ${accountId}`);
  const res2 = await executor.execute(
    `SELECT balance FROM "${QLDB_TABLE_NAME}" WHERE accountId = ?`,
    accountId,
  );

  const firstDoc2 = res2.getResultList()[0];

  console.info(`Updating balance with ${amount} for ${accountId}`);
  const balance = firstDoc2.get("balance")?.numberValue() || 0;
  returnMessage.accountId = accountId;
  returnMessage.oldBalance = balance;
  returnMessage.newBalance = balance + amount;
  await executor.execute(
    `UPDATE "${QLDB_TABLE_NAME}" SET balance = ? WHERE accountId = ?`,
    returnMessage.newBalance,
    accountId,
  );

  returnObject = {
    statusCode: 200,
    body: JSON.stringify({ ...returnMessage, status: "Ok" }),
    isBase64Encoded: false,
  };
};

export const lambdaHandler: Handler = async (event, context) => {
  console.debug(`Event received: ${JSON.stringify(event)}`);
  let body: any = {};

  try {
    body = JSON.parse(event.body);
  } catch (error: any) {
    setError(error.message, 400);
  }

  if (body.accountId && body.amount && body.amount > 0) {
    try {
      await qldbDriver.executeLambda((executor: TransactionExecutor) =>
        addFunds(body.accountId, body.amount, executor),
      );
    } catch (error: any) {
      setError(error.message, 500);
    }
  } else {
    setError(
      "accountId and amount not specified, or amount is less than zero",
      400,
    );
  }

  return returnObject;
};
