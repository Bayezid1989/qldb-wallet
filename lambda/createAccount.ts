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

let returnObject: any = {};

const setError = (message: string, httpStatusCode: number = 500) => {
  const returnMessage = { status: "error", message };
  returnObject = {
    statusCode: httpStatusCode,
    body: JSON.stringify(returnMessage),
    isBase64Encoded: false,
  };
  console.error(returnMessage);
  return returnObject;
};

const createAccount = async (
  accountId: string,
  executor: TransactionExecutor,
) => {
  const returnMessage: any = {};

  console.info(`Verifying account with id ${accountId} does not exist`);
  let res = await executor.execute(
    `SELECT * FROM "${QLDB_TABLE_NAME}" WHERE accountId = ? `,
    accountId,
  );

  let firstDoc: dom.Value = res.getResultList()[0];

  if (firstDoc) {
    setError(`Account with user id ${accountId} already exists`, 400);
    return;
  } else {
    const doc = { accountId, balance: 0 };
    console.log(
      `Creating account with id ${accountId} and balance = ${doc.balance}`,
    );
    await executor.execute(`INSERT INTO "${QLDB_TABLE_NAME}" ?`, doc);
  }

  returnMessage.accountId = accountId;
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

  if (body.accountId) {
    try {
      await qldbDriver.executeLambda((executor: TransactionExecutor) =>
        createAccount(body.accountId, executor),
      );
    } catch (error: any) {
      setError(error.message, 500);
    }
  } else {
    setError("accountId not specified", 400);
  }

  return returnObject;
};
