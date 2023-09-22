import {
  QldbDriver,
  RetryConfig,
  TransactionExecutor,
} from "amazon-qldb-driver-nodejs";
import type { Handler } from "aws-lambda";
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

const queryFunds = async (
  accountId: string,
  executor: TransactionExecutor,
): Promise<void> => {
  const returnMessage: any = {};

  console.info(`Looking up balance for account with id ${accountId}`);
  const res = await executor.execute(
    `SELECT accountId, balance FROM "${QLDB_TABLE_NAME}" WHERE accountId = ?`,
    accountId,
  );
  const firstDoc = res.getResultList()[0];

  if (firstDoc) {
    returnMessage.accountId = firstDoc.get("accountId")?.stringValue();
    returnMessage.balance = firstDoc.get("balance")?.stringValue();
  } else {
    setError(`Account ${accountId} not found`, 400);
    return;
  }

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
    body = JSON.parse(event.body || "{}");
  } catch (error: any) {
    setError(error.message, 400);
  }

  if (body.accountId) {
    try {
      await qldbDriver.executeLambda((executor: TransactionExecutor) =>
        queryFunds(body.accountId, executor),
      );
    } catch (error: any) {
      setError(error.message, 500);
    }
  } else {
    setError("accountId not specified", 400);
  }

  return returnObject;
};
