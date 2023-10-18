import { TransactionExecutor } from "amazon-qldb-driver-nodejs";
import type { APIGatewayProxyHandler } from "aws-lambda";
import type { dom } from "ion-js";
import {
  checkAvailableBalances,
  getLastTxRequestTime,
  initQldbDriver,
  ionString,
  returnError,
  returnResponse,
} from "../util/util";
import { config } from "../../config";
import { ISO8601_REGEX, TX_STATUS } from "../util/constant";

const { QLDB_TABLE_NAME } = config;

// Initialize the driver
const qldbDriver = initQldbDriver();

const transferFunds = async (
  fromAccountId: string,
  toAccountId: string,
  amount: number,
  requestTime: string,
  executor: TransactionExecutor,
) => {
  const idsString = `(From: ${fromAccountId}, To: ${toAccountId})`;
  console.info(`Retrieving accounts ${idsString}`);
  const res = await executor.execute(
    `SELECT accountId, balance, lastTx, 
    FROM "${QLDB_TABLE_NAME}"
    WHERE accountId IN (?, ?)`,
    fromAccountId,
    toAccountId,
  );

  const records: dom.Value[] = res.getResultList();
  if (!records.length) {
    return returnError(`Both accounts${idsString} not found.`, 400);
  }
  if (records.length > 2) {
    return returnError(`More than 2 accounts for ids${idsString}`, 500);
  }

  let fromAccount: dom.Value | undefined;
  let toAccount: dom.Value | undefined;
  let hasSameRequestId = false;

  records.forEach((record) => {
    if (ionString(record, "accountId") === fromAccountId) {
      fromAccount = record;
    } else if (ionString(record, "accountId") === toAccountId) {
      toAccount = record;
    }
    if (getLastTxRequestTime(record) === requestTime) {
      hasSameRequestId = true;
    }
  });

  if (!fromAccount) {
    return returnError(`From account ${fromAccountId} not found.`, 400);
  }
  if (!toAccount) {
    return returnError(`To account ${toAccountId} not found.`, 400);
  }
  if (hasSameRequestId) {
    return returnError(
      `Transaction Request ${requestTime} already processed`,
      400,
    );
  }

  const obj = checkAvailableBalances(fromAccount, fromAccountId, amount);
  if ("statusCode" in obj) return obj; // Error object

  console.info(`Transfering with ${amount} for accounts${idsString}`);

  // Deduct the amount from account
  await executor.execute(
    `UPDATE "${QLDB_TABLE_NAME}"
    SET balance = balance - ?, lastTx = ?
    WHERE accountId = ?`,
    amount,
    {
      amount: -amount,
      from: fromAccountId,
      to: toAccountId,
      status: TX_STATUS.IMMEDIATE,
      requestTime,
    },
    fromAccountId,
  );

  // Add the amount to account
  await executor.execute(
    `UPDATE "${QLDB_TABLE_NAME}"
    SET balance = balance + ?, lastTx = ?
    WHERE accountId = ?`,
    amount,
    {
      amount,
      from: fromAccountId,
      to: toAccountId,
      status: TX_STATUS.IMMEDIATE,
      requestTime,
    },
    toAccountId,
  );

  return returnResponse({
    fromAccountId,
    toAccountId,
    amount,
    requestTime,
  });
};

export const handler: APIGatewayProxyHandler = async (event) => {
  console.debug(`Event received: ${JSON.stringify(event)}`);
  let body: any = {};

  try {
    body = JSON.parse(event.body || "{}");
  } catch (error: any) {
    return returnError(error.message, 400);
  }

  if (
    typeof body.fromAccountId !== "string" ||
    typeof body.toAccountId !== "string" ||
    !ISO8601_REGEX.test(body.requestTime) ||
    typeof body.amount !== "number" ||
    body.amount <= 0
  ) {
    return returnError(
      "accountId, amount or requestTime not specified or invalid",
      400,
    );
  }

  try {
    const res = await qldbDriver.executeLambda((executor) =>
      transferFunds(
        body.fromAccountId,
        body.toAccountId,
        body.amount,
        body.requestTime,
        executor,
      ),
    );
    return res;
  } catch (error: any) {
    return returnError(error.message, 500);
  }
};
