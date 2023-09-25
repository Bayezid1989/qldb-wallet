import { TransactionExecutor } from "amazon-qldb-driver-nodejs";
import type { APIGatewayProxyHandler } from "aws-lambda";
import type { dom } from "ion-js";
import {
  initQldbDriver,
  ionNumber,
  ionString,
  returnError,
  returnResponse,
} from "../util/util";
import { TX_TYPE } from "../util/constant";

const QLDB_TABLE_NAME = process.env.QLDB_TABLE_NAME || "";

// Initialize the driver
const qldbDriver = initQldbDriver();

const transferFunds = async (
  fromAccountId: string,
  toAccountId: string,
  amount: number,
  txType = TX_TYPE.TRANSFER,
  txRequestId: string,
  executor: TransactionExecutor,
) => {
  const idsString = `(From: ${fromAccountId}, To: ${toAccountId})`;
  console.info(`Retrieving accounts ${idsString}`);
  const res = await executor.execute(
    `SELECT accountId, balance, txRequestId FROM "${QLDB_TABLE_NAME}" WHERE accountId IN (?, ?)`,
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
    if (ionString(record, "txRequestId") === txRequestId) {
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
      `Transaction Request ${txRequestId} is already processed`,
      400,
    );
  }

  const fromBalance = ionNumber(fromAccount, "balance") || 0;
  if (fromBalance - amount < 0) {
    return returnError(
      `Funds too low. Cannot deduct ${amount} from account ${fromAccountId}`,
      400,
    );
  }

  console.info(`Updating balance with ${amount} for accounts${idsString}`);

  // Deduct the amount from account
  await executor.execute(
    `UPDATE "${QLDB_TABLE_NAME}" SET balance = balance - ?, txAmount = ?, txFrom = ?, txTo = ?, txType = ?, txRequestId = ? WHERE accountId = ?`,
    amount,
    amount,
    fromAccountId,
    toAccountId,
    txType,
    txRequestId,
    fromAccountId,
  );

  // Add the amount to account
  await executor.execute(
    `UPDATE "${QLDB_TABLE_NAME}" SET balance = balance + ?, txAmount = ?, txFrom = ?, txTo = ?, txType = ?, txRequestId = ? WHERE accountId = ?`,
    amount,
    amount,
    fromAccountId,
    toAccountId,
    txType,
    txRequestId,
    toAccountId,
  );

  return returnResponse({
    fromAccountId,
    toAccountId,
    amount,
    txType,
    txRequestId,
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
    body.fromAccountId &&
    body.toAccountId &&
    body.txRequestId &&
    body.amount > 0
  ) {
    try {
      const res = await qldbDriver.executeLambda((executor) =>
        transferFunds(
          body.fromAccountId,
          body.toAccountId,
          body.amount,
          body.txType,
          body.txRequestId,
          executor,
        ),
      );
      return res;
    } catch (error: any) {
      return returnError(error.message, 500);
    }
  } else {
    return returnError(
      "accountId, amount or txRequestId not specified, or amount is less than zero",
      400,
    );
  }
};
