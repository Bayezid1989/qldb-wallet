import { TransactionExecutor } from "amazon-qldb-driver-nodejs";
import type { APIGatewayProxyHandler } from "aws-lambda";
import {
  checkAccountBalance,
  initQldbDriver,
  returnError,
  returnResponse,
} from "../util/util";
import { TX_TYPE } from "../util/constant";

const QLDB_TABLE_NAME = process.env.QLDB_TABLE_NAME || "";

// Initialize the driver
const qldbDriver = initQldbDriver(process.env.LEDGER_NAME || "");

const withdrawFunds = async (
  accountId: string,
  amount: number,
  txType = TX_TYPE.WITHDRAW,
  txRequestId: string,
  executor: TransactionExecutor,
) => {
  const balance = await checkAccountBalance(accountId, txRequestId, executor);
  if (typeof balance !== "number") return balance;
  if (balance - amount < 0) {
    return returnError(
      `Funds too low. Cannot deduct ${amount} from account ${accountId}`,
      400,
    );
  }

  console.info(`Updating balance with ${amount} for account ${accountId}`);
  const newBalance = balance - amount;

  await executor.execute(
    `UPDATE "${QLDB_TABLE_NAME}" SET balance = ?, txAmount = ?, txFrom = ?, txTo = NULL, txType = ?, txRequestId = ? WHERE accountId = ?`,
    newBalance,
    amount,
    accountId,
    txType,
    txRequestId,
    accountId,
  );

  return returnResponse({
    accountId,
    oldBalance: balance,
    newBalance,
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

  if (body.accountId && body.txRequestId && body.amount > 0) {
    try {
      const res = await qldbDriver.executeLambda(
        (executor: TransactionExecutor) =>
          withdrawFunds(
            body.accountId,
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
