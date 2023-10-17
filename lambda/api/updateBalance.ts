import { TransactionExecutor } from "amazon-qldb-driver-nodejs";
import type { APIGatewayProxyHandler } from "aws-lambda";
import {
  checkAvailableBalance,
  initQldbDriver,
  returnError,
  returnResponse,
} from "../util/util";
import { TX_TYPE } from "../util/constant";

const QLDB_TABLE_NAME = process.env.QLDB_TABLE_NAME || "";

// Initialize the driver
const qldbDriver = initQldbDriver(process.env.LEDGER_NAME || "");

const updateBalance = async (
  accountId: string,
  amount: number,
  type: string,
  requestId: string,
  executor: TransactionExecutor,
) => {
  const balance = await checkAvailableBalance(accountId, requestId, executor);
  if (typeof balance !== "number") return balance; // Error object
  if (amount < 0 && balance + amount < 0) {
    return returnError(
      `Funds too low. Cannot deduct ${amount} from account ${accountId}`,
      400,
    );
  }

  console.info(`Updating balance with ${amount} for account ${accountId}`);
  const newBalance = balance + amount;

  await executor.execute(
    `UPDATE "${QLDB_TABLE_NAME}" SET balance = ?, txAmount = ?, txFrom = NULL, txTo = NULL, txType = ?, txRequestId = ? WHERE accountId = ?`,
    newBalance,
    amount,
    type,
    requestId,
    accountId,
  );

  return returnResponse({
    accountId,
    oldBalance: balance,
    newBalance,
    type,
    requestId,
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
    body.accountId &&
    body.requestId &&
    body.type &&
    typeof body.amount === "number"
  ) {
    if (
      (body.amount > 0 && body.type === TX_TYPE.WITHDRAW) ||
      (body.amount < 0 && body.type === TX_TYPE.DEPOSIT)
    ) {
      return returnError(
        `Transaction type ${body.type} mismatches amount ${body.amount}`,
        400,
      );
    }
    try {
      const res = await qldbDriver.executeLambda(
        (executor: TransactionExecutor) =>
          updateBalance(
            body.accountId,
            body.amount,
            body.type,
            body.requestId,
            executor,
          ),
      );
      return res;
    } catch (error: any) {
      return returnError(error.message, 500);
    }
  } else {
    return returnError("accountId, amount or requestId not specified", 400);
  }
};
