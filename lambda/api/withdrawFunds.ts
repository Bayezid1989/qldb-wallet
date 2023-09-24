import { TransactionExecutor } from "amazon-qldb-driver-nodejs";
import type { APIGatewayProxyHandler } from "aws-lambda";
import {
  checkAccountBalance,
  initQldbDriver,
  returnError,
  returnResponse,
} from "../utils";

const QLDB_TABLE_NAME = process.env.QLDB_TABLE_NAME || "";

// Initialize the driver
const qldbDriver = initQldbDriver();

const withdrawFunds = async (
  accountId: string,
  amount: number,
  executor: TransactionExecutor,
) => {
  const balance = await checkAccountBalance(accountId, executor);
  if (typeof balance !== "number") return balance;
  if (balance - amount < 0) {
    return returnError(
      `Funds too low. Cannot deduct ${amount} from account ${accountId}`,
      400,
    );
  }

  console.info(`Updating balance with ${amount} for ${accountId}`);
  const newBalance = balance - amount;

  await executor.execute(
    `UPDATE "${QLDB_TABLE_NAME}" SET balance = ? WHERE accountId = ?`,
    newBalance,
    accountId,
  );

  return returnResponse({ accountId, oldBalance: balance, newBalance });
};

export const handler: APIGatewayProxyHandler = async (event) => {
  console.debug(`Event received: ${JSON.stringify(event)}`);
  let body: any = {};

  try {
    body = JSON.parse(event.body || "{}");
  } catch (error: any) {
    return returnError(error.message, 400);
  }

  if (body.accountId && body.amount > 0) {
    try {
      const obj = await qldbDriver.executeLambda(
        (executor: TransactionExecutor) =>
          withdrawFunds(body.accountId, body.amount, executor),
      );
      return obj;
    } catch (error: any) {
      return returnError(error.message, 500);
    }
  } else {
    return returnError(
      "accountId and amount not specified, or amount is less than zero",
      400,
    );
  }
};
