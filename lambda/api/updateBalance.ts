import { TransactionExecutor } from "amazon-qldb-driver-nodejs";
import type { APIGatewayProxyHandler } from "aws-lambda";
import {
  FullTx,
  getValidBalances,
  initQldbDriver,
  returnError,
  returnResponse,
} from "../util/util";
import { ISO8601_REGEX, TX_STATUS } from "../util/constant";
import { config } from "../../config";

const { QLDB_TABLE_NAME } = config;

// Initialize the driver
const qldbDriver = initQldbDriver();

const updateBalance = async (
  accountId: string,
  amount: number,
  requestTime: string,
  executor: TransactionExecutor,
) => {
  const obj = await getValidBalances(accountId, executor, requestTime, amount);
  if ("statusCode" in obj) return obj; // Error object

  console.info(`Updating balance with ${amount} for account ${accountId}`);
  const newBalance = obj.balance + amount;

  const lastTx: FullTx = {
    amount,
    from: null,
    to: null,
    status: TX_STATUS.IMMEDIATE,
    requestTime,
  };
  await executor.execute(
    `UPDATE "${QLDB_TABLE_NAME}"
    SET balance = ?, lastTx = ?
    WHERE accountId = ?`,
    newBalance,
    lastTx,
    accountId,
  );

  return returnResponse({
    accountId,
    oldBalance: obj.balance,
    newBalance,
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
    typeof body.accountId !== "string" ||
    !ISO8601_REGEX.test(body.requestTime) ||
    typeof body.amount !== "number" ||
    body.amount === 0
  ) {
    return returnError(
      "accountId, amount or requestTime not specified or invalid",
      400,
    );
  }

  try {
    const res = await qldbDriver.executeLambda(
      (executor: TransactionExecutor) =>
        updateBalance(body.accountId, body.amount, body.requestTime, executor),
    );
    return res;
  } catch (error: any) {
    return returnError(error.message, 500);
  }
};
