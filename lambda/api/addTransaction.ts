import { TransactionExecutor } from "amazon-qldb-driver-nodejs";
import type { APIGatewayProxyHandler } from "aws-lambda";
import {
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

const appendTransaction = async (
  accountId: string,
  amount: number,
  requestTime: string,
  executor: TransactionExecutor,
) => {
  const obj = await getValidBalances(accountId, executor, requestTime, amount);
  if ("statusCode" in obj) return obj; // Error object

  if (obj.pendingTxs.some((tx) => tx.requestTime === requestTime)) {
    return returnError(
      `Transaction Request ${requestTime} is already requested`,
      400,
    );
  }

  console.info(`Adding transaction with ${amount} for account ${accountId}`);

  await executor.execute(
    `UPDATE "${QLDB_TABLE_NAME}"
    SET lastTx = ?, pendingTxs = ?
    WHERE accountId = ?`,
    { amount, from: null, to: null, status: TX_STATUS.REQUESTED, requestTime },
    obj.pendingTxs.concat({ amount, requestTime }),
    accountId,
  );

  return returnResponse({
    accountId,
    amount,
    requestTime,
    txStatus: TX_STATUS.REQUESTED,
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
        appendTransaction(
          body.accountId,
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
