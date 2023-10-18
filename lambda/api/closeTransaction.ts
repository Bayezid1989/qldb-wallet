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

const closeTransaction = async (
  accountId: string,
  requestTime: string,
  status: keyof typeof TX_STATUS,
  executor: TransactionExecutor,
) => {
  const obj = await getValidBalances(accountId, executor); // Omit checking requestTime
  if ("statusCode" in obj) return obj; // Error object

  const txIndex = obj.pendingTxs.findIndex(
    (tx) => tx.requestTime === requestTime,
  );
  if (txIndex === -1) {
    return returnError(`Transaction ${requestTime} not found`, 400);
  }
  const [tx] = obj.pendingTxs.splice(txIndex, 1); // Remove tx from pending

  console.info(
    `Closing transaction to ${status} with ${tx.amount} for account ${accountId}`,
  );
  const newBalance =
    status === "CANCELED" ? obj.balance : obj.balance + (tx.amount || 0);

  await executor.execute(
    `UPDATE "${QLDB_TABLE_NAME}"
      SET balance = ?, lastTx = ?, pendingTxs = ?
      WHERE accountId = ?`,
    newBalance,
    {
      amount: tx.amount,
      requestTime: tx.requestTime,
      status,
      from: null,
      to: null,
    },
    obj.pendingTxs,
    accountId,
  );

  return returnResponse({
    accountId,
    oldBalance: obj.balance,
    newBalance,
    amount: tx.amount,
    requestTime,
    txStatus: status,
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
    (body.status !== TX_STATUS.CANCELED && body.status !== TX_STATUS.COMMITED)
  ) {
    return returnError(
      "accountId requestTime or status not specified or invalid",
      400,
    );
  }

  try {
    const res = await qldbDriver.executeLambda(
      (executor: TransactionExecutor) =>
        closeTransaction(
          body.accountId,
          body.requestTime,
          body.status,
          executor,
        ),
    );
    return res;
  } catch (error: any) {
    return returnError(error.message, 500);
  }
};
