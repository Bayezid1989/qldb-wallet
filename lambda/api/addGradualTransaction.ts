import { TransactionExecutor } from "amazon-qldb-driver-nodejs";
import type { APIGatewayProxyHandler } from "aws-lambda";
import {
  checkGetBalances,
  initQldbDriver,
  returnError,
  returnResponse,
} from "../util/util";
import { TX_STATUS } from "../util/constant";
import { config } from "../../config";

const { QLDB_TABLE_NAME } = config;

// Initialize the driver
const qldbDriver = initQldbDriver();

const appendTransaction = async (
  accountId: string,
  amount: number,
  requestId: string,
  executor: TransactionExecutor,
) => {
  const obj = await checkGetBalances(accountId, executor, requestId, amount);
  if ("statusCode" in obj) return obj; // Error object

  const tx = obj.pendingTxs.find((tx) => tx.requestId === requestId);
  if (tx) {
    return returnError(
      `Transaction Request ${requestId} is already ${tx.status}`,
      400,
    );
  }

  console.info(`Adding transaction with ${amount} for account ${accountId}`);
  const status = TX_STATUS.REQUESTED;

  await executor.execute(
    `UPDATE "${QLDB_TABLE_NAME}"
    SET lastTx = ?, pendingTxs = ?
    WHERE accountId = ?`,
    { amount, from: null, to: null, status, requestId },
    obj.pendingTxs.concat({ amount, requestId, status }),
    accountId,
  );

  return returnResponse({
    accountId,
    amount,
    requestId,
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
    body.accountId &&
    body.requestId &&
    typeof body.amount === "number" &&
    body.amount !== 0
  ) {
    try {
      const res = await qldbDriver.executeLambda(
        (executor: TransactionExecutor) =>
          appendTransaction(
            body.accountId,
            body.amount,
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
