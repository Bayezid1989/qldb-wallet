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

const closeTransaction = async (
  accountId: string,
  requestId: string,
  status: keyof typeof TX_STATUS,
  executor: TransactionExecutor,
) => {
  const obj = await checkGetBalances(accountId, executor); // Omit checking requestId
  if ("statusCode" in obj) return obj; // Error object

  const txIndex = obj.pendingTxs.findIndex((tx) => tx.requestId === requestId);
  if (txIndex === -1) {
    return returnError(`Transaction ${requestId} not found`, 400);
  }
  const [tx] = obj.pendingTxs.splice(txIndex, 1);

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
    { ...tx, from: null, to: null },
    obj.pendingTxs,
    accountId,
  );

  return returnResponse({
    accountId,
    oldBalance: obj.balance,
    newBalance,
    amount: tx.amount,
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

  if (body.accountId && body.requestId && body.status) {
    if (
      body.status !== TX_STATUS.CANCELED &&
      body.status !== TX_STATUS.COMMITED
    ) {
      return returnError(`Wrong transaction status ${body.status}`, 400);
    }
    try {
      const res = await qldbDriver.executeLambda(
        (executor: TransactionExecutor) =>
          closeTransaction(
            body.accountId,
            body.requestId,
            body.status,
            executor,
          ),
      );
      return res;
    } catch (error: any) {
      return returnError(error.message, 500);
    }
  } else {
    return returnError("accountId or requestId not specified", 400);
  }
};
