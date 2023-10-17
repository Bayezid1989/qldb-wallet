import { TransactionExecutor } from "amazon-qldb-driver-nodejs";
import type { APIGatewayProxyHandler } from "aws-lambda";
import {
  checkGetBalances,
  initQldbDriver,
  returnError,
  returnResponse,
} from "../util/util";
import { TX_STATUS, TX_TYPE } from "../util/constant";
import { config } from "../../config";

const { QLDB_TABLE_NAME } = config;

// Initialize the driver
const qldbDriver = initQldbDriver();

const appendTransaction = async (
  accountId: string,
  amount: number,
  type: string,
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

  console.info(
    `Adding pending ${type} transaction with ${amount} for account ${accountId}`,
  );
  const status = TX_STATUS.REQUESTED;

  await executor.execute(
    `UPDATE "${QLDB_TABLE_NAME}"
    SET txAmount = NULL, txFrom = NULL, txTo = NULL, txType = NULL, txRequestId = NULL, pendingTxs = ?
    WHERE accountId = ?`,
    obj.pendingTxs.concat({ amount, type, requestId, status }),
    accountId,
  );

  return returnResponse({
    accountId,
    amount,
    type,
    requestId,
    status,
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
      body.type !== TX_TYPE.WITHDRAW_TO_BANK &&
      body.type !== TX_TYPE.DEPOSIT_FROM_WALLET
    ) {
      return returnError(`Wrong transaction type ${body.type}`, 400);
    }
    if (
      (body.amount > 0 && body.type === TX_TYPE.WITHDRAW_TO_BANK) ||
      (body.amount < 0 && body.type === TX_TYPE.DEPOSIT_FROM_WALLET)
    ) {
      return returnError(
        `Transaction type ${body.type} mismatches amount ${body.amount}`,
        400,
      );
    }
    try {
      const res = await qldbDriver.executeLambda(
        (executor: TransactionExecutor) =>
          appendTransaction(
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
    return returnError(
      "accountId, amount, type or requestId not specified",
      400,
    );
  }
};
