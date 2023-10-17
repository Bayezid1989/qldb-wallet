import { TransactionExecutor } from "amazon-qldb-driver-nodejs";
import type { APIGatewayProxyHandler } from "aws-lambda";
import {
  checkGetBalances,
  initQldbDriver,
  returnError,
  returnResponse,
} from "../util/util";
import { TX_TYPE } from "../util/constant";
import { config } from "../../config";

const { QLDB_TABLE_NAME } = config;

// Initialize the driver
const qldbDriver = initQldbDriver();

const updateBalance = async (
  accountId: string,
  amount: number,
  type: string,
  requestId: string,
  executor: TransactionExecutor,
) => {
  const obj = await checkGetBalances(accountId, executor, requestId, amount);
  if ("statusCode" in obj) return obj; // Error object

  console.info(
    `Updating balance for ${type} with ${amount} for account ${accountId}`,
  );
  const newBalance = obj.balance + amount;

  await executor.execute(
    `UPDATE "${QLDB_TABLE_NAME}"
    SET balance = ?, txAmount = ?, txFrom = NULL, txTo = NULL, txType = ?, txRequestId = ?
    WHERE accountId = ?`,
    newBalance,
    amount,
    type,
    requestId,
    accountId,
  );

  return returnResponse({
    accountId,
    oldBalance: obj.balance,
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
    if (body.type !== TX_TYPE.WITHDRAW && body.type !== TX_TYPE.DEPOSIT) {
      return returnError(`Wrong transaction type ${body.type}`, 400);
    }
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
    return returnError(
      "accountId, amount, type or requestId not specified",
      400,
    );
  }
};
