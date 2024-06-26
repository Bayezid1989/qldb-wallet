import { TransactionExecutor } from "amazon-qldb-driver-nodejs";
import type { APIGatewayProxyHandler } from "aws-lambda";
import {
  initQldbDriver,
  returnError,
  returnResponse,
  getValidBalances,
} from "../util/util";

// Initialize the driver
const qldbDriver = initQldbDriver();

const queryBalance = async (
  accountId: string,
  executor: TransactionExecutor,
) => {
  console.info(`Looking up balance for account ${accountId}`);
  const obj = await getValidBalances(accountId, executor);
  if ("statusCode" in obj) return obj; // Error object

  return returnResponse({
    accountId,
    balance: obj.balance,
    availableBalance: obj.availableBalance,
  });
};

export const handler: APIGatewayProxyHandler = async (event) => {
  console.debug(`Event received: ${JSON.stringify(event)}`);
  const accountId = event.pathParameters?.accountId;

  if (typeof accountId !== "string") {
    return returnError("accountId not specified or invalid", 400);
  }

  try {
    const res = await qldbDriver.executeLambda((executor) =>
      queryBalance(accountId, executor),
    );
    return res;
  } catch (error: any) {
    return returnError(error.message, 500);
  }
};
