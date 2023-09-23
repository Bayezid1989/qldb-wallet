import { TransactionExecutor } from "amazon-qldb-driver-nodejs";
import type { APIGatewayProxyHandler } from "aws-lambda";
import {
  getQldbAccountBalance,
  initQldbDriver,
  returnError,
  returnResponse,
} from "../utils";

const QLDB_TABLE_NAME = process.env.QLDB_TABLE_NAME || "";

// Initialize the driver
const qldbDriver = initQldbDriver();

// Ref: QLDB driver NodeJs CRUD: https://docs.aws.amazon.com/qldb/latest/developerguide/driver-cookbook-nodejs.html
const addFunds = async (
  accountId: string,
  amount: number,
  executor: TransactionExecutor,
) => {
  const returnBody: Record<string, any> = {};

  const balance = await getQldbAccountBalance(accountId, executor);
  if (typeof balance !== "number") return balance;

  console.info(`Updating balance with ${amount} for ${accountId}`);
  returnBody.accountId = accountId;
  returnBody.oldBalance = balance;
  returnBody.newBalance = balance + amount;
  await executor.execute(
    `UPDATE "${QLDB_TABLE_NAME}" SET balance = ? WHERE accountId = ?`,
    returnBody.newBalance,
    accountId,
  );

  return returnResponse(returnBody);
};

export const handler: APIGatewayProxyHandler = async (event) => {
  console.debug(`Event received: ${JSON.stringify(event)}`);
  let body: any = {};

  try {
    body = JSON.parse(event.body || "{}");
  } catch (error: any) {
    return returnError(error.message, 400);
  }

  if (body.accountId && body.amount && body.amount > 0) {
    try {
      const res = await qldbDriver.executeLambda((executor) =>
        addFunds(body.accountId, body.amount, executor),
      );
      return res;
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
