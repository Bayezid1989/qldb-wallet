import { TransactionExecutor } from "amazon-qldb-driver-nodejs";
import type { APIGatewayProxyHandler } from "aws-lambda";
import { getQldbAccountBalance, initQldbDriver, returnError } from "../utils";

const QLDB_TABLE_NAME = process.env.QLDB_TABLE_NAME || "";

// Initialize the driver
const qldbDriver = initQldbDriver();

// Ref: QLDB driver NodeJs CRUD: https://docs.aws.amazon.com/qldb/latest/developerguide/driver-cookbook-nodejs.html
const addFunds = async (
  accountId: string,
  amount: number,
  executor: TransactionExecutor,
) => {
  const returnMessage: any = {};

  const balance = await getQldbAccountBalance(accountId, executor);
  if (typeof balance !== "number") return balance;

  console.info(`Updating balance with ${amount} for ${accountId}`);
  returnMessage.accountId = accountId;
  returnMessage.oldBalance = balance;
  returnMessage.newBalance = balance + amount;
  await executor.execute(
    `UPDATE "${QLDB_TABLE_NAME}" SET balance = ? WHERE accountId = ?`,
    returnMessage.newBalance,
    accountId,
  );

  return {
    statusCode: 200,
    body: JSON.stringify({ ...returnMessage, status: "Ok" }),
    isBase64Encoded: false,
  };
};

export const lambdaHandler: APIGatewayProxyHandler = async (event) => {
  console.debug(`Event received: ${JSON.stringify(event)}`);
  let body: any = {};

  try {
    body = JSON.parse(event.body || "{}");
  } catch (error: any) {
    return returnError(error.message, 400);
  }

  if (body.accountId && body.amount && body.amount > 0) {
    try {
      const obj = await qldbDriver.executeLambda(
        (executor: TransactionExecutor) =>
          addFunds(body.accountId, body.amount, executor),
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
