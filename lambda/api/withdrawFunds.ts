import { TransactionExecutor } from "amazon-qldb-driver-nodejs";
import type { APIGatewayProxyEvent } from "aws-lambda";
import type { dom } from "ion-js";
import { initQldbDriver, returnError } from "../util";
import type { ReturnObj } from "../util/types";

const QLDB_TABLE_NAME = process.env.QLDB_TABLE_NAME || "";

// Initialize the driver
const qldbDriver = initQldbDriver();

const withdrawFunds = async (
  accountId: string,
  amount: number,
  executor: TransactionExecutor,
): Promise<ReturnObj> => {
  const returnMessage: any = {};

  console.info(`Retrieving number of accounts for id ${accountId}`);
  const res1 = await executor.execute(
    `SELECT count(accountId) as numberOfAccounts FROM "${QLDB_TABLE_NAME}" WHERE accountId = ?`,
    accountId,
  );

  const firstDoc1: dom.Value = res1.getResultList()[0];

  if (firstDoc1) {
    const numOfAccounts = firstDoc1.get("numberOfAccounts")?.numberValue();
    if (numOfAccounts && numOfAccounts > 1) {
      return returnError(
        `More than one account with user id ${accountId}`,
        500,
      );
    }
    if (numOfAccounts === 0) {
      return returnError(`Account ${accountId} not found`, 400);
    }
  }

  console.info(`Retrieving balance for UPDATE... for ${accountId}`);
  const res2 = await executor.execute(
    `SELECT balance FROM "${QLDB_TABLE_NAME}" WHERE accountId = ?`,
    accountId,
  );

  const firstDoc2 = res2.getResultList()[0];

  console.info(`Updating balance with ${amount} for ${accountId}`);
  const balance = firstDoc2.get("balance")?.numberValue() || 0;
  if (balance - amount < 0) {
    return returnError(
      `Funds too low. Cannot deduct ${amount} from account ${accountId}`,
      400,
    );
  }

  returnMessage.accountId = accountId;
  returnMessage.oldBalance = balance;
  returnMessage.newBalance = balance - amount;
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

export const lambdaHandler = async (event: APIGatewayProxyEvent) => {
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
          withdrawFunds(body.accountId, body.amount, executor),
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
