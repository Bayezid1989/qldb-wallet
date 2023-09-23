import { TransactionExecutor } from "amazon-qldb-driver-nodejs";
import type { APIGatewayProxyHandler } from "aws-lambda";
import { initQldbDriver, returnError } from "../utils";

const QLDB_TABLE_NAME = process.env.QLDB_TABLE_NAME || "";

// Initialize the driver
const qldbDriver = initQldbDriver();

const queryBalance = async (
  accountId: string,
  executor: TransactionExecutor,
) => {
  const returnMessage: any = {};

  console.info(`Looking up balance for account with id ${accountId}`);
  const res = await executor.execute(
    `SELECT balance FROM "${QLDB_TABLE_NAME}" WHERE accountId = ?`,
    accountId,
  );
  const firstDoc = res.getResultList()[0];

  if (firstDoc) {
    returnMessage.accountId = accountId;
    returnMessage.balance = firstDoc.get("balance")?.stringValue();
  } else {
    return returnError(`Account ${accountId} not found`, 400);
  }

  return {
    statusCode: 200,
    body: JSON.stringify({ ...returnMessage, status: "Ok" }),
    isBase64Encoded: false,
  };
};

export const handler: APIGatewayProxyHandler = async (event) => {
  console.debug(`Event received: ${JSON.stringify(event)}`);
  const accountId = event.pathParameters?.["accountId"];

  if (accountId) {
    try {
      const obj = await qldbDriver.executeLambda(
        (executor: TransactionExecutor) => queryBalance(accountId, executor),
      );
      return obj;
    } catch (error: any) {
      return returnError(error.message, 500);
    }
  } else {
    return returnError("accountId not specified", 400);
  }
};
