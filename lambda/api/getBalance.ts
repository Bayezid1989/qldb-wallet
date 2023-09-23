import { TransactionExecutor } from "amazon-qldb-driver-nodejs";
import type { APIGatewayProxyEvent } from "aws-lambda";
import { initQldbDriver, returnError } from "../util";
import type { ReturnObj } from "../util/types";

const QLDB_TABLE_NAME = process.env.QLDB_TABLE_NAME || "";

// Initialize the driver
const qldbDriver = initQldbDriver();

const queryBalance = async (
  accountId: string,
  executor: TransactionExecutor,
): Promise<ReturnObj> => {
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

export const lambdaHandler = async (event: APIGatewayProxyEvent) => {
  console.debug(`Event received: ${JSON.stringify(event)}`);
  let body: any = {};

  try {
    body = JSON.parse(event.body || "{}");
  } catch (error: any) {
    return returnError(error.message, 400);
  }

  if (body.accountId) {
    try {
      const obj = await qldbDriver.executeLambda(
        (executor: TransactionExecutor) =>
          queryBalance(body.accountId, executor),
      );
      return obj;
    } catch (error: any) {
      return returnError(error.message, 500);
    }
  } else {
    return returnError("accountId not specified", 400);
  }
};
