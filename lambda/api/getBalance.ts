import { TransactionExecutor } from "amazon-qldb-driver-nodejs";
import type { APIGatewayProxyHandler } from "aws-lambda";
import { initQldbDriver, returnError, returnResponse } from "../utils";

const QLDB_TABLE_NAME = process.env.QLDB_TABLE_NAME || "";

// Initialize the driver
const qldbDriver = initQldbDriver();

const queryBalance = async (
  accountId: string,
  executor: TransactionExecutor,
) => {
  const returnBody: Record<string, any> = {};

  console.info(`Looking up balance for account with id ${accountId}`);
  const res = await executor.execute(
    `SELECT balance FROM "${QLDB_TABLE_NAME}" WHERE accountId = ?`,
    accountId,
  );
  const firstRecord = res.getResultList()[0];

  if (firstRecord) {
    returnBody.accountId = accountId;
    returnBody.balance = firstRecord.get("balance")?.numberValue();
  } else {
    return returnError(`Account ${accountId} not found`, 400);
  }

  return returnResponse(returnBody);
};

export const handler: APIGatewayProxyHandler = async (event) => {
  console.debug(`Event received: ${JSON.stringify(event)}`);
  const accountId = event.pathParameters?.["accountId"];

  if (accountId) {
    try {
      const res = await qldbDriver.executeLambda((executor) =>
        queryBalance(accountId, executor),
      );
      return res;
    } catch (error: any) {
      return returnError(error.message, 500);
    }
  } else {
    return returnError("accountId not specified", 400);
  }
};
