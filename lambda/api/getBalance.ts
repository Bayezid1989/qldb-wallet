import { TransactionExecutor } from "amazon-qldb-driver-nodejs";
import type { APIGatewayProxyHandler } from "aws-lambda";
import {
  initQldbDriver,
  ionNumber,
  returnError,
  returnResponse,
} from "../util/util";

const QLDB_TABLE_NAME = process.env.QLDB_TABLE_NAME || "";

// Initialize the driver
const qldbDriver = initQldbDriver();

const queryBalance = async (
  accountId: string,
  executor: TransactionExecutor,
) => {
  console.info(`Looking up balance for account with id ${accountId}`);
  const res = await executor.execute(
    `SELECT balance FROM "${QLDB_TABLE_NAME}" WHERE accountId = ?`,
    accountId,
  );
  const firstRecord = res.getResultList()[0];

  if (firstRecord) {
    return returnResponse({
      accountId,
      balance: ionNumber(firstRecord, "balance"),
    });
  } else {
    return returnError(`Account ${accountId} not found`, 400);
  }
};

export const handler: APIGatewayProxyHandler = async (event) => {
  console.debug(`Event received: ${JSON.stringify(event)}`);
  const accountId = event.pathParameters?.accountId;

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
