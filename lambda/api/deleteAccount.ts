import { TransactionExecutor } from "amazon-qldb-driver-nodejs";
import type { APIGatewayProxyHandler } from "aws-lambda";
import type { dom } from "ion-js";
import { initQldbDriver, returnError, returnResponse } from "../util/util";
import { config } from "../../config";

const { QLDB_TABLE_NAME } = config;

// Initialize the driver
const qldbDriver = initQldbDriver();

const deleteAccount = async (
  accountId: string,
  executor: TransactionExecutor,
) => {
  console.info(`Verifying account with id ${accountId} exists`);
  const res = await executor.execute(
    `SELECT accountId FROM "${QLDB_TABLE_NAME}"
    WHERE accountId = ?`,
    accountId,
  );

  const firstRecord: dom.Value = res.getResultList()[0];

  if (!firstRecord) {
    return returnError(`Account ${accountId} doesn't exist`, 400);
  } else {
    console.log(`Deleting account ${accountId}`);
    await executor.execute(
      `DELETE FROM "${QLDB_TABLE_NAME}"
      WHERE accountId = ?`,
      accountId,
    );
  }

  return returnResponse({ accountId });
};

export const handler: APIGatewayProxyHandler = async (event) => {
  console.debug(`Event received: ${JSON.stringify(event)}`);
  let body: any = {};

  try {
    body = JSON.parse(event.body || "{}");
  } catch (error: any) {
    return returnError(error.message, 400);
  }

  if (body.accountId) {
    try {
      const res = await qldbDriver.executeLambda((executor) =>
        deleteAccount(body.accountId, executor),
      );
      return res;
    } catch (error: any) {
      return returnError(error.message, 500);
    }
  } else {
    return returnError("accountId not specified", 400);
  }
};
