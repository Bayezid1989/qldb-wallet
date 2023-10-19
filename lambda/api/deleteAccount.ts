import { TransactionExecutor } from "amazon-qldb-driver-nodejs";
import type { APIGatewayProxyHandler } from "aws-lambda";
import {
  getValidRecord,
  initQldbDriver,
  returnError,
  returnResponse,
} from "../util/util";
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
    `SELECT accountId, deletedAt
    FROM "${QLDB_TABLE_NAME}"
    WHERE accountId = ?`,
    accountId,
  );
  const record = getValidRecord(res, accountId);
  if ("statusCode" in record) return record; // Error object

  console.log(`Deleting account ${accountId}`);

  const deletedAt = new Date();
  await executor.execute(
    `UPDATE "${QLDB_TABLE_NAME}"
    SET deletedAt = ?, lastTx = ?
    WHERE accountId = ?`,
    deletedAt,
    null, // Set null otherwise transaction will be created
    accountId,
  );

  return returnResponse({ accountId, deletedAt });
};

export const handler: APIGatewayProxyHandler = async (event) => {
  console.debug(`Event received: ${JSON.stringify(event)}`);
  let body: any = {};

  try {
    body = JSON.parse(event.body || "{}");
  } catch (error: any) {
    return returnError(error.message, 400);
  }

  if (typeof body.accountId !== "string") {
    return returnError("accountId not specified or invalid", 400);
  }

  try {
    const res = await qldbDriver.executeLambda((executor) =>
      deleteAccount(body.accountId, executor),
    );
    return res;
  } catch (error: any) {
    return returnError(error.message, 500);
  }
};
