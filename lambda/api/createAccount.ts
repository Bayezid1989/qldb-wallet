import { TransactionExecutor } from "amazon-qldb-driver-nodejs";
import type { APIGatewayProxyHandler } from "aws-lambda";
import type { dom } from "ion-js";
import { initQldbDriver, returnError, returnResponse } from "../util/util";
import { config } from "../../config";

const { QLDB_TABLE_NAME } = config;

// Initialize the driver
const qldbDriver = initQldbDriver();

const createAccount = async (
  accountId: string,
  executor: TransactionExecutor,
) => {
  console.info(`Verifying account with id ${accountId} does not exist`);
  const res = await executor.execute(
    `SELECT accountId FROM "${QLDB_TABLE_NAME}"
    WHERE accountId = ?`,
    accountId,
  );

  const firstRecord: dom.Value = res.getResultList()[0];

  if (firstRecord) {
    return returnError(`Account ${accountId} already exists`, 400);
  }

  console.log(`Creating account with id ${accountId}`);
  const createdAt = new Date();
  await executor.execute(`INSERT INTO "${QLDB_TABLE_NAME}" ?`, {
    accountId,
    balance: 0,
    lastTx: null,
    pendingTxs: [],
    createdAt,
  });
  return returnResponse({ accountId, createdAt });
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
      createAccount(body.accountId, executor),
    );
    return res;
  } catch (error: any) {
    return returnError(error.message, 500);
  }
};
