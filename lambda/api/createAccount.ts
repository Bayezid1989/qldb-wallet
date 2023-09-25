import { TransactionExecutor } from "amazon-qldb-driver-nodejs";
import type { APIGatewayProxyHandler } from "aws-lambda";
import type { dom } from "ion-js";
import { initQldbDriver, returnError, returnResponse } from "../util/util";

const QLDB_TABLE_NAME = process.env.QLDB_TABLE_NAME || "";

// Initialize the driver
const qldbDriver = initQldbDriver();

const createAccount = async (
  accountId: string,
  executor: TransactionExecutor,
) => {
  console.info(`Verifying account with id ${accountId} does not exist`);
  const res = await executor.execute(
    `SELECT accountId FROM "${QLDB_TABLE_NAME}" WHERE accountId = ? `,
    accountId,
  );

  const firstRecord: dom.Value = res.getResultList()[0];

  if (firstRecord) {
    return returnError(`Account with user id ${accountId} already exists`, 400);
  } else {
    const doc = {
      accountId,
      balance: 0,

      // Last transaction data
      txAmount: null,
      txFrom: null,
      txTo: null,
      txType: null,
      txRequestId: null,
    };
    console.log(
      `Creating account with id ${accountId} and balance = ${doc.balance}`,
    );
    await executor.execute(`INSERT INTO "${QLDB_TABLE_NAME}" ?`, doc);
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
        createAccount(body.accountId, executor),
      );
      return res;
    } catch (error: any) {
      return returnError(error.message, 500);
    }
  } else {
    return returnError("accountId not specified", 400);
  }
};
