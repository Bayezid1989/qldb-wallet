import { TransactionExecutor } from "amazon-qldb-driver-nodejs";
import type { APIGatewayProxyEvent } from "aws-lambda";
import type { dom } from "ion-js";
import { initQldbDriver, returnError } from "../util";
import type { ReturnObj } from "../util/types";

const QLDB_TABLE_NAME = process.env.QLDB_TABLE_NAME || "";

// Initialize the driver
const qldbDriver = initQldbDriver();

const createAccount = async (
  accountId: string,
  executor: TransactionExecutor,
): Promise<ReturnObj> => {
  const returnMessage: any = {};

  console.info(`Verifying account with id ${accountId} does not exist`);
  let res = await executor.execute(
    `SELECT * FROM "${QLDB_TABLE_NAME}" WHERE accountId = ? `,
    accountId,
  );

  let firstDoc: dom.Value = res.getResultList()[0];

  if (firstDoc) {
    return returnError(`Account with user id ${accountId} already exists`, 400);
  } else {
    const doc = { accountId, balance: 0 };
    console.log(
      `Creating account with id ${accountId} and balance = ${doc.balance}`,
    );
    await executor.execute(`INSERT INTO "${QLDB_TABLE_NAME}" ?`, doc);
  }

  returnMessage.accountId = accountId;
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
          createAccount(body.accountId, executor),
      );
      return obj;
    } catch (error: any) {
      return returnError(error.message, 500);
    }
  } else {
    return returnError("accountId not specified", 400);
  }
};
