import { TransactionExecutor } from "amazon-qldb-driver-nodejs";
import type { APIGatewayProxyHandler } from "aws-lambda";
import type { dom } from "ion-js";
import { initQldbDriver, returnError, returnResponse } from "../utils";

const QLDB_TABLE_NAME = process.env.QLDB_TABLE_NAME || "";

// Initialize the driver
const qldbDriver = initQldbDriver();

const transferFunds = async (
  fromAccountId: string,
  toAccountId: string,
  amount: number,
  executor: TransactionExecutor,
) => {
  const returnBody: Record<string, any> = {};
  const idsString = `(From: ${fromAccountId}, To: ${toAccountId})`;
  console.info(`Retrieving accounts ${idsString}`);
  const res = await executor.execute(
    `SELECT * FROM "${QLDB_TABLE_NAME}" WHERE accountId IN (?, ?)`,
    fromAccountId,
    toAccountId,
  );

  const records: dom.Value[] = res.getResultList();
  if (!records.length) {
    return returnError(`Both accounts are found. ${idsString}`, 400);
  }
  if (records.length > 2) {
    return returnError(`More than 2 accounts for ids${idsString}`, 500);
  }
  const fromAccount = records.find(
    (doc) => doc.get("accountId")?.stringValue() === fromAccountId,
  );
  const toAccount = records.find(
    (doc) => doc.get("accountId")?.stringValue() === toAccountId,
  );

  if (!fromAccount) {
    return returnError(`From account ${fromAccountId} not found.`, 400);
  }
  if (!toAccount) {
    return returnError(`To account ${toAccountId} not found.`, 400);
  }
  const fromBalance = fromAccount.get("balance")?.numberValue() || 0;
  if (fromBalance - amount < 0) {
    return returnError(
      `Funds too low. Cannot deduct ${amount} from account ${fromAccountId}`,
      400,
    );
  }

  console.info(`Updating balance with ${amount} for ${idsString}`);
  returnBody.fromAccountId = fromAccountId;
  returnBody.toAccountId = toAccountId;
  returnBody.transferAmount = amount;

  // Deduct the amount from account
  await executor.execute(
    `UPDATE "${QLDB_TABLE_NAME}" SET balance = balance - ? WHERE accountId = ?`,
    amount,
    fromAccountId,
  );

  // Add the amount to account
  await executor.execute(
    `UPDATE "${QLDB_TABLE_NAME}" SET balance = balance + ? WHERE accountId = ?`,
    amount,
    toAccountId,
  );

  return returnResponse(returnBody);
};

export const handler: APIGatewayProxyHandler = async (event) => {
  console.debug(`Event received: ${JSON.stringify(event)}`);
  let body: any = {};

  try {
    body = JSON.parse(event.body || "{}");
  } catch (error: any) {
    return returnError(error.message, 400);
  }

  if (body.fromAccountId && body.toAccountId && body.amount > 0) {
    try {
      const res = await qldbDriver.executeLambda((executor) =>
        transferFunds(
          body.fromAccountId,
          body.toAccountId,
          body.amount,
          executor,
        ),
      );
      return res;
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
