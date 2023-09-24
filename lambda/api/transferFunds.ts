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
  const idsString = `accounts(From: ${fromAccountId}, To: ${toAccountId})`;
  console.info(`Retrieving number of accounts for ${idsString}`);
  const res1 = await executor.execute(
    `SELECT count(accountId) as numberOfAccounts FROM "${QLDB_TABLE_NAME}" WHERE accountId IN (?, ?)`,
    fromAccountId,
    toAccountId,
  );

  const firstDoc1: dom.Value = res1.getResultList()[0];

  if (firstDoc1) {
    const numOfAccounts = firstDoc1.get("numberOfAccounts")?.numberValue();
    if (numOfAccounts && numOfAccounts > 2) {
      return returnError(`Account count is more than 2 with ${idsString}`, 500);
    }
    if (!numOfAccounts || numOfAccounts < 2) {
      return returnError(
        `Either or both account(s) is(are) not found with ${idsString}`,
        400,
      );
    }
  }

  console.info(`Retrieving balance for UPDATE... for ${idsString}`);
  const res2 = await executor.execute(
    `SELECT balance FROM "${QLDB_TABLE_NAME}" WHERE accountId = ?`,
    fromAccountId,
  );

  const firstDoc2 = res2.getResultList()[0];
  const balance = firstDoc2.get("balance")?.numberValue() || 0;
  if (balance - amount < 0) {
    return returnError(
      `Funds too low. Cannot deduct ${amount} from account ${fromAccountId}`,
      400,
    );
  }

  console.info(`Updating balance with ${amount} for ${idsString}`);
  returnBody.fromAccountId = fromAccountId;
  returnBody.toAccountId = toAccountId;
  returnBody.transferAmount = amount;

  await executor.execute(
    `UPDATE "${QLDB_TABLE_NAME}"
    SET balance = CASE WHEN accountId = ? THEN balance - ? WHEN accountId = ? THEN balance + ? ELSE balance END
    WHERE accountId IN (?, ?)`,
    fromAccountId,
    amount,
    toAccountId,
    amount,
    fromAccountId,
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

  if (
    body.fromAccountId &&
    body.toAccountId &&
    body.amount &&
    body.amount > 0
  ) {
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
