import { TransactionExecutor } from "amazon-qldb-driver-nodejs";
import type { APIGatewayProxyHandler } from "aws-lambda";
import {
  checkAvailableBalance,
  daysToSeconds,
  initQldbDriver,
  returnError,
  returnResponse,
} from "../util/util";
import { TX_STATUS, TX_TYPE } from "../util/constant";
import {
  DynamoDBClient,
  PutItemCommand,
  QueryCommand,
} from "@aws-sdk/client-dynamodb";
import { marshall } from "@aws-sdk/util-dynamodb";

// Initialize the driver
const qldbDriver = initQldbDriver(process.env.LEDGER_NAME || "");

const client = new DynamoDBClient();
const TABLE_NAME = process.env.DDB_TX_REQUEST_TABLE_NAME || "";
const EXPIRE_AFTER_DAYS = process.env.EXPIRE_AFTER_DAYS;

const createWithdrawlRequest = async (
  accountId: string,
  amount: number,
  type = TX_TYPE.WITHDRAW,
  requestId: string,
  executor: TransactionExecutor,
) => {
  const balance = await checkAvailableBalance(accountId, requestId, executor);
  if (typeof balance !== "number") return balance;
  if (balance - amount < 0) {
    return returnError(
      `Funds too low. Cannot deduct ${amount} from account ${accountId}`,
      400,
    );
  }

  const command = new QueryCommand({
    TableName: TABLE_NAME,
    KeyConditionExpression: "accountId = :accountId AND requestId = :requestId",
    ExpressionAttributeValues: marshall({
      ":accountId": accountId,
      ":requestId": requestId,
    }),
  });
  const res = await client.send(command);
  if (res.Count) {
    return returnError(
      `Transaction Request ${requestId} is already requested`,
      400,
    );
  }

  console.info(`Creating transaction request for account ${accountId}`);

  const date = new Date().toISOString();
  const ddbItem = {
    accountId,
    amount,
    from: accountId,
    to: null,
    type,
    requestId,
    status: TX_STATUS.REQUESTED,
    txId: null,
    createdAt: date,
    updatedAt: date,
    expireTimestamp: null, // Date.now() + daysToSeconds(Number(EXPIRE_AFTER_DAYS)),
  };
  const putCommand = new PutItemCommand({
    TableName: TABLE_NAME,
    Item: marshall(ddbItem),
  });

  await client.send(putCommand);

  return returnResponse(ddbItem);
};

export const handler: APIGatewayProxyHandler = async (event) => {
  console.debug(`Event received: ${JSON.stringify(event)}`);
  let body: any = {};

  try {
    body = JSON.parse(event.body || "{}");
  } catch (error: any) {
    return returnError(error.message, 400);
  }

  if (body.accountId && body.requestId && body.amount > 0) {
    try {
      const res = await qldbDriver.executeLambda(
        (executor: TransactionExecutor) =>
          createWithdrawlRequest(
            body.accountId,
            body.amount,
            body.type,
            body.requestId,
            executor,
          ),
      );
      return res;
    } catch (error: any) {
      return returnError(error.message, 500);
    }
  } else {
    return returnError(
      "accountId, amount or requestId not specified, or amount is less than zero",
      400,
    );
  }
};
