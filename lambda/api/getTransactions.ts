import { DynamoDBClient, QueryCommand } from "@aws-sdk/client-dynamodb";
import { marshall } from "@aws-sdk/util-dynamodb";
import type { APIGatewayProxyHandler } from "aws-lambda";
import { returnError, returnResponse } from "../util/util";

const client = new DynamoDBClient();
const TX_TABLE_NAME = process.env.DDB_TX_TABLE_NAME || "";

// Ref: DynamoDB Query: https://docs.aws.amazon.com/ja_jp/amazondynamodb/latest/developerguide/GettingStarted.Query.html
const queryTransactions = async (
  accountId: string,
  afterDate?: string, // ISO 8601
  beforeDate?: string, // ISO 8601
) => {
  console.info(
    `Querying transactions for account ${accountId} with afterDate: ${
      afterDate || "undefiend"
    } and beforeDate: ${beforeDate || "undefiend"}`,
  );

  let condition = "accountId = :accountId";
  const attributeValues: Record<string, string> = {
    ":accountId": accountId,
  };
  if (afterDate && beforeDate) {
    condition += " AND txTime BETWEEN :afterDate AND :beforeDate";
    attributeValues[`:afterDate`] = afterDate;
    attributeValues[`:beforeDate`] = beforeDate;
  } else if (afterDate) {
    condition += " AND txTime >= :afterDate";
    attributeValues[`:afterDate`] = afterDate;
  } else if (beforeDate) {
    condition += " AND txTime <= :beforeDate";
    attributeValues[`:beforeDate`] = beforeDate;
  }

  const command = new QueryCommand({
    TableName: TX_TABLE_NAME,
    KeyConditionExpression: condition,
    ExpressionAttributeValues: marshall(attributeValues),
  });
  const res = await client.send(command);

  return returnResponse({ transactions: res.Items });
};

export const handler: APIGatewayProxyHandler = async (event) => {
  console.debug(`Event received: ${JSON.stringify(event)}`);
  const accountId = event.pathParameters?.accountId;
  const afterDate = event.queryStringParameters?.afterDate;
  const beforeDate = event.queryStringParameters?.beforeDate;

  if (accountId) {
    try {
      const res = await queryTransactions(accountId, afterDate, beforeDate);
      return res;
    } catch (error: any) {
      return returnError(error.message, 500);
    }
  } else {
    return returnError("accountId not specified", 400);
  }
};
