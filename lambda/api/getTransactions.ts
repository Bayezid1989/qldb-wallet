import { DynamoDBClient, QueryCommand } from "@aws-sdk/client-dynamodb";
import { marshall } from "@aws-sdk/util-dynamodb";
import type { APIGatewayProxyHandler } from "aws-lambda";
import { returnError, returnResponse } from "../util/util";
import { config } from "../../config";

const client = new DynamoDBClient();
const { DDB_TABLE_NAME } = config;

// Ref: DynamoDB Query: https://docs.aws.amazon.com/ja_jp/amazondynamodb/latest/developerguide/GettingStarted.Query.html
const queryTransactions = async (
  accountId: string,
  afterTime?: string, // timestamp
  beforeTime?: string, // timestamp
) => {
  console.info(
    `Querying transactions for account ${accountId} with afterTime: ${
      afterTime || "undefiend"
    } and beforeTime: ${beforeTime || "undefiend"}`,
  );

  let condition = "accountId = :accountId";
  const attributeValues: Record<string, string> = {
    ":accountId": accountId,
  };
  if (afterTime && beforeTime) {
    condition += " AND requestTime#txTime BETWEEN :afterTime AND :beforeTime";
    attributeValues[`:afterTime`] = afterTime;
    attributeValues[`:beforeTime`] = `${beforeTime}#9999999999999`;
  } else if (afterTime) {
    condition += " AND requestTime#txTime >= :afterTime";
    attributeValues[`:afterTime`] = afterTime;
  } else if (beforeTime) {
    condition += " AND requestTime#txTime <= :beforeTime";
    attributeValues[`:beforeTime`] = `${beforeTime}#9999999999999`;
  }

  const command = new QueryCommand({
    TableName: DDB_TABLE_NAME,
    KeyConditionExpression: condition,
    ExpressionAttributeValues: marshall(attributeValues),
  });
  const res = await client.send(command);

  return returnResponse({ transactions: res.Items });
};

export const handler: APIGatewayProxyHandler = async (event) => {
  console.debug(`Event received: ${JSON.stringify(event)}`);
  const accountId = event.pathParameters?.accountId;
  const afterTime = event.queryStringParameters?.afterTime;
  const beforeTime = event.queryStringParameters?.beforeTime;

  if (
    typeof accountId !== "string" ||
    (afterTime && isNaN(Number(afterTime))) ||
    (beforeTime && isNaN(Number(beforeTime)))
  ) {
    return returnError(
      "accountId, beforeTime or afterTime not specified or invalid",
      400,
    );
  }

  try {
    const res = await queryTransactions(accountId, afterTime, beforeTime);
    return res;
  } catch (error: any) {
    return returnError(error.message, 500);
  }
};
