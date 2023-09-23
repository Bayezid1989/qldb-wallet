import { DynamoDBClient, QueryCommand } from "@aws-sdk/client-dynamodb";
import { marshall } from "@aws-sdk/util-dynamodb";
import type { APIGatewayProxyHandler } from "aws-lambda";
import { returnError } from "../utils";

const client = new DynamoDBClient();
const TABLE_NAME = process.env.DDB_TABLE_NAME || "";

// Ref: DynamoDB Query: https://docs.aws.amazon.com/ja_jp/amazondynamodb/latest/developerguide/GettingStarted.Query.html
const queryTransactions = async (accountId: string) => {
  console.info(`Querying DynamoDB for account with id ${accountId}`);
  const command = new QueryCommand({
    TableName: TABLE_NAME,
    KeyConditionExpression: "accountId = :accountId",
    ExpressionAttributeValues: marshall({ ":accountId": accountId }),
  });
  const response = await client.send(command);

  const returnMessage = {
    Transactions: response.Items,
  };

  return {
    statusCode: 200,
    body: JSON.stringify({ ...returnMessage, status: "Ok" }),
    isBase64Encoded: false,
  };
};

export const lambdaHandler: APIGatewayProxyHandler = async (event) => {
  console.debug(`Event received: ${JSON.stringify(event)}`);
  const accountId = event.pathParameters?.["id"];

  if (accountId) {
    try {
      const obj = await queryTransactions(accountId);
      return obj;
    } catch (error: any) {
      return returnError(error.message, 500);
    }
  } else {
    return returnError("accountId not specified", 400);
  }
};
