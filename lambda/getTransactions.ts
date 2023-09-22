import { DynamoDBClient, QueryCommand } from "@aws-sdk/client-dynamodb";
import { marshall } from "@aws-sdk/util-dynamodb";
import type { Handler } from "aws-lambda";

const client = new DynamoDBClient();
const TABLE_NAME = process.env.DDB_TABLE_NAME || "";

let returnObject: {
  statusCode?: number;
  body?: string;
  isBase64Encoded?: boolean;
} = {};

const setError = (message: string, httpStatusCode: number = 500): void => {
  const returnMessage = { status: "error", message };
  returnObject = {
    statusCode: httpStatusCode,
    body: JSON.stringify(returnMessage),
    isBase64Encoded: false,
  };
  console.error(returnMessage);
};

// Ref: DynamoDB Query: https://docs.aws.amazon.com/ja_jp/amazondynamodb/latest/developerguide/GettingStarted.Query.html
const queryTransactions = async (accountId: string): Promise<void> => {
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

  returnObject = {
    statusCode: 200,
    body: JSON.stringify({ ...returnMessage, status: "Ok" }),
    isBase64Encoded: false,
  };
};

export const lambdaHandler: Handler = async (event, context) => {
  console.debug(`Event received: ${JSON.stringify(event)}`);
  let body: any = {};

  try {
    body = JSON.parse(event.body || "{}");
  } catch (error: any) {
    setError(error.message, 400);
  }

  if (body.accountId) {
    try {
      await queryTransactions(body.accountId);
    } catch (error: any) {
      setError(error.message, 500);
    }
  } else {
    setError("accountId not specified", 400);
  }

  return returnObject;
};
