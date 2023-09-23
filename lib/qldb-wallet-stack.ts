import {
  Stack,
  StackProps,
  RemovalPolicy,
  CfnOutput,
  aws_qldb as qldb,
  aws_lambda as lambda,
  aws_lambda_event_sources as lambdaEventSources,
  aws_iam as iam,
  aws_dynamodb as dynamodb,
  aws_kinesis as kinesis,
  aws_apigateway as apigw,
} from "aws-cdk-lib";
import {
  NodejsFunction,
  NodejsFunctionProps,
} from "aws-cdk-lib/aws-lambda-nodejs";
import { Construct } from "constructs";
import { config } from "../config";

const LEDGER_NAME = config.ledgerName;
const ACCOUNT = process.env.CDK_DEFAULT_ACCOUNT;
const REGION = process.env.CDK_DEFAULT_REGION;
const LOG_LEVEL = config.logLevel;
const QLDB_TABLE_NAME = config.qldbTableName;
const LOG_RETENTION = config.logRetention;
const SHARD_COUNT = config.shardCount;
const TTL_ATTRIBUTE = config.ttlAttribute;
const EXPIRE_AFTER_DAYS = config.expireAfterDays;

export class QldbWalletStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // Create QLDB Ledger and IAM Roles
    const qldbLedger = new qldb.CfnLedger(this, "wallet-ledger", {
      permissionsMode: "ALLOW_ALL",
      name: LEDGER_NAME,
    });

    // Create Kinesis Stream
    const kinesisStream = new kinesis.Stream(this, "wallet-kinesis-stream", {
      shardCount: SHARD_COUNT,
      streamName: `kinesis-stream-${LEDGER_NAME}`,
    });

    // Grant QLDB access to the Kinesis stream
    const qldbStreamInlinePolicyStatement = new iam.PolicyStatement({
      actions: [
        "kinesis:DescribeStream",
        "kinesis:PutRecord",
        "kinesis:PutRecords",
        "kinesis:ListShards",
        "kinesis:ListShardIterators",
      ],
      effect: iam.Effect.ALLOW,
      resources: [kinesisStream.streamArn],
    });
    const qldbStreamInlinePolicyDocument = new iam.PolicyDocument({
      statements: [qldbStreamInlinePolicyStatement],
    });
    const qldbStreamRole = new iam.Role(this, "qldb-stream-role", {
      // Ref: ServicePrincipal: https://docs.aws.amazon.com/cdk/api/v2/docs/aws-cdk-lib.aws_iam.ServicePrincipal.html
      assumedBy: new iam.ServicePrincipal("qldb.amazonaws.com"),
      inlinePolicies: {
        qldb_inline_policy: qldbStreamInlinePolicyDocument,
      },
    });

    // Create the QLDB stream
    const qldbStream = new qldb.CfnStream(this, "wallet-qldb-stream", {
      ledgerName: LEDGER_NAME,
      streamName: `qldb-stream-${LEDGER_NAME}`,
      inclusiveStartTime: "2019-06-13T21:36:34Z",
      kinesisConfiguration: {
        aggregationEnabled: false,
        streamArn: kinesisStream.streamArn,
      },
      roleArn: qldbStreamRole.roleArn,
    });

    // DynamoDB Table definition
    const ddbTable = new dynamodb.Table(this, "ddb-transactions-table", {
      tableName: `wallet-transactions-${LEDGER_NAME}`,
      partitionKey: { name: "accountId", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      sortKey: { name: "txTime", type: dynamodb.AttributeType.STRING },
      removalPolicy: RemovalPolicy.DESTROY,
      timeToLiveAttribute: TTL_ATTRIBUTE,
    });

    // Create IAM Roles and policies for Lambda functions
    const qldbAccessPolicy = new iam.PolicyStatement({
      actions: ["qldb:SendCommand"],
      effect: iam.Effect.ALLOW,
      resources: [`arn:aws:qldb:${REGION}:${ACCOUNT}:ledger/${LEDGER_NAME}`],
    });

    const ddbTablePolicy = new iam.PolicyStatement({
      actions: ["dynamodb:Query", "dynamodb:PutItem"],
      effect: iam.Effect.ALLOW,
      resources: [ddbTable.tableArn],
    });

    // Create Lambda role and policies
    const lambdaQldbRole = new iam.Role(this, "lambda-qldb-role", {
      assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
    });
    lambdaQldbRole.addToPolicy(qldbAccessPolicy);
    lambdaQldbRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName("AWSLambdaExecute"),
    );

    const lambdaDdbRole = new iam.Role(this, "lambda-ddb-role", {
      assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
    });
    lambdaDdbRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName("AWSLambdaExecute"),
    );
    lambdaDdbRole.addToPolicy(ddbTablePolicy);

    // Create Lambda functions
    // Ref: https://github.com/aws-samples/aws-cdk-examples/blob/master/typescript/api-cors-lambda-crud-dynamodb/index.ts
    const nodeJsFunctionProps: NodejsFunctionProps = {
      runtime: lambda.Runtime.NODEJS_18_X,
      role: lambdaQldbRole,
      logRetention: LOG_RETENTION,
      memorySize: 512,
      tracing: lambda.Tracing.ACTIVE,
    };

    const lambdaGetBalance = new NodejsFunction(this, "get-balance-lambda", {
      entry: "lambda/api/getBalance.ts",
      ...nodeJsFunctionProps,
    });

    const lambdaWithdrawFunds = new NodejsFunction(
      this,
      "withdraw-funds-lambda",
      { entry: "lambda/api/withdrawFunds.ts", ...nodeJsFunctionProps },
    );

    const lambdaAddFunds = new NodejsFunction(this, "add-funds-lambda", {
      entry: "lambda/api/addFunds.ts",
      ...nodeJsFunctionProps,
    });

    const lambdaCreateAccount = new NodejsFunction(
      this,
      "create-account-lambda",
      { entry: "lambda/api/createAccount.ts", ...nodeJsFunctionProps },
    );

    const lambdaGetTransactions = new NodejsFunction(
      this,
      "get-transactions-lambda",
      { entry: "lambda/api/getTransactions.ts", ...nodeJsFunctionProps },
    );

    const lambdaStreamTransactions = new NodejsFunction(
      this,
      "stream-transactions-lambda",
      { entry: "lambda/streamTransactions.ts", ...nodeJsFunctionProps },
    );

    // Associate the Kinesis stream to lambda_stream_transactions as an event source
    const eventSource = new lambdaEventSources.KinesisEventSource(
      kinesisStream,
      {
        startingPosition: lambda.StartingPosition.TRIM_HORIZON,
        bisectBatchOnError: true,
      },
    );
    lambdaStreamTransactions.addEventSource(eventSource);

    // Add environment variables to Lambda functions
    const lambdas = [
      lambdaCreateAccount,
      lambdaGetBalance,
      lambdaWithdrawFunds,
      lambdaAddFunds,
      lambdaGetTransactions,
      lambdaStreamTransactions,
    ];
    for (const lmbd of lambdas) {
      lmbd.addEnvironment("LEDGER_NAME", LEDGER_NAME);
      lmbd.addEnvironment("QLDB_TABLE_NAME", QLDB_TABLE_NAME);
      lmbd.addEnvironment("LOG_LEVEL", LOG_LEVEL);
    }

    const ddbTableName = `wallet-transactions-${LEDGER_NAME}`;
    lambdaGetTransactions.addEnvironment("DDB_TABLE_NAME", ddbTableName);
    lambdaStreamTransactions.addEnvironment("DDB_TABLE_NAME", ddbTableName);

    if (TTL_ATTRIBUTE && EXPIRE_AFTER_DAYS) {
      lambdaStreamTransactions.addEnvironment("TTL_ATTRIBUTE", TTL_ATTRIBUTE);
      lambdaStreamTransactions.addEnvironment(
        "EXPIRE_AFTER_DAYS",
        String(EXPIRE_AFTER_DAYS),
      );
    }

    // Create APIs in API Gateway

    // Single API with some resources(endpoints)
    // Ref: https://qiita.com/misaosyushi/items/104445be7d7d3ba304bc
    const api = new apigw.RestApi(this, "wallet-api", {
      restApiName: "QLDB Wallet API",
      description: "Lambda functions for QLDB wallet",
      apiKeySourceType: apigw.ApiKeySourceType.HEADER,
      defaultCorsPreflightOptions: {
        allowOrigins: apigw.Cors.ALL_ORIGINS,
        allowMethods: apigw.Cors.ALL_METHODS,
        allowHeaders: apigw.Cors.DEFAULT_HEADERS,
        statusCode: 200,
      },
      endpointTypes: [apigw.EndpointType.EDGE],
      defaultMethodOptions: {
        authorizationType: apigw.AuthorizationType.NONE, // Needs to be properly authorized in PRD
      },
    });
    const apiKey = api.addApiKey("api-key", { apiKeyName: "wallet-api-key" });

    const getBalanceRsc = api.root
      .addResource("getBalance")
      .addResource("{accountId}");
    getBalanceRsc.addMethod(
      "GET",
      new apigw.LambdaIntegration(lambdaGetBalance),
      {
        apiKeyRequired: true,
      },
    );

    const createAccountRsc = api.root.addResource("createAccount");
    createAccountRsc.addMethod(
      "POST",
      new apigw.LambdaIntegration(lambdaCreateAccount),
      { apiKeyRequired: true },
    );

    const withdrawFundsRsc = api.root.addResource("withdrawFunds");
    withdrawFundsRsc.addMethod(
      "POST",
      new apigw.LambdaIntegration(lambdaWithdrawFunds),
      { apiKeyRequired: true },
    );

    const addFundsRsc = api.root.addResource("addFunds");
    addFundsRsc.addMethod("POST", new apigw.LambdaIntegration(lambdaAddFunds), {
      apiKeyRequired: true,
    });

    const getTransactionsRsc = api.root
      .addResource("getTransactions")
      .addResource("{accountId}");
    getTransactionsRsc.addMethod(
      "GET",
      new apigw.LambdaIntegration(lambdaGetTransactions),
      { apiKeyRequired: true },
    );

    // ORIGINAL: Separate API, using IAM authorization
    // const lambdaRestApiProps: Omit<apigw.LambdaRestApiProps, "handler"> = {
    //   endpointTypes: [apigw.EndpointType.REGIONAL],
    //   defaultMethodOptions: {
    //     authorizationType: apigw.AuthorizationType.IAM,
    //   },
    // };
    // const getBalanceApi = new apigw.LambdaRestApi(this, "get-balance-api", {
    //   handler: lambdaGetBalance,
    //   ...lambdaRestApiProps,
    // });

    // const createAccountApi = new apigw.LambdaRestApi(
    //   this,
    //   "create-account-api",
    //   {
    //     handler: lambdaCreateAccount,
    //     ...lambdaRestApiProps,
    //   },
    // );

    // const withdrawFundsApi = new apigw.LambdaRestApi(
    //   this,
    //   "withdraw-funds-api",
    //   {
    //     handler: lambdaWithdrawFunds,
    //     ...lambdaRestApiProps,
    //   },
    // );

    // const addFundsApi = new apigw.LambdaRestApi(this, "add-funds-api", {
    //   handler: lambdaAddFunds,
    //   ...lambdaRestApiProps,
    // });

    // const getTransactionsApi = new apigw.LambdaRestApi(
    //   this,
    //   "get-transactions-api",
    //   {
    //     handler: lambdaGetTransactions,
    //     ...lambdaRestApiProps,
    //   },
    // );

    const output1 = `Execute the following queries in QLDB query editor for ledger ${LEDGER_NAME} before using:`;
    const output2 = `CREATE TABLE "${QLDB_TABLE_NAME}"`;
    const output3 = `CREATE INDEX ON "${QLDB_TABLE_NAME}" (accountId)`;
    const output4 = `API Key ID: ${apiKey.keyId}, ARN: ${apiKey.keyArn}`;

    new CfnOutput(this, "stack-output1", { value: output1 });
    new CfnOutput(this, "stack-output2", { value: output2 });
    new CfnOutput(this, "stack-output3", { value: output3 });
    new CfnOutput(this, "stack-output4", { value: output4 });
  }
}
