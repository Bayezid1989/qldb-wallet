import {
  Stack,
  StackProps,
  RemovalPolicy,
  CfnOutput,
  aws_qldb as qldb,
  aws_lambda as lambda,
  aws_lambda_nodejs as lambdaNodeJs,
  aws_lambda_event_sources as lambdaEventSources,
  aws_iam as iam,
  aws_dynamodb as dynamodb,
  aws_kinesis as kinesis,
  aws_apigateway as apigw,
} from "aws-cdk-lib";
import { Construct } from "constructs";
import { config } from "../config";

const {
  LEDGER_NAME,
  LOG_RETENTION,
  QLDB_TABLE_NAME,
  DDB_TABLE_NAME,
  SHARD_COUNT,
} = config;

const ACCOUNT = process.env.CDK_DEFAULT_ACCOUNT;
const REGION = process.env.CDK_DEFAULT_REGION;

export class QldbWalletStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // Create QLDB Ledger and IAM Roles
    new qldb.CfnLedger(this, "wallet-ledger", {
      permissionsMode: "ALLOW_ALL", // Not recommended
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
    new qldb.CfnStream(this, "wallet-qldb-stream", {
      ledgerName: LEDGER_NAME,
      streamName: `qldb-stream-${LEDGER_NAME}`,
      inclusiveStartTime: "2019-06-13T21:36:34Z",
      kinesisConfiguration: {
        aggregationEnabled: false,
        streamArn: kinesisStream.streamArn,
      },
      roleArn: qldbStreamRole.roleArn,
    });

    // DynamoDB Transaction Table definition
    const ddbTxTable = new dynamodb.Table(this, "ddb-transactions-table", {
      tableName: DDB_TABLE_NAME,
      partitionKey: { name: "accountId", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      sortKey: {
        name: "requestTxTimestamp",
        type: dynamodb.AttributeType.STRING,
      },
      removalPolicy: RemovalPolicy.DESTROY,
      // timeToLiveAttribute: "expire_timestamp",
    });

    // Uncomment out if you use pending transactions and query order by txTime, this must be created when creating table
    // ddbTxTable.addLocalSecondaryIndex({
    //   indexName: "txTime-index",
    //   sortKey: {
    //     name: "txTime",
    //     type: dynamodb.AttributeType.STRING,
    //   },
    // });

    // Create IAM Roles and policies for Lambda functions
    const qldbAccessPolicy = new iam.PolicyStatement({
      actions: ["qldb:SendCommand"],
      effect: iam.Effect.ALLOW,
      resources: [`arn:aws:qldb:${REGION}:${ACCOUNT}:ledger/${LEDGER_NAME}`],
    });

    const ddbTablePolicy = new iam.PolicyStatement({
      actions: ["dynamodb:Query", "dynamodb:PutItem"],
      effect: iam.Effect.ALLOW,
      resources: [ddbTxTable.tableArn],
      // resources: [ddbTxTable.tableArn, ddbTxReqTable.tableArn],
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
    const nodeJsFunctionProps: lambdaNodeJs.NodejsFunctionProps = {
      runtime: lambda.Runtime.NODEJS_18_X,
      logRetention: LOG_RETENTION,
      memorySize: 512,
      tracing: lambda.Tracing.ACTIVE,
      handler: "handler",
    };

    const lambdaCreateAccount = new lambdaNodeJs.NodejsFunction(
      this,
      "create-account-lambda",
      {
        entry: "lambda/api/createAccount.ts",
        role: lambdaQldbRole,
        ...nodeJsFunctionProps,
      },
    );

    const lambdaDeleteAccount = new lambdaNodeJs.NodejsFunction(
      this,
      "delete-account-lambda",
      {
        entry: "lambda/api/deleteAccount.ts",
        role: lambdaQldbRole,
        ...nodeJsFunctionProps,
      },
    );

    const lambdaGetBalance = new lambdaNodeJs.NodejsFunction(
      this,
      "get-balance-lambda",
      {
        entry: "lambda/api/getBalance.ts",
        role: lambdaQldbRole,
        ...nodeJsFunctionProps,
      },
    );

    const lambdaUpdateBalance = new lambdaNodeJs.NodejsFunction(
      this,
      "update-balance-lambda",
      {
        entry: "lambda/api/updateBalance.ts",
        role: lambdaQldbRole,
        ...nodeJsFunctionProps,
      },
    );

    const lambdaTransferFunds = new lambdaNodeJs.NodejsFunction(
      this,
      "transfer-funds-lambda",
      {
        entry: "lambda/api/transferFunds.ts",
        role: lambdaQldbRole,
        ...nodeJsFunctionProps,
      },
    );

    const lambdaAddTransaction = new lambdaNodeJs.NodejsFunction(
      this,
      "add-transaction-lambda",
      {
        entry: "lambda/api/addTransaction.ts",
        role: lambdaQldbRole,
        ...nodeJsFunctionProps,
      },
    );

    const lambdaCloseTransaction = new lambdaNodeJs.NodejsFunction(
      this,
      "close-transaction-lambda",
      {
        entry: "lambda/api/closeTransaction.ts",
        role: lambdaQldbRole,
        ...nodeJsFunctionProps,
      },
    );

    const lambdaGetTransactions = new lambdaNodeJs.NodejsFunction(
      this,
      "get-transactions-lambda",
      {
        entry: "lambda/api/getTransactions.ts",
        role: lambdaDdbRole,
        ...nodeJsFunctionProps,
      },
    );

    const lambdaStreamTransactions = new lambdaNodeJs.NodejsFunction(
      this,
      "stream-transactions-lambda",
      {
        entry: "lambda/streamTransactions.ts",
        role: lambdaDdbRole,
        ...nodeJsFunctionProps,
      },
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

    // Create APIs in API Gateway

    // Single API with some resources(endpoints)
    // Ref: https://qiita.com/misaosyushi/items/104445be7d7d3ba304bc, https://maku.blog/p/k7eoer5/
    // TODO: Properly authorize in PRD: API key, Cofgnito, IAM, etc.
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
      endpointTypes: [apigw.EndpointType.REGIONAL],
      defaultMethodOptions: {
        authorizationType: apigw.AuthorizationType.NONE, // TODO: Change this
      },
    });

    const createAccountRsc = api.root.addResource("createAccount");
    createAccountRsc.addMethod(
      "POST",
      new apigw.LambdaIntegration(lambdaCreateAccount),
    );

    const createDeleteRsc = api.root.addResource("deleteAccount");
    createDeleteRsc.addMethod(
      "POST",
      new apigw.LambdaIntegration(lambdaDeleteAccount),
    );

    const getBalanceRsc = api.root
      .addResource("getBalance")
      .addResource("{accountId}");
    getBalanceRsc.addMethod(
      "GET",
      new apigw.LambdaIntegration(lambdaGetBalance),
    );

    const updateBalanceRsc = api.root.addResource("updateBalance");
    updateBalanceRsc.addMethod(
      "POST",
      new apigw.LambdaIntegration(lambdaUpdateBalance),
    );

    const transferFundsRsc = api.root.addResource("transferFunds");
    transferFundsRsc.addMethod(
      "POST",
      new apigw.LambdaIntegration(lambdaTransferFunds),
    );

    const addTransactionRsc = api.root.addResource("addTransaction");
    addTransactionRsc.addMethod(
      "POST",
      new apigw.LambdaIntegration(lambdaAddTransaction),
    );

    const closeTransactionRsc = api.root.addResource("closeTransaction");
    closeTransactionRsc.addMethod(
      "POST",
      new apigw.LambdaIntegration(lambdaCloseTransaction),
    );

    const getTransactionsRsc = api.root
      .addResource("getTransactions")
      .addResource("{accountId}");
    getTransactionsRsc.addMethod(
      "GET",
      new apigw.LambdaIntegration(lambdaGetTransactions),
    );

    const output1 = `Execute the following queries in QLDB query editor for ledger ${LEDGER_NAME} before using:`;
    const output2 = `CREATE TABLE "${QLDB_TABLE_NAME}"`;
    const output3 = `CREATE INDEX ON "${QLDB_TABLE_NAME}" (accountId)`;

    new CfnOutput(this, "stack-output1", { value: output1 });
    new CfnOutput(this, "stack-output2", { value: output2 });
    new CfnOutput(this, "stack-output3", { value: output3 });
  }
}
