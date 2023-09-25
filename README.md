# QLDB DynamoDB Serverless Wallet

This is NodeJs(Typescript) Digital Wallet Project based on [AWS serverless-wallet blog](https://aws.amazon.com/blogs/architecture/building-a-serverless-wallet-service-for-in-game-currency/)

## Architecture

This project will deploy digital wallet service using serverless technologies on AWS.
Including:

- REST API on API Gateway
- Lambda Functions
- QLDB Ledger
- QLDB Ledger stream and Kinesis Data Stream
- DynamoDB Table
- IAM roles

Please see the following [architecture diagram](readme-architecture.png)

## API Parameters

Get APIs require GET method with pathParameters, while others require POST method with body.
The **body** of the request must be a JSON object with the following attributes:

- /getBalance(GET): `/{accountId}`
- /getTransactions(GET): `/{accountId}?afterDate={ISO8601 string}&beforeDate={ISO8601 string}`
- /createAccount(POST): `{ "accountId": <string> }`
- /withdrawFunds(POST): `{ "accountId": <string>, "amount": <number>, txRequestId: <string> }`
- /addFunds(POST): `{ "accountId": <string>, "amount": <number>, txRequestId: <string> }`
- /transferFunds(POST): `{ "fromAccountId": <string>, "toAccountId": <string>, "amount": <number>, txRequestId: <string> }`

The `cdk.json` file tells the CDK Toolkit how to execute your app.

## Useful commands

- `npm run build` compile typescript to js
- `npm run watch` watch for changes and compile
- `npm run test` perform the jest unit tests
- `cdk deploy` deploy this stack to your default AWS account/region
- `cdk diff` compare deployed stack with current state
- `cdk synth` emits the synthesized CloudFormation template

## Process log to deploy CDK (with Macbook Pro Apple Silicon)

1. Follow [instruction to set up CDK](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html)

   - Install awscli via brew --> aws command failed --> Install six via brew
   - Install aws-cdk
   - `aws configure sso`
   - `cdk bootstrap aws://ACCOUNT-NUMBER/REGION` --> Error: "no credentials have been configured" --> Look into the hidden files ~/.aws/config and ~/.aws/credentials, then copy and paste the value from "Coomand line or programmatic access" popup SSO login screen. (**Credential often expires hence need to update ~/.aws/credentials everytime.**)

2. Folow [instruction to deploy CDK](https://docs.aws.amazon.com/cdk/v2/guide/hello_world.html)

   - Test deploy: `cdk init app --language typescript`, `npm run build`, `cdk ls`, `cdk synth`, `cdk deploy`
   - Add actual code based on [AWS Sample serverless-wallet (Python)](https://github.com/aws-samples/serverless-wallet)
   - `cdk deploy` --> Error: "Ledger is not yet active" --> Deploy only creating QLDB and DynamoDB portion first, then after confirming Ledger is active, deploy all.

## Post-deployment setup

1. Create the QLDB table. You may use the QLDB query editor on the Amazon QLDB Console to execute these queries. The table name must match the 'qldb_table_name' parameter in config.py:
   -- `CREATE TABLE "<qldb_table_name>"`

2. Create an index on the table for the `accountId` attribute:
   -- `CREATE INDEX ON "<qldb_table_name>" (accountId)`
