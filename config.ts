import { aws_logs as logs } from "aws-cdk-lib";

export const config = {
  logRetention: logs.RetentionDays.ONE_MONTH, // See https://docs.aws.amazon.com/cdk/api/latest/python/aws_cdk.aws_logs/RetentionDays.html for possible values
  ledgerName: "test-wallet", // See for valid names: https://docs.aws.amazon.com/qldb/latest/developerguide/limits.html#limits.naming
  logLevel: "ERROR|INFO|DEBUG",
  qldbTableName: "Wallet",
  shardCount: 1, // Kinesis Stream shard count
  expireAfterDays: 30, // This property needs to be set to enable TTL on the transactions table in DynamoDB
};
