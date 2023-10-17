import { aws_logs as logs } from "aws-cdk-lib";

export const config = {
  LOG_RETENTION: logs.RetentionDays.ONE_MONTH, // See https://docs.aws.amazon.com/cdk/api/latest/python/aws_cdk.aws_logs/RetentionDays.html for possible values
  LEDGER_NAME: "wallet-ledger", // See for valid names: https://docs.aws.amazon.com/qldb/latest/developerguide/limits.html#limits.naming
  LOG_LEVEL: "ERROR|INFO|DEBUG",
  QLDB_TABLE_NAME: "Wallet",
  DDB_TABLE_NAME: "wallet-transactions",
  SHARD_COUNT: 1, // Kinesis Stream shard count
  //  EXPIRE_AFTER_DAYS: 180, // This property needs to be set to enable TTL on the transactions table in DynamoDB
};
