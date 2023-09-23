import { QldbDriver, RetryConfig } from "amazon-qldb-driver-nodejs";
import { config } from "../../config";
import type { ReturnObj } from "./types";

export const returnError = (
  message: string,
  httpStatusCode: number = 500,
): ReturnObj => {
  const returnMessage = { status: "error", message };
  const returnObject = {
    statusCode: httpStatusCode,
    body: JSON.stringify(returnMessage),
    isBase64Encoded: false,
  };
  console.error(returnMessage);
  return returnObject;
};

export const initQldbDriver = () => {
  const LEDGER_NAME = config.ledgerName;
  const retryLimit = 3;

  const retryConfig = new RetryConfig(retryLimit);

  // Initialize the driver
  return new QldbDriver(LEDGER_NAME, retryConfig);
};
