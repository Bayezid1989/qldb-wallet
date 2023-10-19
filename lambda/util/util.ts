import {
  QldbDriver,
  Result,
  RetryConfig,
  TransactionExecutor,
} from "amazon-qldb-driver-nodejs";
import { APIGatewayProxyResult } from "aws-lambda";
import type { dom } from "ion-js";
import { config } from "../../config";

const { QLDB_TABLE_NAME, LEDGER_NAME } = config;

export const daysToSeconds = (days: number) => Math.floor(days) * 24 * 60 * 60;

export const returnError = (
  message: string,
  httpStatusCode: number = 500,
): APIGatewayProxyResult => {
  const returnMessage = { status: "Error", message };
  const returnObject = {
    statusCode: httpStatusCode,
    body: JSON.stringify(returnMessage),
    isBase64Encoded: false,
  };
  console.error(returnMessage);
  return returnObject;
};

export const returnResponse = (body: Record<string, any>) => ({
  statusCode: 200,
  body: JSON.stringify({ ...body, status: "OK" }),
  isBase64Encoded: false,
});

export const initQldbDriver = () => {
  const retryLimit = 3;

  const retryConfig = new RetryConfig(retryLimit);

  // Initialize the driver
  return new QldbDriver(LEDGER_NAME, retryConfig);
};

const isIonNull = (ion: dom.Value | null | undefined, key: string) =>
  !ion || !ion.get(key) || ion.get(key)?.isNull();

export const ionString = (ion: dom.Value | null | undefined, key: string) =>
  isIonNull(ion, key) ? null : ion?.get(key)?.stringValue();

export const ionNumber = (ion: dom.Value | null | undefined, key: string) =>
  isIonNull(ion, key) ? null : ion?.get(key)?.numberValue();

export const ionTimestamp = (ion: dom.Value | null | undefined, key: string) =>
  isIonNull(ion, key) ? null : ion?.get(key)?.timestampValue()?.getDate();

export const ionArray = (ion: dom.Value | null | undefined, key: string) =>
  isIonNull(ion, key) ? null : ion?.get(key)?.elements();

export const parseBaseTx = (txStruct: dom.Value | null | undefined) => ({
  amount: ionNumber(txStruct, "amount"),
  requestTime: ionString(txStruct, "requestTime"),
});

const parseFullTx = (txStruct: dom.Value | null | undefined) => ({
  ...parseBaseTx(txStruct),
  status: ionString(txStruct, "status"),
  from: ionString(txStruct, "from"),
  to: ionString(txStruct, "to"),
});

export type FullTx = ReturnType<typeof parseFullTx>;

const parsePendingTxs = (data: dom.Value | null | undefined) =>
  ionArray(data, "pendingTxs")?.map(parseBaseTx) || [];

export const checkAvailableBalances = (
  record: dom.Value,
  accountId: string,
  amount?: number,
) => {
  const pendingTxs = parsePendingTxs(record);
  const pendingMinus =
    pendingTxs.reduce((sum, cur) => {
      if (typeof cur?.amount === "number" && cur.amount < 0) {
        return sum + cur?.amount;
      }
      return sum;
    }, 0) || 0;

  const balance = ionNumber(record, "balance") || 0;
  const availableBalance = balance + pendingMinus;

  if (amount !== undefined && amount < 0 && availableBalance + amount < 0) {
    return returnError(
      `Funds too low. Cannot deduct ${amount} from account ${accountId}`,
      400,
    );
  }

  return { balance, availableBalance, pendingTxs };
};

export const validateRecord = (
  record: dom.Value,
  accountId: string,
  requestTime?: string,
) => {
  if (ionTimestamp(record, "deletedAt")) {
    return returnError(`Account ${accountId} already deleted`, 400);
  }
  if (requestTime) {
    const lastReqTime = parseBaseTx(record?.get("lastTx")).requestTime;
    if (lastReqTime === requestTime) {
      return returnError(
        `Transaction request ${requestTime} already processed`,
        400,
      );
    }
    if (lastReqTime) {
      const lastTime = new Date(lastReqTime).getTime();
      const thisTime = new Date(requestTime).getTime();
      if (thisTime < lastTime) {
        return returnError(
          `Transaction request time ${requestTime} must be later than the last tx time ${lastReqTime}`,
          400,
        );
      }
    }
  }
  return record;
};

export const getValidRecord = (
  res: Result,
  accountId: string,
  requestTime?: string,
) => {
  const records: dom.Value[] = res.getResultList();

  if (!records.length) {
    return returnError(`Account ${accountId} not found`, 400);
  }
  if (records.length > 1) {
    return returnError(`More than one account with ${accountId}`, 500);
  }
  const record = validateRecord(records[0], accountId, requestTime);
  if ("statusCode" in record) return record; // Error object

  return record;
};

export const getValidBalances = async (
  accountId: string,
  executor: TransactionExecutor,
  requestTime?: string,
  amount?: number,
) => {
  console.info(`Retrieving account for id ${accountId}`);

  const res = await executor.execute(
    `SELECT accountId, balance, lastTx, pendingTxs, deletedAt
    FROM "${QLDB_TABLE_NAME}"
    WHERE accountId = ?`,
    accountId,
  );
  const record = getValidRecord(res, accountId, requestTime);
  if ("statusCode" in record) return record; // Error object

  return checkAvailableBalances(record, accountId, amount);
};

export const parseIonRecord = (ionRecord: dom.Value | null) => {
  const payload = ionRecord?.get("payload");
  const tableInfo = payload?.get("tableInfo");
  const revision = payload?.get("revision");
  const data = revision?.get("data");
  const metadata = revision?.get("metadata");
  const lastTx = data?.get("lastTx");

  return {
    tableInfo: {
      tableName: ionString(tableInfo, "tableName"),
      tableId: ionString(tableInfo, "tableId"),
    },
    revision: {
      data: {
        accountId: ionString(data, "accountId"),
        balance: ionNumber(data, "balance"),

        // Last transaction data
        lastTx: parseFullTx(lastTx),
        pendingTxs: parsePendingTxs(data),
      },
      metadata: {
        txTime: ionTimestamp(metadata, "txTime"),
        txId: ionString(metadata, "txId"),
      },
    },
  };
};

// Ion record:
// {
//   qldbStreamArn: "arn:aws:qldb:ap-northeast-1:670756400362:stream/wallet-ledger/Au1FAL3LsysIxV6ioRJGqI",
//   recordType: "REVISION_DETAILS",
//   payload: {
//     tableInfo: {
//       tableName: "Wallet",
//       tableId: "GZKrbXvDPFm8QtvYNupqo0"
//     },
//     revision: {
//       blockAddress: {
//         strandId: "0FyCSjhIEUfC13M6NoNAOh",
//         sequenceNo: 253
//       },
//       hash: {{XivyjUPTBiVmfVofInbO26WOAWxkjEfMuWHG2rPUdZg=}},
//       data: {
//         accountId: "user2",
//         createdAt: 2023-10-18T13:38:56.329Z,
//         deletedAt: null,
//         pendingTxs: [
//           {
//             amount: -10,
//             requestTime: "2023-10-18T13:40:28.121Z"
//           }
//         ],
//         balance: 130,
//         lastTx: {
//           amount: 10,
//           from: null,
//           to: null,
//           status: "IMMEDIATE",
//           requestTime: "2023-10-19T01:40:45.908Z"
//         }
//       },
//       metadata: {
//         id: "KWBzbimkHYSBjEgqiAFfIc",
//         version: 7,
//         txTime: 2023-10-19T00:33:12.051Z,
//         txId: "3mDCrAG87Xb4LAXTluxh57"
//       }
//     }
//   }
// }}
