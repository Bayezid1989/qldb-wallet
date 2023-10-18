export const TX_STATUS = {
  IMMEDIATE: "IMMEDIATE",
  REQUESTED: "REQUESTED",
  COMMITED: "COMMITED",
  CANCELED: "CANCELED",
} as const;

export const ISO8601_REGEX =
  /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2}).(\d{3})Z$/;
