export const TOPICS = {
  LOGS_RAW: "logs.raw",
  LOGS_DLQ: "logs.dlq",
} as const;

export const LOGS_RAW_PARTITIONS = 6;

export const LOGS_DLQ_PARTITIONS = 6;

export const CONSUMER_GROUPS = {
  LOG_PROCESSOR: "log-processor",
} as const;