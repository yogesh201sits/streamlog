const getEnv = (
  name: string,
  fallback: string,
): string => {
  return process.env[name] ?? fallback;
};

const getNumberEnv = (
  name: string,
  fallback: number,
): number => {
  const value = process.env[name];

  if (!value) {
    return fallback;
  }

  const parsed = Number(value);

  if (!Number.isFinite(parsed)) {
    throw new Error(
      `${name} must be a valid number`,
    );
  }

  return parsed;
};

export const config = {
  kafka: {
    brokers: getEnv(
      "KAFKA_BROKERS",
      "localhost:9092",
    ),

    clientId: getEnv(
      "KAFKA_CLIENT_ID",
      "streamlog-processor",
    ),

    groupId: getEnv(
      "KAFKA_GROUP_ID",
      "log-processor",
    ),

    topic: getEnv(
      "KAFKA_LOG_TOPIC",
      "logs.raw",
    ),

    dlqTopic: getEnv(
      "KAFKA_DLQ_TOPIC",
      "logs.dlq",
    ),
  },

  retry: {
    maxAttempts: getNumberEnv(
      "PROCESSOR_MAX_ATTEMPTS",
      3,
    ),

    baseDelayMs: getNumberEnv(
      "PROCESSOR_RETRY_BASE_DELAY_MS",
      100,
    ),

    maxDelayMs: getNumberEnv(
      "PROCESSOR_RETRY_MAX_DELAY_MS",
      2_000,
    ),
  },
} as const;