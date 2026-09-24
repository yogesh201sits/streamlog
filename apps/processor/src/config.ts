const getEnv = (
  name: string,
  fallback: string,
): string => {
  return process.env[name] ?? fallback;
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
  },
} as const;