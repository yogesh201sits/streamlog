import {
  KafkaClient,
  KafkaConsumer,
  CONSUMER_GROUPS,
  TOPICS,
} from "@streamlog/kafka";

import type { LogEvent } from "@streamlog/shared";

const kafkaClient = new KafkaClient({
  clientId: "streamlog-processor",
});

const consumer = new KafkaConsumer(
  kafkaClient,
  CONSUMER_GROUPS.LOG_PROCESSOR,
);

function parseLogEvent(value: string): LogEvent {
  const parsed: unknown = JSON.parse(value);

  if (!parsed || typeof parsed !== "object") {
    throw new Error("Invalid log event");
  }

  const event = parsed as Partial<LogEvent>;

  if (
    typeof event.eventId !== "string" ||
    typeof event.timestamp !== "string" ||
    typeof event.service !== "string" ||
    typeof event.level !== "string" ||
    typeof event.message !== "string"
  ) {
    throw new Error("Invalid LogEvent structure");
  }

  return event as LogEvent;
}

await consumer.connect();

console.log("Kafka consumer connected");

await consumer.subscribe(TOPICS.LOGS_RAW);

console.log(`Subscribed to ${TOPICS.LOGS_RAW}`);

await consumer.run(async (message) => {
  if (!message.value) {
    console.warn("Received message without value");
    return;
  }

  try {
    const event = parseLogEvent(message.value);

    console.log("Processing log:");

    console.log({
      eventId: event.eventId,
      service: event.service,
      level: event.level,
      message: event.message,
      timestamp: event.timestamp,
      partition: message.partition,
      offset: message.offset,
    });
  } catch (error) {
    console.error("Failed to process Kafka message", {
      error,
      partition: message.partition,
      offset: message.offset,
    });
  }
});

const shutdown = async () => {
  console.log("Shutting down processor...");

  await consumer.disconnect();

  process.exit(0);
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);