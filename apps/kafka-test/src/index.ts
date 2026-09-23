import {
  CONSUMER_GROUPS,
  KafkaAdmin,
  KafkaClient,
  KafkaConsumer,
  KafkaProducer,
  LOGS_RAW_PARTITIONS,
  TOPICS,
} from "@streamlog/kafka";

import type { LogEvent } from "@streamlog/shared";

const client = new KafkaClient({
  clientId: "streamlog-kafka-test",
});

const admin = new KafkaAdmin(client);
const producer = new KafkaProducer(client);
const consumer = new KafkaConsumer(
  client,
  CONSUMER_GROUPS.LOG_PROCESSOR,
);

async function main(): Promise<void> {
  console.log("Connecting to Kafka...");

  await admin.connect();

  const topics = await admin.listTopics();

  if (!topics.includes(TOPICS.LOGS_RAW)) {
    console.log(`Creating topic: ${TOPICS.LOGS_RAW}`);

    await admin.createTopic(
      TOPICS.LOGS_RAW,
      LOGS_RAW_PARTITIONS,
    );
  } else {
    console.log(`Topic already exists: ${TOPICS.LOGS_RAW}`);
  }

  await producer.connect();
  await consumer.connect();

  await consumer.subscribe(TOPICS.LOGS_RAW);

  console.log("Kafka is ready.");
  console.log("");

  await consumer.run(async (message) => {
    console.log("CONSUMED");

    console.log({
      partition: message.partition,
      offset: message.offset,
      key: message.key,
      value: message.value,
    });

    console.log("");
  });

  const logs: LogEvent[] = [
    {
      eventId: crypto.randomUUID(),
      timestamp: new Date().toISOString(),
      service: "payment-api",
      level: "ERROR",
      message: "Payment failed",
    },
    {
      eventId: crypto.randomUUID(),
      timestamp: new Date().toISOString(),
      service: "payment-api",
      level: "INFO",
      message: "Payment request received",
    },
    {
      eventId: crypto.randomUUID(),
      timestamp: new Date().toISOString(),
      service: "payment-api",
      level: "WARN",
      message: "Payment provider is slow",
    },
    {
      eventId: crypto.randomUUID(),
      timestamp: new Date().toISOString(),
      service: "auth-api",
      level: "INFO",
      message: "User authenticated",
    },
    {
      eventId: crypto.randomUUID(),
      timestamp: new Date().toISOString(),
      service: "auth-api",
      level: "ERROR",
      message: "Invalid token",
    },
  ];

  console.log("Publishing logs...");
  console.log("");

  for (const log of logs) {
    await producer.publish(TOPICS.LOGS_RAW, {
      key: log.service,
      value: JSON.stringify(log),
    });

    console.log(
      `PRODUCED → ${log.service} → ${log.level}`,
    );
  }

  console.log("");
  console.log("Messages published.");
}

main().catch(async (error) => {
  console.error("Kafka test failed:", error);

  await producer.disconnect().catch(() => {});
  await consumer.disconnect().catch(() => {});
  await admin.disconnect().catch(() => {});

  process.exit(1);
});