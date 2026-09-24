import {
  KafkaClient,
  KafkaConsumer,
} from "@streamlog/kafka";

import { config } from "./config";
import { LogParser } from "./parser";
import { LogProcessor } from "./processor";
import { ConsoleSink } from "./sink/console";

const kafkaClient = new KafkaClient({
  clientId: config.kafka.clientId,
  brokers: config.kafka.brokers.split(","),
});

const consumer = new KafkaConsumer(
  kafkaClient,
  config.kafka.groupId,
);

const processor = new LogProcessor(
  new LogParser(),
  new ConsoleSink(),
);

await consumer.connect();

console.log("Kafka consumer connected");

await consumer.subscribe(
  config.kafka.topic,
);

console.log(
  `Subscribed to ${config.kafka.topic}`,
);

await consumer.run(async (message) => {
  await processor.process(message);
});

const shutdown = async () => {
  console.log("Shutting down processor...");

  await consumer.disconnect();

  process.exit(0);
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);