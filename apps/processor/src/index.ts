import {
  KafkaAdmin,
  KafkaClient,
  KafkaConsumer,
  KafkaProducer,
  LOGS_DLQ_PARTITIONS,
  TOPICS,
} from "@streamlog/kafka";

import { config } from "./config";
import { DlqPublisher } from "./dlq";
import {
  PermanentProcessingError,
  RetryExhaustedError,
} from "./errors";
import { LogParser } from "./parser";
import { LogProcessor } from "./processor";
import { withRetry } from "./retry";
import { ConsoleSink } from "./sink/console";

const kafkaClient = new KafkaClient({
  clientId: config.kafka.clientId,
  brokers: config.kafka.brokers.split(","),
});

const admin = new KafkaAdmin(kafkaClient);

await admin.connect();

const topics = await admin.listTopics();

if (!topics.includes(TOPICS.LOGS_DLQ)) {
  await admin.createTopic(
    TOPICS.LOGS_DLQ,
    LOGS_DLQ_PARTITIONS,
  );

  console.log(
    `Created topic ${TOPICS.LOGS_DLQ}`,
  );
}

await admin.disconnect();

const producer = new KafkaProducer(kafkaClient);

await producer.connect();

const consumer = new KafkaConsumer(
  kafkaClient,
  config.kafka.groupId,
);

const processor = new LogProcessor(
  new LogParser(),
  new ConsoleSink(),
);

const dlqPublisher = new DlqPublisher(producer);

await consumer.connect();

console.log("Kafka consumer connected");

await consumer.subscribe(config.kafka.topic);

console.log(
  `Subscribed to ${config.kafka.topic}`,
);

const processMessage = async (
  message: Parameters<
    typeof processor.process
  >[0],
): Promise<void> => {
  try {
    await processor.process(message);

    console.log({
      event: "log_processed",
      topic: message.topic,
      partition: message.partition,
      offset: message.offset,
    });
  } catch (error) {
    if (error instanceof PermanentProcessingError) {
      console.error({
        event: "log_permanent_failure",
        topic: message.topic,
        partition: message.partition,
        offset: message.offset,
        error: error.message,
      });

      await dlqPublisher.publish(
        message,
        error,
      );

      return;
    }

    try {
      await withRetry(
        () => processor.process(message),
        {
          maxAttempts:
            config.retry.maxAttempts,
          baseDelayMs:
            config.retry.baseDelayMs,
          maxDelayMs:
            config.retry.maxDelayMs,
        },
      );

      console.log({
        event: "log_processed_after_retry",
        topic: message.topic,
        partition: message.partition,
        offset: message.offset,
      });
    } catch (retryError) {
      console.error({
        event: "log_retry_exhausted",
        topic: message.topic,
        partition: message.partition,
        offset: message.offset,
      });

      await dlqPublisher.publish(
        message,
        retryError instanceof RetryExhaustedError
          ? retryError.cause
          : retryError,
      );
    }
  }
};

const shutdown = async () => {
  console.log("Shutting down processor...");

  await consumer.disconnect();
  await producer.disconnect();

  process.exit(0);
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

await consumer.run(processMessage);