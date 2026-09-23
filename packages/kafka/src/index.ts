export const KAFKA_BROKER = "localhost:9092";
export { KafkaClient } from "./client";
export type { KafkaClientConfig } from "./client";

export { KafkaAdmin } from "./admin";

export { KafkaProducer } from "./producer";
export type { PublishMessage } from "./producer";

export { KafkaConsumer } from "./consumer";
export type {
  ConsumedMessage,
  MessageHandler,
} from "./consumer";

export {
  TOPICS,
  LOGS_RAW_PARTITIONS,
  CONSUMER_GROUPS,
} from "./topics";