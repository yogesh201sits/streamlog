import { Kafka } from "kafkajs";

const DEFAULT_BROKERS = ["localhost:9092"];

export type KafkaClientConfig = {
  clientId: string;
  brokers?: string[];
};

export class KafkaClient {
  private readonly kafka: Kafka;

  constructor(config: KafkaClientConfig) {
    this.kafka = new Kafka({
      clientId: config.clientId,
      brokers: config.brokers ?? DEFAULT_BROKERS,
    });
  }

  get instance(): Kafka {
    return this.kafka;
  }
}