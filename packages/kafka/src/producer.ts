import type { Producer } from "kafkajs";

import { KafkaClient } from "./client";

export type PublishMessage = {
  key: string;
  value: string;
};

export class KafkaProducer {
  private readonly producer: Producer;

  constructor(private readonly client: KafkaClient) {
    this.producer = client.instance.producer();
  }

  async connect(): Promise<void> {
    await this.producer.connect();
  }

  async publish(
    topic: string,
    message: PublishMessage,
  ): Promise<void> {
    await this.producer.send({
      topic,
      messages: [
        {
          key: message.key,
          value: message.value,
        },
      ],
    });
  }

  async disconnect(): Promise<void> {
    await this.producer.disconnect();
  }
}