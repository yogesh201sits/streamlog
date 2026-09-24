import type { Consumer } from "kafkajs";

import { KafkaClient } from "./client";

export type ConsumedMessage = {
  topic: string;
  partition: number;
  offset: string;
  key: string | null;
  value: string | null;
};

export type MessageHandler = (
  message: ConsumedMessage,
) => Promise<void>;

export class KafkaConsumer {
  private readonly consumer: Consumer;

  constructor(
    private readonly client: KafkaClient,
    groupId: string,
  ) {
    this.consumer = client.instance.consumer({
      groupId,
    });
  }

  async connect(): Promise<void> {
    await this.consumer.connect();
  }

  async subscribe(topic: string): Promise<void> {
    await this.consumer.subscribe({
      topic,
      fromBeginning: true,
    });
  }

  async run(handler: MessageHandler): Promise<void> {
    await this.consumer.run({
      autoCommit: false,

      eachMessage: async ({
        topic,
        partition,
        message,
      }) => {
        const consumedMessage: ConsumedMessage = {
          topic,
          partition,
          offset: message.offset,
          key: message.key?.toString() ?? null,
          value: message.value?.toString() ?? null,
        };

        await handler(consumedMessage);

        const nextOffset = (
          BigInt(message.offset) + 1n
        ).toString();

        await this.consumer.commitOffsets([
          {
            topic,
            partition,
            offset: nextOffset,
          },
        ]);
      },
    });
  }

  async disconnect(): Promise<void> {
    await this.consumer.disconnect();
  }
}