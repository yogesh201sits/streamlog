import {
  KafkaProducer,
  TOPICS,
  type ConsumedMessage,
} from "@streamlog/kafka";

type DlqRecord = {
  originalTopic: string;
  partition: number;
  offset: string;
  key: string | null;
  value: string | null;
  error: string;
  failedAt: string;
};

export class DlqPublisher {
  constructor(
    private readonly producer: KafkaProducer,
  ) {}

  async publish(
    message: ConsumedMessage,
    error: unknown,
  ): Promise<void> {
    const record: DlqRecord = {
      originalTopic: message.topic,
      partition: message.partition,
      offset: message.offset,
      key: message.key,
      value: message.value,
      error:
        error instanceof Error
          ? error.message
          : String(error),
      failedAt: new Date().toISOString(),
    };

    await this.producer.publish(
      TOPICS.LOGS_DLQ,
      {
        key:
          message.key ??
          `${message.topic}:${message.partition}`,
        value: JSON.stringify(record),
      },
    );
  }
}