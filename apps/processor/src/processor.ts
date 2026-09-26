import type { ConsumedMessage } from "@streamlog/kafka";

import { LogParser } from "./parser";
import type { IdempotencyStore } from "./idempotency";
import type { LogSink } from "./sink/interface";

export class LogProcessor {
  constructor(
    private readonly parser: LogParser,
    private readonly sink: LogSink,
    private readonly idempotencyStore: IdempotencyStore,
  ) {}

  async process(
    message: ConsumedMessage,
  ): Promise<void> {
    if (!message.value) {
      throw new Error(
        "Kafka message has no value",
      );
    }

    const event = this.parser.parse(message.value);

    const alreadyProcessed =
      await this.idempotencyStore.hasProcessed(
        event.eventId,
      );

    if (alreadyProcessed) {
      console.log({
        event: "log_duplicate",
        eventId: event.eventId,
        topic: message.topic,
        partition: message.partition,
        offset: message.offset,
      });

      return;
    }

    try {
      await this.sink.write(event);

      await this.idempotencyStore.markProcessed(
        event.eventId,
      );
    } catch (error) {
      await this.idempotencyStore.remove(
        event.eventId,
      );

      throw error;
    }
  }
}