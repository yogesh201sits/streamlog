import type { ConsumedMessage } from "@streamlog/kafka";

import { LogParser } from "./parser";
import type { LogSink } from "./sink/interface";

export class LogProcessor {
  constructor(
    private readonly parser: LogParser,
    private readonly sink: LogSink,
  ) {}

  async process(
    message: ConsumedMessage,
  ): Promise<void> {
    if (!message.value) {
      throw new Error("Kafka message has no value");
    }

    const event = this.parser.parse(message.value);

    await this.sink.write(event);
  }
}