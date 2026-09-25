import type { LogEvent } from "@streamlog/shared";

import type { LogSink } from "./interface";

export class ConsoleSink implements LogSink {
  async write(event: LogEvent): Promise<void> {
    console.log({
      event: "log_processed",
      eventId: event.eventId,
      service: event.service,
      level: event.level,
      message: event.message,
      timestamp: event.timestamp,
    });
  }
}