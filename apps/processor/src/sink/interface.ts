import type { LogEvent } from "@streamlog/shared";

export interface LogSink {
  write(event: LogEvent): Promise<void>;
}