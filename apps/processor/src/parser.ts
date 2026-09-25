import {
  LogEventSchema,
  type LogEvent,
} from "@streamlog/shared";

import { PermanentProcessingError } from "./errors";

export class LogParser {
  parse(value: string): LogEvent {
    let parsed: unknown;

    try {
      parsed = JSON.parse(value);
    } catch {
      throw new PermanentProcessingError(
        "Invalid JSON",
      );
    }

    const result = LogEventSchema.safeParse(parsed);

    if (!result.success) {
      throw new PermanentProcessingError(
        `Invalid LogEvent: ${result.error.message}`,
      );
    }

    return result.data;
  }
}