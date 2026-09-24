import {
  LogEventSchema,
  type LogEvent,
} from "@streamlog/shared";

export class LogParser {
  parse(value: string): LogEvent {
    let parsed: unknown;

    try {
      parsed = JSON.parse(value);
    } catch {
      throw new Error("Invalid JSON");
    }

    const result = LogEventSchema.safeParse(parsed);

    if (!result.success) {
      throw new Error(
        `Invalid LogEvent: ${result.error.message}`,
      );
    }

    return result.data;
  }
}