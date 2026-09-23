export type LogLevel =
  | "DEBUG"
  | "INFO"
  | "WARN"
  | "ERROR"
  | "FATAL";

export type LogEvent = {
  eventId: string;
  timestamp: string;
  service: string;
  level: LogLevel;
  message: string;
  metadata?: Record<string, unknown>;
};