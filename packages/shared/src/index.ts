import { z } from "zod";

export const LogLevelSchema = z.enum([
  "DEBUG",
  "INFO",
  "WARN",
  "ERROR",
  "FATAL",
]);

export type LogLevel = z.infer<typeof LogLevelSchema>;

export const LogEventSchema = z.object({
  eventId: z.string().uuid(),
  timestamp: z.string().datetime(),
  service: z.string().min(1),
  level: LogLevelSchema,
  message: z.string().min(1),
  metadata: z.record(z.string(), z.unknown()).optional(),
});

export type LogEvent = z.infer<typeof LogEventSchema>;