import { Hono } from "hono";

import {
  KafkaProducer,
  TOPICS,
} from "@streamlog/kafka";

import type {
  LogEvent,
  LogLevel,
} from "@streamlog/shared";

const VALID_LEVELS: LogLevel[] = [
  "DEBUG",
  "INFO",
  "WARN",
  "ERROR",
  "FATAL",
];

type Dependencies = {
  producer: KafkaProducer;
};

export function createLogsRoute({
  producer,
}: Dependencies) {
  const app = new Hono();

  app.post("/", async (c) => {
    const body = await c.req.json();

    if (!body || typeof body !== "object") {
      return c.json(
        {
          error: "Request body must be an object",
        },
        400,
      );
    }

    const {
      service,
      level,
      message,
      metadata,
    } = body as Record<string, unknown>;

    if (
      typeof service !== "string" ||
      service.trim().length === 0
    ) {
      return c.json(
        {
          error: "service is required",
        },
        400,
      );
    }

    if (
      typeof level !== "string" ||
      !VALID_LEVELS.includes(level as LogLevel)
    ) {
      return c.json(
        {
          error:
            "level must be DEBUG, INFO, WARN, ERROR, or FATAL",
        },
        400,
      );
    }

    if (
      typeof message !== "string" ||
      message.trim().length === 0
    ) {
      return c.json(
        {
          error: "message is required",
        },
        400,
      );
    }

    const event: LogEvent = {
      eventId: crypto.randomUUID(),
      timestamp: new Date().toISOString(),
      service,
      level: level as LogLevel,
      message,
      ...(metadata &&
      typeof metadata === "object"
        ? {
            metadata:
              metadata as Record<string, unknown>,
          }
        : {}),
    };

    await producer.publish(TOPICS.LOGS_RAW, {
      key: service,
      value: JSON.stringify(event),
    });

    return c.json(
      {
        accepted: true,
        eventId: event.eventId,
      },
      202,
    );
  });

  return app;
}