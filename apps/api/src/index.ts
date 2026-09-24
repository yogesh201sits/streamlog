import { Hono } from "hono";

import {
  KafkaClient,
  KafkaProducer,
} from "@streamlog/kafka";

import { createLogsRoute } from "./routes/logs";

const kafkaClient = new KafkaClient({
  clientId: "streamlog-api",
});

const producer = new KafkaProducer(kafkaClient);

await producer.connect();

const app = new Hono();

app.get("/health", (c) => {
  return c.json({
    status: "ok",
  });
});

app.route(
  "/logs",
  createLogsRoute({
    producer,
  }),
);

const server = Bun.serve({
  port: 3000,
  fetch: app.fetch,
});

console.log(
  `StreamLog API running on http://localhost:${server.port}`,
);

const shutdown = async () => {
  console.log("Shutting down...");

  await producer.disconnect();

  process.exit(0);
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);