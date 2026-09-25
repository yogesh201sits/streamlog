import {
  KafkaClient,
  KafkaProducer,
  TOPICS,
} from "@streamlog/kafka";

const client = new KafkaClient({
  clientId: "streamlog-dlq-test",
});

const producer = new KafkaProducer(client);

await producer.connect();

await producer.publish(TOPICS.LOGS_RAW, {
  key: "broken-service",
  value: "{ invalid json",
});

console.log("Invalid message published");

await producer.disconnect();