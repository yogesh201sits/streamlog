import type { Admin } from "kafkajs";

import { KafkaClient } from "./client";

export class KafkaAdmin {
  private readonly admin: Admin;

  constructor(private readonly client: KafkaClient) {
    this.admin = client.instance.admin();
  }

  async connect(): Promise<void> {
    await this.admin.connect();
  }

  async createTopic(
    topic: string,
    numPartitions: number,
  ): Promise<void> {
    await this.admin.createTopics({
      waitForLeaders: true,
      topics: [
        {
          topic,
          numPartitions,
          replicationFactor: 1,
        },
      ],
    });
  }

  async listTopics(): Promise<string[]> {
    return this.admin.listTopics();
  }

  async disconnect(): Promise<void> {
    await this.admin.disconnect();
  }
}