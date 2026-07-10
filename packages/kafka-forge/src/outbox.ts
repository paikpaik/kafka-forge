import type { Kafka, Producer } from "kafkajs";

export interface OutboxRecord {
  id: string | number;
  topic: string;
  key: string;
  payload: unknown;
}

export interface OutboxStore {
  fetchPending(limit: number): Promise<OutboxRecord[]>;
  markPublished(ids: Array<string | number>): Promise<void>;
}

export class OutboxPublisher {
  private readonly producer: Producer;

  constructor(
    kafka: Kafka,
    private readonly store: OutboxStore,
  ) {
    this.producer = kafka.producer();
  }

  async connect(): Promise<void> {
    await this.producer.connect();
  }

  async disconnect(): Promise<void> {
    await this.producer.disconnect();
  }

  async publishPending(limit = 50): Promise<number> {
    const pending = await this.store.fetchPending(limit);
    const publishedIds: Array<string | number> = [];

    for (const record of pending) {
      await this.producer.send({
        topic: record.topic,
        messages: [{ key: record.key, value: JSON.stringify(record.payload) }],
      });
      publishedIds.push(record.id);
    }

    if (publishedIds.length > 0) {
      await this.store.markPublished(publishedIds);
    }

    return publishedIds.length;
  }
}
