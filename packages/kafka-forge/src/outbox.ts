import type { Kafka, Producer } from "kafkajs";
import { withProducerSpan } from "./tracing";
import { producedTotal, produceErrorsTotal } from "./metrics";

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
      try {
        await withProducerSpan(record.topic, async (traceHeaders) => {
          await this.producer.send({
            topic: record.topic,
            messages: [
              { key: record.key, value: JSON.stringify(record.payload), headers: traceHeaders },
            ],
          });
        });
        producedTotal.inc({ topic: record.topic });
        publishedIds.push(record.id);
      } catch (err) {
        produceErrorsTotal.inc({ topic: record.topic });
        throw err;
      }
    }

    if (publishedIds.length > 0) {
      await this.store.markPublished(publishedIds);
    }

    return publishedIds.length;
  }
}
