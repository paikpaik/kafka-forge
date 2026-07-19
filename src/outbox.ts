import type { Kafka, Producer, ProducerConfig } from "kafkajs";
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
  /**
   * (선택) 발행 실패를 통보한다. 구현하지 않으면(하위 호환) 호출되지 않고 지금처럼 계속
   * 재시도된다. "몇 번 실패하면 포기할지", "포기한 레코드를 어떻게 할지"는 이 메서드
   * 안에서 store가 전적으로 결정한다 — OutboxPublisher는 관여하지 않는다.
   */
  markFailed?(id: string | number, error: unknown): Promise<void>;
}

export class OutboxPublisher {
  private readonly producer: Producer;

  constructor(
    kafka: Kafka,
    private readonly store: OutboxStore,
    producerOptions: ProducerConfig = {},
  ) {
    this.producer = kafka.producer(producerOptions);
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
        await withProducerSpan(record.topic, record.key, async (traceHeaders) => {
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
        console.error(
          `[OutboxPublisher] 발행 실패, 다음 폴링에서 재시도: id=${record.id} topic=${record.topic}`,
          err instanceof Error ? err.message : String(err),
        );
        // 던지지 않고 다음 레코드로 계속 진행한다 — 이 레코드의 실패가 앞서 성공한 레코드의
        // markPublished를 막거나, 뒤에 남은 레코드의 시도를 막으면 안 된다.
        try {
          await this.store.markFailed?.(record.id, err);
        } catch (hookErr) {
          console.error(
            `[OutboxPublisher] markFailed 훅 실패: id=${record.id}`,
            hookErr instanceof Error ? hookErr.message : String(hookErr),
          );
        }
      }
    }

    if (publishedIds.length > 0) {
      await this.store.markPublished(publishedIds);
    }

    return publishedIds.length;
  }
}
