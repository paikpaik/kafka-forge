import type { Kafka, Consumer, Producer, Admin, KafkaMessage } from "kafkajs";
import type { z, ZodType } from "zod";
import type { EventContract } from "./event-contract";
import type { IdempotencyStore } from "./idempotency";
import { toDlqTopicName } from "./topic-name";
import { withConsumerSpan, withProducerSpan } from "./tracing";
import { consumedTotal, consumeErrorsTotal, consumeDurationSeconds, consumerLag } from "./metrics";

export interface RetryOptions {
  attempts: number;
  initialBackoffMs: number;
}

const DEFAULT_RETRY: RetryOptions = { attempts: 3, initialBackoffMs: 1000 };
const LAG_POLL_INTERVAL_MS = 10000;

export interface SubscribeOptions {
  retry?: RetryOptions | false;
  idempotencyStore?: IdempotencyStore;
}

interface Route<T extends ZodType> {
  event: EventContract<T>;
  handler: (payload: z.infer<T>) => Promise<void> | void;
  retry: RetryOptions | null;
  idempotencyStore?: IdempotencyStore;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export class StandardConsumer {
  private readonly kafka: Kafka;
  private readonly groupId: string;
  private readonly consumer: Consumer;
  private readonly routes = new Map<string, Route<any>>();
  private dlqProducer: Producer | null = null;
  private lagAdmin: Admin | null = null;
  private lagPollTimer: ReturnType<typeof setInterval> | null = null;
  private running = false;

  constructor(kafka: Kafka, groupId: string) {
    this.kafka = kafka;
    this.groupId = groupId;
    this.consumer = kafka.consumer({ groupId });
  }

  async connect(): Promise<void> {
    await this.consumer.connect();
  }

  async disconnect(): Promise<void> {
    if (this.lagPollTimer) {
      clearInterval(this.lagPollTimer);
      this.lagPollTimer = null;
    }
    await this.consumer.disconnect();
    if (this.dlqProducer) {
      await this.dlqProducer.disconnect();
    }
    if (this.lagAdmin) {
      await this.lagAdmin.disconnect();
    }
  }

  registerShutdown(): void {
    const shutdown = async () => {
      await this.disconnect();
      process.exit(0);
    };
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);
  }

  /**
   * 이벤트 핸들러를 등록한다. 실제 소비는 모든 subscribe() 호출이 끝난 뒤 run()을 호출해야 시작된다.
   * 하나의 컨슈머(그룹)로 여러 토픽을 동시에 처리할 수 있다.
   */
  async subscribe<T extends ZodType>(
    event: EventContract<T>,
    handler: (payload: z.infer<T>) => Promise<void> | void,
    options: SubscribeOptions = {},
  ): Promise<void> {
    if (this.running) {
      throw new Error("run() 호출 이후에는 subscribe()를 추가할 수 없습니다. 모든 subscribe()를 먼저 호출하세요.");
    }
    if (this.routes.has(event.topic)) {
      throw new Error(`이미 구독 중인 토픽입니다: ${event.topic}`);
    }

    const retry = options.retry === false ? null : { ...DEFAULT_RETRY, ...options.retry };
    this.routes.set(event.topic, { event, handler, retry, idempotencyStore: options.idempotencyStore });

    await this.consumer.subscribe({ topic: event.topic, fromBeginning: true });
  }

  /** subscribe()로 등록한 모든 토픽에 대해 실제 소비를 시작한다. 컨슈머당 한 번만 호출한다. */
  async run(): Promise<void> {
    if (this.running) return;
    this.running = true;

    this.startLagPolling();

    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const route = this.routes.get(topic);
        if (!route) return;
        await this.processMessage(route, partition, message);
      },
    });
  }

  private async processMessage<T extends ZodType>(
    route: Route<T>,
    partition: number,
    message: KafkaMessage,
  ): Promise<void> {
    const { event, handler, retry, idempotencyStore } = route;
    const raw = message.value?.toString();
    if (!raw) return;

    const parsed = event.schema.safeParse(JSON.parse(raw));
    if (!parsed.success) {
      console.error(
        `[StandardConsumer] 스키마 검증 실패, 메시지 스킵: topic=${event.topic}`,
        parsed.error.message,
      );
      return;
    }

    const idempotencyKey = `${event.topic}:${partition}:${message.offset}`;
    if (idempotencyStore && (await idempotencyStore.wasProcessed(idempotencyKey))) {
      console.log(`[StandardConsumer] 이미 처리된 메시지, 스킵: ${idempotencyKey}`);
      return;
    }

    await withConsumerSpan(event.topic, message.headers ?? {}, async () => {
      const stopTimer = consumeDurationSeconds.startTimer({
        topic: event.topic,
        group: this.groupId,
      });
      try {
        await this.runWithRetry(event, parsed.data, handler, retry);
        consumedTotal.inc({ topic: event.topic, group: this.groupId });
      } finally {
        stopTimer();
      }
    });

    if (idempotencyStore) {
      await idempotencyStore.markProcessed(idempotencyKey);
    }
  }

  private async runWithRetry<T extends ZodType>(
    event: EventContract<T>,
    payload: z.infer<T>,
    handler: (payload: z.infer<T>) => Promise<void> | void,
    retry: RetryOptions | null,
  ): Promise<void> {
    if (!retry) {
      try {
        await handler(payload);
      } catch (err) {
        consumeErrorsTotal.inc({ topic: event.topic, group: this.groupId });
        await this.sendToDlq(event, payload, err);
      }
      return;
    }

    let lastError: unknown;
    for (let attempt = 1; attempt <= retry.attempts; attempt++) {
      try {
        await handler(payload);
        return;
      } catch (err) {
        lastError = err;
        console.error(
          `[StandardConsumer] handler 실패 (시도 ${attempt}/${retry.attempts}): topic=${event.topic}`,
          (err as Error).message,
        );
        if (attempt < retry.attempts) {
          await sleep(retry.initialBackoffMs * 2 ** (attempt - 1));
        }
      }
    }

    consumeErrorsTotal.inc({ topic: event.topic, group: this.groupId });
    await this.sendToDlq(event, payload, lastError);
  }

  private async sendToDlq<T extends ZodType>(
    event: EventContract<T>,
    payload: z.infer<T>,
    error: unknown,
  ): Promise<void> {
    if (!this.dlqProducer) {
      this.dlqProducer = this.kafka.producer();
      await this.dlqProducer.connect();
    }

    const dlqTopic = toDlqTopicName(event.topic);
    await withProducerSpan(dlqTopic, async (traceHeaders) => {
      await this.dlqProducer!.send({
        topic: dlqTopic,
        messages: [
          {
            key: event.partitionKey(payload),
            headers: traceHeaders,
            value: JSON.stringify({
              payload,
              error: error instanceof Error ? error.message : String(error),
              failedAt: new Date().toISOString(),
            }),
          },
        ],
      });
    });

    console.error(`[StandardConsumer] 최종 실패, DLQ로 이동: topic=${event.topic}`);
  }

  private startLagPolling(): void {
    this.lagAdmin = this.kafka.admin();
    const admin = this.lagAdmin;
    let connected = false;

    this.lagPollTimer = setInterval(async () => {
      try {
        if (!connected) {
          await admin.connect();
          connected = true;
        }

        for (const topic of this.routes.keys()) {
          const topicOffsets = await admin.fetchTopicOffsets(topic);
          const highWatermarks = new Map(topicOffsets.map((o) => [o.partition, Number(o.offset)]));

          const groupOffsets = await admin.fetchOffsets({ groupId: this.groupId, topics: [topic] });
          for (const { partitions } of groupOffsets) {
            for (const { partition, offset } of partitions) {
              const highWatermark = highWatermarks.get(partition) ?? 0;
              const committed = Number(offset);
              const lag = Math.max(highWatermark - committed, 0);
              consumerLag.set({ topic, group: this.groupId, partition: String(partition) }, lag);
            }
          }
        }
      } catch (err) {
        console.error("[StandardConsumer] 랙 조회 실패:", (err as Error).message);
      }
    }, LAG_POLL_INTERVAL_MS);
  }
}
