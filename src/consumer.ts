import type { Kafka, Consumer, Producer, Admin, KafkaMessage } from "kafkajs";
import type { z, ZodType } from "zod";
import type { EventContract } from "./event-contract";
import type { IdempotencyStore } from "./idempotency";
import { toDlqTopicName } from "./topic-name";
import { NonRetryableError } from "./errors";
import { withConsumerSpan, withProducerSpan } from "./tracing";
import {
  consumedTotal,
  consumeErrorsTotal,
  consumeDurationSeconds,
  consumerLag,
  dedupedTotal,
} from "./metrics";

export interface RetryOptions {
  attempts: number;
  initialBackoffMs: number;
}

const DEFAULT_RETRY: RetryOptions = { attempts: 3, initialBackoffMs: 1000 };
const LAG_POLL_INTERVAL_MS = 10000;

export interface SubscribeOptions<T = unknown> {
  retry?: RetryOptions | false;
  idempotencyStore?: IdempotencyStore;
  /**
   * 멱등성 키를 어떻게 뽑을지 커스텀한다. 기본값(생략 시)은 `topic:partition:offset` —
   * "이 메시지가 재배달됐는가"만 잡는다. Outbox가 같은 이벤트를 서로 다른 offset으로 두 번
   * 발행한 경우처럼 "이 비즈니스 이벤트를 이미 처리했는가"까지 잡고 싶으면 orderId 같은
   * 비즈니스 키를 반환하도록 넘긴다.
   */
  dedupeKey?: (payload: T) => string;
}

export interface ShutdownOptions {
  exitProcess?: boolean;
}

export interface RunOptions {
  partitionsConsumedConcurrently?: number;
}

interface Route<T extends ZodType> {
  event: EventContract<T>;
  handler: (payload: z.infer<T>) => Promise<void> | void;
  retry: RetryOptions | null;
  idempotencyStore?: IdempotencyStore;
  dedupeKey?: (payload: z.infer<T>) => string;
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

  /**
   * SIGINT/SIGTERM 수신 시 정상 탈퇴(disconnect)한다. 기본값으로는 프로세스를 강제 종료하지 않는다 —
   * 라이브러리가 process.exit()을 호출하면 같은 프로세스에서 돌고 있는 다른 리소스(다른 consumer,
   * HTTP 서버 등)가 자기 정리 없이 같이 죽는다. 이 프로세스에 이 컨슈머 하나뿐이라 종료까지
   * 맡기고 싶다면 { exitProcess: true }를 명시한다.
   */
  registerShutdown(options: ShutdownOptions = {}): void {
    const exitProcess = options.exitProcess ?? false;
    const shutdown = async () => {
      await this.disconnect();
      if (exitProcess) {
        process.exit(0);
      }
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
    options: SubscribeOptions<z.infer<T>> = {},
  ): Promise<void> {
    if (this.running) {
      throw new Error(
        "run() 호출 이후에는 subscribe()를 추가할 수 없습니다. 모든 subscribe()를 먼저 호출하세요.",
      );
    }
    if (this.routes.has(event.topic)) {
      throw new Error(`이미 구독 중인 토픽입니다: ${event.topic}`);
    }

    const retry = options.retry === false ? null : { ...DEFAULT_RETRY, ...options.retry };
    this.routes.set(event.topic, {
      event,
      handler,
      retry,
      idempotencyStore: options.idempotencyStore,
      dedupeKey: options.dedupeKey,
    });

    await this.consumer.subscribe({ topic: event.topic, fromBeginning: true });
  }

  /**
   * subscribe()로 등록한 모든 토픽에 대해 실제 소비를 시작한다. 컨슈머당 한 번만 호출한다.
   * `partitionsConsumedConcurrently`로 이 컨슈머 인스턴스 하나가 할당받은 파티션들을 몇 개까지
   * 동시에 처리할지 정할 수 있다 (기본 1 — kafkajs 기본값과 동일, 파티션을 순차 처리).
   */
  async run(options: RunOptions = {}): Promise<void> {
    if (this.running) return;
    this.running = true;

    this.startLagPolling();

    await this.consumer.run({
      partitionsConsumedConcurrently: options.partitionsConsumedConcurrently,
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
    const { event, handler, retry, idempotencyStore, dedupeKey } = route;
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

    const idempotencyKey = dedupeKey
      ? `${event.topic}:${dedupeKey(parsed.data)}`
      : `${event.topic}:${partition}:${message.offset}`;
    if (idempotencyStore && (await idempotencyStore.wasProcessed(idempotencyKey))) {
      dedupedTotal.inc({ topic: event.topic, group: this.groupId });
      console.log(`[StandardConsumer] 이미 처리된 메시지, 스킵: ${idempotencyKey}`);
      return;
    }

    await withConsumerSpan(
      event.topic,
      message.key?.toString(),
      message.headers ?? {},
      async () => {
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
      },
    );

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
        if (err instanceof NonRetryableError) {
          break;
        }
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
    const dlqKey = event.partitionKey(payload);
    await withProducerSpan(dlqTopic, dlqKey, async (traceHeaders) => {
      await this.dlqProducer!.send({
        topic: dlqTopic,
        messages: [
          {
            key: dlqKey,
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
