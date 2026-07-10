import type { Kafka, Consumer, Producer } from "kafkajs";
import type { z, ZodType } from "zod";
import type { EventContract } from "./event-contract";
import type { IdempotencyStore } from "./idempotency";
import { toDlqTopicName } from "./topic-name";

export interface RetryOptions {
  attempts: number;
  initialBackoffMs: number;
}

const DEFAULT_RETRY: RetryOptions = { attempts: 3, initialBackoffMs: 1000 };

export interface SubscribeOptions {
  retry?: RetryOptions | false;
  idempotencyStore?: IdempotencyStore;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export class StandardConsumer {
  private readonly kafka: Kafka;
  private readonly consumer: Consumer;
  private dlqProducer: Producer | null = null;

  constructor(kafka: Kafka, groupId: string) {
    this.kafka = kafka;
    this.consumer = kafka.consumer({ groupId });
  }

  async connect(): Promise<void> {
    await this.consumer.connect();
  }

  async disconnect(): Promise<void> {
    await this.consumer.disconnect();
    if (this.dlqProducer) {
      await this.dlqProducer.disconnect();
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

  async subscribe<T extends ZodType>(
    event: EventContract<T>,
    handler: (payload: z.infer<T>) => Promise<void> | void,
    options: SubscribeOptions = {},
  ): Promise<void> {
    const retry = options.retry === false ? null : { ...DEFAULT_RETRY, ...options.retry };

    await this.consumer.subscribe({ topic: event.topic, fromBeginning: true });

    await this.consumer.run({
      eachMessage: async ({ partition, message }) => {
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
        if (options.idempotencyStore && (await options.idempotencyStore.wasProcessed(idempotencyKey))) {
          console.log(`[StandardConsumer] 이미 처리된 메시지, 스킵: ${idempotencyKey}`);
          return;
        }

        await this.runWithRetry(event, parsed.data, handler, retry);

        if (options.idempotencyStore) {
          await options.idempotencyStore.markProcessed(idempotencyKey);
        }
      },
    });
  }

  private async runWithRetry<T extends ZodType>(
    event: EventContract<T>,
    payload: z.infer<T>,
    handler: (payload: z.infer<T>) => Promise<void> | void,
    retry: RetryOptions | null,
  ): Promise<void> {
    if (!retry) {
      await handler(payload);
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

    await this.dlqProducer.send({
      topic: toDlqTopicName(event.topic),
      messages: [
        {
          value: JSON.stringify({
            payload,
            error: error instanceof Error ? error.message : String(error),
            failedAt: new Date().toISOString(),
          }),
        },
      ],
    });

    console.error(`[StandardConsumer] 최종 실패, DLQ로 이동: topic=${event.topic}`);
  }
}
