import type { Kafka, Consumer } from "kafkajs";
import type { z, ZodType } from "zod";
import type { EventContract } from "./event-contract";

export class StandardConsumer {
  private readonly consumer: Consumer;

  constructor(kafka: Kafka, groupId: string) {
    this.consumer = kafka.consumer({ groupId });
  }

  async connect(): Promise<void> {
    await this.consumer.connect();
  }

  async disconnect(): Promise<void> {
    await this.consumer.disconnect();
  }

  async subscribe<T extends ZodType>(
    event: EventContract<T>,
    handler: (payload: z.infer<T>) => Promise<void> | void,
  ): Promise<void> {
    await this.consumer.subscribe({ topic: event.topic, fromBeginning: true });

    await this.consumer.run({
      eachMessage: async ({ message }) => {
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

        await handler(parsed.data);
      },
    });
  }
}
