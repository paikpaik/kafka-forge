import type { Kafka, Producer } from "kafkajs";
import type { z, ZodType } from "zod";
import type { EventContract } from "./event-contract";

export class StandardProducer {
  private readonly producer: Producer;

  constructor(kafka: Kafka) {
    this.producer = kafka.producer();
  }

  async connect(): Promise<void> {
    await this.producer.connect();
  }

  async disconnect(): Promise<void> {
    await this.producer.disconnect();
  }

  async send<T extends ZodType>(event: EventContract<T>, payload: z.infer<T>): Promise<void> {
    const validated = event.schema.parse(payload);
    const key = event.partitionKey(validated);

    await this.producer.send({
      topic: event.topic,
      messages: [{ key, value: JSON.stringify(validated) }],
    });
  }
}
