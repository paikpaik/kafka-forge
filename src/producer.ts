import type { Kafka, Producer, ProducerConfig } from "kafkajs";
import type { z, ZodType } from "zod";
import type { EventContract } from "./event-contract";
import { withProducerSpan } from "./tracing";
import { producedTotal, produceErrorsTotal } from "./metrics";

export class StandardProducer {
  private readonly producer: Producer;

  /**
   * options는 kafkajs의 ProducerConfig를 그대로 받는다 — 예를 들어
   * `{ idempotent: true, maxInFlightRequests: 1 }`로 네트워크 재시도로 인한 중복 발행을 막을 수 있다.
   */
  constructor(kafka: Kafka, options: ProducerConfig = {}) {
    this.producer = kafka.producer(options);
  }

  async connect(): Promise<void> {
    await this.producer.connect();
  }

  async disconnect(): Promise<void> {
    await this.producer.disconnect();
  }

  async send<T extends ZodType>(event: EventContract<T>, payload: z.infer<T>): Promise<void> {
    try {
      const validated = event.schema.parse(payload);
      const key = event.partitionKey(validated);

      await withProducerSpan(event.topic, key, async (traceHeaders) => {
        await this.producer.send({
          topic: event.topic,
          messages: [{ key, value: JSON.stringify(validated), headers: traceHeaders }],
        });
      });

      producedTotal.inc({ topic: event.topic });
    } catch (err) {
      produceErrorsTotal.inc({ topic: event.topic });
      throw err;
    }
  }
}
