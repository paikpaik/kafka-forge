import { describe, expect, it, vi } from "vitest";
import type { Kafka } from "kafkajs";
import { z } from "zod";
import { StandardProducer } from "./producer";
import { defineEvent } from "./event-contract";

function createFakeKafka() {
  const send = vi.fn().mockResolvedValue(undefined);
  const producer = {
    connect: vi.fn().mockResolvedValue(undefined),
    disconnect: vi.fn().mockResolvedValue(undefined),
    send,
  };
  const producerFactory = vi.fn().mockReturnValue(producer);
  const kafka = { producer: producerFactory } as unknown as Kafka;
  return { kafka, producer, producerFactory };
}

const OrderCreated = defineEvent({
  topic: "order.created.v1",
  schema: z.object({ orderId: z.string(), amount: z.number().positive() }),
  partitionKey: (payload) => payload.orderId,
});

describe("StandardProducer.send", () => {
  it("스키마를 통과한 payload를 파티션 키와 함께 발행한다", async () => {
    const { kafka, producer } = createFakeKafka();
    const standardProducer = new StandardProducer(kafka);

    await standardProducer.send(OrderCreated, { orderId: "order-1", amount: 10 });

    expect(producer.send).toHaveBeenCalledWith({
      topic: "order.created.v1",
      messages: [
        expect.objectContaining({
          key: "order-1",
          value: JSON.stringify({ orderId: "order-1", amount: 10 }),
        }),
      ],
    });
  });

  it("스키마 검증에 실패하면 발행 자체를 하지 않고 예외를 던진다", async () => {
    const { kafka, producer } = createFakeKafka();
    const standardProducer = new StandardProducer(kafka);

    await expect(
      standardProducer.send(OrderCreated, { orderId: "order-1", amount: -5 }),
    ).rejects.toThrow();
    expect(producer.send).not.toHaveBeenCalled();
  });

  it("Kafka 발행이 실패하면 예외를 그대로 전파한다", async () => {
    const { kafka, producer } = createFakeKafka();
    producer.send.mockRejectedValueOnce(new Error("broker unavailable"));
    const standardProducer = new StandardProducer(kafka);

    await expect(
      standardProducer.send(OrderCreated, { orderId: "order-1", amount: 10 }),
    ).rejects.toThrow("broker unavailable");
  });

  it("생성자에 넘긴 옵션(idempotent 등)을 kafka.producer()로 그대로 전달한다", () => {
    const { kafka, producerFactory } = createFakeKafka();
    new StandardProducer(kafka, { idempotent: true, maxInFlightRequests: 1 });

    expect(producerFactory).toHaveBeenCalledWith({ idempotent: true, maxInFlightRequests: 1 });
  });
});
