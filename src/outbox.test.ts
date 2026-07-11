import { describe, expect, it, vi } from "vitest";
import type { Kafka } from "kafkajs";
import { OutboxPublisher, type OutboxStore, type OutboxRecord } from "./outbox";

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

function createFakeStore(
  records: OutboxRecord[],
): OutboxStore & { markPublished: ReturnType<typeof vi.fn> } {
  return {
    fetchPending: vi.fn().mockResolvedValue(records),
    markPublished: vi.fn().mockResolvedValue(undefined),
  };
}

describe("OutboxPublisher.publishPending", () => {
  it("미발행 레코드를 하나씩 Kafka로 발행하고 markPublished를 호출한다", async () => {
    const { kafka, producer } = createFakeKafka();
    const store = createFakeStore([
      { id: 1, topic: "order.created.v1", key: "order-1", payload: { orderId: "order-1" } },
      { id: 2, topic: "order.created.v1", key: "order-2", payload: { orderId: "order-2" } },
    ]);
    const publisher = new OutboxPublisher(kafka, store);

    const count = await publisher.publishPending(50);

    expect(count).toBe(2);
    expect(producer.send).toHaveBeenCalledTimes(2);
    expect(producer.send).toHaveBeenNthCalledWith(1, {
      topic: "order.created.v1",
      messages: [
        expect.objectContaining({ key: "order-1", value: JSON.stringify({ orderId: "order-1" }) }),
      ],
    });
    expect(store.markPublished).toHaveBeenCalledWith([1, 2]);
  });

  it("미발행 레코드가 없으면 markPublished를 호출하지 않는다", async () => {
    const { kafka, producer } = createFakeKafka();
    const store = createFakeStore([]);
    const publisher = new OutboxPublisher(kafka, store);

    const count = await publisher.publishPending();

    expect(count).toBe(0);
    expect(producer.send).not.toHaveBeenCalled();
    expect(store.markPublished).not.toHaveBeenCalled();
  });

  it("발행 실패 시 예외를 그대로 던지고 markPublished를 호출하지 않는다", async () => {
    const { kafka, producer } = createFakeKafka();
    producer.send.mockRejectedValueOnce(new Error("network error"));
    const store = createFakeStore([
      { id: 1, topic: "order.created.v1", key: "order-1", payload: {} },
    ]);
    const publisher = new OutboxPublisher(kafka, store);

    await expect(publisher.publishPending()).rejects.toThrow("network error");
    expect(store.markPublished).not.toHaveBeenCalled();
  });

  it("세 번째 인자로 넘긴 producer 옵션을 kafka.producer()로 그대로 전달한다", () => {
    const { kafka, producerFactory } = createFakeKafka();
    const store = createFakeStore([]);
    new OutboxPublisher(kafka, store, { idempotent: true });

    expect(producerFactory).toHaveBeenCalledWith({ idempotent: true });
  });
});
