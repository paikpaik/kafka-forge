import { afterEach, describe, expect, it, vi } from "vitest";
import type { Kafka } from "kafkajs";
import { z } from "zod";
import { StandardConsumer } from "./consumer";
import { defineEvent } from "./event-contract";
import { InMemoryIdempotencyStore } from "./idempotency";

type EachMessageHandler = (args: {
  partition: number;
  message: { value: Buffer | null; offset: string; headers?: Record<string, unknown> };
}) => Promise<void>;

function createFakeKafka() {
  let eachMessage: EachMessageHandler | undefined;

  const consumerObj = {
    connect: vi.fn().mockResolvedValue(undefined),
    disconnect: vi.fn().mockResolvedValue(undefined),
    subscribe: vi.fn().mockResolvedValue(undefined),
    run: vi.fn().mockImplementation(async (config: { eachMessage: EachMessageHandler }) => {
      eachMessage = config.eachMessage;
    }),
  };

  const dlqProducerObj = {
    connect: vi.fn().mockResolvedValue(undefined),
    disconnect: vi.fn().mockResolvedValue(undefined),
    send: vi.fn().mockResolvedValue(undefined),
  };

  const adminObj = {
    connect: vi.fn().mockResolvedValue(undefined),
    fetchTopicOffsets: vi.fn().mockResolvedValue([]),
    fetchOffsets: vi.fn().mockResolvedValue([]),
  };

  const kafka = {
    consumer: () => consumerObj,
    producer: () => dlqProducerObj,
    admin: () => adminObj,
  } as unknown as Kafka;

  return {
    kafka,
    consumerObj,
    dlqProducerObj,
    adminObj,
    emit: async (value: unknown, options: { partition?: number; offset?: string } = {}) => {
      if (!eachMessage) throw new Error("subscribe()가 아직 run()을 호출하지 않았습니다");
      await eachMessage({
        partition: options.partition ?? 0,
        message: {
          value: Buffer.from(JSON.stringify(value)),
          offset: options.offset ?? "0",
        },
      });
    },
  };
}

const OrderCreated = defineEvent({
  topic: "order.created.v1",
  schema: z.object({ orderId: z.string(), amount: z.number().positive() }),
  partitionKey: (payload) => payload.orderId,
});

const activeConsumers: StandardConsumer[] = [];

afterEach(async () => {
  await Promise.all(activeConsumers.splice(0).map((c) => c.disconnect()));
});

describe("StandardConsumer.subscribe", () => {
  it("스키마 검증에 실패한 메시지는 handler를 호출하지 않는다", async () => {
    const { kafka, emit } = createFakeKafka();
    const consumer = new StandardConsumer(kafka, "test-group");
    activeConsumers.push(consumer);
    const handler = vi.fn();

    await consumer.subscribe(OrderCreated, handler);
    await emit({ orderId: "order-1", amount: -1 }); // amount는 양수여야 함

    expect(handler).not.toHaveBeenCalled();
  });

  it("이미 처리된 메시지는 멱등성 스토어에 의해 스킵된다", async () => {
    const { kafka, emit } = createFakeKafka();
    const consumer = new StandardConsumer(kafka, "test-group");
    activeConsumers.push(consumer);
    const handler = vi.fn();
    const idempotencyStore = new InMemoryIdempotencyStore();
    await idempotencyStore.markProcessed("order.created.v1:0:0");

    await consumer.subscribe(OrderCreated, handler, { idempotencyStore });
    await emit({ orderId: "order-1", amount: 10 }, { partition: 0, offset: "0" });

    expect(handler).not.toHaveBeenCalled();
  });

  it("handler가 몇 번 실패하다 성공하면 재시도 후 정상 처리되고 DLQ로 가지 않는다", async () => {
    const { kafka, emit, dlqProducerObj } = createFakeKafka();
    const consumer = new StandardConsumer(kafka, "test-group");
    activeConsumers.push(consumer);

    let attempts = 0;
    const handler = vi.fn().mockImplementation(async () => {
      attempts++;
      if (attempts < 3) throw new Error("일시적 오류");
    });

    await consumer.subscribe(OrderCreated, handler, {
      retry: { attempts: 3, initialBackoffMs: 1 },
    });
    await emit({ orderId: "order-1", amount: 10 });

    expect(handler).toHaveBeenCalledTimes(3);
    expect(dlqProducerObj.send).not.toHaveBeenCalled();
  });

  it("재시도를 다 소진하면 DLQ로 원본 payload와 에러를 발행한다", async () => {
    const { kafka, emit, dlqProducerObj } = createFakeKafka();
    const consumer = new StandardConsumer(kafka, "test-group");
    activeConsumers.push(consumer);

    const handler = vi.fn().mockRejectedValue(new Error("영구 실패"));

    await consumer.subscribe(OrderCreated, handler, {
      retry: { attempts: 2, initialBackoffMs: 1 },
    });
    await emit({ orderId: "order-1", amount: 10 });

    expect(handler).toHaveBeenCalledTimes(2);
    expect(dlqProducerObj.send).toHaveBeenCalledTimes(1);
    const dlqCall = dlqProducerObj.send.mock.calls[0][0];
    expect(dlqCall.topic).toBe("order.created.v1.dlq");
    const dlqPayload = JSON.parse(dlqCall.messages[0].value);
    expect(dlqPayload.payload).toEqual({ orderId: "order-1", amount: 10 });
    expect(dlqPayload.error).toBe("영구 실패");
    expect(dlqCall.messages[0].key).toBe("order-1"); // 원본 파티션 키가 보존되어야 재처리 시 순서를 지킬 수 있음
  });

  it("retry: false면 재시도 없이 한 번만 시도하고 바로 DLQ로 보낸다", async () => {
    const { kafka, emit, dlqProducerObj } = createFakeKafka();
    const consumer = new StandardConsumer(kafka, "test-group");
    activeConsumers.push(consumer);

    const handler = vi.fn().mockRejectedValue(new Error("실패"));

    await consumer.subscribe(OrderCreated, handler, { retry: false });
    await emit({ orderId: "order-1", amount: 10 });

    expect(handler).toHaveBeenCalledTimes(1);
    expect(dlqProducerObj.send).toHaveBeenCalledTimes(1);
  });
});
