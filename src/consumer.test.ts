import { afterEach, describe, expect, it, vi } from "vitest";
import type { Kafka } from "kafkajs";
import { z } from "zod";
import { StandardConsumer } from "./consumer";
import { defineEvent } from "./event-contract";
import { InMemoryIdempotencyStore } from "./idempotency";
import { NonRetryableError } from "./errors";
import { dedupedTotal } from "./metrics";

type EachMessageHandler = (args: {
  topic: string;
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
    disconnect: vi.fn().mockResolvedValue(undefined),
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
    emit: async (
      topic: string,
      value: unknown,
      options: { partition?: number; offset?: string } = {},
    ) => {
      if (!eachMessage) throw new Error("run()이 아직 호출되지 않았습니다");
      await eachMessage({
        topic,
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

const PaymentCompleted = defineEvent({
  topic: "payment.completed.v1",
  schema: z.object({ paymentId: z.string() }),
  partitionKey: (payload) => payload.paymentId,
});

const activeConsumers: StandardConsumer[] = [];

afterEach(async () => {
  await Promise.all(activeConsumers.splice(0).map((c) => c.disconnect()));
});

function createConsumer(kafka: Kafka) {
  const consumer = new StandardConsumer(kafka, "test-group");
  activeConsumers.push(consumer);
  return consumer;
}

describe("StandardConsumer.subscribe/run", () => {
  it("스키마 검증에 실패한 메시지는 handler를 호출하지 않는다", async () => {
    const { kafka, emit } = createFakeKafka();
    const consumer = createConsumer(kafka);
    const handler = vi.fn();

    await consumer.subscribe(OrderCreated, handler);
    await consumer.run();
    await emit("order.created.v1", { orderId: "order-1", amount: -1 }); // amount는 양수여야 함

    expect(handler).not.toHaveBeenCalled();
  });

  it("이미 처리된 메시지는 멱등성 스토어에 의해 스킵된다", async () => {
    const { kafka, emit } = createFakeKafka();
    const consumer = createConsumer(kafka);
    const handler = vi.fn();
    const idempotencyStore = new InMemoryIdempotencyStore();
    await idempotencyStore.markProcessed("order.created.v1:0:0");

    await consumer.subscribe(OrderCreated, handler, { idempotencyStore });
    await consumer.run();
    await emit(
      "order.created.v1",
      { orderId: "order-1", amount: 10 },
      { partition: 0, offset: "0" },
    );

    expect(handler).not.toHaveBeenCalled();
  });

  it("멱등성 스토어에 의해 스킵되면 kafka_forge_deduped_total이 증가한다", async () => {
    const { kafka, emit } = createFakeKafka();
    const consumer = createConsumer(kafka);
    const idempotencyStore = new InMemoryIdempotencyStore();
    await idempotencyStore.markProcessed("order.created.v1:0:0");

    const before = (await dedupedTotal.get()).values.find(
      (v) => v.labels.topic === "order.created.v1" && v.labels.group === "test-group",
    )?.value ?? 0;

    await consumer.subscribe(OrderCreated, vi.fn(), { idempotencyStore });
    await consumer.run();
    await emit(
      "order.created.v1",
      { orderId: "order-1", amount: 10 },
      { partition: 0, offset: "0" },
    );

    const after = (await dedupedTotal.get()).values.find(
      (v) => v.labels.topic === "order.created.v1" && v.labels.group === "test-group",
    )?.value ?? 0;
    expect(after - before).toBe(1);
  });

  it("dedupeKey를 넘기면 offset이 달라도 같은 비즈니스 키면 중복으로 스킵한다", async () => {
    const { kafka, emit } = createFakeKafka();
    const consumer = createConsumer(kafka);
    const handler = vi.fn();
    const idempotencyStore = new InMemoryIdempotencyStore();

    await consumer.subscribe(OrderCreated, handler, {
      idempotencyStore,
      dedupeKey: (payload) => payload.orderId,
    });
    await consumer.run();

    // 같은 orderId를 서로 다른 offset(재발행 상황을 흉내)으로 두 번 보냄
    await emit("order.created.v1", { orderId: "order-1", amount: 10 }, { offset: "0" });
    await emit("order.created.v1", { orderId: "order-1", amount: 10 }, { offset: "1" });

    expect(handler).toHaveBeenCalledTimes(1);
  });

  it("dedupeKey를 생략하면 기본값(topic:partition:offset)으로 동작해 다른 offset은 중복으로 안 본다", async () => {
    const { kafka, emit } = createFakeKafka();
    const consumer = createConsumer(kafka);
    const handler = vi.fn();
    const idempotencyStore = new InMemoryIdempotencyStore();

    await consumer.subscribe(OrderCreated, handler, { idempotencyStore });
    await consumer.run();

    await emit("order.created.v1", { orderId: "order-1", amount: 10 }, { offset: "0" });
    await emit("order.created.v1", { orderId: "order-1", amount: 10 }, { offset: "1" });

    expect(handler).toHaveBeenCalledTimes(2);
  });

  it("handler가 몇 번 실패하다 성공하면 재시도 후 정상 처리되고 DLQ로 가지 않는다", async () => {
    const { kafka, emit, dlqProducerObj } = createFakeKafka();
    const consumer = createConsumer(kafka);

    let attempts = 0;
    const handler = vi.fn().mockImplementation(async () => {
      attempts++;
      if (attempts < 3) throw new Error("일시적 오류");
    });

    await consumer.subscribe(OrderCreated, handler, {
      retry: { attempts: 3, initialBackoffMs: 1 },
    });
    await consumer.run();
    await emit("order.created.v1", { orderId: "order-1", amount: 10 });

    expect(handler).toHaveBeenCalledTimes(3);
    expect(dlqProducerObj.send).not.toHaveBeenCalled();
  });

  it("재시도를 다 소진하면 DLQ로 원본 payload와 에러를 발행한다", async () => {
    const { kafka, emit, dlqProducerObj } = createFakeKafka();
    const consumer = createConsumer(kafka);

    const handler = vi.fn().mockRejectedValue(new Error("영구 실패"));

    await consumer.subscribe(OrderCreated, handler, {
      retry: { attempts: 2, initialBackoffMs: 1 },
    });
    await consumer.run();
    await emit("order.created.v1", { orderId: "order-1", amount: 10 });

    expect(handler).toHaveBeenCalledTimes(2);
    expect(dlqProducerObj.send).toHaveBeenCalledTimes(1);
    const dlqCall = dlqProducerObj.send.mock.calls[0][0];
    expect(dlqCall.topic).toBe("order.created.v1.dlq");
    const dlqPayload = JSON.parse(dlqCall.messages[0].value);
    expect(dlqPayload.payload).toEqual({ orderId: "order-1", amount: 10 });
    expect(dlqPayload.error).toBe("영구 실패");
    expect(dlqCall.messages[0].key).toBe("order-1"); // 원본 파티션 키가 보존되어야 재처리 시 순서를 지킬 수 있음
  });

  it("NonRetryableError를 던지면 남은 재시도를 건너뛰고 바로 DLQ로 보낸다", async () => {
    const { kafka, emit, dlqProducerObj } = createFakeKafka();
    const consumer = createConsumer(kafka);
    const handler = vi.fn().mockRejectedValue(new NonRetryableError("이미 취소된 주문"));

    await consumer.subscribe(OrderCreated, handler, {
      retry: { attempts: 5, initialBackoffMs: 1 },
    });
    await consumer.run();
    await emit("order.created.v1", { orderId: "order-1", amount: 10 });

    expect(handler).toHaveBeenCalledTimes(1); // 5번이 아니라 1번만 시도하고 포기
    expect(dlqProducerObj.send).toHaveBeenCalledTimes(1);
    const dlqPayload = JSON.parse(dlqProducerObj.send.mock.calls[0][0].messages[0].value);
    expect(dlqPayload.error).toBe("이미 취소된 주문");
  });

  it("retry: false면 재시도 없이 한 번만 시도하고 바로 DLQ로 보낸다", async () => {
    const { kafka, emit, dlqProducerObj } = createFakeKafka();
    const consumer = createConsumer(kafka);

    const handler = vi.fn().mockRejectedValue(new Error("실패"));

    await consumer.subscribe(OrderCreated, handler, { retry: false });
    await consumer.run();
    await emit("order.created.v1", { orderId: "order-1", amount: 10 });

    expect(handler).toHaveBeenCalledTimes(1);
    expect(dlqProducerObj.send).toHaveBeenCalledTimes(1);
  });

  it("한 컨슈머에 여러 토픽을 구독하면 각자 자기 handler로만 라우팅된다", async () => {
    const { kafka, emit } = createFakeKafka();
    const consumer = createConsumer(kafka);
    const orderHandler = vi.fn();
    const paymentHandler = vi.fn();

    await consumer.subscribe(OrderCreated, orderHandler);
    await consumer.subscribe(PaymentCompleted, paymentHandler);
    await consumer.run();

    await emit("order.created.v1", { orderId: "order-1", amount: 10 });
    await emit("payment.completed.v1", { paymentId: "payment-1" });

    expect(orderHandler).toHaveBeenCalledTimes(1);
    expect(orderHandler).toHaveBeenCalledWith({ orderId: "order-1", amount: 10 });
    expect(paymentHandler).toHaveBeenCalledTimes(1);
    expect(paymentHandler).toHaveBeenCalledWith({ paymentId: "payment-1" });
  });

  it("run()에 넘긴 partitionsConsumedConcurrently를 kafkajs consumer.run으로 그대로 전달한다", async () => {
    const { kafka, consumerObj } = createFakeKafka();
    const consumer = createConsumer(kafka);

    await consumer.subscribe(OrderCreated, vi.fn());
    await consumer.run({ partitionsConsumedConcurrently: 4 });

    expect(consumerObj.run).toHaveBeenCalledWith(
      expect.objectContaining({ partitionsConsumedConcurrently: 4 }),
    );
  });

  it("같은 토픽을 두 번 구독하면 예외를 던진다", async () => {
    const { kafka } = createFakeKafka();
    const consumer = createConsumer(kafka);

    await consumer.subscribe(OrderCreated, vi.fn());
    await expect(consumer.subscribe(OrderCreated, vi.fn())).rejects.toThrow();
  });

  it("run() 이후에 subscribe()를 호출하면 예외를 던진다", async () => {
    const { kafka } = createFakeKafka();
    const consumer = createConsumer(kafka);

    await consumer.subscribe(OrderCreated, vi.fn());
    await consumer.run();

    await expect(consumer.subscribe(PaymentCompleted, vi.fn())).rejects.toThrow();
  });
});

describe("StandardConsumer.registerShutdown", () => {
  it("기본값으로는 SIGINT 수신 시 disconnect만 하고 프로세스를 종료하지 않는다", async () => {
    const { kafka, consumerObj } = createFakeKafka();
    const consumer = createConsumer(kafka);
    const exitSpy = vi.spyOn(process, "exit").mockImplementation(() => undefined as never);
    const onSpy = vi.spyOn(process, "on");

    consumer.registerShutdown();
    const sigintHandler = onSpy.mock.calls.find(
      ([signal]) => signal === "SIGINT",
    )?.[1] as () => Promise<void>;
    await sigintHandler();

    expect(consumerObj.disconnect).toHaveBeenCalled();
    expect(exitSpy).not.toHaveBeenCalled();

    exitSpy.mockRestore();
    onSpy.mockRestore();
  });

  it("{ exitProcess: true }를 넘기면 disconnect 후 프로세스를 종료한다", async () => {
    const { kafka, consumerObj } = createFakeKafka();
    const consumer = createConsumer(kafka);
    const exitSpy = vi.spyOn(process, "exit").mockImplementation(() => undefined as never);
    const onSpy = vi.spyOn(process, "on");

    consumer.registerShutdown({ exitProcess: true });
    const sigtermHandler = onSpy.mock.calls.find(
      ([signal]) => signal === "SIGTERM",
    )?.[1] as () => Promise<void>;
    await sigtermHandler();

    expect(consumerObj.disconnect).toHaveBeenCalled();
    expect(exitSpy).toHaveBeenCalledWith(0);

    exitSpy.mockRestore();
    onSpy.mockRestore();
  });
});
