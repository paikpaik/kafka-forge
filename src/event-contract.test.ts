import { describe, expect, it } from "vitest";
import { z } from "zod";
import { defineEvent, defineDlqEvent } from "./event-contract";

describe("defineEvent", () => {
  const schema = z.object({ orderId: z.string(), amount: z.number() });

  it("topic/schema/partitionKey를 그대로 담은 계약을 반환한다", () => {
    const event = defineEvent({
      topic: "order.created.v1",
      schema,
      partitionKey: (payload) => payload.orderId,
    });

    expect(event.topic).toBe("order.created.v1");
    expect(event.schema).toBe(schema);
    expect(event.partitionKey({ orderId: "order-1", amount: 10 })).toBe("order-1");
  });

  it("토픽명이 컨벤션을 어기면 예외를 던진다", () => {
    expect(() =>
      defineEvent({
        topic: "OrderCreated",
        schema,
        partitionKey: (payload) => payload.orderId,
      }),
    ).toThrow();
  });
});

describe("defineDlqEvent", () => {
  const OrderCreated = defineEvent({
    topic: "order.created.v1",
    schema: z.object({ orderId: z.string(), amount: z.number() }),
    partitionKey: (payload) => payload.orderId,
  });

  it("topic이 toDlqTopicName(원본 토픽)과 정확히 일치한다(<domain>.<event>.v<N> 컨벤션을 벗어나도 예외를 던지지 않는다)", () => {
    const dlqEvent = defineDlqEvent(OrderCreated);
    expect(dlqEvent.topic).toBe("order.created.v1.dlq");
  });

  it("{ payload, error, failedAt } 형태의 envelope을 올바르게 검증한다", () => {
    const dlqEvent = defineDlqEvent(OrderCreated);

    const valid = dlqEvent.schema.safeParse({
      payload: { orderId: "order-1", amount: 10 },
      error: "영구 실패",
      failedAt: "2026-07-19T00:00:00.000Z",
    });
    expect(valid.success).toBe(true);

    const invalidPayload = dlqEvent.schema.safeParse({
      payload: { orderId: "order-1", amount: "열개" },
      error: "영구 실패",
      failedAt: "2026-07-19T00:00:00.000Z",
    });
    expect(invalidPayload.success).toBe(false);
  });

  it("partitionKey는 원본 이벤트의 partitionKey에 envelope.payload를 위임한다", () => {
    const dlqEvent = defineDlqEvent(OrderCreated);

    const key = dlqEvent.partitionKey({
      payload: { orderId: "order-1", amount: 10 },
      error: "영구 실패",
      failedAt: "2026-07-19T00:00:00.000Z",
    });
    expect(key).toBe("order-1");
  });
});
