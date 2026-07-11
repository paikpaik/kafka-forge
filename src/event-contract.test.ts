import { describe, expect, it } from "vitest";
import { z } from "zod";
import { defineEvent } from "./event-contract";

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
