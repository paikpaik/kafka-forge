import { describe, expect, it } from "vitest";
import { InMemoryIdempotencyStore } from "./idempotency";

describe("InMemoryIdempotencyStore", () => {
  it("처리한 적 없는 키는 wasProcessed가 false를 반환한다", async () => {
    const store = new InMemoryIdempotencyStore();
    await expect(store.wasProcessed("order.created.v1:0:1")).resolves.toBe(false);
  });

  it("markProcessed 이후에는 wasProcessed가 true를 반환한다", async () => {
    const store = new InMemoryIdempotencyStore();
    await store.markProcessed("order.created.v1:0:1");
    await expect(store.wasProcessed("order.created.v1:0:1")).resolves.toBe(true);
  });

  it("서로 다른 키는 독립적으로 취급한다", async () => {
    const store = new InMemoryIdempotencyStore();
    await store.markProcessed("order.created.v1:0:1");
    await expect(store.wasProcessed("order.created.v1:0:2")).resolves.toBe(false);
  });
});
