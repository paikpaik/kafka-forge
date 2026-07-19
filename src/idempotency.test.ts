import { afterEach, describe, expect, it, vi } from "vitest";
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

  describe("claim", () => {
    it("처음 선점하면 true를 반환하고, 이후 markProcessed 없이도 처리된 것으로 취급한다", async () => {
      const store = new InMemoryIdempotencyStore();

      await expect(store.claim("order.created.v1:0:1")).resolves.toBe(true);
      await expect(store.wasProcessed("order.created.v1:0:1")).resolves.toBe(true);
    });

    it("이미 선점(또는 처리)된 키는 false를 반환한다", async () => {
      const store = new InMemoryIdempotencyStore();
      await store.claim("order.created.v1:0:1");

      await expect(store.claim("order.created.v1:0:1")).resolves.toBe(false);
    });
  });

  describe("release", () => {
    it("release 이후에는 같은 키를 다시 claim할 수 있다", async () => {
      const store = new InMemoryIdempotencyStore();
      await store.claim("order.created.v1:0:1");

      await store.release("order.created.v1:0:1");

      await expect(store.wasProcessed("order.created.v1:0:1")).resolves.toBe(false);
      await expect(store.claim("order.created.v1:0:1")).resolves.toBe(true);
    });
  });

  describe("ttlMs", () => {
    afterEach(() => {
      vi.useRealTimers();
    });

    it("TTL이 지나면 처리 안 한 것으로 취급한다", async () => {
      vi.useFakeTimers();
      const store = new InMemoryIdempotencyStore({ ttlMs: 100 });

      await store.markProcessed("order.created.v1:0:1");
      await expect(store.wasProcessed("order.created.v1:0:1")).resolves.toBe(true);

      vi.advanceTimersByTime(150);

      await expect(store.wasProcessed("order.created.v1:0:1")).resolves.toBe(false);
      store.stop();
    });

    it("TTL을 지정하지 않으면 영구히 처리된 것으로 취급한다", async () => {
      vi.useFakeTimers();
      const store = new InMemoryIdempotencyStore();

      await store.markProcessed("order.created.v1:0:1");
      vi.advanceTimersByTime(1000 * 60 * 60 * 24);

      await expect(store.wasProcessed("order.created.v1:0:1")).resolves.toBe(true);
    });
  });
});
