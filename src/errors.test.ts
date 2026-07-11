import { describe, expect, it } from "vitest";
import { NonRetryableError } from "./errors";

describe("NonRetryableError", () => {
  it("일반 Error처럼 message를 갖는다", () => {
    const err = new NonRetryableError("잘못된 주문 상태");
    expect(err.message).toBe("잘못된 주문 상태");
    expect(err.name).toBe("NonRetryableError");
    expect(err).toBeInstanceOf(Error);
  });
});
