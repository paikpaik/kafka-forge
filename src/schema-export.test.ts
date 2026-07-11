import { describe, expect, it } from "vitest";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { z } from "zod";
import { defineEvent } from "./event-contract";
import { toJsonSchema, writeJsonSchema } from "./schema-export";

const OrderCreated = defineEvent({
  topic: "order.created.v1",
  schema: z.object({ orderId: z.string(), amount: z.number().positive() }),
  partitionKey: (payload) => payload.orderId,
});

describe("toJsonSchema", () => {
  it("이벤트 토픽과 JSON Schema로 변환된 스키마를 반환한다", () => {
    const result = toJsonSchema(OrderCreated);

    expect(result.topic).toBe("order.created.v1");
    expect(result.schema).toMatchObject({
      type: "object",
      properties: {
        orderId: { type: "string" },
        amount: { type: "number", exclusiveMinimum: 0 },
      },
      required: ["orderId", "amount"],
    });
  });
});

describe("writeJsonSchema", () => {
  it("toJsonSchema의 결과를 파일로 기록한다", () => {
    const dir = mkdtempSync(join(tmpdir(), "kafka-forge-schema-"));
    const filePath = join(dir, "order-created.schema.json");

    try {
      writeJsonSchema(OrderCreated, filePath);
      const written = JSON.parse(readFileSync(filePath, "utf-8"));
      expect(written.topic).toBe("order.created.v1");
      expect(written.schema.properties.orderId).toEqual({ type: "string" });
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
