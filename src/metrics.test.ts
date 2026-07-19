import { describe, expect, it } from "vitest";
import { Registry } from "prom-client";
import { metricsRegistry, producedTotal, dedupedTotal, registerMetricsInto } from "./metrics";

describe("registerMetricsInto", () => {
  it("kafka-forge 지표를 외부 Registry에도 등록해 함께 노출한다", async () => {
    const externalRegistry = new Registry();

    registerMetricsInto(externalRegistry);
    producedTotal.inc({ topic: "order.created.v1" });

    const exported = await externalRegistry.metrics();
    expect(exported).toContain("kafka_forge_produced_total");
    expect(exported).toContain("kafka_forge_deduped_total");
    expect(exported).toContain("kafka_forge_handled_total");
  });

  it("호출 이후에도 기존 metricsRegistry는 그대로 동작한다(하위 호환)", async () => {
    const externalRegistry = new Registry();
    registerMetricsInto(externalRegistry);

    const own = await metricsRegistry.metrics();
    expect(own).toContain("kafka_forge_produced_total");
  });

  it("dedupedTotal은 topic/group 라벨로 증가시킬 수 있다", async () => {
    dedupedTotal.inc({ topic: "order.created.v1", group: "test-group" });

    const value = await dedupedTotal.get();
    expect(
      value.values.some(
        (v) => v.labels.topic === "order.created.v1" && v.labels.group === "test-group",
      ),
    ).toBe(true);
  });
});
