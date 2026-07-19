import { Registry, Counter, Histogram, Gauge } from "prom-client";

export const metricsRegistry = new Registry();

export const producedTotal = new Counter({
  name: "kafka_forge_produced_total",
  help: "발행에 성공한 메시지 수",
  labelNames: ["topic"],
  registers: [metricsRegistry],
});

export const produceErrorsTotal = new Counter({
  name: "kafka_forge_produce_errors_total",
  help: "발행 실패 횟수 (스키마 검증 실패 포함)",
  labelNames: ["topic"],
  registers: [metricsRegistry],
});

export const consumedTotal = new Counter({
  name: "kafka_forge_consumed_total",
  help:
    "처리를 시도한 메시지 수(성공 + 재시도 소진 후 DLQ 이동 포함). 순수 성공 건수는 " +
    "kafka_forge_handled_total을 쓰거나 consumed_total - consume_errors_total로 계산한다.",
  labelNames: ["topic", "group"],
  registers: [metricsRegistry],
});

export const consumeErrorsTotal = new Counter({
  name: "kafka_forge_consume_errors_total",
  help: "재시도까지 모두 실패해 DLQ로 이동한 메시지 수",
  labelNames: ["topic", "group"],
  registers: [metricsRegistry],
});

export const consumeDurationSeconds = new Histogram({
  name: "kafka_forge_consume_duration_seconds",
  help: "handler 처리 시간(초), 재시도 대기 포함",
  labelNames: ["topic", "group"],
  buckets: [0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10],
  registers: [metricsRegistry],
});

export const consumerLag = new Gauge({
  name: "kafka_forge_consumer_lag",
  help: "파티션별 컨슈머 랙 (high watermark - committed offset)",
  labelNames: ["topic", "group", "partition"],
  registers: [metricsRegistry],
});

export const dedupedTotal = new Counter({
  name: "kafka_forge_deduped_total",
  help: "IdempotencyStore에 의해 중복으로 판정되어 스킵된 메시지 수",
  labelNames: ["topic", "group"],
  registers: [metricsRegistry],
});

export const handledTotal = new Counter({
  name: "kafka_forge_handled_total",
  help: "핸들러가 최종적으로 성공한 메시지 수 (재시도 성공 포함, DLQ 이동은 제외)",
  labelNames: ["topic", "group"],
  registers: [metricsRegistry],
});

const allMetrics = [
  producedTotal,
  produceErrorsTotal,
  consumedTotal,
  consumeErrorsTotal,
  consumeDurationSeconds,
  consumerLag,
  dedupedTotal,
  handledTotal,
];

/**
 * kafka-forge 지표들을 외부 Registry에도 등록한다. 기존 metricsRegistry로의 노출은
 * 그대로 유지되므로(하위 호환), 소비 서비스가 자기 Registry와 합쳐서 하나의
 * `/metrics` 엔드포인트로 노출하고 싶을 때 초기화 시 한 번 호출하면 된다.
 */
export function registerMetricsInto(registry: Registry): void {
  allMetrics.forEach((metric) => registry.registerMetric(metric));
}
