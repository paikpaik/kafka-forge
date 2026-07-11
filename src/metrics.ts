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
  help: "정상 처리된 메시지 수",
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
