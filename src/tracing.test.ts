import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { trace, propagation, context } from "@opentelemetry/api";
import { SpanKind, SpanStatusCode } from "@opentelemetry/api";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import { withProducerSpan, withConsumerSpan } from "./tracing";

const exporter = new InMemorySpanExporter();
let provider: BasicTracerProvider;
let contextManager: AsyncLocalStorageContextManager;

beforeAll(() => {
  provider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] });
  trace.setGlobalTracerProvider(provider);
  propagation.setGlobalPropagator(new W3CTraceContextPropagator());
  contextManager = new AsyncLocalStorageContextManager().enable();
  context.setGlobalContextManager(contextManager);
});

afterEach(() => {
  exporter.reset();
});

afterAll(async () => {
  trace.disable();
  propagation.disable();
  context.disable();
  contextManager.disable();
  await provider.shutdown();
});

describe("withProducerSpan", () => {
  it("kafka.produce span을 만들고 messaging 시맨틱 컨벤션 속성을 붙인다", async () => {
    await withProducerSpan("order.created.v1", "order-1", async () => "ok");

    const spans = exporter.getFinishedSpans();
    expect(spans).toHaveLength(1);
    expect(spans[0].name).toBe("kafka.produce order.created.v1");
    expect(spans[0].kind).toBe(SpanKind.PRODUCER);
    expect(spans[0].attributes["messaging.system"]).toBe("kafka");
    expect(spans[0].attributes["messaging.destination.name"]).toBe("order.created.v1");
    expect(spans[0].attributes["messaging.operation"]).toBe("publish");
    expect(spans[0].attributes["messaging.kafka.message.key"]).toBe("order-1");
    expect(spans[0].status.code).toBe(SpanStatusCode.OK);
  });

  it("key가 없으면 messaging.kafka.message.key 속성을 안 붙인다", async () => {
    await withProducerSpan("order.created.v1", undefined, async () => "ok");

    const spans = exporter.getFinishedSpans();
    expect(spans[0].attributes["messaging.kafka.message.key"]).toBeUndefined();
  });

  it("fn이 실패하면 span에 에러 상태와 예외를 기록하고 그대로 전파한다", async () => {
    await expect(
      withProducerSpan("order.created.v1", "order-1", async () => {
        throw new Error("발행 실패");
      }),
    ).rejects.toThrow("발행 실패");

    const spans = exporter.getFinishedSpans();
    expect(spans[0].status.code).toBe(SpanStatusCode.ERROR);
    expect(spans[0].events.some((e) => e.name === "exception")).toBe(true);
  });

  it("injectTraceHeaders로 만든 헤더는 실제 traceparent를 담고 있다", async () => {
    let headers: Record<string, string> = {};
    await withProducerSpan("order.created.v1", "order-1", async (h) => {
      headers = h;
    });

    expect(headers.traceparent).toBeDefined();
    expect(headers.traceparent).toMatch(/^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$/);
  });
});

describe("withConsumerSpan", () => {
  it("kafka.consume span을 만들고 messaging 시맨틱 컨벤션 속성을 붙인다", async () => {
    await withConsumerSpan("order.created.v1", "order-1", {}, async () => "ok");

    const spans = exporter.getFinishedSpans();
    expect(spans[0].name).toBe("kafka.consume order.created.v1");
    expect(spans[0].kind).toBe(SpanKind.CONSUMER);
    expect(spans[0].attributes["messaging.operation"]).toBe("process");
    expect(spans[0].attributes["messaging.kafka.message.key"]).toBe("order-1");
  });

  it("produce에서 심어둔 traceparent 헤더를 읽어 같은 트레이스의 자식 span으로 이어붙인다", async () => {
    let producerSpanId = "";
    let producerTraceId = "";
    let capturedHeaders: Record<string, string> = {};

    await withProducerSpan("order.created.v1", "order-1", async (headers) => {
      capturedHeaders = headers;
      const activeSpan = trace.getActiveSpan();
      producerSpanId = activeSpan!.spanContext().spanId;
      producerTraceId = activeSpan!.spanContext().traceId;
    });

    await withConsumerSpan("order.created.v1", "order-1", capturedHeaders, async () => {
      const activeSpan = trace.getActiveSpan();
      expect(activeSpan!.spanContext().traceId).toBe(producerTraceId);
    });

    const spans = exporter.getFinishedSpans();
    const consumeSpan = spans.find((s) => s.name === "kafka.consume order.created.v1")!;
    expect(consumeSpan.parentSpanContext?.spanId).toBe(producerSpanId);
    expect(consumeSpan.spanContext().traceId).toBe(producerTraceId);
  });
});
