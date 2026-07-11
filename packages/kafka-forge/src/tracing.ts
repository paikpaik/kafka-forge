import { trace, context, propagation, SpanKind, SpanStatusCode } from "@opentelemetry/api";

const tracer = trace.getTracer("kafka-forge");

export type TraceHeaders = Record<string, string>;
type IncomingHeaders = Record<string, string | Buffer | (string | Buffer)[] | undefined>;

export function injectTraceHeaders(): TraceHeaders {
  const headers: TraceHeaders = {};
  propagation.inject(context.active(), headers);
  return headers;
}

function toCarrier(headers: IncomingHeaders): Record<string, string> {
  const carrier: Record<string, string> = {};
  for (const [key, value] of Object.entries(headers)) {
    if (value === undefined) continue;
    carrier[key] = Array.isArray(value) ? value[0]!.toString() : value.toString();
  }
  return carrier;
}

export async function withProducerSpan<T>(
  topic: string,
  fn: (headers: TraceHeaders) => Promise<T>,
): Promise<T> {
  return tracer.startActiveSpan(`kafka.produce ${topic}`, { kind: SpanKind.PRODUCER }, async (span) => {
    try {
      const result = await fn(injectTraceHeaders());
      span.setStatus({ code: SpanStatusCode.OK });
      return result;
    } catch (err) {
      span.recordException(err as Error);
      span.setStatus({ code: SpanStatusCode.ERROR, message: (err as Error).message });
      throw err;
    } finally {
      span.end();
    }
  });
}

export async function withConsumerSpan<T>(
  topic: string,
  headers: IncomingHeaders,
  fn: () => Promise<T>,
): Promise<T> {
  const parentContext = propagation.extract(context.active(), toCarrier(headers));
  return context.with(parentContext, () =>
    tracer.startActiveSpan(`kafka.consume ${topic}`, { kind: SpanKind.CONSUMER }, async (span) => {
      try {
        const result = await fn();
        span.setStatus({ code: SpanStatusCode.OK });
        return result;
      } catch (err) {
        span.recordException(err as Error);
        span.setStatus({ code: SpanStatusCode.ERROR, message: (err as Error).message });
        throw err;
      } finally {
        span.end();
      }
    }),
  );
}
