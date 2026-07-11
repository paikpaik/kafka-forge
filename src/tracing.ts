import { trace, context, propagation, SpanKind, SpanStatusCode } from "@opentelemetry/api";

const tracer = trace.getTracer("kafka-forge");

export type TraceHeaders = Record<string, string>;
type IncomingHeaders = Record<string, string | Buffer | (string | Buffer)[] | undefined>;

// https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/ 의 Kafka 관련 속성 키.
// 패키지 하나 추가할 만큼 무겁지 않아 상수로 직접 둔다.
const ATTR_MESSAGING_SYSTEM = "messaging.system";
const ATTR_MESSAGING_DESTINATION_NAME = "messaging.destination.name";
const ATTR_MESSAGING_KAFKA_MESSAGE_KEY = "messaging.kafka.message.key";
const ATTR_MESSAGING_OPERATION = "messaging.operation";

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
  key: string | undefined,
  fn: (headers: TraceHeaders) => Promise<T>,
): Promise<T> {
  return tracer.startActiveSpan(
    `kafka.produce ${topic}`,
    {
      kind: SpanKind.PRODUCER,
      attributes: {
        [ATTR_MESSAGING_SYSTEM]: "kafka",
        [ATTR_MESSAGING_DESTINATION_NAME]: topic,
        [ATTR_MESSAGING_OPERATION]: "publish",
        ...(key ? { [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: key } : {}),
      },
    },
    async (span) => {
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
    },
  );
}

export async function withConsumerSpan<T>(
  topic: string,
  key: string | undefined,
  headers: IncomingHeaders,
  fn: () => Promise<T>,
): Promise<T> {
  const parentContext = propagation.extract(context.active(), toCarrier(headers));
  return context.with(parentContext, () =>
    tracer.startActiveSpan(
      `kafka.consume ${topic}`,
      {
        kind: SpanKind.CONSUMER,
        attributes: {
          [ATTR_MESSAGING_SYSTEM]: "kafka",
          [ATTR_MESSAGING_DESTINATION_NAME]: topic,
          [ATTR_MESSAGING_OPERATION]: "process",
          ...(key ? { [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: key } : {}),
        },
      },
      async (span) => {
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
      },
    ),
  );
}
