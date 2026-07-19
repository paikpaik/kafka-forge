import { z } from "zod";
import type { ZodType } from "zod";
import { assertValidTopicName, toDlqTopicName } from "./topic-name";

export interface EventContract<T extends ZodType> {
  topic: string;
  schema: T;
  partitionKey: (payload: z.infer<T>) => string;
}

export function defineEvent<T extends ZodType>(config: {
  topic: string;
  schema: T;
  partitionKey: (payload: z.infer<T>) => string;
}): EventContract<T> {
  assertValidTopicName(config.topic);
  return config;
}

/**
 * 원본 EventContract로부터 DLQ용 EventContract를 만든다. DLQ 토픽(`<topic>.dlq`)은
 * `<domain>.<event>.v<N>` 네이밍 컨벤션의 대상이 아니므로 assertValidTopicName을 거치지
 * 않는다 — toDlqTopicName()이 이미 원본 토픽의 유효성을 검증했다는 전제다.
 */
export function defineDlqEvent<T extends ZodType>(original: EventContract<T>) {
  const schema = z.object({
    payload: original.schema,
    error: z.string(),
    failedAt: z.string(),
  });

  const dlqEvent: EventContract<typeof schema> = {
    topic: toDlqTopicName(original.topic),
    schema,
    partitionKey: (envelope) => original.partitionKey(envelope.payload),
  };
  return dlqEvent;
}
