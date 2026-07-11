import type { z, ZodType } from "zod";
import { assertValidTopicName } from "./topic-name";

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
