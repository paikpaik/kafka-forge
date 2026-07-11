import { writeFileSync } from "node:fs";
import { zodToJsonSchema } from "zod-to-json-schema";
import type { ZodType } from "zod";
import type { EventContract } from "./event-contract";

const toJsonSchemaUntyped: (schema: any) => Record<string, unknown> = zodToJsonSchema;

export interface JsonSchemaExport {
  topic: string;
  schema: Record<string, unknown>;
}

export function toJsonSchema<T extends ZodType>(event: EventContract<T>): JsonSchemaExport {
  return {
    topic: event.topic,
    schema: toJsonSchemaUntyped(event.schema),
  };
}

export function writeJsonSchema<T extends ZodType>(event: EventContract<T>, filePath: string): void {
  writeFileSync(filePath, JSON.stringify(toJsonSchema(event), null, 2));
}
