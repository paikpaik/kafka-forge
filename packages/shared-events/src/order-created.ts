import { z } from "zod";
import { createTopicName, defineEvent } from "kafka-forge";

export const OrderCreated = defineEvent({
  topic: createTopicName("order", "created", 1),
  schema: z.object({
    orderId: z.string(),
    amount: z.number().positive(),
  }),
  partitionKey: (payload) => payload.orderId,
});
