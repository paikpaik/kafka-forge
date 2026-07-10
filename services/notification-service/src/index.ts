import { Kafka } from "kafkajs";
import { StandardConsumer, InMemoryIdempotencyStore } from "kafka-forge";
import { OrderCreated } from "shared-events";

const kafka = new Kafka({
  clientId: "notification-service",
  brokers: ["localhost:19092"],
});

async function main() {
  const consumer = new StandardConsumer(kafka, "notification-service");
  await consumer.connect();
  consumer.registerShutdown();

  const idempotencyStore = new InMemoryIdempotencyStore();

  await consumer.subscribe(
    OrderCreated,
    async (payload) => {
      console.log(`알림 발송: 주문 ${payload.orderId} (금액 ${payload.amount}) 접수 완료 알림을 보냈습니다.`);
    },
    { idempotencyStore },
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
