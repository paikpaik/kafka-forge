import { Kafka } from "kafkajs";
import { OrderCreated } from "shared-events";

const kafka = new Kafka({
  clientId: "kafka-forge-phase2-setup",
  brokers: ["localhost:19092"],
});

async function main() {
  const admin = kafka.admin();
  await admin.connect();

  const existingTopics = await admin.listTopics();
  if (existingTopics.includes(OrderCreated.topic)) {
    console.log(`토픽 "${OrderCreated.topic}"이 이미 존재합니다. 스킵합니다.`);
  } else {
    await admin.createTopics({
      topics: [{ topic: OrderCreated.topic, numPartitions: 3, replicationFactor: 1 }],
    });
    console.log(`토픽 "${OrderCreated.topic}" 생성 완료 (파티션 3개, replication factor 1)`);
  }

  await admin.disconnect();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
