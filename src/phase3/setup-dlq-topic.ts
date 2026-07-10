import { Kafka } from "kafkajs";
import { toDlqTopicName } from "kafka-forge";
import { OrderCreated } from "shared-events";

const kafka = new Kafka({
  clientId: "kafka-forge-phase3-setup",
  brokers: ["localhost:19092"],
});

async function main() {
  const dlqTopic = toDlqTopicName(OrderCreated.topic);
  const admin = kafka.admin();
  await admin.connect();

  const existingTopics = await admin.listTopics();
  if (existingTopics.includes(dlqTopic)) {
    console.log(`토픽 "${dlqTopic}"이 이미 존재합니다. 스킵합니다.`);
  } else {
    await admin.createTopics({
      topics: [{ topic: dlqTopic, numPartitions: 1, replicationFactor: 1 }],
    });
    console.log(`토픽 "${dlqTopic}" 생성 완료 (파티션 1개, replication factor 1)`);
  }

  await admin.disconnect();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
