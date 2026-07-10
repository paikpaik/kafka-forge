import { kafka, TOPIC } from "./client";

async function main() {
  const admin = kafka.admin();
  await admin.connect();

  const existingTopics = await admin.listTopics();
  if (existingTopics.includes(TOPIC)) {
    console.log(`토픽 "${TOPIC}"이 이미 존재합니다. 스킵합니다.`);
  } else {
    await admin.createTopics({
      topics: [
        {
          topic: TOPIC,
          numPartitions: 3,
          replicationFactor: 1, // 단일 노드 Redpanda라 1로 고정
        },
      ],
    });
    console.log(`토픽 "${TOPIC}" 생성 완료 (파티션 3개, replication factor 1)`);
  }

  const metadata = await admin.fetchTopicMetadata({ topics: [TOPIC] });
  console.log(JSON.stringify(metadata, null, 2));

  await admin.disconnect();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
