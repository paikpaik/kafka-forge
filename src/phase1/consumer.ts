import { kafka, TOPIC } from "./client";

const GROUP_ID = "phase1.order-logger";

async function main() {
  const consumer = kafka.consumer({ groupId: GROUP_ID });

  consumer.on(consumer.events.GROUP_JOIN, (e) => {
    console.log(
      `[group join] memberId=${e.payload.memberId} 파티션 할당=${JSON.stringify(
        e.payload.memberAssignment,
      )}`,
    );
  });

  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      console.log(
        `partition=${partition} offset=${message.offset} key=${message.key?.toString()} value=${message.value?.toString()}`,
      );
    },
  });

  const shutdown = async () => {
    console.log("종료 신호 수신, 컨슈머 그룹에서 정상 탈퇴합니다...");
    await consumer.disconnect();
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
