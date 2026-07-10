import { kafka, TOPIC } from "./client";

// 같은 key는 항상 같은 파티션으로 가는지 확인하기 위해 3개의 key를 반복 발행한다.
const orderIds = ["order-A", "order-B", "order-C"];

async function main() {
  const producer = kafka.producer();
  await producer.connect();

  for (let round = 1; round <= 3; round++) {
    for (const orderId of orderIds) {
      const payload = {
        orderId,
        round,
        createdAt: new Date().toISOString(),
      };

      const result = await producer.send({
        topic: TOPIC,
        messages: [
          {
            key: orderId,
            value: JSON.stringify(payload),
          },
        ],
      });

      const { partition } = result[0];
      console.log(`[round ${round}] key=${orderId} -> partition ${partition}`);
    }
  }

  await producer.disconnect();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
