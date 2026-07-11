import { Kafka } from "kafkajs";

const kafka = new Kafka({ clientId: "kafka-forge-phase5-benchmark", brokers: ["localhost:19092"] });

const MESSAGE_COUNT = 5000;
const PARTITION_COUNTS = [1, 3, 6];
const BATCH_SIZE = 500;
const SIMULATED_WORK_MS = 2; // 메시지당 가짜 처리 시간을 줘야 병렬 처리 효과가 눈에 보임
const runId = Date.now().toString(36);

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function createTopic(topic: string, partitions: number): Promise<void> {
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({ topics: [{ topic, numPartitions: partitions, replicationFactor: 1 }] });
  await admin.disconnect();
}

async function produceBatch(topic: string): Promise<number> {
  const producer = kafka.producer();
  await producer.connect();

  const start = Date.now();
  for (let offset = 0; offset < MESSAGE_COUNT; offset += BATCH_SIZE) {
    const messages = Array.from({ length: Math.min(BATCH_SIZE, MESSAGE_COUNT - offset) }, (_, i) => ({
      key: `key-${offset + i}`,
      value: JSON.stringify({ i: offset + i, ts: Date.now() }),
    }));
    await producer.send({ topic, messages });
  }
  const elapsed = Date.now() - start;

  await producer.disconnect();
  return elapsed;
}

// consumerCount만큼 같은 그룹에 컨슈머를 동시에 띄워서, 파티션이 실제로 여러 컨슈머에 나뉘어
// "병렬로" 처리될 때의 처리량을 측정한다. (컨슈머 1개로는 파티션이 몇 개든 혼자 순차 처리하니
// 파티션 수 차이가 드러나지 않는다.)
async function consumeAllParallel(topic: string, consumerCount: number): Promise<number> {
  const consumers = Array.from({ length: consumerCount }, () =>
    kafka.consumer({ groupId: `benchmark-${topic}` }),
  );

  for (const consumer of consumers) {
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });
  }

  let count = 0;
  const start = Date.now();

  const elapsed = await new Promise<number>((resolve, reject) => {
    Promise.all(
      consumers.map((consumer) =>
        consumer.run({
          eachMessage: async () => {
            await sleep(SIMULATED_WORK_MS);
            count++;
            if (count >= MESSAGE_COUNT) {
              resolve(Date.now() - start);
            }
          },
        }),
      ),
    ).catch(reject);
  });

  await Promise.all(consumers.map((consumer) => consumer.disconnect()));
  return elapsed;
}

async function main() {
  const results: Array<{ partitions: number; produceMs: number; consumeMs: number }> = [];

  for (const partitions of PARTITION_COUNTS) {
    const topic = `benchmark.p${partitions}.${runId}`;
    console.log(`\n=== 파티션 ${partitions}개, 컨슈머 ${partitions}개 (${topic}) ===`);

    await createTopic(topic, partitions);

    const produceMs = await produceBatch(topic);
    console.log(
      `발행 완료: ${MESSAGE_COUNT}건, ${produceMs}ms (${Math.round(MESSAGE_COUNT / (produceMs / 1000))} msg/s)`,
    );

    const consumeMs = await consumeAllParallel(topic, partitions);
    console.log(
      `소비 완료: ${MESSAGE_COUNT}건, ${consumeMs}ms (${Math.round(MESSAGE_COUNT / (consumeMs / 1000))} msg/s)`,
    );

    results.push({ partitions, produceMs, consumeMs });
  }

  console.log("\n=== 결과 요약 ===");
  console.table(results);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
