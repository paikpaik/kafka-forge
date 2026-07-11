import "./tracing";
import { Kafka } from "kafkajs";
import { OutboxPublisher } from "kafka-forge";
import { MySqlOutboxStore } from "./outbox-store";
import { startMetricsServer } from "./metrics-server";

const kafka = new Kafka({
  clientId: "outbox-relay",
  brokers: ["localhost:19092"],
});

async function main() {
  startMetricsServer(9465);
  const store = new MySqlOutboxStore();
  const publisher = new OutboxPublisher(kafka, store);
  await publisher.connect();

  setInterval(async () => {
    const count = await publisher.publishPending();
    if (count > 0) {
      console.log(`Outbox 발행: ${count}건 처리`);
    }
  }, 2000);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
