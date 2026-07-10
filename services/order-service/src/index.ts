import { Kafka } from "kafkajs";
import { StandardProducer } from "kafka-forge";
import { OrderCreated } from "shared-events";

const kafka = new Kafka({
  clientId: "order-service",
  brokers: ["localhost:19092"],
});

async function main() {
  const producer = new StandardProducer(kafka);
  await producer.connect();

  let seq = 1;
  setInterval(async () => {
    const orderId = `order-${seq++}`;
    const amount = Math.round(Math.random() * 10000) / 100;

    await producer.send(OrderCreated, { orderId, amount });
    console.log(`발행: ${orderId} (amount=${amount})`);
  }, 1500);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
