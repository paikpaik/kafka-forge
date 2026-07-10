import { OrderCreated } from "shared-events";
import { pool } from "./db";

const runId = Date.now().toString(36);
let seq = 1;

async function createOrder() {
  const orderId = `order-${runId}-${seq++}`;
  const amount = Math.round(Math.random() * 10000) / 100;
  const payload = OrderCreated.schema.parse({ orderId, amount });
  const partitionKey = OrderCreated.partitionKey(payload);

  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();
    await connection.execute("INSERT INTO orders (order_id, amount) VALUES (?, ?)", [
      payload.orderId,
      payload.amount,
    ]);
    await connection.execute(
      "INSERT INTO outbox (topic, message_key, payload) VALUES (?, ?, ?)",
      [OrderCreated.topic, partitionKey, JSON.stringify(payload)],
    );
    await connection.commit();
    console.log(`주문 저장: ${orderId} (amount=${amount}) — outbox에 발행 예약됨`);
  } catch (err) {
    await connection.rollback();
    throw err;
  } finally {
    connection.release();
  }
}

async function main() {
  setInterval(() => {
    createOrder().catch((err) => console.error("주문 저장 실패:", err));
  }, 1500);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
