import type { OutboxStore, OutboxRecord } from "kafka-forge";
import type { RowDataPacket } from "mysql2";
import { pool } from "./db";

interface OutboxRow extends RowDataPacket {
  id: number;
  topic: string;
  message_key: string;
  payload: string | object;
}

export class MySqlOutboxStore implements OutboxStore {
  async fetchPending(limit: number): Promise<OutboxRecord[]> {
    const [rows] = await pool.query<OutboxRow[]>(
      "SELECT id, topic, message_key, payload FROM outbox WHERE published = FALSE ORDER BY id LIMIT ?",
      [limit],
    );

    return rows.map((row) => ({
      id: row.id,
      topic: row.topic,
      key: row.message_key,
      payload: typeof row.payload === "string" ? JSON.parse(row.payload) : row.payload,
    }));
  }

  async markPublished(ids: Array<string | number>): Promise<void> {
    if (ids.length === 0) return;
    await pool.query("UPDATE outbox SET published = TRUE, published_at = NOW() WHERE id IN (?)", [ids]);
  }
}
