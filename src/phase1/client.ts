import { Kafka, logLevel } from "kafkajs";

export const TOPIC = "phase1.orders.created";

export const kafka = new Kafka({
  clientId: "kafka-forge-phase1",
  brokers: ["localhost:19092"],
  logLevel: logLevel.WARN,
});
