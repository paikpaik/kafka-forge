import { describe, expect, it, vi } from "vitest";
import type { Kafka } from "kafkajs";
import { OutboxPublisher, type OutboxStore, type OutboxRecord } from "./outbox";

function createFakeKafka() {
  const send = vi.fn().mockResolvedValue(undefined);
  const producer = {
    connect: vi.fn().mockResolvedValue(undefined),
    disconnect: vi.fn().mockResolvedValue(undefined),
    send,
  };
  const producerFactory = vi.fn().mockReturnValue(producer);
  const kafka = { producer: producerFactory } as unknown as Kafka;
  return { kafka, producer, producerFactory };
}

function createFakeStore(
  records: OutboxRecord[],
): OutboxStore & { markPublished: ReturnType<typeof vi.fn> } {
  return {
    fetchPending: vi.fn().mockResolvedValue(records),
    markPublished: vi.fn().mockResolvedValue(undefined),
  };
}

describe("OutboxPublisher.publishPending", () => {
  it("미발행 레코드를 하나씩 Kafka로 발행하고 markPublished를 호출한다", async () => {
    const { kafka, producer } = createFakeKafka();
    const store = createFakeStore([
      { id: 1, topic: "order.created.v1", key: "order-1", payload: { orderId: "order-1" } },
      { id: 2, topic: "order.created.v1", key: "order-2", payload: { orderId: "order-2" } },
    ]);
    const publisher = new OutboxPublisher(kafka, store);

    const count = await publisher.publishPending(50);

    expect(count).toBe(2);
    expect(producer.send).toHaveBeenCalledTimes(2);
    expect(producer.send).toHaveBeenNthCalledWith(1, {
      topic: "order.created.v1",
      messages: [
        expect.objectContaining({ key: "order-1", value: JSON.stringify({ orderId: "order-1" }) }),
      ],
    });
    expect(store.markPublished).toHaveBeenCalledWith([1, 2]);
  });

  it("미발행 레코드가 없으면 markPublished를 호출하지 않는다", async () => {
    const { kafka, producer } = createFakeKafka();
    const store = createFakeStore([]);
    const publisher = new OutboxPublisher(kafka, store);

    const count = await publisher.publishPending();

    expect(count).toBe(0);
    expect(producer.send).not.toHaveBeenCalled();
    expect(store.markPublished).not.toHaveBeenCalled();
  });

  it("배치 중간에 하나가 실패해도 나머지 정상 레코드는 계속 발행되고 커밋된다(헤드-오프-라인 블로킹 방지)", async () => {
    const { kafka, producer } = createFakeKafka();
    producer.send
      .mockResolvedValueOnce(undefined) // 정상1
      .mockRejectedValueOnce(new Error("invalid topic name!!!")) // 독성
      .mockResolvedValueOnce(undefined); // 정상2
    const store = createFakeStore([
      { id: 1, topic: "order.created.v1", key: "order-1", payload: {} },
      { id: 2, topic: "invalid topic name!!!", key: "order-2", payload: {} },
      { id: 3, topic: "order.created.v1", key: "order-3", payload: {} },
    ]);
    const publisher = new OutboxPublisher(kafka, store);

    const count = await publisher.publishPending();

    expect(count).toBe(2);
    expect(producer.send).toHaveBeenCalledTimes(3); // 독성 뒤 정상2도 시도됨
    expect(store.markPublished).toHaveBeenCalledWith([1, 3]); // 정상1/정상2만 커밋, 독성은 제외
  });

  it("배치 전체가 실패해도 예외를 던지지 않고 0을 반환한다", async () => {
    const { kafka, producer } = createFakeKafka();
    producer.send.mockRejectedValue(new Error("invalid topic name!!!"));
    const store = createFakeStore([
      { id: 1, topic: "invalid topic name!!!", key: "order-1", payload: {} },
    ]);
    const publisher = new OutboxPublisher(kafka, store);

    await expect(publisher.publishPending()).resolves.toBe(0);
    expect(store.markPublished).not.toHaveBeenCalled();
  });

  it("발행 실패 시 markFailed에 실패한 레코드의 id와 에러가 전달된다", async () => {
    const { kafka, producer } = createFakeKafka();
    const error = new Error("invalid topic name!!!");
    producer.send.mockRejectedValueOnce(error);
    const store = { ...createFakeStore([]), markFailed: vi.fn().mockResolvedValue(undefined) };
    store.fetchPending.mockResolvedValue([
      { id: 1, topic: "invalid topic name!!!", key: "order-1", payload: {} },
    ]);
    const publisher = new OutboxPublisher(kafka, store);

    await publisher.publishPending();

    expect(store.markFailed).toHaveBeenCalledWith(1, error);
  });

  it("markFailed를 구현하지 않은(레거시) 스토어도 그대로 동작한다(하위 호환)", async () => {
    const { kafka, producer } = createFakeKafka();
    producer.send.mockRejectedValueOnce(new Error("invalid topic name!!!"));
    const store = createFakeStore([
      { id: 1, topic: "invalid topic name!!!", key: "order-1", payload: {} },
    ]);
    const publisher = new OutboxPublisher(kafka, store);

    await expect(publisher.publishPending()).resolves.toBe(0);
  });

  it("markFailed가 예외를 던져도 나머지 배치 처리를 막지 않는다", async () => {
    const { kafka, producer } = createFakeKafka();
    producer.send
      .mockRejectedValueOnce(new Error("invalid topic name!!!")) // 독성
      .mockResolvedValueOnce(undefined); // 정상
    const store = {
      ...createFakeStore([]),
      markFailed: vi.fn().mockRejectedValue(new Error("훅 자체가 고장남")),
    };
    store.fetchPending.mockResolvedValue([
      { id: 1, topic: "invalid topic name!!!", key: "order-1", payload: {} },
      { id: 2, topic: "order.created.v1", key: "order-2", payload: {} },
    ]);
    const publisher = new OutboxPublisher(kafka, store);

    const count = await publisher.publishPending();

    expect(count).toBe(1);
    expect(store.markPublished).toHaveBeenCalledWith([2]);
  });

  it("세 번째 인자로 넘긴 producer 옵션을 kafka.producer()로 그대로 전달한다", () => {
    const { kafka, producerFactory } = createFakeKafka();
    const store = createFakeStore([]);
    new OutboxPublisher(kafka, store, { idempotent: true });

    expect(producerFactory).toHaveBeenCalledWith({ idempotent: true });
  });
});
