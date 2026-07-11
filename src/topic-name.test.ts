import { describe, expect, it } from "vitest";
import { assertValidTopicName, createTopicName, toDlqTopicName } from "./topic-name";

describe("createTopicName", () => {
  it("도메인/이벤트/버전으로 컨벤션에 맞는 토픽명을 만든다", () => {
    expect(createTopicName("order", "created", 1)).toBe("order.created.v1");
  });

  it("대문자가 섞이면 예외를 던진다", () => {
    expect(() => createTopicName("Order", "created", 1)).toThrow();
  });

  it("언더스코어가 섞이면 예외를 던진다", () => {
    expect(() => createTopicName("order_domain", "created", 1)).toThrow();
  });
});

describe("assertValidTopicName", () => {
  it("컨벤션에 맞는 이름은 통과시킨다", () => {
    expect(() => assertValidTopicName("order.created.v1")).not.toThrow();
  });

  it("버전 접미사가 없으면 예외를 던진다", () => {
    expect(() => assertValidTopicName("order.created")).toThrow();
  });
});

describe("toDlqTopicName", () => {
  it("원본 토픽명에 .dlq를 붙인다", () => {
    expect(toDlqTopicName("order.created.v1")).toBe("order.created.v1.dlq");
  });

  it("원본 토픽명이 유효하지 않으면 예외를 던진다", () => {
    expect(() => toDlqTopicName("invalid_topic")).toThrow();
  });
});
