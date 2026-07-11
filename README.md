# kafka-forge

여러 서비스가 표준화된 방식으로 Kafka를 쓸 수 있게 해주는 코어 모듈입니다. 토픽 네이밍 컨벤션, 스키마 검증(Zod), 파티션 키 전략, 재시도+DLQ, 멱등성, Outbox 패턴, OTel 트레이싱, Prometheus 메트릭을 표준 기능으로 제공합니다.

## 설치

```bash
npm install kafka-forge kafkajs zod
```

## 빠른 사용 예시

이벤트 계약을 한 곳에 정의합니다 (여러 서비스가 이 정의를 그대로 import해서 씁니다):

```ts
import { z } from "zod";
import { createTopicName, defineEvent } from "kafka-forge";

export const OrderCreated = defineEvent({
  topic: createTopicName("order", "created", 1), // "order.created.v1"
  schema: z.object({
    orderId: z.string(),
    amount: z.number().positive(),
  }),
  partitionKey: (payload) => payload.orderId,
});
```

발행:

```ts
import { Kafka } from "kafkajs";
import { StandardProducer } from "kafka-forge";
import { OrderCreated } from "./events";

const kafka = new Kafka({ brokers: ["localhost:19092"] });
const producer = new StandardProducer(kafka);
await producer.connect();

await producer.send(OrderCreated, { orderId: "order-1", amount: 42.5 });
// payload는 발행 전 자동으로 zod 검증되고, 실패하면 예외를 던져 발행 자체가 막힙니다.
```

구독 (`subscribe()`는 등록만 하고, `run()`을 호출해야 실제 소비가 시작됩니다 — 하나의 컨슈머(그룹)로 여러 토픽을 동시에 처리할 수 있습니다):

```ts
import { StandardConsumer, InMemoryIdempotencyStore } from "kafka-forge";

const consumer = new StandardConsumer(kafka, "notification-service");
await consumer.connect();
consumer.registerShutdown(); // SIGINT/SIGTERM 수신 시 정상 탈퇴

await consumer.subscribe(
  OrderCreated,
  async (payload) => {
    console.log(`주문 접수: ${payload.orderId}`);
  },
  {
    retry: { attempts: 3, initialBackoffMs: 1000 }, // 기본값, 생략 가능
    idempotencyStore: new InMemoryIdempotencyStore(),
  },
);

// 같은 그룹으로 다른 이벤트도 같이 처리하고 싶으면 run() 전에 subscribe()를 더 호출한다
await consumer.subscribe(PaymentCompleted, async (payload) => {
  console.log(`결제 완료: ${payload.paymentId}`);
});

await consumer.run(); // 모든 subscribe()가 끝난 뒤 한 번만 호출
```

## 핵심 기능

| 기능 | 설명 |
|------|------|
| 토픽 네이밍 | `createTopicName(domain, event, version)`이 `<domain>.<event>.v<N>` 컨벤션을 강제 |
| Event Contract | `defineEvent()`로 토픽/스키마/파티션 키를 한 곳에서만 정의, Producer/Consumer가 공유 |
| 스키마 검증 | Zod로 발행 전 검증, 실패 시 발행 차단 |
| 재시도 + DLQ | handler 실패 시 지수 백오프로 재시도(기본 3회), 최종 실패 시 `<topic>.dlq`로 이동 (원본 key/trace 헤더 보존) |
| 멱등성 | `IdempotencyStore` 인터페이스 + 인메모리 기본 구현. Redis 등 영속 저장소는 인터페이스를 구현해 직접 연결 |
| Outbox 패턴 | `OutboxStore` 인터페이스 + `OutboxPublisher`로, DB 트랜잭션과 Kafka 발행 사이의 정합성 문제 해결 |
| Graceful shutdown | `registerShutdown()` 한 줄로 SIGINT/SIGTERM 시 정상 탈퇴 |
| 분산 트레이싱 | `@opentelemetry/api` 기반, produce span과 consume span이 Kafka 메시지 헤더를 통해 자동으로 연결됨 |
| 메트릭 | `prom-client` 기반 공유 Registry(`metricsRegistry`) — 발행/소비 카운터, 처리시간 히스토그램, 컨슈머 랙 게이지 |
| JSON Schema 내보내기 | `toJsonSchema(event)`로 Zod 스키마를 JSON Schema로 변환 — 다른 언어(Python 등) 서비스가 같은 토픽의 페이로드 구조를 코드젠할 수 있게 함 |

## 폴리글랏 지원 (JSON Schema)

Kafka 메시지는 이미 JSON이라 다른 언어도 그냥 읽을 수 있지만, "이 메시지 모양이 어떻게 생겼는지"는 지금까지 TS 코드를 봐야만 알 수 있었습니다. `toJsonSchema()`로 이 문제를 풉니다:

```ts
import { toJsonSchema, writeJsonSchema } from "kafka-forge";
import { OrderCreated } from "./events";

const jsonSchema = toJsonSchema(OrderCreated);
// { topic: "order.created.v1", schema: { type: "object", properties: {...}, required: [...] } }

writeJsonSchema(OrderCreated, "./schemas/order-created.schema.json");
```

이렇게 내보낸 `.schema.json` 파일을 공유 위치(git 저장소, 사내 문서 등)에 두면, Python 같은 다른 언어 서비스가 `datamodel-code-generator` 같은 도구로 타입/검증 모델을 자동 생성할 수 있습니다. 와이어 포맷(JSON)이나 기존 TS 코드는 전혀 바뀌지 않는 가벼운 방법입니다 — Schema Registry(Avro/Protobuf)처럼 바이너리 포맷과 별도 인프라가 필요한 무거운 대안도 있지만, 실제로 폴리글랏 소비자가 생기기 전까지는 이 정도로 충분합니다.

## 설계 원칙

- **DB/저장소 비의존**: `IdempotencyStore`, `OutboxStore`는 인터페이스만 제공하고 구현체(Redis, MySQL 등)는 강제하지 않습니다.
- **계측 SDK/서버 비의존**: `@opentelemetry/api`, `prom-client`는 코어가 직접 의존하지만, 실제 OTel Exporter 설정이나 `/metrics` HTTP 서버는 이 라이브러리를 쓰는 서비스가 직접 구성합니다.
- **프레임워크 비의존**: NestJS, Fastify, Express 등 특정 프레임워크에 종속되지 않습니다.

## 로컬 개발

```bash
docker compose up -d   # Redpanda(로컬 Kafka 호환 브로커) + 웹 콘솔(localhost:8080)
npm install
npm test               # vitest, 실제 브로커 없이 fake kafkajs로 검증
npm run build
```

## 이 프로젝트의 배경

`kafka-core-project-plan.md`와 `docs/`에 이 라이브러리를 만들며 Kafka를 학습한 과정(파티션, 컨슈머 그룹 리밸런싱, 재시도/DLQ, Outbox 패턴, 옵저버빌리티 실험 기록)이 남아있습니다.

## License

MIT
