# kafka-forge

여러 서비스가 표준화된 방식으로 Kafka를 쓸 수 있게 해주는 코어 모듈입니다. 토픽 네이밍 컨벤션, 스키마 검증(Zod), 파티션 키 전략, 재시도+DLQ, 멱등성, Outbox 패턴, OTel 트레이싱, Prometheus 메트릭을 표준 기능으로 제공합니다.

## 설치

GitHub Packages로 배포합니다. 설치하는 쪽 프로젝트의 `.npmrc`에 `@paikpaik` 스코프 레지스트리를 등록해야 합니다:

```
# .npmrc
@paikpaik:registry=https://npm.pkg.github.com
```

```bash
npm install @paikpaik/kafka-forge kafkajs zod
```

## 빠른 사용 예시

이벤트 계약을 한 곳에 정의합니다 (여러 서비스가 이 정의를 그대로 import해서 씁니다):

```ts
import { z } from "zod";
import { createTopicName, defineEvent } from "@paikpaik/kafka-forge";

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
import { StandardProducer } from "@paikpaik/kafka-forge";
import { OrderCreated } from "./events";

const kafka = new Kafka({ brokers: ["localhost:19092"] });
const producer = new StandardProducer(kafka);
// 네트워크 재시도로 인한 중복 발행이 걱정되면: new StandardProducer(kafka, { idempotent: true })
// (kafkajs의 ProducerConfig를 그대로 받는다. OutboxPublisher도 세 번째 인자로 동일하게 받는다.)
await producer.connect();

await producer.send(OrderCreated, { orderId: "order-1", amount: 42.5 });
// payload는 발행 전 자동으로 zod 검증되고, 실패하면 예외를 던져 발행 자체가 막힙니다.
```

구독 (`subscribe()`는 등록만 하고, `run()`을 호출해야 실제 소비가 시작됩니다 — 하나의 컨슈머(그룹)로 여러 토픽을 동시에 처리할 수 있습니다):

```ts
import { StandardConsumer, InMemoryIdempotencyStore } from "@paikpaik/kafka-forge";

const consumer = new StandardConsumer(kafka, "notification-service");
await consumer.connect();
consumer.registerShutdown(); // SIGINT/SIGTERM 수신 시 정상 탈퇴 (기본값: 프로세스 강제 종료 안 함)
// 이 프로세스에 컨슈머 하나뿐이라 종료까지 맡기고 싶다면: registerShutdown({ exitProcess: true })

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
// 파티션이 여러 개면 consumer.run({ partitionsConsumedConcurrently: 4 })로 이 인스턴스 안에서 병렬 처리 가능
```

재시도해봐야 절대 성공할 리 없는 에러(예: 이미 취소된 주문)는 `NonRetryableError`를 던지면 남은 재시도를 건너뛰고 바로 DLQ로 보냅니다:

```ts
import { NonRetryableError } from "@paikpaik/kafka-forge";

await consumer.subscribe(OrderCreated, async (payload) => {
  if (payload.amount < 0) {
    throw new NonRetryableError("금액이 음수인 주문은 재시도해도 소용없음");
  }
  // ...
});
```

`InMemoryIdempotencyStore`는 기본값으로는 TTL이 없어서 오래 도는 프로세스에 그대로 쓰면 메모리가 계속 쌓입니다. `ttlMs`를 지정하면 그 시간이 지난 키를 주기적으로 정리합니다:

```ts
const idempotencyStore = new InMemoryIdempotencyStore({ ttlMs: 10 * 60 * 1000 }); // 10분
```

멱등성 키는 기본값으로 `topic:partition:offset`(같은 메시지의 재배달만 방지)을 씁니다. Outbox가 같은 이벤트를 서로 다른 offset으로 두 번 발행한 경우처럼, "같은 비즈니스 이벤트"를 기준으로 막고 싶으면 `dedupeKey`를 넘깁니다:

```ts
await consumer.subscribe(OrderCreated, handler, {
  idempotencyStore,
  dedupeKey: (payload) => payload.orderId, // offset이 달라도 같은 orderId면 중복으로 스킵
});
```

멱등성으로 걸러진 메시지 수는 `IdempotencyStore` 구현체(Redis, DB 등)와 무관하게 `kafka_forge_deduped_total{topic,group}` 카운터로 표준화되어 잡힙니다 — 구현체마다 각자 지표를 재지 않아도 여러 서비스를 같은 대시보드에서 비교할 수 있습니다.

기본(`wasProcessed` → handler 실행 → `markProcessed`) 순서는 handler 실행 후 마킹 전에 프로세스가 죽으면 재배달 시 handler가 다시 실행되는 크래시 윈도우가 있습니다. `IdempotencyStore`에 `claim(key)`을 구현하면 `StandardConsumer`가 handler 실행 **전**에 원자적으로 선점을 시도합니다 — `false`가 돌아오면 handler를 아예 실행하지 않고, `true`인 경우 이미 선점 시점에 마킹까지 끝난 것으로 보고 `markProcessed`를 다시 부르지 않습니다. 이펙트가 중복 적용되는 대신 (극단적인 크래시 타이밍에) 유실될 수 있는 트레이드오프이므로, 중복보다 유실이 덜 위험한 이펙트(예: 누적 카운터)에 적합합니다. `claim`을 구현하지 않으면 기존 동작 그대로입니다(하위 호환).

```ts
class RedisIdempotencyStore implements IdempotencyStore {
  async wasProcessed(key: string) { /* ... */ }
  async markProcessed(key: string) { /* ... */ }
  async claim(key: string): Promise<boolean> {
    // 예: Redis SET NX PX 기반 분산 락 — 선점에 성공하면 true
  }
}
```

## 핵심 기능

| 기능 | 설명 |
|------|------|
| 토픽 네이밍 | `createTopicName(domain, event, version)`이 `<domain>.<event>.v<N>` 컨벤션을 강제 |
| Event Contract | `defineEvent()`로 토픽/스키마/파티션 키를 한 곳에서만 정의, Producer/Consumer가 공유 |
| 스키마 검증 | Zod로 발행 전 검증, 실패 시 발행 차단 |
| 재시도 + DLQ | handler 실패 시 지수 백오프로 재시도(기본 3회), 최종 실패 시 `<topic>.dlq`로 이동 (원본 key/trace 헤더 보존) |
| 멱등성 | `IdempotencyStore` 인터페이스 + 인메모리 기본 구현. Redis 등 영속 저장소는 인터페이스를 구현해 직접 연결. 선택적 `claim()`으로 handler 실행 전 원자적 선점 가능(사후 마킹의 크래시 윈도우 제거) |
| Outbox 패턴 | `OutboxStore` 인터페이스 + `OutboxPublisher`로, DB 트랜잭션과 Kafka 발행 사이의 정합성 문제 해결 |
| Graceful shutdown | `registerShutdown()` 한 줄로 SIGINT/SIGTERM 시 정상 탈퇴 |
| 분산 트레이싱 | `@opentelemetry/api` 기반, produce span과 consume span이 Kafka 메시지 헤더를 통해 자동으로 연결됨 |
| 메트릭 | `prom-client` 기반 공유 Registry(`metricsRegistry`) — 발행/소비/멱등성 중복 스킵 카운터, 처리시간 히스토그램, 컨슈머 랙 게이지. `registerMetricsInto()`로 서비스 자체 Registry에도 합쳐서 노출 가능 |
| JSON Schema 내보내기 | `toJsonSchema(event)`로 Zod 스키마를 JSON Schema로 변환 — 다른 언어(Python 등) 서비스가 같은 토픽의 페이로드 구조를 코드젠할 수 있게 함 |

## 폴리글랏 지원 (JSON Schema)

Kafka 메시지는 이미 JSON이라 다른 언어도 그냥 읽을 수 있지만, "이 메시지 모양이 어떻게 생겼는지"는 지금까지 TS 코드를 봐야만 알 수 있었습니다. `toJsonSchema()`로 이 문제를 풉니다:

```ts
import { toJsonSchema, writeJsonSchema } from "@paikpaik/kafka-forge";
import { OrderCreated } from "./events";

const jsonSchema = toJsonSchema(OrderCreated);
// { topic: "order.created.v1", schema: { type: "object", properties: {...}, required: [...] } }

writeJsonSchema(OrderCreated, "./schemas/order-created.schema.json");
```

이렇게 내보낸 `.schema.json` 파일을 공유 위치(git 저장소, 사내 문서 등)에 두면, Python 같은 다른 언어 서비스가 `datamodel-code-generator` 같은 도구로 타입/검증 모델을 자동 생성할 수 있습니다. 와이어 포맷(JSON)이나 기존 TS 코드는 전혀 바뀌지 않는 가벼운 방법입니다 — Schema Registry(Avro/Protobuf)처럼 바이너리 포맷과 별도 인프라가 필요한 무거운 대안도 있지만, 실제로 폴리글랏 소비자가 생기기 전까지는 이 정도로 충분합니다.

## 설계 원칙

- **DB/저장소 비의존**: `IdempotencyStore`, `OutboxStore`는 인터페이스만 제공하고 구현체(Redis, MySQL 등)는 강제하지 않습니다.
- **계측 SDK/서버 비의존**: `@opentelemetry/api`, `prom-client`는 코어가 직접 의존하지만, 실제 OTel Exporter 설정이나 `/metrics` HTTP 서버는 이 라이브러리를 쓰는 서비스가 직접 구성합니다. 서비스가 이미 자체 Registry로 `/metrics`를 서빙하고 있다면, `registerMetricsInto()`로 kafka-forge 지표를 그 Registry에 합쳐서 엔드포인트 하나로 노출할 수 있습니다.

  ```ts
  import { registerMetricsInto } from "@paikpaik/kafka-forge";

  registerMetricsInto(myServiceRegistry); // 이후 기존 /metrics 응답에 kafka_forge_* 지표도 포함됨
  ```
- **프레임워크 비의존**: NestJS, Fastify, Express 등 특정 프레임워크에 종속되지 않습니다.

## 로컬 개발

```bash
docker compose up -d   # Redpanda(로컬 Kafka 호환 브로커) + 웹 콘솔(localhost:8080)
npm install
npm test               # vitest, 실제 브로커 없이 fake kafkajs로 검증
npm run lint            # eslint
npm run format          # prettier --write
npm run build
```

`main` 브랜치 push와 PR마다 GitHub Actions(`.github/workflows/ci.yml`)가 lint/format/test/build를 검증합니다.

## 이 프로젝트의 배경

`kafka-core-project-plan.md`와 `docs/`에 이 라이브러리를 만들며 Kafka를 학습한 과정(파티션, 컨슈머 그룹 리밸런싱, 재시도/DLQ, Outbox 패턴, 옵저버빌리티 실험 기록)이 남아있습니다.

## License

MIT
