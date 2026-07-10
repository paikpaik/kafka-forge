## 플랜 실행 이력

### 완료: 2026-07-10

**결과**: 성공

**실제 변경 파일**:
- `scripts/kafka_forge/setup/schema.sql` — 신규 (경로가 초안 `docs/phase4-schema.sql`에서 사용자 요청으로 `scripts/kafka_forge/setup/schema.sql`로 변경됨), 사용자가 직접 실행하고 권한(GRANT)도 직접 부여함
- `packages/kafka-forge/src/outbox.ts` — 신규, `OutboxRecord`/`OutboxStore`/`OutboxPublisher`
- `packages/kafka-forge/src/index.ts` — export 추가
- `.env.example` — 신규 (플레이스홀더만), `.gitignore`에 `.env` 추가
- `.env` — 사용자가 직접 생성/기입 (assistant는 만들지 않음)
- `services/order-service/src/db.ts`, `src/index.ts` — mysql2 기반으로 재작성, Kafka 직접 발행 제거하고 DB 트랜잭션(orders+outbox insert)으로 교체
- `services/order-service/package.json` — `kafka-forge`/`kafkajs` 의존성 제거, `mysql2` 추가
- `services/outbox-relay/` — 신규 서비스 (`package.json`, `src/db.ts`, `src/outbox-store.ts`, `src/index.ts`)
- `package.json` (루트) — `dotenv-cli` 추가, `phase4:build-core`/`phase4:order-service`/`phase4:outbox-relay` 스크립트 추가
- `docs/phase4-notes.md` — 신규, 실험 결과 기록

**계획과의 차이**:
- SQL 스키마 파일 위치를 `docs/`에서 `scripts/kafka_forge/setup/`으로 변경 (사용자 요청)
- 플랜 초안에 MySQL 계정명(`rewardlocal`)이 노출되어 있던 것을 사용자가 지적해 제거함 — 이후 계정명도 노출 금지 원칙으로 메모리에 기록 (`secrets-handling` 메모리 참고)
- DDL 실행 중 사용자 계정에 `kafka_forge` DB 권한이 없어 GRANT 처리가 추가로 필요했음 (계획에 없던 단계지만 사소한 이슈)

**잔존 작업**:
- 없음. Phase 5(OTel/Prometheus, 파티션-처리량 실측)로 이어감.

---

# phase4-outbox-pattern — DB 트랜잭션과 Kafka 발행 사이의 정합성 문제를 Outbox 패턴으로 해결

## 목표

`kafka-core-project-plan.md`의 Phase 4를 실행한다. `order-service`가 "주문 저장(DB)"과 "이벤트 발행(Kafka)"을 각각 따로 수행하면, 그 사이 프로세스가 죽었을 때 DB엔 있는데 이벤트는 안 나간(혹은 그 반대) 정합성 문제가 생긴다. 이를 실제로 재현 가능한 구조로 만들고, Outbox 패턴(같은 DB 트랜잭션에 "발행할 이벤트"도 같이 저장한 뒤 별도 프로세스가 폴링해서 발행)으로 해결한다.

## 현재 상태 (AS-IS)

`services/order-service/src/index.ts`는 `StandardProducer.send()`로 DB 없이 바로 Kafka에 발행한다 (Phase 2~3 상태). DB 저장이라는 개념 자체가 없어서 정합성 문제가 재현되지 않는다.

## 변경 후 상태 (TO-BE)

- 로컬에 이미 떠 있는 MySQL에 `kafka_forge` 전용 DB를 새로 만들어 `orders`, `outbox` 두 테이블을 둔다. 접속 자격증명(계정/비밀번호)은 `.env`로만 관리하며 플랜/코드 어디에도 노출하지 않는다.
- `order-service`는 주문 생성 시 **하나의 트랜잭션**으로 `orders`에 INSERT + `outbox`에 (topic, key, payload) INSERT만 하고, Kafka에는 더 이상 직접 발행하지 않는다.
- 신규 `services/outbox-relay`가 주기적으로 `outbox` 테이블에서 `published=false`인 행을 읽어 Kafka로 발행하고, 성공하면 `published=true`로 마크한다.
- `kafka-forge` 코어에는 `OutboxStore` 인터페이스(`fetchPending`/`markPublished`)와 `OutboxPublisher`(폴링→발행→마크를 캡슐화)만 추가한다. MySQL 관련 코드는 전부 `outbox-relay` 서비스 쪽에만 존재한다 (IdempotencyStore와 동일한 "인터페이스는 코어, 구현은 서비스" 원칙).

```ts
// packages/kafka-forge/src/outbox.ts
export interface OutboxRecord {
  id: string | number;
  topic: string;
  key: string;
  payload: unknown;
}
export interface OutboxStore {
  fetchPending(limit: number): Promise<OutboxRecord[]>;
  markPublished(ids: Array<string | number>): Promise<void>;
}
export class OutboxPublisher {
  constructor(kafka: Kafka, private store: OutboxStore) { ... }
  async publishPending(limit = 50): Promise<number> { ... } // fetch → producer.send → markPublished
}
```

## 변경 범위

| 파일 | 변경 내용 |
|------|----------|
| `packages/kafka-forge/src/outbox.ts` | 신규 — `OutboxRecord`, `OutboxStore`, `OutboxPublisher` |
| `packages/kafka-forge/src/index.ts` | export 추가 |
| `.env.example` (루트) | 신규 — `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME` 플레이스홀더 (실제 값 없음, 커밋 대상) |
| `.gitignore` | `.env` 추가 (실제 자격증명 파일은 커밋 금지) |
| `scripts/kafka_forge/setup/schema.sql` | 신규 — `kafka_forge` DB 생성 DDL, `orders`/`outbox` 테이블 정의. **사용자가 직접 실행** |
| `services/order-service/src/db.ts` | 신규 — `mysql2` 커넥션 풀 (env var 기반 설정, 자격증명은 코드에 없음) |
| `services/order-service/src/index.ts` | Kafka 직접 발행 제거, DB 트랜잭션(orders+outbox insert)으로 변경 |
| `services/outbox-relay/` | 신규 서비스 — MySQL 기반 `OutboxStore` 구현체 + `OutboxPublisher`로 폴링 루프 실행 |
| `package.json` (루트) | `dotenv-cli` devDependency 추가, `phase4:*` 스크립트 추가 |
| `docs/phase4-notes.md` | 신규 — 실험 결과 기록 (Phase 1~3과 동일 패턴) |

## 영향성

| 영향 대상 | 영향 내용 |
|-----------|----------|
| `notification-service` | 변경 없음 — 여전히 `order.created.v1`을 구독. 발행 주체가 `order-service`(직접)에서 `outbox-relay`(경유)로 바뀌지만 토픽/이벤트 구조는 동일해서 컨슈머 입장에서는 차이 없음 |
| `packages/kafka-forge` | `mysql2` 등 DB 드라이버 의존성 추가 없음 — 인터페이스만 추가 |
| 기존 `phase1`, `phase2`, `phase3` 스크립트 | 영향 없음 |

## Breaking Changes

`order-service`의 동작이 바뀐다 — 더 이상 즉시 Kafka로 발행하지 않고 DB에 먼저 쓴 뒤 `outbox-relay`가 비동기로 발행한다. 즉 발행까지 폴링 주기만큼의 지연(예: 2초)이 생긴다. 이건 Outbox 패턴 자체의 트레이드오프(정합성을 얻는 대신 약간의 지연 감수)이므로 의도된 변경.

## 위험도

**MEDIUM** — 새로운 외부 의존성(로컬 MySQL)이 추가되고, `order-service`의 핵심 동작이 바뀐다. 다만 로컬 개발 DB이고 `kafka_forge` 전용 스키마로 완전히 격리되어 있어 다른 프로젝트 데이터에 영향 없음.

## 주의사항

- **DB 자격증명(계정명/비밀번호 포함)을 코드/커밋/플랜 문서 어디에도 노출하지 않는다.** `.env`(gitignore 처리)로만 관리하고, `.env.example`에는 플레이스홀더만 남긴다.
- `kafka_forge` DB와 `orders`/`outbox` 테이블 생성은 `scripts/kafka_forge/setup/schema.sql`을 작성해서 제공하고, **실제 실행은 사용자가 직접 한다** (assistant가 DDL을 대신 실행하지 않음).
- `outbox-relay`가 발행에 성공했지만 `markPublished` 호출 전에 죽으면 같은 이벤트가 중복 발행될 수 있다 (at-least-once) — 이건 Phase 3에서 만든 컨슈머 쪽 멱등성(`IdempotencyStore`)으로 흡수되는 영역이라, 이번 Phase에서 새로 막을 필요는 없음을 문서에 명시한다.
- `packages/kafka-forge`는 여전히 `mysql2` 등 특정 DB 드라이버에 의존하지 않는다 (인터페이스만 제공).

## 작업 단계

### 1단계: 스키마 준비
1. `scripts/kafka_forge/setup/schema.sql` 작성 (`kafka_forge` DB, `orders`, `outbox` 테이블)
2. 사용자가 직접 실행하도록 안내

### 2단계: kafka-forge 코어 확장
1. `outbox.ts` — `OutboxRecord`/`OutboxStore`/`OutboxPublisher`
2. `index.ts` export 추가, 빌드

### 3단계: 환경변수/시크릿 분리
1. `.env.example` 작성, `.gitignore`에 `.env` 추가
2. 루트에 `dotenv-cli` 추가, `phase4:*` npm 스크립트에서 `.env` 로드하도록 구성

### 4단계: order-service 변경
1. `mysql2` 의존성 추가, `db.ts`(커넥션 풀)
2. 주문 생성 로직을 "Kafka 직접 발행"에서 "DB 트랜잭션(orders+outbox)"으로 교체

### 5단계: outbox-relay 신규 서비스
1. MySQL 기반 `OutboxStore` 구현 (`fetchPending`: `SELECT ... WHERE published=false LIMIT ...`, `markPublished`: `UPDATE ... SET published=true`)
2. `OutboxPublisher`로 폴링 루프(예: 2초 간격) 실행

### 6단계: 통합 검증
1. order-service 실행 → DB에 orders/outbox 행이 쌓이는지 확인 (Kafka엔 아직 안 감)
2. outbox-relay 실행 → outbox 행이 Kafka로 발행되고 `published=true`로 바뀌는지 확인
3. notification-service가 정상적으로 이벤트를 받는지 확인 (Phase 2/3와 동일하게)

## 검증 방법

- order-service만 실행한 상태에서 `SELECT * FROM outbox WHERE published=false`로 미발행 행이 쌓이는지 확인 (Kafka로는 아직 안 나간 상태 재현)
- outbox-relay 실행 후 해당 행들이 `published=true`로 바뀌고, notification-service 로그에 알림이 찍히는지 확인
- `rpk topic describe order.created.v1 -p`로 실제 offset이 outbox-relay 발행 시점에 맞춰 늘어나는지 확인

## 참조 규칙

- `.claude/rules/project/convention.md` — kafka-forge 독립성 원칙(DB 드라이버 비의존)을 Outbox에도 동일 적용
- `.claude/rules/common/principles.md` — 의존성 최소화, Phase 스코프 준수
- `.claude-plans/20260710/phase3-reliability-features.md` — IdempotencyStore와 동일한 "인터페이스는 코어, 구현은 서비스" 패턴을 OutboxStore에도 적용
