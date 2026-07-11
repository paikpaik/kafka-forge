## 플랜 실행 이력

### 완료: 2026-07-10

**결과**: 성공

**실제 변경 파일**:
- `packages/kafka-forge/src/idempotency.ts` — 신규, `IdempotencyStore` 인터페이스 + `InMemoryIdempotencyStore`
- `packages/kafka-forge/src/topic-name.ts` — `toDlqTopicName()` 추가
- `packages/kafka-forge/src/consumer.ts` — `subscribe()`에 `retry`/`idempotencyStore` 옵션 추가, 지수 백오프 재시도, DLQ 발행, `registerShutdown()` 추가
- `packages/kafka-forge/src/index.ts` — 신규 타입/클래스 export 추가
- `services/notification-service/src/index.ts` — `registerShutdown()` 적용, 실험용 실패 케이스(영구 실패/일시적 실패) 추가, `idempotencyStore` 연결
- `src/phase3/setup-dlq-topic.ts` — 신규, `order.created.v1.dlq` 토픽 생성
- `package.json` — `phase3:build-core`, `phase3:setup-dlq-topic` 스크립트 추가
- `docs/phase3-notes.md` — 신규, 실험 결과 기록

**계획과의 차이**:
- 재시도 기본값을 "켜짐"으로 확정 (사용자 확인 후 진행) — 플랜 초안에서 열어뒀던 질문 해소
- 멱등성은 코드 구현은 완료했지만, "같은 오프셋을 재처리시켜 스킵되는지" 실측 검증은 생략함 (in-memory 저장소 특성상 프로세스 재시작 없이 재현하려면 `consumer.seek()` 노출이 필요한데 v0 스코프 밖이라 판단, `docs/phase3-notes.md`에 한계로 기록)

**잔존 작업**:
- 멱등성 스킵 동작의 실측 검증 (필요 시 별도로 `consumer.seek()` 노출 여부 논의)
- 없으면 Phase 4(Outbox 패턴)로 이어감

---

# phase3-reliability-features — StandardConsumer에 재시도+DLQ, 멱등성, graceful shutdown 추가

## 목표

`kafka-core-project-plan.md`의 Phase 3(신뢰성 기능)을 실행한다. Phase 2의 `StandardConsumer`는 스키마 검증까지만 하고, handler가 예외를 던지면 그대로 kafkajs 기본 동작(전체 배치 재시도/크래시)에 맡겨져 있다. 여기에 앱 레벨 재시도+DLQ, 메시지 중복 처리 방지(멱등성), graceful shutdown을 코어 모듈에 표준 기능으로 추가해서, 레퍼런스 서비스가 이 세 가지를 따로 구현할 필요 없게 만든다.

## 현재 상태 (AS-IS)

`packages/kafka-forge/src/consumer.ts`:
```ts
await this.consumer.run({
  eachMessage: async ({ message }) => {
    const raw = message.value?.toString();
    if (!raw) return;
    const parsed = event.schema.safeParse(JSON.parse(raw));
    if (!parsed.success) { console.error(...); return; }
    await handler(parsed.data);   // 여기서 던지면 kafkajs 기본 재시도/크래시로 넘어감
  },
});
```
- handler 실패 시 앱 레벨 재시도 없음, DLQ 없음
- 메시지 중복 처리 방지 수단 없음 (재시도/재배정 시 같은 메시지를 handler가 두 번 실행할 수 있음)
- graceful shutdown은 Phase 1 raw 스크립트에서만 수동으로 구현되어 있고, `StandardConsumer`엔 없음

## 변경 후 상태 (TO-BE)

`StandardConsumer.subscribe()`가 옵션을 받아 재시도+DLQ+멱등성을 자동 처리:

```ts
await consumer.subscribe(OrderCreated, handler, {
  retry: { attempts: 3, initialBackoffMs: 1000 },   // 기본값, 생략 가능
  idempotencyStore: new InMemoryIdempotencyStore(), // 생략 시 멱등성 체크 안 함
});
consumer.registerShutdown(); // SIGINT/SIGTERM 수신 시 정상 탈퇴
```

- handler가 실패하면 지수 백오프(1s→2s→4s)로 최대 3회 재시도
- 그래도 실패하면 원본 메시지+실패 사유를 `<topic>.dlq` 토픽에 발행하고 원본 파티션은 다음 메시지로 진행 (무한 재시도로 인한 컨슈머 정지 방지)
- `idempotencyStore`를 넘기면 `wasProcessed(key)` → 이미 처리했으면 handler 호출 스킵, 처리 성공 후 `markProcessed(key)` 호출. 기본 키는 `${topic}:${partition}:${offset}`.
- `registerShutdown()` 한 줄로 graceful shutdown 적용

## 변경 범위

| 파일 | 변경 내용 |
|------|----------|
| `packages/kafka-forge/src/idempotency.ts` | 신규 — `IdempotencyStore` 인터페이스, `InMemoryIdempotencyStore` 기본 구현 (Map 기반, 외부 의존성 없음) |
| `packages/kafka-forge/src/topic-name.ts` | `toDlqTopicName(topic)` 헬퍼 추가 — `<topic>.dlq` 생성, 기존 `assertValidTopicName` 패턴 검사는 우회(내부 파생 이름이므로) |
| `packages/kafka-forge/src/consumer.ts` | `subscribe()`에 3번째 옵션 인자(`retry`, `idempotencyStore`) 추가, 재시도/DLQ 로직 구현, `registerShutdown()` 메서드 추가 |
| `packages/kafka-forge/src/index.ts` | `IdempotencyStore`, `InMemoryIdempotencyStore`, `toDlqTopicName` export 추가 |
| `services/notification-service/src/index.ts` | `registerShutdown()` 적용, (실습용으로) 일부러 실패하는 처리 케이스를 넣어 재시도+DLQ 흐름 확인 |
| `src/phase3/setup-dlq-topic.ts` | 신규 — `order.created.v1.dlq` 토픽 생성 스크립트 |
| `docs/phase3-notes.md` | 신규 — 실험 결과 기록 (Phase 1/2와 동일한 패턴) |

## 영향성

| 영향 대상 | 영향 내용 |
|-----------|----------|
| `order-service` | 변경 없음 (Producer 쪽은 이번 스코프 아님) |
| 기존 `subscribe()` 호출부 | `retry`/`idempotencyStore` 옵션이 전부 선택값이라 기존 호출(`consumer.subscribe(OrderCreated, handler)`)은 그대로 동작. 다만 기본 옵션을 켤지(재시도 기본 3회 적용) 안 켤지는 아래 "주의사항"에서 확정 필요 |
| `src/phase1`, `src/phase2` raw 스크립트 | 영향 없음 |

## Breaking Changes

컴파일 관점에서는 없음(`subscribe()` 새 파라미터는 optional). 다만 **동작 변경**이 있음: 기존 호출부(`consumer.subscribe(OrderCreated, handler)`)도 이제 handler 실패 시 자동으로 3회 재시도+DLQ 발행이 적용된다 (Phase 2에서는 실패 시 kafkajs 기본 동작으로 넘어갔던 것과 다름). 재시도 없이 Phase 2와 완전히 같은 동작을 원하면 `{ retry: false }`를 명시해야 한다.

## 위험도

**MEDIUM** — `StandardConsumer`는 이미 레퍼런스 서비스(notification-service)가 쓰고 있는 공용 모듈이라, 옵션 기본값을 잘못 정하면 기존 동작이 조용히 바뀔 수 있음. 재시도 로직 자체도 백오프 타이밍/무한 루프 방지 로직에 버그가 있으면 컨슈머가 멈출 수 있어 신중히 구현.

## 주의사항

- **kafka-forge 코어는 Redis 등 특정 저장소나 `node-forge`에 의존하지 않는다** (사용자 확정 원칙, [[forge-series-direction]] 메모리 참고). `IdempotencyStore`는 인터페이스 + 인메모리 기본 구현만 코어에 두고, 영속 저장소(Redis 등) 구현은 필요하면 서비스 쪽에서 인터페이스를 구현해 넘기게 한다.
- 재시도 중 `eachMessage`가 너무 오래 걸리면 컨슈머 그룹 하트비트 타임아웃으로 불필요한 리밸런싱이 트리거될 수 있다 — 대기는 시도 횟수보다 1번 적게 일어나므로(마지막 시도 후에는 대기 없이 DLQ), 3회 기준 총 대기 시간(1+2=3초)이 세션 타임아웃보다 짧은지 확인 필요.
- DLQ 토픽도 `createTopicName`과 동일하게 파티션 수를 명시적으로 정해서 생성해야 한다 (자동 생성에 맡기지 않음).
- 재시도 기본값: **기본으로 켜짐**(3회 시도, 실패마다 1초→2초 지수 백오프, 마지막 시도 후엔 대기 없이 DLQ). 끄고 싶으면 `{ retry: false }`를 명시적으로 넘겨야 함 (사용자 확정, 2026-07-10).

## 작업 단계

### 1단계: 멱등성 헬퍼
1. `idempotency.ts` — `IdempotencyStore` 인터페이스, `InMemoryIdempotencyStore` 구현
2. `index.ts`에 export 추가

### 2단계: DLQ 토픽 네이밍
1. `topic-name.ts`에 `toDlqTopicName()` 추가
2. `src/phase3/setup-dlq-topic.ts`로 `order.created.v1.dlq` 생성

### 3단계: StandardConsumer 재시도+DLQ+멱등성 통합
1. `subscribe()` 시그니처에 옵션 인자 추가
2. handler 실패 시 지수 백오프 재시도 로직
3. 최종 실패 시 DLQ 발행 로직
4. `idempotencyStore` 훅 연결 (wasProcessed/markProcessed)

### 4단계: graceful shutdown
1. `registerShutdown()` 메서드 추가 (SIGINT/SIGTERM → disconnect)

### 5단계: 레퍼런스 서비스 검증
1. `notification-service`에 일부러 실패하는 케이스 추가해서 재시도 로그 확인
2. 계속 실패하는 케이스로 DLQ 토픽에 실제로 메시지가 쌓이는지 확인
3. 같은 메시지를 오프셋 리셋 등으로 재처리시켜 멱등성 스킵이 동작하는지 확인
4. `Ctrl+C`로 종료해 `registerShutdown()`이 정상 동작하는지 확인

## 검증 방법

- notification-service handler를 일부러 N번 실패하게 만들었을 때, 로그에 재시도 시도(1초→2초 간격)가 찍히는지 확인
- 3회 재시도 후에도 실패하면 `rpk topic describe order.created.v1.dlq -p`로 메시지가 실제로 쌓였는지 확인
- 같은 offset을 가진 메시지를 다시 처리시켰을 때 `InMemoryIdempotencyStore`가 handler 재호출을 막는지 로그로 확인
- `Ctrl+C` 시 "정상 탈퇴" 로그가 찍히고 컨슈머 그룹 리밸런싱이 즉시 일어나는지 확인 (Phase 1 방식과 동일)

## 참조 규칙

- `.claude/rules/project/convention.md` — kafka-forge 독립성 원칙(다른 저장소/모듈 비의존)
- `.claude/rules/common/principles.md` — Phase 스코프 준수, 의존성 최소화
- `.claude-plans/20260710/phase2-core-module-v0.md` — Event Contract/StandardConsumer의 기존 설계 전제
