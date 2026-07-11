## 플랜 실행 이력

### 완료: 2026-07-10

**결과**: 성공

**실제 변경 파일**:
- `package.json` (루트) — `workspaces: ["packages/*", "services/*"]` 및 phase2 관련 스크립트 추가 (name/description은 사용자 요청으로 최종 완성 기준을 유지하되, description만 "코어 모듈" 관점으로 수정)
- `packages/kafka-forge/{package.json, tsconfig.json, src/{index,topic-name,event-contract,producer,consumer}.ts}` — 신규 작성
- `packages/shared-events/{package.json, src/{index,order-created}.ts}` — 신규 작성 (tsconfig 없이 `main: src/index.ts`로 tsx가 직접 실행, 빌드 스텝 생략)
- `services/order-service/{package.json, src/index.ts}` — 신규 작성
- `services/notification-service/{package.json, src/index.ts}` — 신규 작성
- `src/phase2/setup-topics.ts` — 신규 작성 (`order.created.v1` 토픽을 파티션 3개로 생성)

**계획과의 차이**:
- 루트 `package.json`의 `name`/`description`을 플랜대로 "kafka-forge-monorepo"로 바꾸려다 사용자 지시로 철회함 — 이 레포 자체가 최종 `kafka-forge` 결과물이라는 방향이 확정되어, 루트 name은 그대로 `kafka-forge` 유지, description만 "코어 모듈" 포지셔닝으로 수정. 루트와 `packages/kafka-forge`가 이름이 같아도 npm workspaces 설치/링크에는 문제 없음을 확인함.
- `shared-events`는 플랜에 명시되지 않았던 패키지지만, Event Contract를 두 레퍼런스 서비스가 공유하려면 별도 패키지가 필요하다고 판단해 설계 단계에서 추가함 (별도 확인 없이 진행, Phase 2 목표와 직접 부합하는 자연스러운 확장이라 판단).

**잔존 작업**:
- 없음. Phase 3(재시도+DLQ, 멱등성, graceful shutdown)으로 이어감.

---

# phase2-core-module-v0 — 코어 모듈(`kafka-forge`) v0: Producer/Consumer 래핑

## 목표

`kafka-core-project-plan.md`의 Phase 2를 실행한다. Phase 1에서 서비스마다 반복해서 짜야 했던 raw KafkaJS 보일러플레이트(토픽 이름 하드코딩, 파티션 키 전략 개별 구현)를 `node-core-module` 스타일의 npm 패키지 `kafka-forge`로 추출하고, 레퍼런스 서비스 2개에 실제로 설치해서 "표준화가 실제로 되는지" 검증한다.

## 현재 상태 (AS-IS)

- 단일 `package.json` 루트 프로젝트, `src/phase1/`에 raw KafkaJS 스크립트만 존재
- 토픽 이름(`phase1.orders.created`)이 producer/consumer 양쪽에 각각 문자열로 하드코딩되어 있어 오타/불일치 위험이 구조적으로 존재
- 파티션 키 전략, 스키마 검증이 전혀 표준화되어 있지 않음 (매번 직접 짜야 함)

## 변경 후 상태 (TO-BE)

- npm workspaces 모노레포 구조로 전환 (`packages/*`, `services/*`)
- `packages/kafka-forge`가 독립 배포 가능한 npm 패키지로 존재 (모노레포 상대경로 의존 없음, 자체 `package.json`)
- `defineEvent()`로 토픽명+스키마+파티션키 전략을 한 곳에 정의하는 "Event Contract" 패턴 도입
- `StandardProducer.send(eventContract, payload)` / `StandardConsumer.subscribe(eventContract, handler)`로, 스키마 검증과 파티션 키 계산이 자동으로 이루어짐
- `services/order-service`(발행) → `services/notification-service`(소비)가 같은 Event Contract를 import해서 사용, 토픽 이름 불일치가 타입 레벨에서 원천 차단됨

## 변경 범위

| 파일 | 변경 내용 |
|------|----------|
| `package.json` (루트) | `workspaces: ["packages/*", "services/*"]` 추가, private 유지 |
| `packages/kafka-forge/package.json` | 신규 작성, `name: "kafka-forge"`, `version: "0.1.0"` |
| `packages/kafka-forge/src/topic-name.ts` | 신규 — `<domain>.<event>.<version>` 컨벤션 강제 헬퍼 |
| `packages/kafka-forge/src/event-contract.ts` | 신규 — `defineEvent({ topic, schema, partitionKey })` |
| `packages/kafka-forge/src/producer.ts` | 신규 — `StandardProducer` |
| `packages/kafka-forge/src/consumer.ts` | 신규 — `StandardConsumer` |
| `packages/kafka-forge/src/index.ts` | 신규 — public API export |
| `services/order-service/*` | 신규 — `order.created.v1` 발행 레퍼런스 서비스 |
| `services/notification-service/*` | 신규 — 위 이벤트 구독 후 로그만 남기는 레퍼런스 서비스 |
| `src/phase1/*` | 변경 없음 (학습용 raw 스크립트로 그대로 유지) |

## 영향성

| 영향 대상 | 영향 내용 |
|-----------|----------|
| `src/phase1` 스크립트 | 영향 없음 — workspaces 밖에 그대로 유지 |
| 기존 `phase1.*` npm 스크립트 | 영향 없음 |
| 향후 Phase 3(신뢰성 기능) | `StandardConsumer`에 재시도/DLQ/멱등성 훅을 얹을 확장 지점을 v0 설계 단계에서 미리 고려해야 함 |

## Breaking Changes

없음 (신규 패키지/서비스 추가, 기존 Phase 1 코드는 건드리지 않음)

## 위험도

**MEDIUM** — 단일 스크립트 구조에서 모노레포 구조로 전환하는 작업이라 workspaces 설정 실수 시 기존 phase1 스크립트 실행에 영향을 줄 수 있음. 코어 모듈 API(`defineEvent`, `StandardProducer/Consumer`)는 Phase 3~5에서 계속 확장될 예정이라 초기 설계가 이후 작업에 영향을 줌.

## 주의사항

- `packages/kafka-forge`는 나중에 별도 레포로 추출 가능해야 하므로, workspace 내 다른 패키지를 상대경로(`../../src/...`)로 참조하지 않는다. 의존성은 전부 `package.json`의 `dependencies`로 명시.
- DLQ/재시도/멱등성/Outbox/OTel은 v0 스코프 아님 (Phase 3~5). 스키마 검증 실패 시 v0에서는 로그만 남기고 스킵.
- 토픽 버전(`v1`, `v2`)을 이름에 박아두는 컨벤션은 이후 스키마 마이그레이션 논의의 전제가 되므로 임의로 바꾸지 않는다.

## 작업 단계

### 1단계: 모노레포 전환
1. 루트 `package.json`에 `workspaces` 필드 추가
2. `packages/`, `services/` 디렉토리 생성

### 2단계: 코어 모듈 v0 구현
1. `topic-name.ts` — 네이밍 컨벤션 검증/생성 함수
2. `event-contract.ts` — `defineEvent()` 구현 (Zod 스키마 + 파티션 키 추출 함수 바인딩)
3. `producer.ts` — `StandardProducer` (내부적으로 kafkajs Producer 래핑, 발행 전 zod parse)
4. `consumer.ts` — `StandardConsumer` (내부적으로 kafkajs Consumer 래핑, 수신 시 zod parse 후 핸들러 호출, 파싱 실패 시 로그 후 스킵)
5. `index.ts`로 public API 정리

### 3단계: 레퍼런스 서비스 2개
1. `services/order-service` — `OrderCreated` Event Contract 정의 + 주기적 가짜 주문 발행
2. `services/notification-service` — 같은 Event Contract import해서 구독 + 콘솔 로그

### 4단계: 통합 검증
1. 두 서비스를 동시에 띄워 실제로 이벤트가 표준 파이프라인을 타고 흐르는지 확인
2. Redpanda Console에서 토픽명이 컨벤션대로(`order.created.v1`) 생성됐는지 확인

## 검증 방법

- `npm run` 스크립트로 order-service, notification-service를 각각 실행했을 때 notification-service 콘솔에 order-service가 발행한 이벤트가 타입 안전하게 파싱되어 로그로 찍히는지 확인
- 일부러 스키마에 안 맞는 payload를 보내봤을 때 producer 단에서 zod 에러로 발행 자체가 막히는지 확인
- `packages/kafka-forge` 안에서 workspace 상대경로 import가 없는지(`grep -r "\.\./\.\./"` 등) 확인 — 추출 가능성 검증

## 참조 규칙

- `kafka-core-project-plan.md` — Phase 2 정의 출처
- `.claude-plans/20260710/phase1-kafka-local-env.md` — Phase 1에서 확인된 파티션/키/컨슈머 그룹 개념이 이 설계의 전제
