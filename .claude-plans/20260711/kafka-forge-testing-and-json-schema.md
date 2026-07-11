## 플랜 실행 이력

### 완료: 2026-07-11

**결과**: 성공

**실제 변경 파일**:
- `vitest.config.ts`, `package.json`(test 스크립트, vitest/zod-to-json-schema 의존성) — 신규/수정
- `src/topic-name.test.ts`, `src/event-contract.test.ts`, `src/idempotency.test.ts`, `src/outbox.test.ts`, `src/producer.test.ts`, `src/consumer.test.ts`, `src/schema-export.test.ts` — 신규, 총 25개 테스트
- `src/schema-export.ts` — 신규, `toJsonSchema()`/`writeJsonSchema()`
- `src/consumer.ts` — 테스트 작성 중 발견한 버그 2건 수정: (1) `retry: false`일 때 handler 실패 시 예외가 그대로 전파돼 DLQ로 안 가던 문제, (2) DLQ 발행 시 원본 `key`와 트레이스 헤더가 유실되던 문제
- `src/index.ts` — `toJsonSchema`, `writeJsonSchema`, `JsonSchemaExport` export 추가
- `tsconfig.json` — `exclude: ["src/**/*.test.ts"]` 추가 (테스트 파일이 `dist/`에 같이 컴파일되던 문제 발견 후 수정)
- `README.md`, `.claude/CLAUDE.md` — 테스트/JSON Schema 섹션 추가

**계획과의 차이**:
- `zod-to-json-schema` 호출부에서 `TS2589: Type instantiation is excessively deep` 빌드 에러 발생 — zod 3.25.x(v4 엔진 내장 전환판)와 `zod-to-json-schema`가 서로 다른 서브패스(`zod` vs `zod/v3`)의 타입을 참조해서 생기는 구조적 충돌로 추정. `as`/`as unknown as` 캐스팅으로는 해결 안 됐고, `zodToJsonSchema` 함수 자체를 단순한 함수 타입(`(schema: any) => Record<string, unknown>`)으로 재할당하는 우회로 해결. 계획에는 없던 트러블슈팅.
- 테스트를 작성하다가 실제 프로덕션 버그 2건을 발견해서 같이 수정함 (계획엔 "테스트 작성"만 있었는데 "버그 수정"이 자연스럽게 딸려옴 — 테스트의 목적이 정확히 이거였음).
- `tsconfig.json`에 테스트 제외 설정이 빠져있던 것도 빌드 검증 중 발견해서 추가.

**잔존 작업**:
- 없음. Lint/Prettier/CI(GitHub Actions)는 이번 스코프에 포함 안 됨 — 평가에서 P0로 짚었던 항목 중 남은 것.

---

# kafka-forge-testing-and-json-schema — vitest 테스트 인프라 도입 + JSON Schema 폴리글랏 지원

## 목표

지난 평가에서 가장 큰 결함으로 지적된 "테스트 0개"를 해소하기 위해 vitest를 도입하고 기존 모듈 전체에 유닛테스트를 작성한다. 동시에, Kafka 이벤트를 다른 언어(Python 등) 서비스도 구독할 수 있도록 Zod 스키마에서 JSON Schema를 추출하는 기능을 추가한다 — 와이어 포맷(JSON)은 그대로 두고, "스키마가 TS 코드 안에만 있어 다른 언어가 알 방법이 없다"는 문제만 해결하는 가벼운 방향(Schema Registry/Avro 도입 대비 저비용).

## 현재 상태 (AS-IS)

`src/` 8개 모듈(475줄) 전체에 테스트 파일이 하나도 없다. `package.json`에 `test` 스크립트도 없다. 이벤트 계약(`defineEvent`)은 Zod 스키마만 갖고 있고, 이를 JSON Schema나 다른 언어가 읽을 수 있는 형태로 내보내는 기능이 없다.

## 변경 후 상태 (TO-BE)

```
src/
├── ...(기존 8개 모듈)
├── schema-export.ts       신규 — toJsonSchema(), writeJsonSchema()
└── *.test.ts               각 모듈별 유닛테스트 (topic-name, event-contract,
                             idempotency, outbox, producer, consumer, schema-export)
vitest.config.ts             신규
```

`npm test`로 전체 스위트 실행, kafkajs 실제 브로커 연결 없이 fake Producer/Consumer/Admin으로 순수 로직만 검증한다.

## 변경 범위

| 파일 | 변경 내용 |
|------|----------|
| `package.json` | `vitest` devDependency 추가, `test`/`test:watch` 스크립트 추가 |
| `vitest.config.ts` | 신규 |
| `src/schema-export.ts` | 신규 — `zod-to-json-schema` 기반 `toJsonSchema(event)`, `writeJsonSchema(event, path)` |
| `src/index.ts` | `toJsonSchema`, `writeJsonSchema` export 추가 |
| `src/topic-name.test.ts` | 신규 |
| `src/event-contract.test.ts` | 신규 |
| `src/idempotency.test.ts` | 신규 |
| `src/outbox.test.ts` | 신규 (fake Kafka/Producer/OutboxStore) |
| `src/producer.test.ts` | 신규 (fake Kafka/Producer) |
| `src/consumer.test.ts` | 신규 (fake Kafka/Consumer/Admin, `eachMessage` 콜백을 캡처해서 직접 호출) |
| `src/schema-export.test.ts` | 신규 |

## 영향성

| 영향 대상 | 영향 내용 |
|-----------|----------|
| 기존 공개 API(`producer.ts`, `consumer.ts` 등) | 로직 변경 없음, 테스트만 추가 |
| `package.json` `dependencies` | `zod-to-json-schema` 추가 (런타임 의존성, 경량) |

## Breaking Changes

없음 — 순수 추가 작업.

## 위험도

**LOW** — 테스트 추가와 새 독립 모듈(schema-export) 추가뿐, 기존 로직을 건드리지 않는다.

## 주의사항

- `consumer.ts`의 `startLagPolling()`이 `subscribe()` 호출 즉시 `setInterval`을 등록하므로, 테스트에서 `consumer.disconnect()`를 반드시 호출해 타이머를 정리한다 (안 하면 vitest가 프로세스 종료를 못 하고 매달릴 수 있음).
- fake Producer/Consumer/Admin은 실제 kafkajs 타입을 최소한으로 만족하는 형태로 작성한다 (전체 인터페이스를 구현할 필요 없이, 코드가 실제로 호출하는 메서드만 스텁).
- `withProducerSpan`/`withConsumerSpan`(OTel)은 SDK가 초기화 안 된 상태에서도 no-op tracer로 동작하므로 테스트에서 별도 모킹 불필요.
- `toJsonSchema()`는 이벤트 계약당 하나씩 호출하는 순수 함수로 두고, "여러 계약을 한 번에 훑어서 내보내는" 자동 탐색/CLI 기능은 이번 스코프에 넣지 않는다 (YAGNI — 필요해지면 다음에 추가).

## 작업 단계

### 1단계: vitest 인프라
1. `vitest` 설치, `vitest.config.ts` 작성
2. `package.json`에 `test`/`test:watch` 스크립트 추가

### 2단계: 기존 모듈 테스트
1. `topic-name.test.ts` — 유효/무효 토픽명, DLQ 이름 변환
2. `event-contract.test.ts` — `defineEvent` 정상/토픽명 위반 시 예외
3. `idempotency.test.ts` — `InMemoryIdempotencyStore`의 wasProcessed/markProcessed
4. `outbox.test.ts` — `OutboxPublisher.publishPending()` 정상/빈 목록 케이스
5. `producer.test.ts` — 스키마 검증 통과/실패, producer.send 호출 인자 검증
6. `consumer.test.ts` — 스키마 검증 실패 스킵, 멱등성 스킵, 재시도 성공/소진, DLQ 발행

### 3단계: JSON Schema 내보내기 기능
1. `zod-to-json-schema` 의존성 추가
2. `schema-export.ts` — `toJsonSchema()`, `writeJsonSchema()`
3. `index.ts` export 추가
4. `schema-export.test.ts`

### 4단계: 검증
1. `npm test` 전체 통과
2. `npm run build` 여전히 성공하는지 확인 (schema-export.ts 포함)
3. README.md에 JSON Schema 내보내기 사용법 섹션 추가

## 검증 방법

- `npm test`가 전체 통과하고, consumer 관련 테스트에서 열린 타이머로 인해 프로세스가 안 끝나는 문제가 없어야 함
- `toJsonSchema(OrderCreated)` 형태 호출 시 유효한 JSON Schema 객체가 반환되는지 실제로 한번 실행해서 확인
- `npm run build` 성공

## 참조 규칙

- `.claude/rules/common/principles.md` — 의존성 최소화 (zod-to-json-schema는 코드 생성/도구용 경량 라이브러리로 원칙에 부합)
- `.claude/rules/project/convention.md` — 프레임워크/저장소 비의존 원칙 (Schema Registry 대신 JSON Schema를 택한 이유)
