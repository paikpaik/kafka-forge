# kafka-forge-hardening-p1 — 평가에서 나온 P0/P1 항목을 순서대로 처리

## 목표

kafka-forge 평가에서 나온 나머지 항목들(테스트/멀티토픽/DLQ 헤더는 이미 완료)을 순서대로 처리한다: `process.exit` 제거, 파티션 병렬 처리 옵션 노출, 재시도 불가 에러 구분, 멱등성 스토어 TTL, idempotent producer 옵션, OTel 시맨틱 컨벤션 속성. 항목마다 작게 끊어서 진행하고, 이 파일에 실행 이력을 계속 추가한다.

## 위험도

**LOW~MEDIUM** — 대부분 옵션 추가(하위 호환)이지만, `registerShutdown()` 기본 동작 변경은 pre-1.0 기준 허용 가능한 breaking change.

## 검증 방법

각 항목마다 `npm test` + `npm run build` 통과.

---

## 항목별 실행 이력

### 1. registerShutdown()의 process.exit(0) 강제 호출 제거 — 완료 (2026-07-11)

**결과**: 성공

**변경 파일**:
- `src/consumer.ts` — `registerShutdown(options: ShutdownOptions = {})` 추가, 기본값 `exitProcess: false`로 변경 (기존엔 항상 `process.exit(0)` 호출). `{ exitProcess: true }`로 옵트인 가능.
- `src/index.ts` — `ShutdownOptions` export 추가
- `src/consumer.test.ts` — 기본값(종료 안 함)/옵트인(종료함) 2개 테스트 추가, `process.exit`/`process.on`을 스파이로 캡처해 실제 시그널 없이 핸들러 직접 호출
- `README.md` — 기본 동작 변경 및 옵트인 방법 설명 추가

**Why**: 라이브러리가 `process.exit()`을 직접 호출하면 같은 프로세스의 다른 리소스(다른 consumer, HTTP 서버 등)가 자기 정리 없이 강제 종료된다. 기본값을 안전한 쪽(종료 안 함)으로 바꾸고, 진짜 그 프로세스에 이것 하나뿐인 경우에만 옵트인하게 함.

**Breaking Change**: `registerShutdown()`의 기본 동작이 "항상 종료"에서 "종료 안 함"으로 바뀜. pre-1.0이라 허용.

**검증**: `npm test`(30개 통과), `npm run build` 성공.

---

### 2. partitionsConsumedConcurrently 옵션 노출 — 완료 (2026-07-11)

**결과**: 성공

**변경 파일**:
- `src/consumer.ts` — `run(options: RunOptions = {})` 추가, `partitionsConsumedConcurrently`를 kafkajs `consumer.run()`으로 그대로 전달
- `src/index.ts` — `RunOptions` export
- `src/consumer.test.ts` — 옵션이 kafkajs로 그대로 전달되는지 확인하는 테스트 추가
- `README.md` — 사용법 한 줄 추가

**검증**: `npm test`(31개), `npm run build` 성공.

### 3. 재시도 불가 에러 구분(NonRetryableError) — 완료 (2026-07-11)

**결과**: 성공

**변경 파일**:
- `src/errors.ts` — 신규, `NonRetryableError extends Error`
- `src/consumer.ts` — 재시도 루프에서 `err instanceof NonRetryableError`면 남은 재시도 없이 바로 DLQ로 이동
- `src/index.ts` — `NonRetryableError` export
- `src/errors.test.ts`, `src/consumer.test.ts` — 테스트 추가 (5회 재시도 설정에도 1번만 시도하고 DLQ로 가는지 확인)
- `README.md` — 사용 예시 추가

**검증**: `npm test`(33개), `npm run build` 성공.

---

### 4. InMemoryIdempotencyStore TTL/메모리 누수 방지 — 완료 (2026-07-11)

**결과**: 성공

**변경 파일**:
- `src/idempotency.ts` — `Set<string>`에서 `Map<string, number>`(만료 시각)로 변경. `ttlMs`/`sweepIntervalMs` 옵션 추가, 지정 시 주기적으로 만료 키를 실제로 제거하는 `setInterval` 등록(`.unref()`로 프로세스 종료를 막지 않게 함). `stop()`으로 명시적 정리도 가능. TTL 미지정 시(기본값) 기존과 동일하게 영구 보관.
- `src/idempotency.test.ts` — TTL 만료/TTL 미지정 시 영구 보관 테스트 추가 (`vi.useFakeTimers()`)
- `README.md` — 사용법 및 기본값 주의사항 추가

**Why**: offset 기반 키는 한 번 조회하고 다시는 안 읽으므로, 읽을 때만 만료 체크하는 "지연 삭제" 방식으로는 메모리가 줄지 않는다. 주기적으로 훑어서 지우는 능동적 sweep이 필요했음.

**검증**: `npm test`(35개), `npm run build` 성공.

---

### 5. idempotent producer 옵션 노출 — 완료 (2026-07-11)

**결과**: 성공

**변경 파일**:
- `src/producer.ts` — 생성자가 kafkajs `ProducerConfig`를 그대로 받도록 변경 (`new StandardProducer(kafka, { idempotent: true })`)
- `src/outbox.ts` — `OutboxPublisher` 생성자도 세 번째 인자로 동일하게 받도록 변경
- `src/producer.test.ts`, `src/outbox.test.ts` — 옵션이 `kafka.producer()`로 그대로 전달되는지 확인하는 테스트 추가
- `README.md` — 사용법 추가

**검증**: `npm test`(37개), `npm run build` 성공.

### 6. OTel 시맨틱 컨벤션 속성 추가 — 완료 (2026-07-11)

**결과**: 성공

**변경 파일**:
- `src/tracing.ts` — `withProducerSpan`/`withConsumerSpan`에 `key` 파라미터 추가, span에 `messaging.system`(kafka)/`messaging.destination.name`(topic)/`messaging.operation`(publish|process)/`messaging.kafka.message.key` 속성 부여. 패키지 하나 더 추가하기엔 가벼워서 `@opentelemetry/semantic-conventions` 대신 속성 키를 문자열 상수로 직접 둠.
- `src/producer.ts`, `src/outbox.ts`, `src/consumer.ts` — 호출부에 key 전달하도록 수정

**계획과의 차이**: span 속성값 검증 테스트는 추가하지 않음 — OTel span 내용을 검증하려면 `@opentelemetry/sdk-trace-base`의 `InMemorySpanExporter` 등 별도 테스트 인프라가 필요해서, 이번 스코프에서는 "빌드/기존 테스트가 깨지지 않는다"까지만 확인. 필요해지면 추후 별도로 추가.

**검증**: `npm test`(37개, 전부 기존 테스트 무사 통과), `npm run build` 성공.

### 7. Lint/Prettier/CI — 완료 (2026-07-11)

**결과**: 성공

**변경 파일**:
- `.eslintrc.js` — node-forge와 동일한 eslint 8.x + `@typescript-eslint` 구성 (no-explicit-any는 warn, no-unused-vars는 error)
- `tsconfig.eslint.json` — 신규. 루트 `tsconfig.json`은 테스트 파일을 build에서 제외하는데, ESLint의 타입 인식 파서는 lint 대상 파일이 tsconfig `include`에 없으면 깨지므로 테스트 파일까지 포함하는 ESLint 전용 tsconfig를 분리함
- `.prettierrc`, `.prettierignore` — 스타일은 node-forge와 다르게 **기존 kafka-forge 스타일 유지**(세미콜론 O, 더블쿼트) — 사용자가 명시적으로 선택, 전체 리포맷 diff 방지
- `package.json` — `lint`/`lint:fix`/`format`/`format:check` 스크립트 추가
- `src/consumer.ts`, `src/schema-export.ts`, `src/consumer.test.ts`, `src/outbox.test.ts` — `prettier --write`로 자동 포맷 (스타일만 변경, 로직 변경 없음)
- `.github/workflows/ci.yml` — 신규. `main` push + PR마다 lint/format-check/test/build 실행. node-forge의 publish.yml과 달리 배포는 하지 않는 순수 검증용 워크플로우

**계획과의 차이**:
- CI publish 워크플로우(node-forge의 `publish.yml`처럼 태그 push 시 자동 배포)는 이번 스코프에 포함 안 함 — registry 선택(공개 npm vs GitHub Packages), 패키지 스코프(`kafka-forge` vs `@paikpaik/kafka-forge`) 등 별도 확인이 필요한 결정이라 분리함
- lint 결과 경고 2건(`no-explicit-any`)은 의도적인 사용이라 코드 수정 없이 그대로 둠

**검증**: `npm run lint`(에러 0, 경고 2), `npm run format:check`(통과), `npm test`(37개), `npm run build` 전부 성공.

### 8. 멱등성 키를 비즈니스 키로 지정 가능하게 — 완료 (2026-07-11)

**결과**: 성공

**변경 파일**:
- `src/consumer.ts` — `SubscribeOptions<T = unknown>`로 제네릭화, `dedupeKey?: (payload: T) => string` 추가. `Route<T>`에도 필드 추가. 생략 시 기존과 동일하게 `topic:partition:offset` 사용, 넘기면 `topic:${dedupeKey(payload)}` 사용
- `src/consumer.test.ts` — dedupeKey 지정 시 서로 다른 offset도 같은 비즈니스 키면 중복 스킵되는 테스트, 생략 시 기존 동작(offset마다 별개) 유지되는 테스트 추가
- `README.md` — 사용법 추가

**Why**: Outbox가 같은 이벤트를 서로 다른 offset으로 두 번 발행하는 경우(at-least-once 특성상 가능), offset 기반 키로는 중복을 못 잡는다. dedupeKey를 열어서 비즈니스 식별자(orderId 등) 기준으로도 막을 수 있게 함.

**검증**: `npm test`(39개), `npm run lint`(에러 0), `npm run format:check`, `npm run build` 전부 성공.

### 9. OTel span 속성 검증 테스트 — 완료 (2026-07-11)

**결과**: 성공

**변경 파일**:
- `src/tracing.test.ts` — 신규. `@opentelemetry/sdk-trace-base`의 `InMemorySpanExporter`+`BasicTracerProvider`를 전역 TracerProvider로 등록해서 실제 span을 메모리에 수집, `withProducerSpan`/`withConsumerSpan`을 직접 검증. `@opentelemetry/core`의 `W3CTraceContextPropagator`, `@opentelemetry/context-async-hooks`의 `AsyncLocalStorageContextManager`도 함께 전역 등록해야 `context.active()`/`propagation.inject·extract`가 실제로 동작함 (등록 안 하면 no-op).
- `package.json` — `@opentelemetry/sdk-trace-base`, `@opentelemetry/core`, `@opentelemetry/context-async-hooks` devDependency 추가

**검증 내용**: produce/consume span의 name·kind·messaging 시맨틱 컨벤션 속성, 에러 시 상태/예외 기록, `injectTraceHeaders()`가 실제 W3C traceparent 포맷을 만드는지, **produce span → consume span이 같은 traceId·부모-자식 관계로 이어지는지**(Phase 5에서 Jaeger로 눈으로만 확인했던 걸 자동화된 테스트로 고정).

**계획과의 차이**: 처음엔 provider 등록만 하면 될 줄 알았는데, `context.active()`가 `NoopContextManager` 때문에 항상 undefined를 반환하는 문제가 있어 `AsyncLocalStorageContextManager` 등록이 추가로 필요했음. `propagation.inject()`도 전역 Propagator 미등록 시 빈 헤더를 반환해서 `W3CTraceContextPropagator` 등록도 필요했음 — OTel 테스트 인프라를 제대로 갖추려면 provider/propagator/context manager 3개를 다 등록해야 한다는 걸 확인.

**검증**: `npm test`(45개), `npm run lint`(에러 0), `npm run format:check`, `npm run build` 전부 성공.

## 남은 항목

### 10. npm publish 워크플로우 (GitHub Packages, @paikpaik 스코프) — 완료 (2026-07-11)

**결과**: 성공

**변경 파일**:
- `package.json` — `name`을 `kafka-forge` → `@paikpaik/kafka-forge`로 변경, `repository`/`publishConfig.registry`(GitHub Packages) 추가
- `.npmrc` — 신규, `@paikpaik:registry=https://npm.pkg.github.com` (node-forge와 동일)
- `.github/workflows/publish.yml` — 신규. node-forge의 `publish.yml`과 동일 패턴(`v*` 태그 push 시 발행)이되, lint/format-check 단계를 추가로 넣음(node-forge에는 없던 단계 — kafka-forge에 이미 있는 CI 품질 게이트를 publish 전에도 한 번 더 거치게 함)
- `README.md` — 설치 안내에 `.npmrc` 등록 방법 추가, 모든 `from "kafka-forge"` import를 `from "@paikpaik/kafka-forge"`로 일괄 변경
- `.claude/CLAUDE.md` — 패키지명 갱신

**결정 배경**: 사용자가 GitHub Packages로 진행하기로 결정, node-forge와 동일한 `@paikpaik` 스코프 컨벤션을 따름 (forge 시리즈 전체가 같은 레지스트리/스코프 규칙 공유).

**아직 안 한 것**: 실제 `v*` 태그를 찍어서 발행을 트리거하는 것은 이번에 하지 않음 — node-forge도 같은 상태(워크플로우는 준비됐지만 태그를 찍은 적 없음)라 동일하게 "준비만 해두고 실제 발행은 보류"로 맞춤. 필요할 때 `git tag v0.1.0 && git push origin v0.1.0`으로 트리거.

**검증**: `npm test`(45개), `npm run lint`, `npm run build` 전부 성공. 패키지명 변경이 내부 import(전부 상대경로)에 영향 없음을 확인.

---

(완료 — kafka-forge 고도화 작업 전체 마무리)
