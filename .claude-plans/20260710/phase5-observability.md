## 플랜 실행 이력

### 완료: 2026-07-10

**결과**: 성공

**실제 변경 파일**:
- `docker-compose.yml` — Jaeger, Prometheus, Grafana 컨테이너 추가 (Grafana는 3000번 포트가 다른 프로세스에 점유돼 있어 3001로 변경)
- `scripts/kafka_forge/observability/prometheus.yml`, `grafana/provisioning/**`, `grafana/dashboards/kafka-forge.json` — 신규
- `packages/kafka-forge/src/tracing.ts`, `src/metrics.ts` — 신규
- `packages/kafka-forge/src/producer.ts`, `src/consumer.ts`, `src/outbox.ts` — 트레이싱/메트릭 통합
- `packages/kafka-forge/src/index.ts`, `package.json` — export 및 `@opentelemetry/api`/`prom-client` 의존성 추가
- `services/*/src/tracing.ts`, `src/metrics-server.ts` — 각 서비스에 신규 (OTel NodeSDK 초기화, `/metrics` 서버)
- `services/order-service/package.json` — OTel 관련 패키지 추가, `kafka-forge` 의존성 재추가(metricsRegistry 사용을 위해)
- `src/phase5/run-benchmark.ts` — 신규, 파티션-처리량 벤치마크
- `package.json` (루트) — `phase5:*` 스크립트 추가
- `.claude/rules/project/convention.md` — "계측용 경량 API는 코어 허용, SDK/HTTP 서버는 서비스 책임" 원칙 추가
- `docs/phase5-notes.md` — 신규, 실험 결과 기록

**계획과의 차이**:
- 벤치마크 스크립트를 처음엔 컨슈머 1개로 설계했다가, 파티션 수 차이가 전혀 드러나지 않는 걸 보고 "파티션 수만큼 컨슈머도 병렬로 띄워야 한다"는 걸 깨닫고 재설계함 (계획에 없던 통찰이지만 오히려 더 정확한 실험이 됨)
- `consumer.disconnect()`를 `eachMessage` 콜백 안에서 호출하는 데드락 버그가 있었음 — 벤치마크 스크립트 최초 버전에서 발견, Promise resolve와 disconnect 호출 시점을 분리해서 해결
- Grafana 데이터소스 provisioning에 `uid` 명시가 빠져 있어 컨테이너가 크래시하는 문제가 있었음 — `uid: Prometheus` 명시 및 컨테이너 완전 재생성으로 해결
- 트레이싱은 order-service의 DB 트랜잭션까지는 연결하지 않음 (outbox-relay의 실제 Kafka 발행부터 시작) — 스코프 한계로 명시적으로 문서화함

**잔존 작업**:
- order-service DB 쓰기 시점부터 완전히 연결되는 종단 간 트레이싱은 스코프 아웃 (필요 시 outbox 테이블에 trace context 컬럼 추가 + DB 계측 라이브러리 도입 논의)
- `kafka-core-project-plan.md`의 Phase 1~5는 이걸로 전부 완료. 이후는 `packages/kafka-forge`를 실제 독립 배포 가능한 상태로 다듬는 마감 작업만 남음.

---

# phase5-observability — OTel 트레이싱, Prometheus 메트릭, 파티션-처리량 실측

## 목표

`kafka-core-project-plan.md`의 Phase 5를 실행한다. 지금까지 만든 Produce/Consume 파이프라인에 "관측 가능성"을 표준 기능으로 내장한다: (1) produce span과 consume span이 실제로 연결되는 분산 트레이싱, (2) 컨슈머 랙/처리량/에러율 Prometheus 메트릭 + Grafana 대시보드, (3) 파티션 수를 늘렸을 때 처리량/랙이 실측으로 어떻게 바뀌는지 확인.

## 현재 상태 (AS-IS)

`StandardProducer`/`StandardConsumer`는 발행/소비 동작만 하고 트레이싱, 메트릭 훅이 전혀 없다. 각 서비스는 `console.log`로만 상태를 확인할 수 있고, 요청이 어느 서비스에서 어느 서비스로 흘렀는지 연결해서 볼 방법이 없다.

## 변경 후 상태 (TO-BE)

### 1) 분산 트레이싱
- `kafka-forge`는 `@opentelemetry/api`만 의존한다 (SDK/Exporter 같은 무거운 설정은 의존하지 않음 — 실제 OTel SDK 초기화는 각 서비스 진입점의 책임).
- `StandardProducer.send()`: `kafka.produce <topic>` span을 만들고, `propagation.inject()`로 W3C traceparent를 Kafka 메시지 헤더에 심는다.
- `StandardConsumer`: 메시지 헤더에서 `propagation.extract()`로 context를 복원해 `kafka.consume <topic>` child span을 만들고, 그 컨텍스트 안에서 handler를 실행한다. 재시도/DLQ 실패도 span에 기록한다.
- 각 서비스(`order-service`, `outbox-relay`, `notification-service`)의 entrypoint에서 OTel NodeSDK를 초기화하고 OTLP exporter로 Jaeger에 전송.
- `docker-compose.yml`에 Jaeger all-in-one 컨테이너 추가 (웹 UI: `localhost:16686`).

### 2) Prometheus 메트릭
- `kafka-forge`가 `prom-client` 기반 공유 `Registry`와 표준 메트릭(발행/소비 카운터, 에러 카운터, 처리시간 히스토그램, 컨슈머 랙 게이지)을 제공한다. 랙은 `kafka.admin()`으로 주기적으로 계산.
- `kafka-forge`는 HTTP 서버를 직접 띄우지 않는다 (프레임워크 비의존 원칙 유지) — 각 서비스가 `node:http`로 직접 `/metrics` 엔드포인트를 열어 `kafka-forge`가 제공하는 Registry를 노출한다.
- `docker-compose.yml`에 Prometheus + Grafana 컨테이너 추가, `prometheus.yml`에 각 서비스 스크레이프 타겟 등록, Grafana에 기본 대시보드(컨슈머 랙, 처리량, 에러율 패널) 구성.

### 3) 파티션-처리량 실측
- `src/phase5/`에 벤치마크 스크립트 작성 — 파티션 1개/3개/6개짜리 토픽을 각각 만들어 동일한 양의 메시지를 발행하고, 컨슈머가 다 따라잡는 데 걸리는 시간과 랙 변화를 실측.

## 변경 범위

| 파일 | 변경 내용 |
|------|----------|
| `packages/kafka-forge/src/tracing.ts` | 신규 — span 생성/context 주입·추출 헬퍼 |
| `packages/kafka-forge/src/metrics.ts` | 신규 — 공유 `Registry`, 카운터/히스토그램/게이지 정의, 컨슈머 랙 계산 로직 |
| `packages/kafka-forge/src/producer.ts` | `send()`에 span 생성 + 헤더 주입 + 메트릭 카운터 추가 |
| `packages/kafka-forge/src/consumer.ts` | `eachMessage`에 span 생성/context 복원 + 메트릭(처리시간, 에러, 랙) 추가 |
| `packages/kafka-forge/src/index.ts` | export 추가 (`metricsRegistry`, 트레이싱 헬퍼는 대부분 내부용) |
| `packages/kafka-forge/package.json` | `@opentelemetry/api`, `prom-client` 의존성 추가 |
| `docker-compose.yml` | Jaeger, Prometheus, Grafana 컨테이너 추가 |
| `scripts/kafka_forge/observability/prometheus.yml` | 신규 — 스크레이프 설정 |
| `scripts/kafka_forge/observability/grafana/` | 신규 — 기본 대시보드 프로비저닝 |
| `services/*/src/tracing.ts` | 각 서비스에 OTel NodeSDK 초기화 코드 (신규, 서비스별 진입점에서 import) |
| `services/*/src/metrics-server.ts` | 각 서비스에 `/metrics` 노출용 최소 HTTP 서버 (신규) |
| `src/phase5/setup-benchmark-topics.ts`, `src/phase5/run-benchmark.ts` | 신규 — 파티션 수별 처리량 실측 스크립트 |
| `docs/phase5-notes.md` | 신규 — 실험 결과 기록 |

## 영향성

| 영향 대상 | 영향 내용 |
|-----------|----------|
| 기존 `order-service`/`outbox-relay`/`notification-service` 로직 | 트레이싱/메트릭 계측이 추가되지만 기존 비즈니스 로직(주문 저장, outbox 폴링, 알림 로그)은 그대로 |
| `packages/kafka-forge` 의존성 | `@opentelemetry/api`(경량, 계측 전용), `prom-client`(경량, 계측 전용) 추가 — 특정 인프라 드라이버(Redis/DB)는 여전히 없음 |
| Phase 1~4 스크립트 | 영향 없음 |

## Breaking Changes

없음 — 트레이싱/메트릭은 계측(instrumentation) 추가일 뿐 기존 `subscribe`/`send` 시그니처를 바꾸지 않는다.

## 위험도

**MEDIUM** — `StandardProducer`/`StandardConsumer` 내부 로직에 계측 코드가 섞여 들어가므로, 스팬/메트릭 코드에 버그가 있으면 기존 발행/소비 흐름에도 영향을 줄 수 있다. 새 컨테이너 3개(Jaeger/Prometheus/Grafana) 추가로 로컬 리소스 사용량 증가.

## 주의사항

- `kafka-forge`는 OTel **SDK**(NodeSDK, Exporter 설정)나 HTTP 프레임워크에 의존하지 않는다 — `@opentelemetry/api`(계측용 경량 API)와 `prom-client`(레지스트리/메트릭 타입 정의)까지만 코어 의존성으로 두고, 실제 배선(익스포터 대상, HTTP 서버)은 서비스 책임으로 남긴다.
- 컨슈머 랙 계산은 매 메시지마다 하면 admin API 호출 비용이 크므로 주기적(예: 10초 간격)으로만 갱신한다.
- Jaeger/Prometheus/Grafana는 전부 로컬 Docker 컨테이너로 무료 범위 안에서 실행된다.

## 작업 단계

### 1단계: 인프라 추가
1. `docker-compose.yml`에 Jaeger, Prometheus, Grafana 추가
2. `scripts/kafka_forge/observability/prometheus.yml` 작성

### 2단계: kafka-forge 코어 — 트레이싱
1. `@opentelemetry/api` 의존성 추가
2. `tracing.ts` — span 생성/헤더 주입·추출 헬퍼
3. `producer.ts`/`consumer.ts`에 통합

### 3단계: kafka-forge 코어 — 메트릭
1. `prom-client` 의존성 추가
2. `metrics.ts` — Registry, 카운터/히스토그램/게이지, 랙 계산 로직
3. `producer.ts`/`consumer.ts`에 통합

### 4단계: 서비스 배선
1. 각 서비스에 OTel NodeSDK 초기화(`tracing.ts`) 추가, OTLP exporter를 Jaeger로
2. 각 서비스에 `/metrics` 노출용 최소 HTTP 서버 추가
3. Prometheus 스크레이프 타겟 등록, Grafana 기본 대시보드 구성

### 5단계: 파티션-처리량 실측
1. 벤치마크 토픽(파티션 1/3/6) 생성 스크립트
2. 동일 물량 발행 후 컨슈머 캐치업 시간/랙 실측 스크립트
3. 결과를 `docs/phase5-notes.md`에 기록

## 검증 방법

- order-service → outbox-relay → notification-service로 이어지는 요청 하나를 Jaeger UI에서 검색했을 때, produce span과 consume span이 하나의 트레이스로 연결되어 보이는지 확인
- Prometheus에서 `kafka_forge_consumed_total`, `kafka_forge_consumer_lag` 등 메트릭이 실제로 스크레이프되는지 확인 (`localhost:9090`에서 쿼리)
- Grafana 대시보드에서 컨슈머 랙/처리량 패널이 실시간으로 값이 바뀌는지 확인
- 파티션 1/3/6개 벤치마크 결과, 파티션이 늘어날수록 처리량이 어떻게(선형에 가깝게? 포화되는지?) 바뀌는지 수치로 기록

## 참조 규칙

- `.claude/rules/project/convention.md` — kafka-forge 독립성 원칙(특정 SDK 배선/DB 비의존)을 트레이싱/메트릭에도 동일 적용
- `.claude/rules/common/principles.md` — 의존성 최소화, Phase 스코프 준수
- `.claude-plans/20260710/phase3-reliability-features.md`, `phase4-outbox-pattern.md` — "인터페이스/경량 API는 코어, 무거운 배선은 서비스" 패턴을 트레이싱 SDK 초기화와 메트릭 HTTP 서버에도 동일 적용
