# kafka-forge — Claude AI Guidelines

## 프로젝트 개요

| 항목 | 내용 |
|------|------|
| 패키지명 | `@paikpaik/kafka-forge` (GitHub Packages, forge 시리즈의 Kafka 담당 모듈, [node-forge](https://github.com/paikpaik/node-forge)와 형제 관계) |
| 기술 스택 | TypeScript, KafkaJS, Zod, `@opentelemetry/api`, `prom-client`, Docker(Redpanda) |
| 역할 | 여러 서비스가 표준화된 방식으로 Kafka를 쓸 수 있게 해주는 코어 모듈 (단일 npm 패키지) |

이 레포 자체가 `@paikpaik/kafka-forge`의 소스이자 배포 대상이다. `.github/workflows/publish.yml`이 `v*` 태그 push 시 GitHub Packages로 배포한다 (node-forge와 동일 패턴, 아직 실제 태그를 찍은 적은 없음 — 배포 준비만 된 상태). Phase 1~5로 Kafka를 학습하며 만든 레퍼런스 서비스/모노레포 구조는 정리 완료(2026-07-11) — `docs/`, `kafka-core-project-plan.md`에 학습 기록만 남아있다.

---

## BOOT SEQUENCE

Claude는 작업 시작 전 아래 순서로 규칙서를 읽는다.

### 항상 적용

| 순서 | 규칙 | 내용 |
|------|------|------|
| 1 | `rules/common/principles.md` | 의존성 최소화, 임의로 결정하지 않기 |
| 2 | `rules/common/workflow.md` | 플랜 작성, 실행 흐름 |

### 상황별 적용

| 상황 | 규칙 |
|------|------|
| `src/` 코드 변경 | `rules/project/convention.md` |
| 새 기능 설계/구현 | `rules/project/convention.md`, `.claude-plans/` |

---

## 저장소 구조

```
kafka-forge/
├── docker-compose.yml       Redpanda(로컬 Kafka) + Console
├── docs/                    Kafka 학습 과정 기록 (히스토리)
├── kafka-core-project-plan.md  최초 기획 문서 (히스토리)
├── src/                     라이브러리 소스
│   ├── index.ts             public API export
│   ├── topic-name.ts        토픽 네이밍 컨벤션
│   ├── event-contract.ts    defineEvent()
│   ├── producer.ts          StandardProducer
│   ├── consumer.ts          StandardConsumer
│   ├── idempotency.ts       IdempotencyStore 인터페이스
│   ├── outbox.ts            OutboxStore 인터페이스, OutboxPublisher
│   ├── tracing.ts           OTel span 생성/context 전파
│   ├── metrics.ts           prom-client Registry, 카운터/히스토그램/게이지
│   ├── schema-export.ts     Zod → JSON Schema 변환 (폴리글랏 지원)
│   └── *.test.ts            모듈별 유닛테스트 (vitest, fake kafkajs로 검증)
├── README.md
└── LICENSE
```

---

## 빌드 / 테스트

```bash
npm install
npm test             # vitest, 실제 브로커 없이 fake kafkajs로 검증
npm run build         # dist 생성 (테스트 파일은 tsconfig exclude로 빠짐)
docker compose up -d  # 로컬 통합 확인용 Redpanda + Console
```

새 기능을 추가할 때는 유닛테스트를 같이 작성한다 (`src/*.test.ts`). kafkajs의 `Producer`/`Consumer`/`Admin`은 필요한 메서드만 만족하는 fake 객체로 대체하고, 실제 브로커 연결 없이 로직만 검증한다.

---

## 핵심 규칙

- 워크스페이스/외부 레포(예: `node-forge`)를 직접 참조하지 않는다. 의존성은 전부 `package.json`의 `dependencies`로 명시.
- Redis/MySQL 등 특정 저장소 구현에 의존하지 않는다. 필요한 확장 지점(멱등성 저장소, Outbox 저장소)은 인터페이스로만 제공하고, 실제 구현은 사용하는 서비스 쪽 책임으로 둔다.
- 계측용 경량 API(`@opentelemetry/api`, `prom-client`)는 코어 의존성으로 허용하되, 실제 배선(OTel SDK/Exporter 초기화, `/metrics` HTTP 서버)은 만들지 않는다.
- 토픽 이름은 반드시 `createTopicName()` 또는 `defineEvent()`를 통해서만 만든다 (직접 문자열로 하드코딩 금지).
