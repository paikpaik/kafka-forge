## 플랜 실행 이력

### 완료: 2026-07-10

**결과**: 성공

**실제 변경 파일**:
- `docker-compose.yml` — Redpanda(단일 노드) + Redpanda Console 컨테이너 정의
- `package.json`, `tsconfig.json` — kafkajs/tsx/typescript 기반 프로젝트 뼈대
- `src/phase1/client.ts` — Kafka 클라이언트 공통 설정 (brokers: localhost:19092)
- `src/phase1/setup-topic.ts` — `phase1.orders.created` 토픽을 파티션 3개로 생성
- `src/phase1/producer.ts` — key 기반 raw produce, 파티션 배정 결과 로그
- `src/phase1/consumer.ts` — 컨슈머 그룹 구독, GROUP_JOIN 이벤트 로깅, graceful shutdown
- `docs/docker-commands.md` — docker/rpk 명령어 케이스별 정리
- `docs/phase1-notes.md` — 실험 결과와 확인된 개념 정리

**계획과의 차이**:
- 최초 계획엔 없었던 `docs/docker-commands.md`, `docs/phase1-notes.md`를 사용자 요청으로 추가 작성함
- Redpanda Console 설정 스키마가 문서와 달라(`schemaRegistry`가 `kafka` 하위가 아닌 최상위 키로 변경) 1회 수정 발생

**잔존 작업**:
- 없음 (Phase 1 스코프는 여기서 종료, 이후 내용은 Phase 2 플랜에서 이어감)

---

# phase1-kafka-local-env — 로컬 Kafka(Redpanda) 환경 구축 + raw Produce/Consume 체감

## 목표

`kafka-core-project-plan.md`의 Phase 1을 실행한다. 실제 Kafka 프로토콜과 호환되는 Redpanda를 로컬 Docker로 띄우고, KafkaJS로 raw produce/consume을 직접 찍어보면서 파티션, 컨슈머 그룹, 오프셋, 리밸런싱, graceful shutdown 개념을 코드 없이 이론으로 배운 뒤 실측으로 검증한다.

## 현재 상태 (AS-IS)

레포에 `kafka-core-project-plan.md` 기획 문서 한 개만 존재. 코드, 인프라 설정 전혀 없음.

## 변경 후 상태 (TO-BE)

- `docker compose up -d` 한 번으로 Redpanda + 웹 콘솔이 뜨는 로컬 환경
- npm 스크립트로 토픽 생성 / producer 실행 / consumer 실행이 가능한 최소 스크립트 세트
- 아래 개념을 실측 로그로 직접 확인한 상태:
  - 같은 key는 항상 같은 파티션으로 감 (결정론적 파티셔닝)
  - 파티션 단위로만 순서 보장됨
  - 컨슈머 그룹 추가/제거 시 리밸런싱 발생, graceful shutdown이 리밸런싱 지연을 막아줌

## 변경 범위

| 파일 | 변경 내용 |
|------|----------|
| `docker-compose.yml` | Redpanda 단일 노드 + Console 컨테이너 신규 작성 |
| `package.json` / `tsconfig.json` | 신규 작성 (kafkajs, tsx, typescript) |
| `src/phase1/*.ts` | 신규 작성 (client/setup-topic/producer/consumer) |
| `docs/*.md` | 신규 작성 (실행 명령어 및 실험 기록) |

## 영향성

| 영향 대상 | 영향 내용 |
|-----------|----------|
| 이후 Phase 2 이상 | Phase 1의 raw 스크립트는 Phase 2에서 만들 코어 모듈과 별개로 `src/phase1/`에 그대로 남겨 학습 레퍼런스로 유지 |

## Breaking Changes

없음 (신규 프로젝트 초기 세팅)

## 위험도

**LOW** — 전부 로컬 Docker 기반 신규 구성, 외부/공유 시스템에 영향 없음

## 주의사항

- Redpanda는 BSL 라이선스지만 로컬 개발/학습 용도는 무료 범위
- 단일 노드 구성이라 replication factor/ISR은 항상 브로커 1개로만 나옴 (멀티 브로커 클러스터링은 v1 스코프 아웃, 계획서에 명시됨)

## 작업 단계

### 1단계: 인프라 기동
1. `docker-compose.yml` 작성 (Redpanda + Console)
2. `docker compose up -d`로 기동 확인

### 2단계: 프로젝트 뼈대
1. `package.json`, `tsconfig.json` 작성
2. kafkajs/tsx/typescript 설치

### 3단계: raw produce/consume 스크립트
1. `client.ts`(공통 접속 설정), `setup-topic.ts`(파티션 3개 토픽 생성)
2. `producer.ts`로 key 기반 발행 후 파티션 배정 확인
3. `consumer.ts`로 구독 + GROUP_JOIN 로깅 + graceful shutdown

### 4단계: 리밸런싱 실측
1. 같은 그룹으로 컨슈머 2개 동시 실행 → 파티션 재분배 확인
2. 컨슈머 1개 종료 → 즉시 리밸런싱으로 파티션 회수 확인

## 검증 방법

- `rpk topic describe -p`로 파티션별 offset이 producer 실행 횟수와 정확히 일치하는지 확인 (실제로 4회 실행 기준 파티션 0: 24, 파티션 2: 12로 일치 확인됨)
- 컨슈머 2개 실행 시 `[group join]` 로그로 파티션 재배정 확인, 1개 종료 시 원복 확인

## 참조 규칙

- `kafka-core-project-plan.md` — 이 작업의 상위 로드맵 문서, Phase 1 정의 출처
