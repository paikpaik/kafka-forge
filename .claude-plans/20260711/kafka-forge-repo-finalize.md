## 플랜 실행 이력

### 완료: 2026-07-11

**결과**: 성공

**실제 변경 파일**:
- `src/*.ts` — `packages/kafka-forge/src`에서 이동 (내용 변경 없음)
- `package.json`, `tsconfig.json` — `packages/kafka-forge`의 것으로 교체, `@types/node` devDependency 추가(빌드 에러로 발견)
- 삭제: `src/phase1~5/`(raw 스크립트), `services/`(order-service, notification-service, outbox-relay), `packages/`(kafka-forge 이동 후 shared-events와 함께), `scripts/kafka_forge/`(MySQL DDL, Prometheus/Grafana 설정), `.env`, `.env.example`, `seq.md`, `enterprise-flow.md`
- `docker-compose.yml` — jaeger/prometheus/grafana 서비스 블록 제거, redpanda+console만 유지. 실행 중이던 jaeger/prometheus/grafana 컨테이너도 `--remove-orphans`로 정리
- `.gitignore` — `.env` 라인 제거
- `README.md`, `LICENSE` — 신규 작성
- `.claude/CLAUDE.md`, `.claude/rules/project/convention.md`, `.claude/rules/common/principles.md` — 새 구조(단일 패키지, Phase 완료)에 맞게 갱신
- `.claude-ops/backups/20260711/`, `.claude-ops/work/` — 규칙 변경 백업/동기화

**계획과의 차이**:
- 빌드 시 `@types/node`가 빠져있어 에러 발생 — 예전엔 워크스페이스 루트에 있던 게 hoisting으로 같이 쓰였는데, 워크스페이스가 없어지면서 직접 필요해짐. 계획엔 없었지만 즉시 추가해서 해결.
- `.claude/rules/common/workflow.md`는 계획대로 이번 스코프에서 변경하지 않음.

**잔존 작업**:
- 없음. 이후는 `kafka-forge`에 새 기능을 확장하는 일반적인 개발 워크플로우로 전환.

---

# kafka-forge-repo-finalize — 학습 저장소를 진짜 kafka-forge 단일 패키지 레포로 정리

## 목표

Phase 1~5로 Kafka를 학습하며 만든 이 레포를, 이제 실제로 확장해나갈 **진짜 kafka-forge 라이브러리 레포**로 정리한다. 학습을 위해 만들었던 레퍼런스 서비스/모노레포 구조/실습 인프라를 걷어내고, `packages/kafka-forge`의 내용을 레포 최상위로 승격시켜 "단일 npm 패키지 레포"로 만든다. 학습 문서(docs/, kafka-core-project-plan.md)는 히스토리로 남긴다.

## 현재 상태 (AS-IS)

```
kafka-forge/
├── package.json              workspaces root (name: kafka-forge, private: true)
├── docker-compose.yml         redpanda, console, jaeger, prometheus, grafana
├── .env, .env.example         MySQL 접속 정보 (Outbox 레퍼런스용)
├── seq.md, enterprise-flow.md 이번 대화에서 만든 mermaid 다이어그램
├── src/phase1~5/               raw 학습 스크립트
├── scripts/kafka_forge/        MySQL DDL, Prometheus/Grafana 설정
├── docs/                       docker-commands.md, phase1/3/4/5-notes.md
├── kafka-core-project-plan.md  최초 기획 문서
├── packages/
│   ├── kafka-forge/            진짜 라이브러리 코드 (src, package.json, tsconfig.json)
│   └── shared-events/          레퍼런스 서비스 공용 Event Contract
└── services/
    ├── order-service/
    ├── notification-service/
    └── outbox-relay/
```

## 변경 후 상태 (TO-BE)

```
kafka-forge/
├── package.json          name: kafka-forge, workspaces 없음, 실제 배포 대상 패키지 자체
├── tsconfig.json
├── src/                  packages/kafka-forge/src의 내용 그대로 이동
│   ├── index.ts, producer.ts, consumer.ts, event-contract.ts,
│   │   topic-name.ts, idempotency.ts, outbox.ts, metrics.ts, tracing.ts
├── docker-compose.yml    redpanda + console만
├── docs/                 docker-commands.md, phase1/3/4/5-notes.md (그대로 보존)
├── kafka-core-project-plan.md (보존)
├── README.md             신규 — 설치/사용법/API 개요
├── LICENSE               신규 — MIT
└── .claude/, .claude-ops/, .claude-plans/  새 구조에 맞게 갱신
```

`packages/`, `services/`, `src/phase1~5/`, `scripts/`, `.env*`, `seq.md`, `enterprise-flow.md`는 삭제.

## 변경 범위

| 파일/디렉토리 | 변경 내용 |
|------|----------|
| `src/phase1/`, `src/phase2/`, `src/phase3/`, `src/phase5/` | 삭제 |
| `services/order-service/`, `services/notification-service/`, `services/outbox-relay/` | 삭제 |
| `packages/shared-events/` | 삭제 |
| `packages/kafka-forge/src/*.ts` | `src/`로 이동 (경로만 변경, 내용 동일) |
| `packages/kafka-forge/package.json`, `tsconfig.json` | 루트로 이동해 기존 루트 파일을 대체 |
| `packages/`, `scripts/kafka_forge/` | 디렉토리 전체 삭제 |
| `.env`, `.env.example` | 삭제 |
| `seq.md`, `enterprise-flow.md` | 삭제 |
| `docker-compose.yml` | jaeger/prometheus/grafana 서비스 블록 제거 |
| `package.json` (루트) | `packages/kafka-forge/package.json` 내용 승계, `workspaces` 제거, `private` 제거(퍼블리시 대상이므로), phase별 스크립트 제거, `tsx`/`dotenv-cli` devDependency 제거 |
| `tsconfig.json` (루트) | `packages/kafka-forge/tsconfig.json` 내용으로 교체 |
| `README.md` | 신규 작성 |
| `LICENSE` | 신규 작성 (MIT) |
| `.gitignore` | `.env` 라인 제거 (더 이상 해당 없음) |
| `.claude/CLAUDE.md` | 저장소 구조 다이어그램, 빌드/실행 섹션을 단일 패키지 기준으로 재작성 |
| `.claude/rules/project/convention.md` | "레퍼런스 서비스" 섹션 제거, 워크스페이스 관련 문구를 "외부 레포 대비 독립성" 관점으로 조정 |
| `docs/*.md`, `kafka-core-project-plan.md` | 그대로 유지 (히스토리 보존) |
| `.claude-plans/`, `.claude-ops/` | 그대로 유지 |

## 영향성

| 영향 대상 | 영향 내용 |
|-----------|----------|
| `docs/phase1-notes.md` 등 학습 문서 내 커맨드 안내 | 이제 실제로 실행 불가능해짐 (`npm run phase2:order-service` 등 스크립트가 사라짐) — 문서 맨 위에 "이 문서는 과거 학습 기록이며 현재 레포 구조와 다를 수 있음" 안내를 추가할지 검토 |
| git 히스토리 | 삭제된 파일도 git log/과거 커밋에는 그대로 남아있어 완전히 사라지지 않음 |
| 로컬 MySQL(`kafka_forge` DB) | 레포에서 더 이상 연결하지 않지만, DB 자체는 그대로 남아있음 (필요시 사용자가 직접 정리) |

## Breaking Changes

있음 — 이 레포를 지금 형태(모노레포+레퍼런스 서비스)로 알고 있는 사람/문서 기준으로는 완전히 다른 구조가 된다. 다만 이 레포를 아직 외부에 공개하거나 다른 곳에서 참조한 적이 없으므로 실질적 영향은 없음.

## 위험도

**HIGH** — 여러 디렉토리를 통째로 삭제하는 되돌리기 어려운 작업. 단, 시작 전 Phase 5까지의 상태를 커밋해두면 git 히스토리로 언제든 복구 가능.

## 주의사항

- **작업 시작 전 현재 상태가 커밋되어 있는지 반드시 확인한다.** (사용자가 먼저 커밋하기로 함)
- `packages/kafka-forge/src`를 루트 `src/`로 옮길 때, 내용을 고치지 않고 그대로 이동만 한다 (경로 이동과 로직 변경을 섞지 않음).
- 삭제 순서: 이동(mv)이 필요한 것을 먼저 처리하고, 그 다음에 불필요해진 디렉토리를 삭제한다.
- `npm install`을 루트에서 다시 실행해 `package-lock.json`을 새 구조에 맞게 재생성해야 한다.
- `.claude/rules/common/workflow.md`의 "실행은 사용자가 직접 해보게 안내" 같은 학습 단계용 문구는 이번엔 그대로 둔다 (제거 여부는 별도 논의 필요, 이번 스코프 아님).

## 작업 단계

### 1단계: 이동
1. `packages/kafka-forge/src/*` → 루트 `src/`로 이동 (기존 루트 `src/phase*`는 아직 지우지 않고 이동 먼저)
2. `packages/kafka-forge/package.json` → 루트 `package.json`으로 교체 (workspaces 제거, private 제거, 스크립트/devDependency 정리)
3. `packages/kafka-forge/tsconfig.json` → 루트 `tsconfig.json`으로 교체

### 2단계: 삭제
1. `src/phase1/`, `src/phase2/`, `src/phase3/`, `src/phase5/` 삭제
2. `services/` 전체 삭제
3. `packages/` 전체 삭제 (kafka-forge 내용은 이미 옮겼으므로 shared-events와 함께 삭제)
4. `scripts/kafka_forge/` 전체 삭제
5. `.env`, `.env.example`, `seq.md`, `enterprise-flow.md` 삭제

### 3단계: docker-compose.yml 정리
1. jaeger, prometheus, grafana 서비스 블록 제거, redpanda/console만 남김

### 4단계: 신규 문서
1. `README.md` 작성 (설치, 사용 예시, API 개요, 라이선스)
2. `LICENSE` 작성 (MIT)

### 5단계: harness 규칙 갱신
1. `.claude/CLAUDE.md` 구조도/빌드 섹션 갱신
2. `.claude/rules/project/convention.md`에서 레퍼런스 서비스 섹션 제거
3. `/harness-ops`로 백업/update 체인 기록

### 6단계: 검증
1. 루트에서 `npm install` → `npm run build` 성공 확인
2. `docker compose up -d` → redpanda/console만 뜨는지 확인
3. git status로 삭제/이동 내역 최종 확인

## 검증 방법

- `npm run build`가 루트에서 성공 (dist/ 생성)
- `docker compose config`로 jaeger/prometheus/grafana가 더 이상 정의되어 있지 않은지 확인
- `find . -maxdepth 1 -not -path './node_modules' -not -path './.git'`로 최상위 구조가 TO-BE와 일치하는지 확인
- `docs/*.md`, `kafka-core-project-plan.md`가 그대로 남아있는지 확인

## 참조 규칙

- `.claude/rules/project/convention.md` — kafka-forge 독립성 원칙 (이번 정리 후에도 계속 유효)
- `.claude-plans/20260710/phase2-core-module-v0.md` — `packages/kafka-forge`를 처음 독립 패키지로 설계했던 배경
- memory: `kafka-forge-user-preferences` — "이 레포 자체가 최종 산출물"이라는 관점이 이번 정리의 직접적 동기
