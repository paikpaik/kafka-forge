# kafka-forge — Claude AI Guidelines

## 프로젝트 개요

| 항목 | 내용 |
|------|------|
| 패키지명 | `kafka-forge` (forge 시리즈의 Kafka 담당 모듈, [node-forge](https://github.com/paikpaik/node-forge)와 형제 관계) |
| 기술 스택 | TypeScript, npm workspaces, KafkaJS, Zod, Docker(Redpanda) |
| 역할 | 여러 서비스가 표준화된 방식으로 Kafka를 쓸 수 있게 해주는 코어 모듈 + 실습/레퍼런스 |

이 레포 자체가 최종 산출물이다. 학습 과정 중인 코드가 섞여 있지만, `packages/kafka-forge`는 언제든 `npm publish` 가능한 상태를 유지해야 한다.

---

## BOOT SEQUENCE

Claude는 작업 시작 전 아래 순서로 규칙서를 읽는다.

### 항상 적용

| 순서 | 규칙 | 내용 |
|------|------|------|
| 1 | `rules/common/principles.md` | 이 프로젝트에서의 YAGNI/단계별 스코프 원칙 |
| 2 | `rules/common/workflow.md` | 플랜 작성, 실행 흐름 |

### 상황별 적용

| 상황 | 규칙 |
|------|------|
| `packages/kafka-forge` 코드 변경 | `rules/project/convention.md` |
| 새 Phase(3, 4, 5...) 설계/구현 | `rules/project/convention.md`, `.claude-plans/` |

---

## 저장소 구조

```
kafka-forge/
├── docker-compose.yml       Redpanda(로컬 Kafka) + Console
├── docs/                    실행 명령어, 실험 기록
├── src/phase1/              raw KafkaJS 학습 스크립트 (패키지 아님)
├── src/phase2/              phase2 셋업 스크립트 (토픽 생성 등)
├── packages/
│   ├── kafka-forge/         배포 가능한 코어 모듈 (진짜 산출물)
│   └── shared-events/       레퍼런스 서비스가 공유하는 Event Contract 정의
└── services/
    ├── order-service/       레퍼런스: 이벤트 발행
    └── notification-service/ 레퍼런스: 이벤트 소비
```

---

## 빌드 / 실행

```bash
npm install
npm run phase2:build-core           # packages/kafka-forge 빌드 (dist 생성)
npm run phase2:setup-topic
npm run phase2:order-service        # 레퍼런스 서비스 실행 (터미널 분리해서 실행)
npm run phase2:notification-service
```

상세 명령어는 `docs/docker-commands.md`, `docs/phase1-notes.md` 참고.

---

## 핵심 규칙

- `packages/kafka-forge`는 워크스페이스 밖(예: `node-forge`, `services/*`, 상대경로 `../../`)을 절대 참조하지 않는다. 의존성은 전부 자체 `package.json`의 `dependencies`로 명시.
- `packages/kafka-forge`는 Redis/DB 등 특정 저장소 구현에 의존하지 않는다. 필요한 확장 지점(예: 멱등성 저장소)은 인터페이스로만 제공하고, 실제 구현은 사용하는 서비스 쪽 책임으로 둔다.
- 토픽 이름은 반드시 `createTopicName()` 또는 `defineEvent()`를 통해서만 만든다 (직접 문자열로 하드코딩 금지).
- 각 Phase의 스코프를 지킨다 — 다음 Phase 기능(DLQ, 멱등성, Outbox, OTel 등)을 앞당겨 구현하지 않는다.
