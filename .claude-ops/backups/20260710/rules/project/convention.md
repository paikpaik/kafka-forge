# kafka-forge 프로젝트 컨벤션

## 토픽 네이밍

`<domain>.<event>.v<N>` 형식만 허용 (소문자, 하이픈만). `packages/kafka-forge/src/topic-name.ts`의 `createTopicName()`을 거치지 않은 토픽 문자열을 직접 만들지 않는다.

## Event Contract 패턴

토픽 이름, Zod 스키마, 파티션 키 전략은 `defineEvent()`로 한 곳에서만 정의한다 (`packages/shared-events` 참고). Producer/Consumer 양쪽 코드가 이 정의를 그대로 import해서 쓰고, 토픽 이름이나 스키마를 각자 다시 선언하지 않는다.

## `packages/kafka-forge`의 독립성

- 워크스페이스 내 다른 패키지를 상대경로(`../../services/...`)로 참조하지 않는다.
- `node-forge`(forge 시리즈의 다른 모듈)를 직접 의존하지 않는다. Kafka는 선택적 인프라이므로, kafka-forge를 쓰기 위해 다른 forge 모듈이 강제되면 안 된다 (레포간 `.claude-ops`/`memory`의 `forge-series-direction` 기록 참고).
- 저장소가 필요한 확장 지점(예: 멱등성 저장소)은 인터페이스만 제공하고 구현체는 강제하지 않는다.

## 레퍼런스 서비스(`services/*`)

- 도메인 로직을 최소화하고, 코어 모듈이 실제로 표준화 역할을 하는지 증명하는 데만 집중한다.
- 레퍼런스 서비스는 `private: true`로 두고 publish 대상이 아니다.

## 문서화

각 Phase 완료 시 `docs/phaseN-notes.md`에 실험 결과와 확인된 개념을 정리한다. `docker-commands.md`처럼 반복 사용하는 명령어는 케이스별로 별도 문서에 정리한다.
