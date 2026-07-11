# kafka-forge 프로젝트 컨벤션

## 토픽 네이밍

`<domain>.<event>.v<N>` 형식만 허용 (소문자, 하이픈만). `src/topic-name.ts`의 `createTopicName()`을 거치지 않은 토픽 문자열을 직접 만들지 않는다.

## Event Contract 패턴

토픽 이름, Zod 스키마, 파티션 키 전략은 `defineEvent()`로 한 곳에서만 정의한다. 이 정의를 여러 서비스가 그대로 import해서 쓰고, 토픽 이름이나 스키마를 각자 다시 선언하지 않는다.

## 독립성

- `node-forge`(forge 시리즈의 다른 모듈)를 직접 의존하지 않는다. Kafka는 선택적 인프라이므로, kafka-forge를 쓰기 위해 다른 forge 모듈이 강제되면 안 된다 (memory의 `forge-series-direction` 기록 참고).
- Redis/MySQL 등 특정 저장소 구현에 의존하지 않는다. 필요한 확장 지점(멱등성 저장소, Outbox 저장소)은 인터페이스만 제공하고 구현체는 강제하지 않는다.
- 계측용 경량 API(`@opentelemetry/api`, `prom-client`)는 코어 의존성으로 허용한다. 다만 실제 배선(OTel SDK/Exporter 초기화, `/metrics` HTTP 서버)은 이 라이브러리를 쓰는 서비스 책임으로 남기고 코어가 직접 만들지 않는다.
- NestJS/Fastify/Express 등 특정 프레임워크에 의존하지 않는다.

## 문서화

`docs/`에는 Kafka를 학습하며 남긴 기록(Phase 1~5)이 있다. 새로 추가하는 기능은 README.md의 해당 섹션을 갱신하는 방식으로 문서화한다.
