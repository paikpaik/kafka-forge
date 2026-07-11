# Phase 5 실습 기록 — 옵저버빌리티

OTel 분산 트레이싱, Prometheus 메트릭 + Grafana 대시보드, 파티션 수 대비 처리량 실측 기록.

## 인프라

`docker-compose.yml`에 3개 컨테이너 추가:
- **Jaeger** (all-in-one) — `localhost:16686` 트레이스 웹 UI, OTLP는 4317(gRPC)/4318(HTTP)
- **Prometheus** — `localhost:9090`, `scripts/kafka_forge/observability/prometheus.yml`로 각 서비스의 `/metrics` 스크레이프
- **Grafana** — `localhost:3001` (기본 3000번은 다른 프로세스가 쓰고 있어서 변경), Prometheus 데이터소스와 `kafka-forge` 대시보드 자동 프로비저닝

## 설계 원칙

- `kafka-forge` 코어는 계측용 **경량 API**(`@opentelemetry/api`, `prom-client`)까지만 의존한다. 실제 OTel SDK 초기화(exporter 대상 설정)와 `/metrics` HTTP 서버는 각 서비스(`order-service`, `outbox-relay`, `notification-service`)가 직접 배선한다 — Phase 3/4의 "인터페이스는 코어, 무거운 배선은 서비스" 원칙을 트레이싱/메트릭에도 동일하게 적용.
- `StandardProducer.send()`와 `OutboxPublisher.publishPending()` 둘 다 producer span을 만들고 W3C traceparent를 Kafka 메시지 헤더에 심는다. `StandardConsumer`는 헤더에서 context를 복원해 연결된 consumer span을 만든다.

## 실험 1 — 분산 트레이싱 연결 확인

order-service → outbox-relay → notification-service를 같이 띄운 뒤 Jaeger API로 트레이스 하나를 조회:

```json
{
  "traceID": "640f243d2ad6c28debb6034ad36de424",
  "spans": [
    { "operationName": "kafka.consume order.created.v1", "service": "notification-service", "refs": [{"refType": "CHILD_OF", "spanID": "72dea6a2..."}] },
    { "operationName": "kafka.produce order.created.v1", "service": "outbox-relay", "spanID": "72dea6a2..." }
  ]
}
```
하나의 `traceID` 안에 outbox-relay의 produce span과 notification-service의 consume span이 `CHILD_OF`로 정확히 연결됨 — Kafka 메시지 헤더를 통한 trace context 전파가 실제로 동작한다는 증거.

**스코프 한계**: order-service의 DB 트랜잭션 자체는 이번에 계측하지 않았다. order-service는 Phase 4부터 Kafka를 직접 건드리지 않고 DB에만 쓰기 때문에, 지금 트레이스는 "outbox-relay가 실제로 Kafka에 발행하는 시점"부터 시작한다. DB 쓰기 시점까지 트레이스를 연결하려면 outbox 테이블에 trace context 컬럼을 추가하고 DB 계측 라이브러리를 붙여야 하는데, 이건 이번 스코프 밖으로 남겨둔다.

## 실험 2 — Prometheus/Grafana 확인

- `curl localhost:946{4,5,6}/metrics`로 세 서비스 모두 `kafka_forge_*` 메트릭이 정상 노출되는 것 확인 (예: notification-service의 `kafka_forge_consumed_total`, `kafka_forge_consume_duration_seconds`, `kafka_forge_consumer_lag`)
- Prometheus `/api/v1/targets`로 세 타겟 전부 `up` 확인
- Grafana에 `kafka-forge` 대시보드(발행률/소비율/랙/에러율 4개 패널) 자동 생성 확인, Prometheus 쿼리로 실제 소비율(`~0.65 msg/s`, 1.5초 간격 발행과 일치) 확인

**트러블슈팅**: Grafana 데이터소스 provisioning에 `uid`를 명시하지 않으면 자동 생성된 UID가 대시보드 JSON에서 참조하는 값과 어긋나 `Datasource provisioning error: data source not found`로 컨테이너가 죽는 문제가 있었다. 데이터소스 YAML에 `uid: Prometheus`를 명시하고, 컨테이너를 완전히 재생성(`docker compose rm -f` 후 재기동, 단순 `restart`로는 내부 상태가 안 지워짐)해서 해결.

## 실험 3 — 파티션 수 대비 처리량 실측

`src/phase5/run-benchmark.ts`: 매 실행마다 고유 접미사가 붙은 토픽을 새로 만들어(`benchmark.p{N}.{runId}`) 이전 실행 데이터와 안 섞이게 하고, 메시지 5000개를 발행 후 **파티션 수만큼 컨슈머를 동시에 띄워** 처리 시간을 측정. 메시지당 2ms의 가짜 처리 시간을 줘서 병렬성 효과가 드러나게 함.

| 파티션 | 컨슈머 | 처리 시간 | 처리량 | 배수 |
|---|---|---|---|---|
| 1 | 1 | 11,370ms | 440 msg/s | 1x |
| 3 | 3 | 4,185ms | 1,195 msg/s | 약 2.7x |
| 6 | 6 | 2,336ms | 2,140 msg/s | 약 4.9x |

**첫 시도에서의 실수**: 처음엔 파티션 수와 무관하게 컨슈머를 1개만 띄우고 측정했더니 파티션 1/3/6개 사이에 유의미한 차이가 안 나타났다 (컨슈머 1개가 파티션이 몇 개든 혼자 순차 처리하기 때문). **파티션의 장점은 파티션 수만큼 컨슈머를 늘려서 병렬로 처리할 때만 나타난다**는 걸 이 실패로 먼저 확인한 셈. 컨슈머 수를 파티션 수에 맞춰 늘리자 거의 선형에 가깝게 처리량이 증가했다.

또한 `eachMessage` 콜백 **안에서 `consumer.disconnect()`를 호출하면 데드락**이 발생한다는 것도 확인됨 — disconnect는 현재 처리 중인 메시지(즉 disconnect를 호출한 그 핸들러 자신)가 끝나길 기다리는데, 핸들러가 disconnect 완료를 기다리며 멈춰있으니 서로를 기다리는 순환 대기가 생김. Promise를 resolve만 하고 handler가 정상 반환된 뒤, 바깥에서 별도로 disconnect를 호출하도록 수정해서 해결.

## 확인된 개념

- Kafka 메시지 헤더는 비즈니스 데이터뿐 아니라 **trace context 같은 메타데이터를 실어 나르는 통로**로도 쓰인다.
- 코어 모듈이 계측을 "내장"한다는 게, 계측 로직을 실행하는 무거운 백엔드(Jaeger, Prometheus)까지 코어가 직접 띄운다는 뜻은 아니다 — 코어는 계측 지점(span 생성, 카운터 증가)만 표준화하고, 어디로 보낼지는 서비스가 정한다.
- 파티션 수는 "이론상 병렬 처리 가능한 최대치"일 뿐, 실제로 처리량이 늘려면 **그 파티션들을 나눠 가져갈 컨슈머가 실제로 여러 개 있어야** 한다.

## 다음 단계

`kafka-core-project-plan.md`에 정의된 Phase 1~5가 전부 완료됨. 이후는 계획서의 "명시적으로 안 하는 것" 스코프(멀티 브로커 클러스터링, 스트림 처리 엔진, 완전한 Exactly-once)를 제외하면, `packages/kafka-forge`를 실제로 독립 배포 가능한 상태로 다듬는 마감 작업이 남는다.
