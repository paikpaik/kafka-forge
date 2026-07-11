# Phase 3 실습 기록 — 재시도+DLQ, 멱등성, graceful shutdown

`StandardConsumer`에 신뢰성 기능 세 가지를 추가하고 실측으로 확인한 내용.

## 사전 준비

```bash
npm run phase3:build-core        # consumer.ts 변경분 반영
npm run phase3:setup-dlq-topic   # order.created.v1.dlq 토픽 생성 (파티션 1개)
```

## 구현 요약

- `subscribe(event, handler, options)` — `options.retry`(기본 3회 시도, 실패마다 1초→2초 지수 백오프 — 마지막 시도 뒤에는 대기 없이 바로 DLQ) / `options.idempotencyStore` 추가
- handler가 계속 실패하면 원본 payload + 에러 메시지 + 실패 시각을 `<topic>.dlq` 토픽에 발행
- `registerShutdown()` — `SIGINT`/`SIGTERM` 수신 시 컨슈머 정상 탈퇴 + DLQ producer도 함께 disconnect

`services/notification-service`에 실험용 실패 케이스를 넣었다:
- `amount > 95`: 항상 실패 (영구 실패 시뮬레이션)
- 그 외 30% 확률로 실패 (일시적 실패 시뮬레이션)

## 실험 결과

**일시적 실패 → 재시도 후 성공** (order-2):
```
[StandardConsumer] handler 실패 (시도 1/3): ... 일시적 알림 서버 오류: order-2
[StandardConsumer] handler 실패 (시도 2/3): ... 일시적 알림 서버 오류: order-2
알림 발송: 주문 order-2 (금액 16.55) 접수 완료 알림을 보냈습니다.
```
3번째 시도에서 성공 → DLQ로 안 가고 정상 처리됨.

**영구 실패 → DLQ 이동** (order-10, amount=98.77):
```
[StandardConsumer] handler 실패 (시도 1/3): ... 금액이 너무 커서 알림 발송 실패: order-10
[StandardConsumer] handler 실패 (시도 2/3): ...
[StandardConsumer] handler 실패 (시도 3/3): ...
[StandardConsumer] 최종 실패, DLQ로 이동: topic=order.created.v1
```
`rpk topic consume order.created.v1.dlq -n 5`로 확인한 실제 DLQ 메시지:
```json
{
  "payload": {"orderId": "order-10", "amount": 98.77},
  "error": "금액이 너무 커서 알림 발송 실패: order-10",
  "failedAt": "2026-07-10T10:00:34.663Z"
}
```
원본 payload와 실패 사유, 실패 시각이 그대로 보존됨.

**Graceful shutdown**:
```
[Runner] consumer not running, exiting
[Consumer] Stopped
```
`Ctrl+C` 이후 강제 종료 없이 컨슈머와 DLQ producer가 순서대로 정상 종료됨.

## 확인된 개념

- **재시도는 순서를 지키며 블로킹으로 진행된다**: `eachMessage` 안에서 `await sleep()`으로 백오프하기 때문에, 재시도가 끝날 때까지 같은 파티션의 다음 메시지는 처리되지 않는다. 시도 횟수(N)만큼 대기가 일어나는 게 아니라 **대기는 N-1번만 일어난다**(마지막 시도 실패 후에는 어차피 DLQ로 넘어가므로 대기 없이 바로 처리). 3회 시도 기준 총 대기는 1초+2초=3초로, 컨슈머 세션 타임아웃(기본 30초)보다 훨씬 짧아서 리밸런싱 걱정은 없다.
- **DLQ는 원본 토픽과 별개의 토픽**이라, DLQ에 쌓인 메시지를 다시 처리하고 싶으면 별도 컨슈머가 `order.created.v1.dlq`를 구독하면 된다 (Phase 3 스코프에는 재처리 로직까지는 포함하지 않음).
- **멱등성 저장소(`InMemoryIdempotencyStore`)는 프로세스 메모리 기반**이라, 프로세스를 재시작하면 dedup 상태가 초기화된다. 재시작을 넘나드는 멱등성이 필요하면 Redis 등 영속 저장소로 `IdempotencyStore` 인터페이스를 직접 구현해서 넘겨야 한다 (kafka-forge 코어는 이 구현체를 강제하지 않음).

## 다음 단계 (Phase 4 예고)

DB 트랜잭션 커밋과 Kafka 발행 사이의 정합성 문제(Outbox 패턴)로 넘어간다.
