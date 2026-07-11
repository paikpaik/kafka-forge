## 플랜 실행 이력

### 완료: 2026-07-11

**결과**: 성공

**실제 변경 파일**:
- `src/consumer.ts` — `subscribe()`를 "등록만" 하도록 분리하고, 실제 소비 시작은 신규 `run()` 메서드로 분리. `routes: Map<topic, Route>`로 토픽별 핸들러/재시도/멱등성 설정을 관리, `eachMessage`에서 `topic`으로 라우팅. 랙 폴링도 admin 클라이언트 하나를 모든 구독 토픽이 공유하도록 통합 (기존엔 `subscribe()`마다 매번 새 admin 커넥션을 만들던 구조). `disconnect()`에서 admin도 정리하도록 수정(기존엔 admin 연결이 끊기지 않고 새던 문제).
- `src/consumer.test.ts` — 기존 5개 테스트를 `subscribe()` + `run()` 2단계 호출로 갱신, 멀티토픽 라우팅/중복 구독 예외/`run()` 이후 `subscribe()` 예외 3개 테스트 추가 (총 8개)
- `README.md` — 구독 예시를 `subscribe()`+`run()` 흐름으로 갱신, 멀티토픽 예시 추가

**계획과의 차이**: 없음. 랙 폴링 admin 커넥션 공유화와 disconnect 시 admin 정리는 계획에 명시했던 리팩터링 범위 그대로 반영.

**Breaking Change**: `StandardConsumer.subscribe()`가 더 이상 자동으로 소비를 시작하지 않는다. 기존에는 `await consumer.subscribe(Event, handler)` 한 줄로 끝났지만, 이제는 모든 `subscribe()` 호출 후 `await consumer.run()`을 명시적으로 호출해야 한다. 버전이 아직 0.1.0(pre-1.0)이라 허용 가능한 변경으로 판단.

**잔존 작업**: 없음. 다음 확장 항목(partitionsConsumedConcurrently, idempotent producer, non-retryable error, TTL 등)은 별도로 진행.

---

# kafka-forge-multi-topic-consumer — StandardConsumer 멀티토픽 구독 지원

## 목표

`StandardConsumer`가 토픽 1개만 구독 가능했던 구조적 한계를 해소한다. kafkajs의 `consumer.run()`은 인스턴스당 한 번만 호출 가능한데, 기존 `subscribe()`가 매 호출마다 내부에서 `run()`을 같이 불러 두 번째 `subscribe()` 호출 시 깨지는 문제가 있었다. "주문 관련 이벤트 3종을 컨슈머 하나(같은 그룹)가 처리"하는 흔한 실무 패턴을 지원한다.

## 현재 상태 (AS-IS)

```ts
async subscribe(event, handler, options) {
  ...
  await this.consumer.subscribe({ topic: event.topic, fromBeginning: true });
  this.startLagPolling(event.topic); // 호출마다 새 admin 커넥션 생성
  await this.consumer.run({ eachMessage: ... }); // 두 번째 subscribe() 호출 시 kafkajs가 에러
}
```

## 변경 후 상태 (TO-BE)

`subscribe()`는 등록만 하고(`this.routes`에 저장 + kafkajs `consumer.subscribe()` 호출), `run()`을 별도로 호출해야 실제 소비가 시작된다. `eachMessage`가 `topic`으로 올바른 route를 찾아 처리한다.

## 위험도

**MEDIUM** — 공개 API 시그니처 변경(Breaking). 다만 pre-1.0이라 허용.

## 검증 방법

`npm test` 전체 통과(28개), `npm run build` 성공.
