# Phase 1 실습 기록 — 로컬 Kafka 환경 + 기본 Produce/Consume

Redpanda를 로컬에 띄우고 KafkaJS로 raw produce/consume을 직접 찍어보면서 확인한 내용과, 실제로 실행했던 순서를 정리한다. 나중에 다시 돌려보고 싶을 때 이 문서만 보고 재현할 수 있도록 작성한다.

## 사전 준비

```bash
cd kafka-forge
docker compose up -d          # redpanda, redpanda-console 기동
docker compose ps              # 둘 다 Up 상태인지 확인
npm install
npm run phase1:setup-topic     # phase1.orders.created 토픽을 파티션 3개로 생성
```

docker 관련 명령어 전체는 [docker-commands.md](./docker-commands.md) 참고.

## 실험 1 — key가 파티션을 어떻게 결정하는가

**방법**: [producer.ts](../src/phase1/producer.ts)에서 `order-A`, `order-B`, `order-C` 세 key를 3라운드 반복 발행. 프로세스를 총 4번 새로 실행(재시작)해서 매번 같은 결과가 나오는지 확인.

**결과**:
| key | 파티션 | 비고 |
|---|---|---|
| order-A | 2 | 4번 실행 전부 동일 |
| order-B | 0 | 4번 실행 전부 동일 |
| order-C | 0 | order-B와 우연히 같은 파티션 (해시 충돌) |
| (파티션 1) | - | 어떤 key도 배정되지 않아 끝까지 비어있었음 |

**확인된 개념**:
- 파티션 배정은 `hash(key) % 파티션수` 방식의 **결정론적(순수 함수) 계산**이라, 프로세스를 몇 번 재시작해도 같은 key는 항상 같은 파티션으로 간다. → "같은 주문 ID는 항상 같은 파티션 → 같은 컨슈머가 순서대로 처리"가 보장되는 근거.
- 파티션 수가 적으면(3개) 서로 다른 key끼리 같은 파티션에 몰릴 수 있다 (order-B, order-C 충돌 사례).
- `rpk topic describe phase1.orders.created -p`로 확인한 누적 offset이 위 표와 정확히 일치 (파티션 0: 24, 파티션 2: 12, 파티션 1: 0 — 4회 실행 × 3라운드 기준 계산과 일치).

## 실험 2 — 컨슈머 그룹 리밸런싱

**방법**: 같은 그룹 ID(`phase1.order-logger`)로 컨슈머를 터미널 창 2개에서 순차적으로 띄우고 내림.

1. 터미널 A에서 `npm run phase1:consume` 실행 → 혼자라서 파티션 `[0,1,2]` 전부 할당받음
2. 터미널 B에서 같은 명령 실행 (같은 그룹으로 조인)
   - 터미널 A: 하트비트가 `"The group is rebalancing, so a rejoin is needed"` 에러(사실상 정상 흐름)를 받고 재조인 → 파티션 `[1]`만 재할당
   - 터미널 B: 파티션 `[0, 2]` 할당
   - → 실제 데이터가 있는 파티션(0, 2)은 전부 터미널 B로 넘어가고, 터미널 A는 빈 파티션(1)만 담당하게 되어 이후로는 아무 메시지도 못 받음
3. 터미널 B를 `Ctrl+C`로 정상 종료(graceful disconnect)
   - 터미널 A: 즉시 리밸런싱 트리거되어 파티션 `[0,1,2]` 전부 재할당받음

**확인된 개념**:
- 컨슈머가 그룹에 추가/제거될 때마다 **코디네이터가 파티션을 다시 계산해서 나눠주는 것이 리밸런싱**이며, 어떤 컨슈머가 어떤 파티션을 받을지는 `RoundRobinAssigner`가 **memberId(랜덤 UUID) 정렬 순서**로 결정한다 — 먼저 조인했다고 유리하지 않다.
- key 분포가 특정 파티션에 쏠려 있으면, 컨슈머를 늘려도 그 컨슈머가 빈 파티션만 받아서 실질적인 처리량 증가로 이어지지 않을 수 있다 (파티션 수 설계가 왜 중요한지 보여주는 사례).
- **Graceful shutdown이 리밸런싱 지연을 막아준다**: `SIGINT`를 받아 `consumer.disconnect()`로 정상 탈퇴하면, 코디네이터가 하트비트 타임아웃을 기다릴 필요 없이 즉시 재조정한다. 만약 프로세스가 `kill -9`로 강제 종료됐다면, 세션 타임아웃(기본 수 초~수십 초)이 지날 때까지 죽은 컨슈머가 파티션을 붙들고 있는 것으로 처리되어 그 사이 해당 파티션은 아무도 처리하지 않는 공백이 생긴다.

## 참고 — KafkaJS 로그에서 헷갈렸던 부분

리밸런싱 도중 다음과 같은 로그가 `ERROR` 레벨로 찍히는데, 실제 장애가 아니라 **정상적인 재조인 흐름의 일부**다:
```
[Connection] Response Heartbeat(...) error: "The group is rebalancing, so a rejoin is needed"
[Runner] The group is rebalancing, re-joining
```
바로 뒤에 `[group join]` 로그가 새 파티션 할당과 함께 찍히면 정상 복구된 것.

## 다음 단계 (Phase 2 예고)

여기까지 확인한 "raw KafkaJS로 짠 코드"는 서비스마다 반복해서 짜야 하는 보일러플레이트다. Phase 2에서는 이걸 `node-core-module` 스타일의 npm 패키지로 추출해서, 토픽 네이밍 컨벤션·스키마 검증·파티션 키 전략을 표준화하는 설계로 넘어간다.
