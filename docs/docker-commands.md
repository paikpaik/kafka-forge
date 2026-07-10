# Docker 명령어 모음 (kafka-forge)

로컬 Redpanda 환경을 다루면서 실제로 쓰게 되는 명령어를 상황별로 정리했습니다. 전부 프로젝트 루트(`kafka-forge/`)에서 실행합니다.

## 1. 기동 / 종료

```bash
# 브로커 + 콘솔 UI 백그라운드로 기동
docker compose up -d

# 특정 서비스만 재기동 (설정 변경 후)
docker compose up -d console

# 전체 중지 (컨테이너 삭제, 볼륨은 유지 → 데이터는 남음)
docker compose down

# 전체 중지 + 볼륨까지 삭제 (토픽/메시지 전부 초기화하고 싶을 때)
docker compose down -v
```

## 2. 상태 확인

```bash
# 실행 중인 컨테이너만
docker compose ps

# 중지된 컨테이너까지 전부
docker compose ps -a
```

## 3. 로그 보기

```bash
# redpanda 브로커 로그 실시간 tail
docker compose logs -f redpanda

# console 로그 (컨테이너가 죽었을 때 원인 파악용)
docker logs redpanda-console

# 마지막 N줄만
docker logs redpanda-console --tail 50
```

## 4. 브로커 컨테이너 안에서 직접 확인 (rpk)

Redpanda는 `rpk`라는 CLI를 자체 내장하고 있어서, 컨테이너 안에 들어가지 않고도 바로 토픽/파티션 상태를 조회할 수 있습니다.

```bash
# 토픽 목록
docker exec redpanda rpk topic list

# 특정 토픽 상세 (파티션 수, replication factor 등)
docker exec redpanda rpk topic describe phase1.orders.created

# 컨슈머 그룹 목록
docker exec redpanda rpk group list

# 특정 컨슈머 그룹의 파티션별 offset/lag 확인 (Phase 1~3에서 자주 쓰게 될 명령어)
docker exec redpanda rpk group describe <group-id>

# 클러스터/브로커 헬스 체크
docker exec redpanda rpk cluster health
```

## 5. 웹 UI (Redpanda Console)

브라우저에서 `http://localhost:8080` 접속하면:
- Topics 탭 — 파티션별 메시지, 오프셋을 눈으로 확인
- Consumer Groups 탭 — 그룹별 lag(밀린 정도), 파티션 할당 상태 확인 (리밸런싱 체감할 때 유용)

## 6. 문제 해결

```bash
# Docker 데몬이 안 떠 있을 때 (macOS)
open -a Docker
# 이후 데몬 준비될 때까지 대기
until docker info >/dev/null 2>&1; do sleep 2; done && echo READY

# 설정 바꾼 뒤 완전히 새로 띄우고 싶을 때 (이미지 캐시 문제 등)
docker compose down
docker compose pull
docker compose up -d

# 포트 충돌 등으로 이상 동작할 때 컨테이너 강제 재생성
docker compose up -d --force-recreate
```

## 참고: 포트 정리

| 포트 | 용도 |
|------|------|
| 19092 | Kafka 프로토콜 (KafkaJS 등 클라이언트가 접속하는 포트) |
| 18081 | Schema Registry |
| 18082 | Pandaproxy (REST API) |
| 19644 | Admin API (rpk, 헬스체크 등) |
| 8080 | Redpanda Console (웹 UI) |
