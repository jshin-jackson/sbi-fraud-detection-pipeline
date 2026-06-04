#!/usr/bin/env bash
# =============================================================================
# Kafka 토픽 생성 스크립트
# 전제조건: Kerberos 티켓 발급 완료 (kinit), kafka-topics.sh PATH 설정
#
# 사용법:
#   chmod +x kafka_setup.sh
#   ./kafka_setup.sh
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# 환경 설정
# ---------------------------------------------------------------------------
KAFKA_HOME="${KAFKA_HOME:-/opt/cloudera/parcels/CDH/lib/kafka}"
ZOOKEEPER="${ZOOKEEPER:-zookeeper1.sbi.local:2181/kafka}"
BOOTSTRAP="${BOOTSTRAP:-kafka-broker1.sbi.local:9093}"
COMMAND_CONFIG="${COMMAND_CONFIG:-/etc/kafka/conf/kafka-client-kerberos.properties}"

TOPIC_RAW="sbi.transactions.raw"
TOPIC_DLQ="sbi.transactions.dlq"   # Dead Letter Queue

PARTITIONS=6
REPLICATION=3
RETENTION_MS=604800000   # 7일

# ---------------------------------------------------------------------------
# 색상 출력 헬퍼
# ---------------------------------------------------------------------------
info()  { echo "[INFO]  $*"; }
ok()    { echo "[OK]    $*"; }
err()   { echo "[ERROR] $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Kerberos 티켓 확인
# ---------------------------------------------------------------------------
info "Kerberos 티켓 확인..."
klist -s || err "Kerberos 티켓이 없습니다. 먼저 kinit 을 실행하세요."
ok "Kerberos 티켓 유효"

KAFKA_TOPICS="${KAFKA_HOME}/bin/kafka-topics.sh"
KAFKA_CONFIGS="${KAFKA_HOME}/bin/kafka-configs.sh"
KAFKA_ACL="${KAFKA_HOME}/bin/kafka-acls.sh"

# ---------------------------------------------------------------------------
# 토픽 생성 함수
# ---------------------------------------------------------------------------
create_topic() {
    local topic="$1"
    local partitions="${2:-$PARTITIONS}"

    if "${KAFKA_TOPICS}" \
        --bootstrap-server "${BOOTSTRAP}" \
        --command-config "${COMMAND_CONFIG}" \
        --list | grep -qx "${topic}"; then
        info "토픽 이미 존재: ${topic}"
    else
        "${KAFKA_TOPICS}" \
            --bootstrap-server "${BOOTSTRAP}" \
            --command-config "${COMMAND_CONFIG}" \
            --create \
            --topic "${topic}" \
            --partitions "${partitions}" \
            --replication-factor "${REPLICATION}" \
            --config retention.ms="${RETENTION_MS}" \
            --config cleanup.policy=delete \
            --config compression.type=snappy
        ok "토픽 생성 완료: ${topic}"
    fi
}

# ---------------------------------------------------------------------------
# 토픽 생성
# ---------------------------------------------------------------------------
info "토픽 생성 시작..."
create_topic "${TOPIC_RAW}" 6
create_topic "${TOPIC_DLQ}" 2

# ---------------------------------------------------------------------------
# Ranger를 사용하는 환경에서는 아래 kafka-acls 명령 대신
# Ranger UI / REST API로 정책을 적용해야 합니다.
# 아래는 Ranger 미사용 환경 또는 테스트 목적의 참고 명령입니다.
# ---------------------------------------------------------------------------
info "ACL 설정 (Ranger 미사용 환경 전용 참고 명령):"
cat <<'ACL_EXAMPLE'
# Producer ACL
kafka-acls.sh --bootstrap-server $BOOTSTRAP \
  --command-config $COMMAND_CONFIG \
  --add --allow-principal User:sbi-kafka \
  --operation Write --topic sbi.transactions.raw

# Consumer ACL
kafka-acls.sh --bootstrap-server $BOOTSTRAP \
  --command-config $COMMAND_CONFIG \
  --add --allow-principal User:sbi-spark \
  --operation Read --topic sbi.transactions.raw \
  --group sbi-spark-stream-group
ACL_EXAMPLE

# ---------------------------------------------------------------------------
# 토픽 목록 확인
# ---------------------------------------------------------------------------
info "생성된 토픽 목록:"
"${KAFKA_TOPICS}" \
    --bootstrap-server "${BOOTSTRAP}" \
    --command-config "${COMMAND_CONFIG}" \
    --describe \
    --topic "${TOPIC_RAW}"

ok "Kafka 토픽 설정 완료"
