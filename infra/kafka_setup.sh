#!/usr/bin/env bash
# =============================================================================
# Kafka 토픽 생성 스크립트
# 전제조건: Kerberos 티켓 발급 완료 (kinit), kafka-topics.sh PATH 설정
#
# 사용법:
#   chmod +x kafka_setup.sh
#   kinit -kt /root/systest.keytab systest@ROOT.COMOPS.SITE
#   bash infra/kafka_setup.sh
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# 환경 설정
# ---------------------------------------------------------------------------
KAFKA_HOME="${KAFKA_HOME:-/opt/cloudera/parcels/CDH/lib/kafka}"
BOOTSTRAP="${BOOTSTRAP:-ccycloud-1.jshin.root.comops.site:9093,ccycloud-2.jshin.root.comops.site:9093,ccycloud-3.jshin.root.comops.site:9093}"

KEYTAB="${KEYTAB:-/root/systest.keytab}"
PRINCIPAL="${PRINCIPAL:-systest@ROOT.COMOPS.SITE}"

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
warn()  { echo "[WARN]  $*"; }
err()   { echo "[ERROR] $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Kerberos 티켓 확인
# ---------------------------------------------------------------------------
info "Kerberos 티켓 확인..."
klist -s || err "Kerberos 티켓이 없습니다. 먼저 'kinit -kt ${KEYTAB} ${PRINCIPAL}' 을 실행하세요."
ok "Kerberos 티켓 유효"

# ---------------------------------------------------------------------------
# CDP Auto-TLS truststore 자동 감지
# ---------------------------------------------------------------------------
CDP_TRUSTSTORE=""
CDP_TRUSTSTORE_PASS=""

# CDP Auto-TLS 표준 경로 순서대로 탐색
for ts_path in \
    "/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks" \
    "/opt/cloudera/security/pki/truststore.jks" \
    "/etc/hadoop/conf/ssl/truststore.jks"; do
    if [[ -f "${ts_path}" ]]; then
        CDP_TRUSTSTORE="${ts_path}"
        break
    fi
done

if [[ -z "${CDP_TRUSTSTORE}" ]]; then
    err "Auto-TLS truststore를 찾을 수 없습니다. TRUSTSTORE_PATH 환경변수로 지정하세요."
fi
ok "Truststore 발견: ${CDP_TRUSTSTORE}"

# truststore 비밀번호 파일 탐색 (CDP Auto-TLS는 .pw 파일로 관리)
PW_FILE="${CDP_TRUSTSTORE%.jks}.pw"
if [[ -f "${PW_FILE}" ]]; then
    CDP_TRUSTSTORE_PASS="$(cat "${PW_FILE}")"
    ok "Truststore 비밀번호 파일: ${PW_FILE}"
else
    # fallback: 환경변수 또는 빈 문자열
    CDP_TRUSTSTORE_PASS="${TRUSTSTORE_PASS:-}"
    if [[ -z "${CDP_TRUSTSTORE_PASS}" ]]; then
        warn "Truststore 비밀번호를 찾지 못했습니다. TRUSTSTORE_PASS 환경변수로 지정하거나 빈 값으로 시도합니다."
    fi
fi

# ---------------------------------------------------------------------------
# 임시 client.properties 생성 (스크립트 종료 시 자동 삭제)
# ---------------------------------------------------------------------------
TMPDIR_LOCAL=$(mktemp -d)
trap 'rm -rf "${TMPDIR_LOCAL}"' EXIT

CLIENT_PROPS="${TMPDIR_LOCAL}/kafka-client.properties"
JAAS_CONF="${TMPDIR_LOCAL}/kafka-client-jaas.conf"

cat > "${JAAS_CONF}" <<EOF
KafkaClient {
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    storeKey=true
    keyTab="${KEYTAB}"
    principal="${PRINCIPAL}";
};
EOF

cat > "${CLIENT_PROPS}" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=kafka
ssl.truststore.location=${CDP_TRUSTSTORE}
ssl.truststore.password=${CDP_TRUSTSTORE_PASS}
ssl.truststore.type=JKS
request.timeout.ms=30000
connections.max.idle.ms=540000
EOF

export KAFKA_OPTS="-Djava.security.auth.login.config=${JAAS_CONF}"
ok "임시 client.properties 생성: ${CLIENT_PROPS}"

# ---------------------------------------------------------------------------
# Kafka 명령 경로
# ---------------------------------------------------------------------------
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
        --command-config "${CLIENT_PROPS}" \
        --list | grep -qx "${topic}"; then
        info "토픽 이미 존재: ${topic}"
    else
        "${KAFKA_TOPICS}" \
            --bootstrap-server "${BOOTSTRAP}" \
            --command-config "${CLIENT_PROPS}" \
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
# ---------------------------------------------------------------------------
info "ACL 설정 (Ranger 미사용 환경 전용 참고 명령):"
cat <<'ACL_EXAMPLE'
# Producer ACL
kafka-acls.sh --bootstrap-server $BOOTSTRAP \
  --command-config $CLIENT_PROPS \
  --add --allow-principal User:systest \
  --operation Write --topic sbi.transactions.raw

# Consumer ACL
kafka-acls.sh --bootstrap-server $BOOTSTRAP \
  --command-config $CLIENT_PROPS \
  --add --allow-principal User:systest \
  --operation Read --topic sbi.transactions.raw \
  --group systest-stream-group
ACL_EXAMPLE

# ---------------------------------------------------------------------------
# 토픽 목록 확인
# ---------------------------------------------------------------------------
info "생성된 토픽 목록:"
"${KAFKA_TOPICS}" \
    --bootstrap-server "${BOOTSTRAP}" \
    --command-config "${CLIENT_PROPS}" \
    --describe \
    --topic "${TOPIC_RAW}"

ok "Kafka 토픽 설정 완료"
