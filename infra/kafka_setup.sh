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

TRUSTSTORE_PATH="${TRUSTSTORE_PATH:-/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks}"
TRUSTSTORE_PASS="${TRUSTSTORE_PASS:-zpXWTjeWPjvNDU4mQnDQPQKn50xfVI9HYX12DSc05x3}"

TOPIC_RAW="sbi_transactions_raw"
TOPIC_DLQ="sbi_transactions_dlq"   # Dead Letter Queue

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
CDP_TRUSTSTORE="${TRUSTSTORE_PATH:-}"
CDP_TRUSTSTORE_PASS="${TRUSTSTORE_PASS:-}"

if [[ -z "${CDP_TRUSTSTORE}" ]]; then
    for ts_path in \
        "/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks" \
        "/opt/cloudera/security/pki/truststore.jks" \
        "/etc/hadoop/conf/ssl/truststore.jks"; do
        if [[ -f "${ts_path}" ]]; then
            CDP_TRUSTSTORE="${ts_path}"
            break
        fi
    done
fi

if [[ -z "${CDP_TRUSTSTORE}" ]]; then
    err "Auto-TLS truststore를 찾을 수 없습니다.
  TRUSTSTORE_PATH 환경변수로 직접 지정하세요:
  export TRUSTSTORE_PATH=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks"
fi
ok "Truststore 발견: ${CDP_TRUSTSTORE}"

# ---------------------------------------------------------------------------
# Truststore 비밀번호 — 우선순위:
#   1) TRUSTSTORE_PASS 환경변수
#   2) <truststore>.pw 파일
#   3) Kafka 프로세스 설정 파일 (CM이 관리하는 process dir)
# ---------------------------------------------------------------------------
if [[ -z "${CDP_TRUSTSTORE_PASS}" ]]; then
    PW_FILE="${CDP_TRUSTSTORE%.jks}.pw"
    if [[ -f "${PW_FILE}" ]]; then
        CDP_TRUSTSTORE_PASS="$(cat "${PW_FILE}")"
        ok "Truststore 비밀번호 파일 사용: ${PW_FILE}"
    fi
fi

if [[ -z "${CDP_TRUSTSTORE_PASS}" ]]; then
    # CM process dir에서 Kafka가 사용하는 truststore 비밀번호 탐색
    KAFKA_PROC_PW=$(grep -r "ssl.truststore.password" \
        /var/run/cloudera-scm-agent/process/*/kafka*/kafka.properties \
        2>/dev/null | head -1 | awk -F'=' '{print $2}' | tr -d ' ')
    if [[ -n "${KAFKA_PROC_PW}" ]]; then
        CDP_TRUSTSTORE_PASS="${KAFKA_PROC_PW}"
        ok "Kafka 프로세스 설정에서 truststore 비밀번호 취득"
    fi
fi

if [[ -z "${CDP_TRUSTSTORE_PASS}" ]]; then
    err "Truststore 비밀번호를 확인할 수 없습니다.
  아래 명령으로 직접 확인 후 환경변수로 지정하세요:

    cat /var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.pw
    grep ssl.truststore.password /var/run/cloudera-scm-agent/process/*/kafka*/kafka.properties

  확인한 비밀번호를 환경변수로 설정:
    export TRUSTSTORE_PASS=<비밀번호>
    bash infra/kafka_setup.sh"
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
# 토픽 생성 함수 (--if-not-exists: 이미 존재해도 오류 없이 통과)
# ---------------------------------------------------------------------------
create_topic() {
    local topic="$1"
    local partitions="${2:-$PARTITIONS}"

    "${KAFKA_TOPICS}" \
        --bootstrap-server "${BOOTSTRAP}" \
        --command-config "${CLIENT_PROPS}" \
        --create \
        --if-not-exists \
        --topic "${topic}" \
        --partitions "${partitions}" \
        --replication-factor "${REPLICATION}" \
        --config "retention.ms=${RETENTION_MS}" \
        --config cleanup.policy=delete \
        --config compression.type=snappy \
    && ok "토픽 생성 완료 (또는 이미 존재): ${topic}" \
    || err "토픽 생성 실패: ${topic}"
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
  --operation Write --topic sbi_transactions_raw

# Consumer ACL
kafka-acls.sh --bootstrap-server $BOOTSTRAP \
  --command-config $CLIENT_PROPS \
  --add --allow-principal User:systest \
  --operation Read --topic sbi_transactions_raw \
  --group systest-stream-group
ACL_EXAMPLE

# ---------------------------------------------------------------------------
# 토픽 최종 확인
# ---------------------------------------------------------------------------
info "전체 토픽 목록:"
"${KAFKA_TOPICS}" \
    --bootstrap-server "${BOOTSTRAP}" \
    --command-config "${CLIENT_PROPS}" \
    --list 2>/dev/null | grep "^sbi_" || warn "sbi_ 접두사 토픽을 찾을 수 없습니다."

ok "Kafka 토픽 설정 완료"
