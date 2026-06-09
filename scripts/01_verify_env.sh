#!/usr/bin/env bash
# ================================================================
# 01_verify_env.sh — 전체 환경 자동 검증 스크립트
# Phase 1에서 실행합니다. 모든 항목이 OK여야 다음 Phase로 진행합니다.
# ================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."

# config 로드
if [ ! -f "${ROOT_DIR}/config/env.conf" ]; then
  echo "[ERROR] config/env.conf 파일이 없습니다."
  echo "        다음 명령으로 설정 파일을 연결하세요:"
  echo "        ln -sf config/env.internal.conf config/env.conf"
  exit 1
fi
source "${ROOT_DIR}/config/env.conf"

PASS=0
FAIL=0

ok()      { echo "  [OK]   $1"; ((PASS+=1)); }
fail()    { echo "  [FAIL] $1"; ((FAIL+=1)); }
section() { echo ""; echo "=== $1 ==="; }

echo ""
echo "================================================================"
echo " SBI Fraud Detection — 환경 검증 (ENV: ${ENV_NAME})"
echo "================================================================"

# ------------------------------------------------------------------
section "1. 설정 파일 확인"
# ------------------------------------------------------------------
[ -n "${KAFKA_BROKERS}" ]        && ok "KAFKA_BROKERS 설정됨"       || fail "KAFKA_BROKERS 미설정"
[ -n "${HMS_HOST}" ]             && ok "HMS_HOST: ${HMS_HOST}"       || fail "HMS_HOST 미설정"
[ -n "${HS2_HOST}" ]             && ok "HS2_HOST: ${HS2_HOST}"       || fail "HS2_HOST 미설정"
[ -n "${OZONE_OM_SERVICE_ID}" ]  && ok "OZONE_OM_SERVICE_ID 설정됨"  || fail "OZONE_OM_SERVICE_ID 미설정"
[ -n "${PRINCIPAL}" ]            && ok "PRINCIPAL: ${PRINCIPAL}"     || fail "PRINCIPAL 미설정"

# ------------------------------------------------------------------
section "2. Kerberos 인증"
# ------------------------------------------------------------------
if [ ! -f "${KEYTAB}" ]; then
  fail "Keytab 파일 없음: ${KEYTAB}"
else
  ok "Keytab 파일 존재: ${KEYTAB}"
  if kinit -kt "${KEYTAB}" "${PRINCIPAL}" 2>/dev/null; then
    ok "kinit 성공 (${PRINCIPAL})"
    klist 2>/dev/null | grep -q "Ticket cache" && ok "TGT 발급 확인" || fail "TGT 확인 실패"
  else
    fail "kinit 실패 — keytab 또는 principal 확인 필요"
  fi
fi

# ------------------------------------------------------------------
section "3. Auto-TLS 인증서 파일 확인"
# ------------------------------------------------------------------
for cert_var in TRUSTSTORE_JKS CA_PEM; do
  cert_path="${!cert_var}"
  if [ -f "${cert_path}" ]; then
    ok "${cert_var}: ${cert_path}"
  else
    fail "${cert_var} 파일 없음: ${cert_path}"
  fi
done

if [ -z "${TRUSTSTORE_PW}" ]; then
  fail "TRUSTSTORE_PW 미설정 — config/env.conf에 TRUSTSTORE_PW 값을 입력하세요"
else
  ok "TRUSTSTORE_PW 설정 확인"
fi

# ------------------------------------------------------------------
section "4. Kafka 연결 테스트 (SASL_SSL + GSSAPI)"
# ------------------------------------------------------------------
KAFKA_TOPICS_CMD="${KAFKA_HOME:-/opt/cloudera/parcels/CDH/lib/kafka}/bin/kafka-topics.sh"

TMPDIR_VERIFY=$(mktemp -d)
trap 'rm -rf "${TMPDIR_VERIFY}"' EXIT

JAAS_CONF="${TMPDIR_VERIFY}/kafka-jaas.conf"
KAFKA_CLIENT_CONF="${TMPDIR_VERIFY}/kafka-client.properties"

cat > "${JAAS_CONF}" <<EOF
KafkaClient {
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    keyTab="${KEYTAB}"
    principal="${PRINCIPAL}";
};
EOF

cat > "${KAFKA_CLIENT_CONF}" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=kafka
ssl.truststore.location=${TRUSTSTORE_JKS}
ssl.truststore.password=${TRUSTSTORE_PW}
ssl.truststore.type=JKS
request.timeout.ms=30000
EOF

export KAFKA_OPTS="-Djava.security.auth.login.config=${JAAS_CONF}"

if "${KAFKA_TOPICS_CMD}" \
    --bootstrap-server "${KAFKA_BROKERS}" \
    --command-config "${KAFKA_CLIENT_CONF}" \
    --list &>/dev/null; then
  ok "Kafka 연결 성공 (${KAFKA_BROKERS})"
  # 토픽 존재 여부 확인
  "${KAFKA_TOPICS_CMD}" \
    --bootstrap-server "${KAFKA_BROKERS}" \
    --command-config "${KAFKA_CLIENT_CONF}" \
    --list 2>/dev/null | grep -q "^${KAFKA_TOPIC}$" \
    && ok "토픽 존재: ${KAFKA_TOPIC}" \
    || fail "토픽 없음: ${KAFKA_TOPIC} (infra/01_kafka_setup.sh 실행 필요)"
else
  fail "Kafka 연결 실패 — 브로커 주소 또는 TRUSTSTORE_PW 확인 필요"
fi

# ------------------------------------------------------------------
section "5. Hive Metastore (HMS) 연결 테스트"
# ------------------------------------------------------------------
if beeline -u "${HS2_JDBC_URL}" \
    -e "SHOW DATABASES" \
    --silent=true 2>/dev/null | grep -q "sbi_raw\|sbi_curated\|default"; then
  ok "HiveServer2 연결 성공 (${HS2_HOST}:${HS2_PORT})"
  beeline -u "${HS2_JDBC_URL}" \
    -e "SHOW DATABASES" \
    --silent=true 2>/dev/null | grep -q "sbi_raw" \
    && ok "데이터베이스 존재: sbi_raw" \
    || fail "데이터베이스 없음: sbi_raw (infra/03_iceberg_ddl.sql 실행 필요)"
else
  fail "HiveServer2 연결 실패 (${HS2_HOST}:${HS2_PORT}) — Kerberos 및 SSL 설정 확인"
fi

# ------------------------------------------------------------------
section "6. Ozone (OFS) 접근 테스트"
# ------------------------------------------------------------------
if ozone sh bucket list "/${OZONE_VOLUME}" &>/dev/null; then
  ok "Ozone 볼륨 접근 성공: /${OZONE_VOLUME}"
  ozone sh bucket list "/${OZONE_VOLUME}" 2>/dev/null | grep -q "${OZONE_BUCKET_RAW}" \
    && ok "버킷 존재: /${OZONE_VOLUME}/${OZONE_BUCKET_RAW}" \
    || fail "버킷 없음: /${OZONE_VOLUME}/${OZONE_BUCKET_RAW} (infra/02_ozone_setup.sh 실행 필요)"
  ozone sh bucket list "/${OZONE_VOLUME}" 2>/dev/null | grep -q "${OZONE_BUCKET_CURATED}" \
    && ok "버킷 존재: /${OZONE_VOLUME}/${OZONE_BUCKET_CURATED}" \
    || fail "버킷 없음: /${OZONE_VOLUME}/${OZONE_BUCKET_CURATED} (infra/02_ozone_setup.sh 실행 필요)"
else
  fail "Ozone 볼륨 접근 실패: /${OZONE_VOLUME} — Ozone ACL 또는 Kerberos 확인"
fi

# ------------------------------------------------------------------
section "7. Spark 환경 확인"
# ------------------------------------------------------------------
for jar_var in ICEBERG_JAR KAFKA_SPARK_JAR KAFKA_CLIENTS_JAR; do
  jar_path="${!jar_var}"
  if [ -f "${jar_path}" ]; then
    ok "${jar_var} 존재"
  else
    fail "${jar_var} 없음: ${jar_path}"
  fi
done

for ozone_jar in ${SPARK_OZONE_JARS//:/ }; do
  [ -f "${ozone_jar}" ] && ok "Ozone JAR 존재: $(basename "${ozone_jar}")" \
    || fail "Ozone JAR 없음: ${ozone_jar}"
done

[ -d "${HADOOP_CONF_DIR}" ] && ok "HADOOP_CONF_DIR 존재: ${HADOOP_CONF_DIR}" \
  || fail "HADOOP_CONF_DIR 없음: ${HADOOP_CONF_DIR}"

# ------------------------------------------------------------------
echo ""
echo "================================================================"
echo " 결과: ${PASS}개 성공 / ${FAIL}개 실패"
echo "================================================================"

if [ "${FAIL}" -gt 0 ]; then
  echo ""
  echo "[주의] FAIL 항목을 먼저 해결한 후 다음 Phase를 진행하세요."
  echo "       문제 해결: README.md > 트러블슈팅 참고"
  exit 1
else
  echo ""
  echo "[완료] 모든 환경 검증 통과! Phase 2를 시작하세요."
  exit 0
fi
