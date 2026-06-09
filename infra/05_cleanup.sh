#!/usr/bin/env bash
# ================================================================
# 05_cleanup.sh — 전체 인프라 및 데이터 완전 초기화
#
# ⚠️  주의: 이 스크립트는 Kafka 토픽, Ozone 버킷, Iceberg 테이블,
#           오프셋 파일 등 모든 데이터를 삭제합니다.
#           실행 전 반드시 확인하세요.
#
# 사용법:
#   source config/env.conf
#   bash infra/05_cleanup.sh
#
# 재시작 시:
#   bash infra/01_kafka_setup.sh
#   bash infra/02_ozone_setup.sh
#   beeline -u "${HS2_JDBC_URL}" -f infra/03_iceberg_ddl.sql
# ================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."

# config 로드
if [ ! -f "${ROOT_DIR}/config/env.conf" ]; then
  echo "[ERROR] config/env.conf 파일이 없습니다."
  echo "        ln -sf config/env.internal.conf config/env.conf"
  exit 1
fi
source "${ROOT_DIR}/config/env.conf"

PASS=0
FAIL=0

ok()      { echo "  [OK]   $1"; ((PASS+=1)); }
fail()    { echo "  [FAIL] $1"; ((FAIL+=1)); }
section() { echo ""; echo "=== $1 ==="; }
skip()    { echo "  [SKIP] $1"; }

echo ""
echo "================================================================"
echo " SBI Fraud Detection — 완전 초기화 (ENV: ${ENV_NAME})"
echo "================================================================"
echo ""
echo "  삭제 대상:"
echo "    - Kafka 토픽: ${KAFKA_TOPIC}, ${KAFKA_TOPIC_DLQ}"
echo "    - Ozone 버킷: /${OZONE_VOLUME}/sbi-raw, /${OZONE_VOLUME}/sbi-curated"
echo "    - Iceberg DB: sbi_raw, sbi_curated"
echo "    - 오프셋 파일: ${KAFKA_OFFSET_FILE}"
echo "    - 로컬 임시 데이터: ${DATA_OUTPUT_DIR}"
echo ""
read -r -p "계속하시겠습니까? (yes/no): " CONFIRM
if [ "${CONFIRM}" != "yes" ]; then
  echo "취소되었습니다."
  exit 0
fi

# ------------------------------------------------------------------
section "1. Kerberos 인증"
# ------------------------------------------------------------------
if kinit -kt "${KEYTAB}" "${PRINCIPAL}" 2>/dev/null; then
  ok "kinit 성공 (${PRINCIPAL})"
else
  fail "kinit 실패 — keytab 확인 필요: ${KEYTAB}"
  exit 1
fi

# ------------------------------------------------------------------
section "2. Kafka 토픽 삭제"
# ------------------------------------------------------------------
KAFKA_TOPICS_CMD="${KAFKA_HOME:-/opt/cloudera/parcels/CDH/lib/kafka}/bin/kafka-topics.sh"

TMPDIR_CLEAN=$(mktemp -d)
trap 'rm -rf "${TMPDIR_CLEAN}"' EXIT

cat > "${TMPDIR_CLEAN}/jaas.conf" <<EOF
KafkaClient {
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    keyTab="${KEYTAB}"
    principal="${PRINCIPAL}";
};
EOF

cat > "${TMPDIR_CLEAN}/client.properties" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=kafka
ssl.truststore.location=${TRUSTSTORE_JKS}
ssl.truststore.password=${TRUSTSTORE_PW}
EOF

export KAFKA_OPTS="-Djava.security.auth.login.config=${TMPDIR_CLEAN}/jaas.conf"

for TOPIC in "${KAFKA_TOPIC}" "${KAFKA_TOPIC_DLQ}"; do
  # 토픽 존재 여부 확인
  if "${KAFKA_TOPICS_CMD}" \
      --bootstrap-server "${KAFKA_BROKERS}" \
      --command-config "${TMPDIR_CLEAN}/client.properties" \
      --list 2>/dev/null | grep -q "^${TOPIC}$"; then
    if "${KAFKA_TOPICS_CMD}" \
        --bootstrap-server "${KAFKA_BROKERS}" \
        --command-config "${TMPDIR_CLEAN}/client.properties" \
        --delete --topic "${TOPIC}" 2>/dev/null; then
      ok "토픽 삭제 완료: ${TOPIC}"
    else
      fail "토픽 삭제 실패: ${TOPIC}"
    fi
  else
    skip "토픽 없음 (이미 삭제됨): ${TOPIC}"
  fi
done

# ------------------------------------------------------------------
section "3. Iceberg 테이블 및 데이터베이스 삭제"
# ------------------------------------------------------------------
BEELINE_SQL="
DROP TABLE IF EXISTS sbi_raw.transactions;
DROP DATABASE IF EXISTS sbi_raw;
DROP TABLE IF EXISTS sbi_curated.transactions;
DROP TABLE IF EXISTS sbi_curated.fraud_alerts;
DROP TABLE IF EXISTS sbi_curated.fraud_summary;
DROP DATABASE IF EXISTS sbi_curated;
"

if beeline -u "${HS2_JDBC_URL}" \
    -e "${BEELINE_SQL}" \
    --silent=true 2>/dev/null; then
  ok "Iceberg 테이블/DB 삭제 완료 (sbi_raw, sbi_curated)"
else
  fail "Iceberg 테이블/DB 삭제 실패 — HiveServer2 연결 확인"
fi

# ------------------------------------------------------------------
section "4. Ozone 데이터 및 버킷 삭제"
# ------------------------------------------------------------------
OFS_PREFIX="ofs://${OZONE_OM_SERVICE_ID}/${OZONE_VOLUME}"

for BUCKET in "${OZONE_BUCKET_RAW}" "${OZONE_BUCKET_CURATED}"; do
  # 버킷 존재 여부 확인
  if ozone sh bucket info "/${OZONE_VOLUME}/${BUCKET}" &>/dev/null; then
    # 버킷 내 데이터 삭제
    ozone fs -rm -r -skipTrash "${OFS_PREFIX}/${BUCKET}/" &>/dev/null || true
    # 버킷 삭제
    if ozone sh bucket delete "/${OZONE_VOLUME}/${BUCKET}" 2>/dev/null; then
      ok "Ozone 버킷 삭제 완료: /${OZONE_VOLUME}/${BUCKET}"
    else
      fail "Ozone 버킷 삭제 실패: /${OZONE_VOLUME}/${BUCKET}"
    fi
  else
    skip "버킷 없음 (이미 삭제됨): /${OZONE_VOLUME}/${BUCKET}"
  fi
done

# ------------------------------------------------------------------
section "5. 오프셋 파일 삭제"
# ------------------------------------------------------------------
if [ -f "${KAFKA_OFFSET_FILE}" ]; then
  rm -f "${KAFKA_OFFSET_FILE}"
  ok "오프셋 파일 삭제: ${KAFKA_OFFSET_FILE}"
else
  skip "오프셋 파일 없음: ${KAFKA_OFFSET_FILE}"
fi

# ------------------------------------------------------------------
section "6. 로컬 임시 데이터 삭제"
# ------------------------------------------------------------------
if [ -d "${DATA_OUTPUT_DIR}" ]; then
  rm -rf "${DATA_OUTPUT_DIR}"
  ok "로컬 데이터 삭제: ${DATA_OUTPUT_DIR}"
else
  skip "로컬 데이터 없음: ${DATA_OUTPUT_DIR}"
fi

# Spark 임시 파일
rm -f /tmp/sbi-kafka-ca.pem 2>/dev/null && ok "임시 PEM 파일 삭제" || true

# ------------------------------------------------------------------
echo ""
echo "================================================================"
echo " 결과: ${PASS}개 성공 / ${FAIL}개 실패"
echo "================================================================"

if [ "${FAIL}" -gt 0 ]; then
  echo ""
  echo "[주의] FAIL 항목을 확인하세요. 수동으로 삭제가 필요할 수 있습니다."
  exit 1
else
  echo ""
  echo "[완료] 초기화 완료! 아래 순서로 재시작하세요:"
  echo ""
  echo "  source config/env.conf"
  echo "  bash infra/01_kafka_setup.sh"
  echo "  bash infra/02_ozone_setup.sh"
  echo "  beeline -u \"\${HS2_JDBC_URL}\" -f infra/03_iceberg_ddl.sql"
  echo "  bash scripts/02_run_ingest.sh"
  exit 0
fi
