#!/usr/bin/env bash
# ================================================================
# 05_cleanup.sh — Full infrastructure and data reset
#
# ⚠️  WARNING: This script deletes all data including Kafka topics,
#              Ozone buckets, Iceberg tables, offset files, and more.
#              Confirm carefully before running.
#
# Usage:
#   source config/env.conf
#   bash infra/05_cleanup.sh
#
# To restart after cleanup:
#   bash infra/01_kafka_setup.sh
#   bash infra/02_ozone_setup.sh
#   beeline -u "${HS2_JDBC_URL}" -f infra/03_iceberg_ddl.sql
# ================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."

# Load config
if [ ! -f "${ROOT_DIR}/config/env.conf" ]; then
  echo "[ERROR] config/env.conf not found."
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
echo " SBI Fraud Detection — Full Reset (ENV: ${ENV_NAME})"
echo "================================================================"
echo ""
echo "  Items to delete:"
echo "    - Kafka topics: ${KAFKA_TOPIC}, ${KAFKA_TOPIC_DLQ}"
echo "    - Ozone buckets: /${OZONE_VOLUME}/sbi-raw, /${OZONE_VOLUME}/sbi-curated"
echo "    - Iceberg databases: sbi_raw, sbi_curated"
echo "    - Offset file: ${KAFKA_OFFSET_FILE}"
echo "    - Local temporary data: ${DATA_OUTPUT_DIR}"
echo ""
read -r -p "Do you want to continue? (yes/no): " CONFIRM
if [ "${CONFIRM}" != "yes" ]; then
  echo "Cancelled."
  exit 0
fi

# ------------------------------------------------------------------
section "1. Kerberos authentication"
# ------------------------------------------------------------------
if kinit -kt "${KEYTAB}" "${PRINCIPAL}" 2>/dev/null; then
  ok "kinit succeeded (${PRINCIPAL})"
else
  fail "kinit failed — check keytab: ${KEYTAB}"
  exit 1
fi

# ------------------------------------------------------------------
section "2. Delete Kafka topics"
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
  # Check if topic exists
  if "${KAFKA_TOPICS_CMD}" \
      --bootstrap-server "${KAFKA_BROKERS}" \
      --command-config "${TMPDIR_CLEAN}/client.properties" \
      --list 2>/dev/null | grep -q "^${TOPIC}$"; then
    if "${KAFKA_TOPICS_CMD}" \
        --bootstrap-server "${KAFKA_BROKERS}" \
        --command-config "${TMPDIR_CLEAN}/client.properties" \
        --delete --topic "${TOPIC}" 2>/dev/null; then
      ok "Topic deleted: ${TOPIC}"
    else
      fail "Failed to delete topic: ${TOPIC}"
    fi
  else
    skip "Topic not found (already deleted): ${TOPIC}"
  fi
done

# ------------------------------------------------------------------
section "3. Drop Iceberg tables and databases"
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
  ok "Iceberg tables/databases dropped (sbi_raw, sbi_curated)"
else
  fail "Failed to drop Iceberg tables/databases — check HiveServer2 connection"
fi

# ------------------------------------------------------------------
section "4. Delete Ozone data and buckets"
# ------------------------------------------------------------------
OFS_PREFIX="ofs://${OZONE_OM_SERVICE_ID}/${OZONE_VOLUME}"

for BUCKET in "${OZONE_BUCKET_RAW}" "${OZONE_BUCKET_CURATED}"; do
  # Check if bucket exists
  if ozone sh bucket info "/${OZONE_VOLUME}/${BUCKET}" &>/dev/null; then
    # Delete bucket contents
    ozone fs -rm -r -skipTrash "${OFS_PREFIX}/${BUCKET}/" &>/dev/null || true
    # Delete bucket
    if ozone sh bucket delete "/${OZONE_VOLUME}/${BUCKET}" 2>/dev/null; then
      ok "Ozone bucket deleted: /${OZONE_VOLUME}/${BUCKET}"
    else
      fail "Failed to delete Ozone bucket: /${OZONE_VOLUME}/${BUCKET}"
    fi
  else
    skip "Bucket not found (already deleted): /${OZONE_VOLUME}/${BUCKET}"
  fi
done

# ------------------------------------------------------------------
section "5. Delete offset file"
# ------------------------------------------------------------------
if [ -f "${KAFKA_OFFSET_FILE}" ]; then
  rm -f "${KAFKA_OFFSET_FILE}"
  ok "Offset file deleted: ${KAFKA_OFFSET_FILE}"
else
  skip "Offset file not found: ${KAFKA_OFFSET_FILE}"
fi

# ------------------------------------------------------------------
section "6. Delete local temporary data"
# ------------------------------------------------------------------
if [ -d "${DATA_OUTPUT_DIR}" ]; then
  rm -rf "${DATA_OUTPUT_DIR}"
  ok "Local data deleted: ${DATA_OUTPUT_DIR}"
else
  skip "Local data not found: ${DATA_OUTPUT_DIR}"
fi

# Spark temporary files
rm -f /tmp/sbi-kafka-ca.pem 2>/dev/null && ok "Temporary PEM file deleted" || true

# ------------------------------------------------------------------
echo ""
echo "================================================================"
echo " Result: ${PASS} passed / ${FAIL} failed"
echo "================================================================"

if [ "${FAIL}" -gt 0 ]; then
  echo ""
  echo "[WARNING] Check the FAIL items. Manual cleanup may be required."
  exit 1
else
  echo ""
  echo "[DONE] Reset complete! Restart in the following order:"
  echo ""
  echo "  source config/env.conf"
  echo "  bash infra/01_kafka_setup.sh"
  echo "  bash infra/02_ozone_setup.sh"
  echo "  beeline -u \"\${HS2_JDBC_URL}\" -f infra/03_iceberg_ddl.sql"
  echo "  bash scripts/02_run_ingest.sh"
  exit 0
fi
