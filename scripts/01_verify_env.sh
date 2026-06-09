#!/usr/bin/env bash
# ================================================================
# 01_verify_env.sh — Full environment verification script
# Run during Phase 1. All checks must pass before proceeding to the next Phase.
# ================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."

# Load config
if [ ! -f "${ROOT_DIR}/config/env.conf" ]; then
  echo "[ERROR] config/env.conf not found."
  echo "        Link a configuration file with:"
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
echo " SBI Fraud Detection — Environment Verification (ENV: ${ENV_NAME})"
echo "================================================================"

# ------------------------------------------------------------------
section "1. Configuration file check"
# ------------------------------------------------------------------
[ -n "${KAFKA_BROKERS}" ]        && ok "KAFKA_BROKERS is set"              || fail "KAFKA_BROKERS not set"
[ -n "${HMS_HOST}" ]             && ok "HMS_HOST: ${HMS_HOST}"             || fail "HMS_HOST not set"
[ -n "${HS2_HOST}" ]             && ok "HS2_HOST: ${HS2_HOST}"             || fail "HS2_HOST not set"
[ -n "${OZONE_OM_SERVICE_ID}" ]  && ok "OZONE_OM_SERVICE_ID is set"        || fail "OZONE_OM_SERVICE_ID not set"
[ -n "${PRINCIPAL}" ]            && ok "PRINCIPAL: ${PRINCIPAL}"           || fail "PRINCIPAL not set"

# ------------------------------------------------------------------
section "2. Kerberos authentication"
# ------------------------------------------------------------------
if [ ! -f "${KEYTAB}" ]; then
  fail "Keytab file not found: ${KEYTAB}"
else
  ok "Keytab file exists: ${KEYTAB}"
  if kinit -kt "${KEYTAB}" "${PRINCIPAL}" 2>/dev/null; then
    ok "kinit succeeded (${PRINCIPAL})"
    klist 2>/dev/null | grep -q "Ticket cache" && ok "TGT issued successfully" || fail "TGT verification failed"
  else
    fail "kinit failed — check keytab or principal"
  fi
fi

# ------------------------------------------------------------------
section "3. Auto-TLS certificate file check"
# ------------------------------------------------------------------
for cert_var in TRUSTSTORE_JKS CA_PEM; do
  cert_path="${!cert_var}"
  if [ -f "${cert_path}" ]; then
    ok "${cert_var}: ${cert_path}"
  else
    fail "${cert_var} file not found: ${cert_path}"
  fi
done

if [ -z "${TRUSTSTORE_PW}" ]; then
  fail "TRUSTSTORE_PW not set — enter the TRUSTSTORE_PW value in config/env.conf"
else
  ok "TRUSTSTORE_PW is set"
fi

# ------------------------------------------------------------------
section "4. Kafka connection test (SASL_SSL + GSSAPI)"
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
  ok "Kafka connection successful (${KAFKA_BROKERS})"
  # Check topic existence
  "${KAFKA_TOPICS_CMD}" \
    --bootstrap-server "${KAFKA_BROKERS}" \
    --command-config "${KAFKA_CLIENT_CONF}" \
    --list 2>/dev/null | grep -q "^${KAFKA_TOPIC}$" \
    && ok "Topic exists: ${KAFKA_TOPIC}" \
    || fail "Topic not found: ${KAFKA_TOPIC} (run infra/01_kafka_setup.sh)"
else
  fail "Kafka connection failed — check broker address or TRUSTSTORE_PW"
fi

# ------------------------------------------------------------------
section "5. Hive Metastore (HMS) connection test"
# ------------------------------------------------------------------
if beeline -u "${HS2_JDBC_URL}" \
    -e "SHOW DATABASES" \
    --silent=true 2>/dev/null | grep -q "sbi_raw\|sbi_curated\|default"; then
  ok "HiveServer2 connection successful (${HS2_HOST}:${HS2_PORT})"
  beeline -u "${HS2_JDBC_URL}" \
    -e "SHOW DATABASES" \
    --silent=true 2>/dev/null | grep -q "sbi_raw" \
    && ok "Database exists: sbi_raw" \
    || fail "Database not found: sbi_raw (run infra/03_iceberg_ddl.sql)"
else
  fail "HiveServer2 connection failed (${HS2_HOST}:${HS2_PORT}) — check Kerberos and SSL configuration"
fi

# ------------------------------------------------------------------
section "6. Ozone (OFS) access test"
# ------------------------------------------------------------------
if ozone sh bucket list "/${OZONE_VOLUME}" &>/dev/null; then
  ok "Ozone volume access successful: /${OZONE_VOLUME}"
  ozone sh bucket list "/${OZONE_VOLUME}" 2>/dev/null | grep -q "${OZONE_BUCKET_RAW}" \
    && ok "Bucket exists: /${OZONE_VOLUME}/${OZONE_BUCKET_RAW}" \
    || fail "Bucket not found: /${OZONE_VOLUME}/${OZONE_BUCKET_RAW} (run infra/02_ozone_setup.sh)"
  ozone sh bucket list "/${OZONE_VOLUME}" 2>/dev/null | grep -q "${OZONE_BUCKET_CURATED}" \
    && ok "Bucket exists: /${OZONE_VOLUME}/${OZONE_BUCKET_CURATED}" \
    || fail "Bucket not found: /${OZONE_VOLUME}/${OZONE_BUCKET_CURATED} (run infra/02_ozone_setup.sh)"
else
  fail "Ozone volume access failed: /${OZONE_VOLUME} — check Ozone ACL or Kerberos"
fi

# ------------------------------------------------------------------
section "7. Spark environment check"
# ------------------------------------------------------------------
for jar_var in ICEBERG_JAR KAFKA_SPARK_JAR KAFKA_CLIENTS_JAR; do
  jar_path="${!jar_var}"
  if [ -f "${jar_path}" ]; then
    ok "${jar_var} exists"
  else
    fail "${jar_var} not found: ${jar_path}"
  fi
done

for ozone_jar in ${SPARK_OZONE_JARS//:/ }; do
  [ -f "${ozone_jar}" ] && ok "Ozone JAR exists: $(basename "${ozone_jar}")" \
    || fail "Ozone JAR not found: ${ozone_jar}"
done

[ -d "${HADOOP_CONF_DIR}" ] && ok "HADOOP_CONF_DIR exists: ${HADOOP_CONF_DIR}" \
  || fail "HADOOP_CONF_DIR not found: ${HADOOP_CONF_DIR}"

# ------------------------------------------------------------------
echo ""
echo "================================================================"
echo " Result: ${PASS} passed / ${FAIL} failed"
echo "================================================================"

if [ "${FAIL}" -gt 0 ]; then
  echo ""
  echo "[WARNING] Resolve all FAIL items before proceeding to the next Phase."
  echo "          Troubleshooting: see README.md > Troubleshooting Guide"
  exit 1
else
  echo ""
  echo "[DONE] All environment checks passed! Proceed to Phase 2."
  exit 0
fi
