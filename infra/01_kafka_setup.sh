#!/usr/bin/env bash
# =============================================================================
# Kafka topic creation script
# Prerequisites: Kerberos ticket issued (kinit), kafka-topics.sh in PATH
#
# Usage:
#   chmod +x kafka_setup.sh
#   kinit -kt /opt/cloudera/systest.keytab systest@ROOT.COMOPS.SITE
#   bash infra/kafka_setup.sh
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_HOME="${KAFKA_HOME:-/opt/cloudera/parcels/CDH/lib/kafka}"
BOOTSTRAP="${BOOTSTRAP:-${KAFKA_BROKERS}}"

KEYTAB="${KEYTAB:-}"
PRINCIPAL="${PRINCIPAL:-}"

TRUSTSTORE_PATH="${TRUSTSTORE_PATH:-/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks}"
TRUSTSTORE_PASS="${TRUSTSTORE_PW:?TRUSTSTORE_PW is not set. Run 'source config/env.conf' first.}"

TOPIC_RAW="sbi-fd-transactions-raw"
TOPIC_DLQ="sbi-fd-transactions-dlq"   # Dead Letter Queue

PARTITIONS=6
REPLICATION=3
RETENTION_MS=604800000   # 7 days

# ---------------------------------------------------------------------------
# Output helper functions
# ---------------------------------------------------------------------------
info()  { echo "[INFO]  $*"; }
ok()    { echo "[OK]    $*"; }
warn()  { echo "[WARN]  $*"; }
err()   { echo "[ERROR] $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Verify Kerberos ticket
# ---------------------------------------------------------------------------
info "Checking Kerberos ticket..."
klist -s || err "No Kerberos ticket found. Run 'kinit -kt ${KEYTAB} ${PRINCIPAL}' first."
ok "Kerberos ticket is valid"

# ---------------------------------------------------------------------------
# Auto-detect CDP Auto-TLS truststore
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
    err "Auto-TLS truststore not found.
  Specify the path via the TRUSTSTORE_PATH environment variable:
  export TRUSTSTORE_PATH=/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks"
fi
ok "Truststore found: ${CDP_TRUSTSTORE}"

# ---------------------------------------------------------------------------
# Truststore password — resolution priority:
#   1) TRUSTSTORE_PASS environment variable
#   2) <truststore>.pw file
#   3) Kafka process config file (CM-managed process dir)
# ---------------------------------------------------------------------------
if [[ -z "${CDP_TRUSTSTORE_PASS}" ]]; then
    PW_FILE="${CDP_TRUSTSTORE%.jks}.pw"
    if [[ -f "${PW_FILE}" ]]; then
        CDP_TRUSTSTORE_PASS="$(cat "${PW_FILE}")"
        ok "Using truststore password file: ${PW_FILE}"
    fi
fi

if [[ -z "${CDP_TRUSTSTORE_PASS}" ]]; then
    # Search for the truststore password used by Kafka in the CM process dir
    KAFKA_PROC_PW=$(grep -r "ssl.truststore.password" \
        /var/run/cloudera-scm-agent/process/*/kafka*/kafka.properties \
        2>/dev/null | head -1 | awk -F'=' '{print $2}' | tr -d ' ')
    if [[ -n "${KAFKA_PROC_PW}" ]]; then
        CDP_TRUSTSTORE_PASS="${KAFKA_PROC_PW}"
        ok "Retrieved truststore password from Kafka process configuration"
    fi
fi

if [[ -z "${CDP_TRUSTSTORE_PASS}" ]]; then
    err "Unable to determine truststore password.
  Retrieve the password manually and set it as an environment variable:

    cat /var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.pw
    grep ssl.truststore.password /var/run/cloudera-scm-agent/process/*/kafka*/kafka.properties

  Set the retrieved password:
    export TRUSTSTORE_PASS=<password>
    bash infra/kafka_setup.sh"
fi

# ---------------------------------------------------------------------------
# Create temporary client.properties (auto-deleted on script exit)
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
ok "Temporary client.properties created: ${CLIENT_PROPS}"

# ---------------------------------------------------------------------------
# Kafka command paths
# ---------------------------------------------------------------------------
KAFKA_TOPICS="${KAFKA_HOME}/bin/kafka-topics.sh"
KAFKA_CONFIGS="${KAFKA_HOME}/bin/kafka-configs.sh"
KAFKA_ACL="${KAFKA_HOME}/bin/kafka-acls.sh"

# ---------------------------------------------------------------------------
# Topic creation function (--if-not-exists: no error if topic already exists)
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
    && ok "Topic created (or already exists): ${topic}" \
    || err "Topic creation failed: ${topic}"
}

# ---------------------------------------------------------------------------
# Create topics
# ---------------------------------------------------------------------------
info "Creating topics..."
create_topic "${TOPIC_RAW}" 6
create_topic "${TOPIC_DLQ}" 2

# ---------------------------------------------------------------------------
# In environments using Ranger, apply policies via the Ranger UI / REST API
# instead of the kafka-acls commands below.
# ---------------------------------------------------------------------------
info "ACL configuration (reference commands for non-Ranger environments only):"
cat <<'ACL_EXAMPLE'
# Producer ACL
kafka-acls.sh --bootstrap-server $BOOTSTRAP \
  --command-config $CLIENT_PROPS \
  --add --allow-principal User:systest \
  --operation Write --topic sbi-fd-transactions-raw

# Consumer ACL
kafka-acls.sh --bootstrap-server $BOOTSTRAP \
  --command-config $CLIENT_PROPS \
  --add --allow-principal User:systest \
  --operation Read --topic sbi-fd-transactions-raw \
  --group systest-stream-group
ACL_EXAMPLE

# ---------------------------------------------------------------------------
# Final topic listing
# ---------------------------------------------------------------------------
info "All topics:"
"${KAFKA_TOPICS}" \
    --bootstrap-server "${BOOTSTRAP}" \
    --command-config "${CLIENT_PROPS}" \
    --list 2>/dev/null | grep "^sbi-" || warn "No topics with 'sbi-' prefix found."

ok "Kafka topic setup complete"
