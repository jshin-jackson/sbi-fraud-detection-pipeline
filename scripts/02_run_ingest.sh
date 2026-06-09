#!/usr/bin/env bash
# ================================================================
# 02_run_ingest.sh — Kafka → Raw Iceberg Spark Batch Job runner
# Intended to be scheduled via cron every minute.
#
# Usage:
#   chmod +x scripts/02_run_ingest.sh
#   scripts/02_run_ingest.sh
#
# cron schedule (every minute):
#   * * * * * /root/sbi-fraud-detection-pipeline/scripts/02_run_ingest.sh >> /var/log/sbi-ingest.log 2>&1
# ================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."

# Load config
if [ ! -f "${ROOT_DIR}/config/env.conf" ]; then
  echo "[ERROR] config/env.conf not found."
  echo "        ln -sf config/env.internal.conf config/env.conf"
  exit 1
fi
source "${ROOT_DIR}/config/env.conf"

echo ""
echo "================================================================"
echo " SBI Fraud Ingest (ENV: ${ENV_NAME}) — $(date)"
echo "================================================================"

# Refresh Kerberos ticket
echo "[INFO] Running kinit: ${PRINCIPAL}"
kinit -kt "${KEYTAB}" "${PRINCIPAL}"
klist

# Add Ozone filesystem JAR to classpath before the driver JVM starts
export SPARK_CLASSPATH="${SPARK_OZONE_JARS}"

# Substitute ${OZONE_OM_SERVICE_ID} / ${OZONE_OM_ADDRESS} placeholders in spark-defaults.conf
SPARK_CONF_TMP=$(mktemp --suffix=.conf)
trap "rm -f ${SPARK_CONF_TMP}" EXIT
envsubst '${OZONE_OM_SERVICE_ID} ${OZONE_OM_ADDRESS} ${HMS_HOST} ${HMS_PORT} ${KRB_REALM} ${KEYTAB} ${PRINCIPAL}' \
  < "${ROOT_DIR}/conf/spark-defaults.conf" > "${SPARK_CONF_TMP}"

spark-submit \
  --master yarn \
  --deploy-mode client \
  --principal "${PRINCIPAL}" \
  --keytab "${KEYTAB}" \
  --properties-file "${SPARK_CONF_TMP}" \
  "${ROOT_DIR}/spark/stream/raw_ingest_job.py"
