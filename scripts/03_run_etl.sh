#!/usr/bin/env bash
# ================================================================
# 03_run_etl.sh — Raw Iceberg → Curated Iceberg Spark ETL runner
#
# Usage:
#   scripts/03_run_etl.sh [YYYY-MM-DD]
#   scripts/03_run_etl.sh 2024-01-07   # process a specific date
#   scripts/03_run_etl.sh               # default: process yesterday
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

# Processing date (defaults to yesterday if no argument provided)
DT="${1:-$(date -d 'yesterday' '+%Y-%m-%d' 2>/dev/null || date -v-1d '+%Y-%m-%d')}"

echo ""
echo "================================================================"
echo " SBI Fraud ETL (ENV: ${ENV_NAME}) — dt=${DT}"
echo "================================================================"

# Refresh Kerberos ticket
kinit -kt "${KEYTAB}" "${PRINCIPAL}"

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
  --py-files "${ROOT_DIR}/spark/etl/rules.py" \
  "${ROOT_DIR}/spark/etl/fraud_detection_etl.py" \
  --dt "${DT}"
