#!/usr/bin/env bash
# ================================================================
# 04_run_report.sh — Hive/beeline report query runner
# Used for Phase 5 demo verification and result review.
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

kinit -kt "${KEYTAB}" "${PRINCIPAL}"

SQL_FILE="${1:-${ROOT_DIR}/report/fraud_report.sql}"

echo ""
echo "================================================================"
echo " SBI Fraud Report (ENV: ${ENV_NAME})"
echo " HS2  : ${HS2_HOST}:${HS2_PORT}"
echo " File : ${SQL_FILE}"
echo "================================================================"
echo ""

beeline \
  -u "${HS2_JDBC_URL}" \
  -f "${SQL_FILE}" \
  --outputformat=table
