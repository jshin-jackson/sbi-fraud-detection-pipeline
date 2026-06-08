#!/usr/bin/env bash
# ================================================================
# run_report.sh — Hive/beeline 리포트 쿼리 실행 래퍼
# Phase 5 Demo 검증 및 결과 확인에 사용합니다.
# ================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."

# config 로드
if [ ! -f "${ROOT_DIR}/config/env.conf" ]; then
  echo "[ERROR] config/env.conf 파일이 없습니다."
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
