#!/usr/bin/env bash
# ================================================================
# run_etl.sh — Raw Iceberg → Curated Iceberg Spark ETL 실행
#
# 사용법:
#   scripts/run_etl.sh [YYYY-MM-DD]
#   scripts/run_etl.sh 2024-01-07   # 특정 날짜 처리
#   scripts/run_etl.sh               # 기본: 어제 날짜 자동 처리
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

# 처리 날짜 (인자 없으면 어제)
DT="${1:-$(date -d 'yesterday' '+%Y-%m-%d' 2>/dev/null || date -v-1d '+%Y-%m-%d')}"

echo ""
echo "================================================================"
echo " SBI Fraud ETL (ENV: ${ENV_NAME}) — dt=${DT}"
echo "================================================================"

# Kerberos 티켓 갱신
kinit -kt "${KEYTAB}" "${PRINCIPAL}"

export SPARK_CLASSPATH="${SPARK_OZONE_JARS}"

# spark-defaults.conf 의 ${OZONE_OM_SERVICE_ID} / ${OZONE_OM_ADDRESS} 를 실제 값으로 치환
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
