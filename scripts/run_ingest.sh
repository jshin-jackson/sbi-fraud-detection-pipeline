#!/usr/bin/env bash
# ================================================================
# run_ingest.sh — Kafka → Raw Iceberg Spark Batch Job 실행
# 1분마다 cron으로 스케줄링하여 실행합니다.
#
# 사용법:
#   chmod +x scripts/run_ingest.sh
#   scripts/run_ingest.sh
#
# cron 등록 (1분마다):
#   * * * * * /root/sbi-fraud-detection-pipeline/scripts/run_ingest.sh >> /var/log/sbi-ingest.log 2>&1
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

echo ""
echo "================================================================"
echo " SBI Fraud Ingest (ENV: ${ENV_NAME}) — $(date)"
echo "================================================================"

# Kerberos 티켓 갱신
echo "[INFO] kinit 실행: ${PRINCIPAL}"
kinit -kt "${KEYTAB}" "${PRINCIPAL}"
klist

# Ozone filesystem JAR을 드라이버 JVM 시작 전에 클래스패스에 추가
export SPARK_CLASSPATH="${SPARK_OZONE_JARS}"

# spark-defaults.conf 의 ${OZONE_OM_SERVICE_ID} / ${OZONE_OM_ADDRESS} 를 실제 값으로 치환
SPARK_CONF_TMP=$(mktemp --suffix=.conf)
trap "rm -f ${SPARK_CONF_TMP}" EXIT
envsubst '${OZONE_OM_SERVICE_ID} ${OZONE_OM_ADDRESS}' \
  < "${ROOT_DIR}/conf/spark-defaults.conf" > "${SPARK_CONF_TMP}"

spark-submit \
  --master yarn \
  --deploy-mode client \
  --principal "${PRINCIPAL}" \
  --keytab "${KEYTAB}" \
  --properties-file "${SPARK_CONF_TMP}" \
  "${ROOT_DIR}/spark/stream/raw_ingest_job.py"
