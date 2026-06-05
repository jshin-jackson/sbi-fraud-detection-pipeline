#!/usr/bin/env bash
# =============================================================================
# SBI Raw Transaction Ingest — Spark Batch Job 실행 스크립트
#
# 사용법:
#   chmod +x run_ingest.sh
#   ./run_ingest.sh
#
# cron 등록 (1분마다):
#   * * * * * /root/sbi-realtime-fraud-detection/run_ingest.sh >> /var/log/sbi-ingest.log 2>&1
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

KEYTAB="/opt/cloudera/systest.keytab"
PRINCIPAL="systest@ROOT.COMOPS.SITE"

# Kerberos 티켓 갱신 (useTicketCache=true 방식으로 Kafka 인증)
echo "[$(date)] kinit 실행: ${PRINCIPAL}"
kinit -kt "${KEYTAB}" "${PRINCIPAL}"
klist

# YARN 실행에 필요한 Hadoop/YARN 설정 디렉터리
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf

# Ozone filesystem JAR을 드라이버 JVM 시작 전에 클래스패스에 추가
export SPARK_CLASSPATH="/opt/cloudera/parcels/CDH/jars/ozone-filesystem-hadoop3-1.4.0.7.3.1.600-325.jar:/opt/cloudera/parcels/CDH/jars/ozone-filesystem-common-1.4.0.7.3.1.600-325.jar"

# spark-defaults.conf 를 자동으로 읽게 설정 (--properties-file 불필요)
export SPARK_CONF_DIR="${SCRIPT_DIR}/conf"

spark-submit \
  --master yarn \
  --deploy-mode client \
  --principal "${PRINCIPAL}" \
  --keytab "${KEYTAB}" \
  "${SCRIPT_DIR}/spark/stream/raw_ingest_job.py"
