#!/usr/bin/env bash
# =============================================================================
# Iceberg 테이블 DDL 실행 래퍼
# 03_iceberg_ddl.sql 의 환경변수를 치환한 후 beeline으로 실행합니다.
#
# 사용법:
#   source config/env.conf   # 최초 1회 또는 환경 전환 시
#   bash infra/03_iceberg_ddl.sh
# =============================================================================

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
echo " SBI Fraud — Iceberg DDL (ENV: ${ENV_NAME})"
echo " HS2     : ${HS2_HOST}:${HS2_PORT}"
echo " OFS Base: ${OFS_BASE}"
echo "================================================================"
echo ""

# SQL 템플릿에서 환경변수 치환 후 임시 파일 생성
SQL_TMP=$(mktemp --suffix=.sql)
trap "rm -f ${SQL_TMP}" EXIT

envsubst '${OZONE_OM_SERVICE_ID} ${OZONE_VOLUME}' \
  < "${SCRIPT_DIR}/03_iceberg_ddl.sql" > "${SQL_TMP}"

beeline -u "${HS2_JDBC_URL}" -f "${SQL_TMP}"
