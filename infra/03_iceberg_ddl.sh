#!/usr/bin/env bash
# =============================================================================
# Iceberg table DDL execution wrapper
# Substitutes environment variables in 03_iceberg_ddl.sql, then runs it via beeline.
#
# Usage:
#   source config/env.conf   # once initially or when switching environments
#   bash infra/03_iceberg_ddl.sh
# =============================================================================

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
echo " SBI Fraud — Iceberg DDL (ENV: ${ENV_NAME})"
echo " HS2     : ${HS2_HOST}:${HS2_PORT}"
echo " OFS Base: ${OFS_BASE}"
echo "================================================================"
echo ""

# Substitute environment variables in the SQL template and write to a temp file
SQL_TMP=$(mktemp --suffix=.sql)
trap "rm -f ${SQL_TMP}" EXIT

envsubst '${OZONE_OM_SERVICE_ID} ${OZONE_VOLUME}' \
  < "${SCRIPT_DIR}/03_iceberg_ddl.sql" > "${SQL_TMP}"

beeline -u "${HS2_JDBC_URL}" -f "${SQL_TMP}"
