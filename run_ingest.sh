#!/usr/bin/env bash
# ================================================================
# run_ingest.sh — 루트 레벨 래퍼 (cron 호환성 유지용)
# 실제 구현은 scripts/run_ingest.sh 에 있습니다.
# ================================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/scripts/run_ingest.sh" "$@"
