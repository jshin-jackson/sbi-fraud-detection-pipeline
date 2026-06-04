#!/usr/bin/env bash
# =============================================================================
# Ozone 버킷 생성 스크립트
# 볼륨 'firstvolume' 은 이미 생성된 볼륨을 사용합니다.
# 전제조건: Kerberos 티켓 발급 완료 (kinit)
#
# 사용법:
#   chmod +x ozone_setup.sh
#   ./ozone_setup.sh
# =============================================================================

set -euo pipefail

OZONE_CMD="${OZONE_CMD:-ozone}"
VOLUME="firstvolume"
BUCKET_RAW="sbi-raw"
BUCKET_CURATED="sbi-curated"

info() { echo "[INFO]  $*"; }
ok()   { echo "[OK]    $*"; }
err()  { echo "[ERROR] $*" >&2; exit 1; }

info "Kerberos 티켓 확인..."
klist -s || err "Kerberos 티켓이 없습니다. 먼저 kinit 을 실행하세요."
ok "Kerberos 티켓 유효"

info "Ozone 볼륨 확인: /${VOLUME}"
if "${OZONE_CMD}" sh volume info "/${VOLUME}" &>/dev/null; then
    info "볼륨 이미 존재: /${VOLUME}"
else
    "${OZONE_CMD}" sh volume create "/${VOLUME}" --user sbi-spark
    ok "볼륨 생성 완료: /${VOLUME}"
fi

# ---------------------------------------------------------------------------
# 버킷 생성
# ---------------------------------------------------------------------------
for BUCKET in "${BUCKET_RAW}" "${BUCKET_CURATED}"; do
    info "Ozone 버킷 생성: /${VOLUME}/${BUCKET}"
    if "${OZONE_CMD}" sh bucket info "/${VOLUME}/${BUCKET}" &>/dev/null; then
        info "버킷 이미 존재: /${VOLUME}/${BUCKET}"
    else
        "${OZONE_CMD}" sh bucket create \
            --layout FILE_SYSTEM_OPTIMIZED \
            "/${VOLUME}/${BUCKET}"
        ok "버킷 생성 완료: /${VOLUME}/${BUCKET}"
    fi
done

# ---------------------------------------------------------------------------
# 버킷 정보 확인
# ---------------------------------------------------------------------------
info "버킷 정보:"
"${OZONE_CMD}" sh bucket info "/${VOLUME}/${BUCKET_RAW}"
"${OZONE_CMD}" sh bucket info "/${VOLUME}/${BUCKET_CURATED}"

ok "Ozone 버킷 설정 완료"
echo ""
echo "S3A 경로:"
echo "  Raw     : s3a://${BUCKET_RAW}/"
echo "  Curated : s3a://${BUCKET_CURATED}/"
