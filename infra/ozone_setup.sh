#!/usr/bin/env bash
# =============================================================================
# Ozone 버킷 생성 스크립트
# S3 Gateway 기본 볼륨 's3v' 에 버킷을 생성합니다.
# s3a://sbi-raw/  →  /s3v/sbi-raw
# s3a://sbi-curated/  →  /s3v/sbi-curated
#
# 전제조건 (Ozone admin 선실행):
#   sudo -u hdfs ozone sh bucket create --layout FILE_SYSTEM_OPTIMIZED /s3v/sbi-raw
#   sudo -u hdfs ozone sh bucket create --layout FILE_SYSTEM_OPTIMIZED /s3v/sbi-curated
# 또는 systest 에게 s3v 볼륨 쓰기 권한 부여:
#   sudo -u hdfs ozone sh volume addacl /s3v --acl "user:systest:rwlc"
#
# 사용법:
#   chmod +x ozone_setup.sh
#   ./ozone_setup.sh
# =============================================================================

set -euo pipefail

OZONE_CMD="${OZONE_CMD:-ozone}"
# s3v: Ozone S3 Gateway 기본 볼륨 (ozone.s3g.volume.name)
# s3a://sbi-raw/ → /s3v/sbi-raw 매핑
VOLUME="s3v"
BUCKET_RAW="sbi-raw"
BUCKET_CURATED="sbi-curated"

info() { echo "[INFO]  $*"; }
ok()   { echo "[OK]    $*"; }
warn() { echo "[WARN]  $*"; }
err()  { echo "[ERROR] $*" >&2; exit 1; }

info "Kerberos 티켓 확인..."
klist -s || err "Kerberos 티켓이 없습니다. 먼저 kinit 을 실행하세요."
ok "Kerberos 티켓 유효"

# ---------------------------------------------------------------------------
# [사전 요구사항] Ozone admin 계정으로 아래 중 하나를 실행해야 합니다:
#
# 방법 A: admin이 직접 버킷 생성 (권장)
#   sudo -u hdfs ozone sh bucket create --layout FILE_SYSTEM_OPTIMIZED /s3v/sbi-raw
#   sudo -u hdfs ozone sh bucket create --layout FILE_SYSTEM_OPTIMIZED /s3v/sbi-curated
#
# 방법 B: systest 에게 s3v 볼륨 권한 부여 후 이 스크립트 실행
#   sudo -u hdfs ozone sh volume addacl /s3v --acl "user:systest:rwlc"
#
# Ranger Ozone 정책으로 관리하는 경우 'sbi-ozone-s3v-policy' import 후 방법 B 생략 가능
# ---------------------------------------------------------------------------
info "Ozone S3G 볼륨 사용: /${VOLUME} (S3A s3a://sbi-raw/ → /${VOLUME}/sbi-raw)"

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
"${OZONE_CMD}" sh bucket info "/${VOLUME}/${BUCKET_RAW}"  2>/dev/null || warn "버킷 정보 조회 실패 (권한 문제일 수 있음): ${BUCKET_RAW}"
"${OZONE_CMD}" sh bucket info "/${VOLUME}/${BUCKET_CURATED}" 2>/dev/null || warn "버킷 정보 조회 실패 (권한 문제일 수 있음): ${BUCKET_CURATED}"

ok "Ozone 버킷 설정 완료"
echo ""
echo "S3A 경로:"
echo "  Raw     : s3a://${BUCKET_RAW}/"
echo "  Curated : s3a://${BUCKET_CURATED}/"
