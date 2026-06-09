#!/usr/bin/env bash
# =============================================================================
# Ozone 버킷 생성 스크립트
# OFS(RootedOzoneFileSystem) 용 볼륨 'firstvolume' 에 버킷을 생성합니다.
# ofs://.../firstvolume/sbi-raw
# ofs://.../firstvolume/sbi-curated
#
# 전제조건 (Ozone admin 선실행):
#   sudo -u hdfs ozone sh bucket create --layout FILE_SYSTEM_OPTIMIZED /firstvolume/sbi-raw
#   sudo -u hdfs ozone sh bucket create --layout FILE_SYSTEM_OPTIMIZED /firstvolume/sbi-curated
# 또는 systest 에게 firstvolume 볼륨 쓰기 권한 부여:
#   sudo -u hdfs ozone sh volume addacl /firstvolume --acl "user:systest:rwlc"
#
# 사용법:
#   chmod +x ozone_setup.sh
#   ./ozone_setup.sh
# =============================================================================

set -euo pipefail

OZONE_CMD="${OZONE_CMD:-ozone}"
# firstvolume: OFS(RootedOzoneFileSystem) 용 볼륨 (env.conf 의 OZONE_VOLUME 우선)
VOLUME="${OZONE_VOLUME:-firstvolume}"
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
info "Ozone OFS 볼륨 사용: /${VOLUME} (ofs://<om-host>:9862/${VOLUME}/sbi-raw)"

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
echo "OFS 경로:"
echo "  Raw     : ofs://${OZONE_OM_SERVICE_ID}/${VOLUME}/${BUCKET_RAW}/"
echo "  Curated : ofs://${OZONE_OM_SERVICE_ID}/${VOLUME}/${BUCKET_CURATED}/"
