#!/usr/bin/env bash
# =============================================================================
# Ozone bucket creation script
# Creates buckets under the 'firstvolume' OFS (RootedOzoneFileSystem) volume:
# ofs://.../firstvolume/sbi-raw
# ofs://.../firstvolume/sbi-curated
#
# Prerequisites (run as Ozone admin first):
#   sudo -u hdfs ozone sh bucket create --layout FILE_SYSTEM_OPTIMIZED /firstvolume/sbi-raw
#   sudo -u hdfs ozone sh bucket create --layout FILE_SYSTEM_OPTIMIZED /firstvolume/sbi-curated
# Or grant write permission on the firstvolume volume to systest:
#   sudo -u hdfs ozone sh volume addacl /firstvolume --acl "user:systest:rwlc"
#
# Usage:
#   chmod +x ozone_setup.sh
#   ./ozone_setup.sh
# =============================================================================

set -euo pipefail

OZONE_CMD="${OZONE_CMD:-ozone}"
# firstvolume: OFS (RootedOzoneFileSystem) volume (OZONE_VOLUME from env.conf takes precedence)
VOLUME="${OZONE_VOLUME:-firstvolume}"
BUCKET_RAW="sbi-raw"
BUCKET_CURATED="sbi-curated"

info() { echo "[INFO]  $*"; }
ok()   { echo "[OK]    $*"; }
warn() { echo "[WARN]  $*"; }
err()  { echo "[ERROR] $*" >&2; exit 1; }

info "Checking Kerberos ticket..."
klist -s || err "No Kerberos ticket found. Run kinit first."
ok "Kerberos ticket is valid"

# ---------------------------------------------------------------------------
# [Prerequisites] One of the following must be run as an Ozone admin:
#
# Option A: Admin creates buckets directly (recommended)
#   sudo -u hdfs ozone sh bucket create --layout FILE_SYSTEM_OPTIMIZED /s3v/sbi-raw
#   sudo -u hdfs ozone sh bucket create --layout FILE_SYSTEM_OPTIMIZED /s3v/sbi-curated
#
# Option B: Grant s3v volume permissions to systest, then run this script
#   sudo -u hdfs ozone sh volume addacl /s3v --acl "user:systest:rwlc"
#
# If managing with a Ranger Ozone policy, import 'sbi-ozone-s3v-policy' and Option B can be skipped
# ---------------------------------------------------------------------------
info "Using Ozone OFS volume: /${VOLUME} (ofs://<om-host>:9862/${VOLUME}/sbi-raw)"

# ---------------------------------------------------------------------------
# Create buckets
# ---------------------------------------------------------------------------
for BUCKET in "${BUCKET_RAW}" "${BUCKET_CURATED}"; do
    info "Creating Ozone bucket: /${VOLUME}/${BUCKET}"
    if "${OZONE_CMD}" sh bucket info "/${VOLUME}/${BUCKET}" &>/dev/null; then
        info "Bucket already exists: /${VOLUME}/${BUCKET}"
    else
        "${OZONE_CMD}" sh bucket create \
            --layout FILE_SYSTEM_OPTIMIZED \
            "/${VOLUME}/${BUCKET}"
        ok "Bucket created: /${VOLUME}/${BUCKET}"
    fi
done

# ---------------------------------------------------------------------------
# Verify bucket information
# ---------------------------------------------------------------------------
info "Bucket details:"
"${OZONE_CMD}" sh bucket info "/${VOLUME}/${BUCKET_RAW}"  2>/dev/null || warn "Failed to retrieve bucket info (possible permission issue): ${BUCKET_RAW}"
"${OZONE_CMD}" sh bucket info "/${VOLUME}/${BUCKET_CURATED}" 2>/dev/null || warn "Failed to retrieve bucket info (possible permission issue): ${BUCKET_CURATED}"

ok "Ozone bucket setup complete"
echo ""
echo "OFS paths:"
echo "  Raw     : ofs://${OZONE_OM_SERVICE_ID}/${VOLUME}/${BUCKET_RAW}/"
echo "  Curated : ofs://${OZONE_OM_SERVICE_ID}/${VOLUME}/${BUCKET_CURATED}/"
