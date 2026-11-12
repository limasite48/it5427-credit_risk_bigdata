#!/bin/sh
# Initialise MinIO: buckets for the landing -> bronze -> silver -> gold architecture, versioning,
# data lifecycle and least-privilege service accounts. Safe to re-run.
set -eu
mc alias set hc "$MINIO_URL" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

for b in landing bronze silver gold quarantine reference checkpoints models; do
  mc mb --ignore-existing "hc/$b"
done

# Keep version history for raw files, reference tables and models
for b in landing reference models; do
  mc version enable "hc/$b"
done

# Quarantined data is kept for 30 days only
if ! mc ilm rule ls hc/quarantine 2>/dev/null | grep -q "30"; then
  mc ilm rule add --expire-days 30 hc/quarantine
fi

add_service() {  # add_service <name> <secret>
  mc admin policy create hc "hc-$1" "/init/policies/$1.json"
  mc admin user add hc "svc-$1" "$2"
  mc admin policy attach hc "hc-$1" --user "svc-$1" 2>/dev/null || true
}
add_service ingest "$SVC_INGEST_SECRET"
add_service spark "$SVC_SPARK_SECRET"
add_service ml "$SVC_ML_SECRET"
add_service dashboard "$SVC_DASHBOARD_SECRET"

echo "Buckets:"
mc ls hc
