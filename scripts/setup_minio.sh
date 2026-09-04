#!/bin/bash
# scripts/setup_minio.sh: Manual fallback to create required buckets in MinIO.
# NOTE: not normally needed — the `minio_init` service in docker-compose.yml
# already creates these buckets automatically on `docker-compose up`.

if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

STORAGE_USER=${STORAGE_USER:-admin}
STORAGE_PASSWORD=${STORAGE_PASSWORD:-strongpassword123}
# host.docker.internal lets the mc container reach MinIO's published port on
# the host, and works on Docker Desktop (Mac/Windows) as well as Linux with
# the --add-host flag below (Docker 20.10+).
S3_ENDPOINT=${S3_ENDPOINT:-http://host.docker.internal:9000}

BUCKETS=(
    "${S3_BUCKET_LANDING:-landing-zone}"
    "${S3_BUCKET_BRONZE:-bronze}"
    "${S3_BUCKET_SILVER:-silver}"
    "${S3_BUCKET_GOLD:-gold}"
    "${S3_BUCKET_QUARANTINE:-quarantine}"
)

echo "🌊 Initializing MinIO buckets at $S3_ENDPOINT..."

# Use the minio/mc docker image to avoid local installation dependency.
# --add-host is required on Linux for host.docker.internal to resolve; it's a
# no-op on Docker Desktop (Mac/Windows), where that hostname already works.
MC_COMMAND="docker run --rm --add-host=host.docker.internal:host-gateway minio/mc"

$MC_COMMAND alias set pureflow "$S3_ENDPOINT" "$STORAGE_USER" "$STORAGE_PASSWORD"

for BUCKET in "${BUCKETS[@]}"; do
    echo "🏗️ Checking bucket: $BUCKET"
    if ! $MC_COMMAND ls "pureflow/$BUCKET" > /dev/null 2>&1; then
        echo "   ✨ Creating bucket: $BUCKET"
        $MC_COMMAND mb "pureflow/$BUCKET"
    else
        echo "   ✅ Bucket already exists: $BUCKET"
    fi
done

echo "✅ MinIO initialization complete."
