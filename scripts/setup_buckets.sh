#!/bin/bash
# Manual fallback for the `storage_init` service: creates the S3 buckets.

if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

STORAGE_USER=${STORAGE_USER:-admin}
STORAGE_PASSWORD=${STORAGE_PASSWORD:-strongpassword123}
# The aws-cli container reaches the host through host.docker.internal.
S3_ENDPOINT=${S3_ENDPOINT:-http://host.docker.internal:18900}
S3_ENDPOINT=${S3_ENDPOINT/127.0.0.1/host.docker.internal}
S3_ENDPOINT=${S3_ENDPOINT/localhost/host.docker.internal}

BUCKETS=(
    "${S3_BUCKET_LANDING:-landing-zone}"
    "${S3_BUCKET_BRONZE:-bronze}"
    "${S3_BUCKET_SILVER:-silver}"
    "${S3_BUCKET_GOLD:-gold}"
)

echo "🌊 Initializing S3 buckets at $S3_ENDPOINT..."

AWS_COMMAND="docker run --rm --add-host=host.docker.internal:host-gateway \
    -e AWS_ACCESS_KEY_ID=$STORAGE_USER -e AWS_SECRET_ACCESS_KEY=$STORAGE_PASSWORD \
    -e AWS_REGION=us-east-1 amazon/aws-cli:2.37.13@sha256:d32864c20087d486f5988c3e70817d67a73f584a47535cada9ad97e0ea1e5196 --endpoint-url $S3_ENDPOINT"

for BUCKET in "${BUCKETS[@]}"; do
    echo "🏗️ Checking bucket: $BUCKET"
    if ! $AWS_COMMAND s3api head-bucket --bucket "$BUCKET" > /dev/null 2>&1; then
        echo "   ✨ Creating bucket: $BUCKET"
        $AWS_COMMAND s3 mb "s3://$BUCKET"
    else
        echo "   ✅ Bucket already exists: $BUCKET"
    fi
done

echo "✅ Bucket initialization complete."
