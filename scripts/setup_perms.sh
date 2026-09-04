#!/bin/bash
# setup_perms.sh: Ensures 'analyst' (UID 1000) owns the data volume.
# Use this on the host terminal (Linux/WSL) if you encounter Permission Denied
# errors on bind-mounted volumes. NOT needed on Docker Desktop (Mac/Windows),
# which maps bind-mount permissions differently — skip this script there.

set -e

PROJECT_ROOT=$(pwd)
ANALYST_UID=1000

echo "🛡️ Fixing permissions for PureFlow (UID $ANALYST_UID)..."

# Ensure data directories exist
mkdir -p "$PROJECT_ROOT/data/minio_data"
mkdir -p "$PROJECT_ROOT/gx/uncommitted/data_docs"

# Apply recursive ownership
sudo chown -R $ANALYST_UID:$ANALYST_UID "$PROJECT_ROOT/data"
sudo chown -R $ANALYST_UID:$ANALYST_UID "$PROJECT_ROOT/gx/uncommitted"

echo "✅ Permissions synchronized for the analyst user."
