#!/bin/bash
# Gives the container user (UID 1000) ownership of the bind-mounted data on Linux/WSL.
# Not needed on Docker Desktop.

set -e

PROJECT_ROOT=$(pwd)
ANALYST_UID=1000

echo "🛡️ Fixing permissions for PureFlow (UID $ANALYST_UID)..."

mkdir -p "$PROJECT_ROOT/data/seaweedfs_data" "$PROJECT_ROOT/data/seaweedfs_console"
mkdir -p "$PROJECT_ROOT/gx/uncommitted/data_docs"

sudo chown -R $ANALYST_UID:$ANALYST_UID "$PROJECT_ROOT/data"
sudo chown -R $ANALYST_UID:$ANALYST_UID "$PROJECT_ROOT/gx/uncommitted"

echo "✅ Permissions synchronized for the analyst user."
